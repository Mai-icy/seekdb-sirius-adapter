"""Serve seekdb GPU SELECTs over mutual TLS or a same-UID Unix socket.

A bounded pool reuses one Sirius Context per child process, serially on its main
thread. The parent enforces disconnect cancellation and deadlines by retiring
the affected child. Parquet bytes are uploaded into compute-side storage.
"""
import argparse
from contextlib import nullcontext
import hashlib
import shutil
import ssl
import tempfile
import json
import os
from pathlib import Path
import queue
import signal
import socket
import stat
import struct
import sys
import threading
import time

from snapshot_store import SnapshotStore, private_directory
from worker_process import WorkerProcess, send_control, receive_control

MAX_FRAME = 16 * 1024 * 1024
BATCH_BYTES = 8 * 1024 * 1024


def integer(value):
    return struct.pack("!I", value)


def string(value):
    value = value.encode("utf-8") if isinstance(value, str) else value
    return integer(len(value)) + value


class Reader:
    def __init__(self, data):
        self.data = data
        self.position = 0

    def take(self, size):
        if size < 0 or size > len(self.data) - self.position:
            raise ValueError("Truncated query frame")
        result = self.data[self.position:self.position + size]
        self.position += size
        return result

    def integer(self):
        return struct.unpack("!I", self.take(4))[0]

    def string(self):
        return self.take(self.integer()).decode("utf-8")

    def end(self):
        if self.position != len(self.data):
            raise ValueError("Trailing query frame bytes")


def receive(sock):
    def exact(size):
        data = bytearray()
        while len(data) < size:
            part = sock.recv(size - len(data))
            if not part:
                raise ConnectionError("SQL connection cancelled")
            data.extend(part)
        return bytes(data)
    kind, size = struct.unpack("!cI", exact(5))
    if size > MAX_FRAME:
        raise ValueError("Query frame exceeds 16 MiB")
    return kind, exact(size)


def send(sock, kind, payload=b""):
    if len(payload) > MAX_FRAME:
        raise ValueError("Query frame exceeds 16 MiB")
    sock.sendall(kind + integer(len(payload)) + payload)


def input_tables(document):
    inputs = {}
    def visit(node, depth=0):
        if depth > 128:
            raise ValueError("Query plan is too deep")
        if not isinstance(node, dict):
            return
        if node.get("op") == "read":
            key = node["database"], node["table"]
            columns = inputs.setdefault(key, [])
            for column in node["columns"]:
                if column not in columns:
                    columns.append(column)
        for name in ("input", "left", "right"):
            if name in node:
                visit(node[name], depth + 1)
    materialized = document.get("materialized", [])
    if not isinstance(materialized, list) or len(materialized) > 64:
        raise ValueError("Invalid native materialized relations")
    visit(document["root"])
    # Reference nodes contain no scans themselves. Collect their producers as
    # well, merging columns so every replay reads the same immutable file.
    # The adapter validates reference indices and deterministic replay support.
    for producer in materialized:
        visit(producer)
    if not 1 <= len(inputs) <= 64:
        raise ValueError("GPU query requires 1..64 input tables")
    return inputs


def compatible_type(actual, expected, pa):
    kinds = {"i8": pa.int8(), "i16": pa.int16(), "i32": pa.int32(),
             "i64": pa.int64(), "fp32": pa.float32(), "fp64": pa.float64(),
             "date": pa.date32()}
    if expected == "string":
        return pa.types.is_string(actual) or pa.types.is_large_string(actual)
    if expected.startswith("decimal:"):
        _, precision, scale = expected.split(":")
        # The adapter may prove that the values fit a narrower GPU decimal.
        # Conversion back to seekdb's declared precision is then lossless.
        return pa.types.is_decimal(actual) and actual.precision <= int(precision) and actual.scale == int(scale)
    # SQL boolean expressions use integer result metadata.
    return actual == kinds.get(expected) or (pa.types.is_boolean(actual) and expected in ("i8", "i32", "i64"))


def stream_result(sock, reader, expected):
    import pyarrow as pa
    if len(reader.schema) != len(expected):
        raise ValueError("GPU result column count mismatch")
    for field, kind in zip(reader.schema, expected):
        if not compatible_type(field.type, kind, pa):
            raise ValueError(f"GPU result type mismatch: expected {kind}, received {field.type}")
    send(sock, b"M", integer(len(expected)))
    batch = None
    columns = None
    scalar = None
    try:
        for batch in reader:
            # Frames contain bounded batches of length-delimited UTF-8 values.
            # This preserves NULL, embedded NULs, exact integers and decimals.
            payload = bytearray()
            rows = 0
            columns = list(batch.columns)
            for row in range(batch.num_rows):
                encoded = bytearray()
                for column in columns:
                    scalar = column[row].as_py()
                    if scalar is None:
                        encoded.extend(integer(0xffffffff))
                    else:
                        if isinstance(scalar, bool):
                            value = "1" if scalar else "0"
                        elif hasattr(scalar, "isoformat"):
                            value = scalar.isoformat()
                        else:
                            value = str(scalar)
                        value = value.encode("utf-8")
                        if len(value) + len(encoded) + 4 > BATCH_BYTES:
                            raise ValueError("GPU result row exceeds 8 MiB")
                        encoded.extend(string(value))
                if rows and (rows == 8192 or len(payload) + len(encoded) > BATCH_BYTES):
                    send(sock, b"R", integer(rows) + payload)
                    payload.clear()
                    rows = 0
                payload.extend(encoded)
                rows += 1
            if rows:
                send(sock, b"R", integer(rows) + payload)
            scalar = columns = batch = None
    finally:
        scalar = columns = batch = None


def receive_table(conn, directory, database, name, columns, remaining, deadline):
    import pyarrow.parquet as pq
    send(conn, b"I", string(database) + string(name) + integer(len(columns))
         + b"".join(string(column) for column in columns))
    parts, total, schema = [], 0, None
    while True:
        kind, payload = receive(conn)
        if kind == b"F" and not payload and parts:
            return dict(database=database, name=name, parts=parts), total
        if kind != b"B" or payload or len(parts) >= 100000:
            raise ValueError("Expected a complete Parquet part")
        filename = hashlib.sha256((database + "\0" + name).encode()).hexdigest() + f"-{len(parts)}.parquet"
        path = directory / filename
        digest, size = hashlib.sha256(), 0
        with path.open("xb") as file:
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Parquet upload deadline exceeded")
                kind, payload = receive(conn)
                if kind == b"E":
                    if payload != digest.hexdigest().encode() or not size:
                        raise ValueError("Parquet upload checksum mismatch")
                    break
                if kind != b"D" or not payload or len(payload) > 1024 * 1024:
                    raise ValueError("Invalid Parquet upload chunk")
                size += len(payload)
                if total + size > remaining:
                    raise ValueError("GPU input exceeds configured snapshot byte limit")
                file.write(payload)
                digest.update(payload)
            file.flush()
            os.fsync(file.fileno())
        current = pq.read_schema(path)
        if columns and current.names != columns:
            raise ValueError("Uploaded Parquet columns differ from requested inputs")
        if schema is not None and not schema.equals(current, check_metadata=False):
            raise ValueError("Parquet parts have inconsistent schemas")
        schema = current
        parts.append(dict(file=filename, bytes=size, sha256=digest.hexdigest()))
        total += size
        send(conn, b"A")


def read_catalog(request):
    count = request.integer()
    if count > 64:
        raise ValueError("Too many snapshot tables")
    catalog, names = [], set()
    for _ in range(count):
        database, name, table_id, schema_version = [request.string() for _ in range(4)]
        width = request.integer()
        if not 1 <= width <= 1024 or (database, name) in names:
            raise ValueError("Invalid snapshot table catalog")
        columns = [request.string() for _ in range(width)]
        if len(set(columns)) != width:
            raise ValueError("Duplicate snapshot columns")
        names.add((database, name))
        catalog.append(dict(database=database, name=name, table_id=table_id,
                            schema_version=schema_version, columns=columns))
    return catalog


def worker(args, engine=None, notify=None):
    import pyarrow.parquet as pq
    from adapter_native_plan import ParquetTable
    from compute_node import open_native_result
    conn = socket.socket(fileno=args.worker_fd)
    conn.settimeout(30)
    try:
        if args.tls_cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(args.tls_cert, args.tls_key)
            context.load_verify_locations(args.tls_ca)
            context.verify_mode = ssl.CERT_REQUIRED
            conn = context.wrap_socket(conn, server_side=True)
        kind, payload = receive(conn)
        if kind != b"Q":
            raise ValueError("Expected query request")
        request = Reader(payload)
        if request.integer() != 2:
            raise ValueError("Unsupported query protocol version")
        timeout_ms = request.integer()
        if not timeout_ms:
            raise ValueError("Invalid query deadline")
        conn.settimeout(timeout_ms / 1000)
        deadline = time.monotonic() + timeout_ms / 1000
        if notify is not None:
            notify({"event": "deadline", "deadline": deadline})
        template = request.string()
        document = json.loads(template)
        count = request.integer()
        if not 1 <= count <= 1024:
            raise ValueError("Invalid result column count")
        expected = [request.string() for _ in range(count)]
        label = request.string()
        source_identity = [request.string() for _ in range(3)]
        principal = hashlib.sha256(conn.getpeercert(binary_form=True)).hexdigest() if args.tls_cert else "local-uid:" + str(os.getuid())
        identity = [principal, *source_identity]
        snapshot = request.string()
        catalog = read_catalog(request)
        request.end()
        inputs = input_tables(document)
        if label:
            if not source_identity[0] or not catalog or len(label) > 128:
                raise ValueError("Named snapshots require a source identity and complete database catalog")
            declared = {(table["database"], table["name"]): table["columns"] for table in catalog}
            for key, columns in inputs.items():
                if key not in declared or not set(columns) <= set(declared[key]):
                    raise ValueError("Query references data outside its snapshot catalog")
            inputs = declared
        elif catalog:
            raise ValueError("Unexpected catalog for a statement snapshot")
        work = Path(args.work_dir) / "files"
        work.mkdir(mode=0o700)
        store = SnapshotStore(args.cache_root, args.cache_bytes)
        lease_context = store.acquire(identity, label, catalog, work, deadline) if label else nullcontext(None)
        with lease_context as lease:
            hit = bool(lease and lease["manifest"])
            if hit:
                manifest, directory = lease["manifest"], lease["destination"]
            else:
                manifest = dict(tables=[], bytes=0, snapshot=snapshot)
                for (database, name), columns in inputs.items():
                    table, size = receive_table(conn, work, database, name, columns,
                        args.snapshot_bytes - manifest["bytes"], deadline)
                    manifest["tables"].append(table)
                    manifest["bytes"] += size
                directory = store.publish(lease, manifest, deadline) if lease else work
            print(json.dumps(dict(event="parquet_inputs", snapshot=label,
                id=lease["key"] if lease else None, cache_hit=hit,
                uploaded_bytes=0 if hit else manifest["bytes"],
                files=sum(len(t["parts"]) for t in manifest["tables"]))), flush=True)
            tables = {}
            for table in manifest["tables"]:
                files = tuple(str(directory / part["file"]) for part in table["parts"])
                key = table["database"] + "." + table["name"]
                if key in tables:
                    raise ValueError("Ambiguous qualified input table name")
                statistics = str(directory / ('.stats-' + hashlib.sha256(key.encode()).hexdigest() + '.json')) if label else None
                tables[key] = ParquetTable(pq.read_schema(files[0]), files, statistics)
            with open_native_result(template, tables, args.library, args.config, engine) as (reader, info):
                result_started = time.monotonic()
                stream_result(conn, reader, expected)
                result_seconds = time.monotonic() - result_started
            print(json.dumps(dict(event="query_timing", worker_pid=os.getpid(),
                                  context_reused=info["context_reused"], result_seconds=result_seconds,
                                  **{key: value for key, value in info.items() if key.endswith('_seconds')})), flush=True)
        # Release the result AND snapshot lease before announcing completion.
        # The Context remains alive only when borrowed from a persistent worker.
        if notify is not None:
            notify({"event": "finishing"})
        send(conn, b"Z")
    except Exception as error:
        try:
            send(conn, b"X", str(error).encode("utf-8", errors="replace")[:65536])
        except (OSError, ValueError):
            pass
        print(f"GPU query failed: {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        conn.close()
    return 0


def worker_loop(args):
    """Receive accepted FDs; keep one Context on this process's main thread."""
    from compute_node import NativeEngine
    control = socket.socket(fileno=args.control_fd)
    engine = NativeEngine(args.library, args.config) if args.context_reuse else None
    try:
        while True:
            try:
                job, fd = receive_control(control)
            except EOFError:
                return 0
            if job.get('event') == 'ping' and fd is None:
                send_control(control, {'event': 'ready'})
                continue
            if job.get('event') != 'query' or fd is None:
                if fd is not None:
                    os.close(fd)
                raise ValueError("Expected worker query descriptor")
            args.worker_fd, args.work_dir = fd, job['work']
            try:
                status = worker(args, engine, lambda message: send_control(control, message))
            finally:
                shutil.rmtree(args.work_dir, ignore_errors=True)
            send_control(control, {'event': 'done', 'success': status == 0})
            # Any failure retires the whole process, even a pre-execution error.
            # This conservatively prevents reuse of questionable engine state.
            if status != 0 or not args.context_reuse:
                return status
    finally:
        try:
            if engine is not None:
                engine.close()
        finally:
            control.close()


def serve(args):
    if not sys.platform.startswith("linux"):
        raise ValueError("GPU query service requires Linux")
    path = Path(args.socket) if args.socket else None
    if path:
        private_directory(path.parent)
        if not path.is_absolute() or path.exists() or path.is_symlink():
            raise ValueError("Use an unused absolute socket path in a private directory")
    private_directory(args.cache_root)
    stopping = threading.Event()
    admission = threading.BoundedSemaphore(args.workers)
    threads = set()
    lock = threading.Lock()
    def stop(*_):
        stopping.set()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    command = [sys.executable, str(Path(__file__).resolve()),
               "--library", args.library, "--config", args.config,
               "--cache-root", args.cache_root, "--cache-bytes", str(args.cache_bytes),
               "--snapshot-bytes", str(args.snapshot_bytes)]
    if args.tls_cert:
        command += ["--tls-cert", args.tls_cert, "--tls-key", args.tls_key, "--tls-ca", args.tls_ca]
    if not args.context_reuse:
        command += ["--no-context-reuse"]
    slots = [WorkerProcess(command, reuse=args.context_reuse) for _ in range(args.workers)]
    available = queue.LifoQueue()
    for slot in slots:
        available.put(slot)

    def handle(conn):
        slot = None
        work = None
        try:
            with conn:
                slot = available.get_nowait()
                work = tempfile.mkdtemp(prefix=".upload-", dir=args.cache_root)
                slot.run(conn, work, stopping)
        except Exception as error:
            print(f"GPU worker failed: {error}", file=sys.stderr, flush=True)
        finally:
            if work is not None:
                shutil.rmtree(work, ignore_errors=True)
            if slot is not None:
                available.put(slot)
            admission.release()
            with lock:
                threads.discard(threading.current_thread())
    server = socket.socket(socket.AF_UNIX if path else socket.AF_INET, socket.SOCK_STREAM)
    bound = False
    try:
        if path:
            server.bind(str(path))
            bound = True
            path.chmod(0o600)
        else:
            host, port = args.listen.rsplit(":", 1)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((host, int(port)))
        server.listen(16)
        server.settimeout(0.5)
        print(f"GPU query service listening on {path or server.getsockname()}", flush=True)
        while not stopping.is_set():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            uid = os.getuid()
            if path:
                _, uid, _ = struct.unpack("3i", conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid != os.getuid():
                conn.close()
                continue
            # An error frame may reach the client before its failed Context has
            # finished shutting down. Queue the next connection briefly instead
            # of rejecting it during normal worker replacement.
            admitted = False
            admission_deadline = time.monotonic() + 30
            while not stopping.is_set() and time.monotonic() < admission_deadline:
                if admission.acquire(timeout=0.1):
                    admitted = True
                    break
            if not admitted:
                conn.close()
                continue
            thread = threading.Thread(target=handle, args=(conn,), daemon=True)
            with lock:
                threads.add(thread)
            thread.start()
    finally:
        stopping.set()
        server.close()
        if bound:
            path.unlink(missing_ok=True)
        with lock:
            active = list(threads)
        for thread in active:
            thread.join()
        for slot in slots:
            slot.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket")
    parser.add_argument("--listen", help="IPv4:port; requires mutual TLS")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--cache-bytes", type=int, default=64 * 1024**3)
    parser.add_argument("--snapshot-bytes", type=int, default=16 * 1024**3)
    parser.add_argument("--tls-ca")
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    parser.add_argument("--work-dir")
    parser.add_argument("--library", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--worker-fd", type=int)
    parser.add_argument("--control-fd", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--context-reuse", action=argparse.BooleanOptionalAction, default=True,
                        help="Reuse one Context per worker (default); disable for one-shot isolation")
    args = parser.parse_args()
    for name in ("library", "config", "cache_root"):
        setattr(args, name, str(Path(getattr(args, name)).resolve(strict=True)))
    if not 0 < args.snapshot_bytes <= args.cache_bytes:
        parser.error("Require 0 < snapshot-bytes <= cache-bytes")
    if args.control_fd is not None:
        return worker_loop(args)
    if args.worker_fd is not None:
        return worker(args)
    if bool(args.socket) == bool(args.listen) or not 1 <= args.workers <= 16:
        parser.error("Select --socket or --listen and 1..16 --workers")
    if args.listen and not all((args.tls_ca, args.tls_cert, args.tls_key)):
        parser.error("TCP requires --tls-ca, --tls-cert and --tls-key")
    if args.socket and any((args.tls_ca, args.tls_cert, args.tls_key)):
        parser.error("TLS options require --listen")
    serve(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
