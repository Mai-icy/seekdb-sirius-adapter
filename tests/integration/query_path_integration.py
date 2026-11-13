"""Exercise the real MySQL -> seekdb -> Parquet -> Sirius -> MySQL path.

Run against a disposable seekdb configured with the supplied socket/export root.
The test owns its GPU service and an IPC relay used to coordinate a concurrent
commit between input exports. It does not replace the query engine with a mock.
"""
import argparse
import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import secrets
import socket
import struct
import subprocess
import sys
import threading
import time

import pymysql

REPO = Path(__file__).resolve().parents[2]


def connect(**overrides):
    config = dict(host=os.getenv("SEEKDB_HOST", "127.0.0.1"),
                  port=int(os.getenv("SEEKDB_PORT", "2881")),
                  user=os.getenv("SEEKDB_USER", "root"), password=os.getenv("SEEKDB_PASSWORD", ""),
                  database=os.getenv("SEEKDB_DATABASE", "test"), charset="utf8mb4", autocommit=True)
    config.update(overrides)
    return pymysql.connect(**config)


def receive(conn):
    def exact(n):
        out = bytearray()
        while len(out) < n:
            part = conn.recv(n - len(out))
            if not part:
                raise EOFError()
            out.extend(part)
        return bytes(out)
    header = exact(5)
    size = struct.unpack("!I", header[1:])[0]
    assert size <= 16 * 1024 * 1024
    return header[:1], header + exact(size)


class Relay:
    def __init__(self, frontend, backend):
        self.backend = str(backend)
        self.path = frontend
        self.callback = None
        self.queries = 0
        self.errors = []
        self.stopping = threading.Event()
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(frontend))
        frontend.chmod(0o600)
        self.server.listen(16)
        self.server.settimeout(0.2)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stopping.is_set():
            try:
                client, _ = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.forward, args=(client,), daemon=True).start()

    def forward(self, client):
        try:
            with client, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as backend:
                backend.connect(self.backend)
                kind, frame = receive(client)
                assert kind == b"Q"
                self.queries += 1
                backend.sendall(frame)
                first = True
                while True:
                    kind, frame = receive(backend)
                    client.sendall(frame)
                    if kind == b"I":
                        while True:
                            upload_kind, frame = receive(client)
                            if upload_kind == b"F":
                                if first and self.callback:
                                    callback, self.callback = self.callback, None
                                    callback()
                                first = False
                            backend.sendall(frame)
                            if upload_kind == b"E":
                                ack, frame = receive(backend)
                                client.sendall(frame)
                                if ack == b"X":
                                    return
                                assert ack == b"A"
                            if upload_kind == b"F":
                                break
                    elif kind in (b"Z", b"X"):
                        return
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(repr(error))

    def close(self):
        self.stopping.set()
        self.server.close()
        self.thread.join(timeout=2)
        self.path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--export-root", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    backend = args.socket.with_name(args.socket.name + ".backend")
    log = (args.output_dir / "service.log").open("w")
    cache = args.output_dir / "compute-inputs"
    cache.mkdir(mode=0o700)
    service = subprocess.Popen([sys.executable, str(REPO / "adapter/query_service.py"),
        "--socket", str(backend), "--cache-root", str(cache),
        "--library", str(args.library), "--config", str(args.config)], stdout=log, stderr=subprocess.STDOUT)
    relay = None
    report = []
    prefix = "gpu_path_" + secrets.token_hex(5)
    a, b = prefix + "a", prefix + "b"
    def passed(name):
        report.append(name)
        (args.output_dir / "results.json").write_text(json.dumps({"passed": report}, indent=2) + "\n")
        print("PASS", name, flush=True)
    try:
        deadline = time.monotonic() + 30
        while not backend.exists():
            if service.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("GPU service did not start")
            time.sleep(0.1)
        relay = Relay(args.socket, backend)
        with connect() as connection, connection.cursor() as cursor:
            cursor.execute("SET ob_query_timeout=120000000")
            cursor.execute("SET parallel_degree_policy='MANUAL'")
            cursor.execute("SET _force_parallel_query_dop=1")
            cursor.execute(f"CREATE TABLE {a} (id BIGINT PRIMARY KEY, grp BIGINT, amount DOUBLE, label VARCHAR(64) COLLATE utf8mb4_bin, d DATE, price DECIMAL(15,2))")
            cursor.execute(f"CREATE TABLE {b} (id BIGINT PRIMARY KEY, value BIGINT)")
            try:
                cursor.executemany(f"INSERT INTO {a} VALUES (%s,%s,%s,%s,%s,%s)", [
                    (1, 1, 1.25, "中文\x00x", datetime.date(2024, 1, 2), Decimal("-12.34")),
                    (2, 1, 2.5, "", datetime.date(2024, 1, 3), Decimal("0.01")),
                    (3, 2, None, None, None, None),
                    (4, None, 4.0, "tail  ", datetime.date(2024, 1, 4), Decimal("9999999999999.99"))])
                cursor.execute(f"INSERT INTO {b} VALUES (1,10),(2,20),(3,30),(4,40)")
                queries = [
                    ("projection_types_null_alias", f"SELECT id AS identity_value, label, amount, d, price FROM {a} ORDER BY id"),
                    ("filter_limit", f"SELECT amount,id FROM {a} WHERE id>=2 ORDER BY id DESC LIMIT 2"),
                    ("count_star", f"SELECT COUNT(*) AS n FROM {a}"),
                    ("sum", f"SELECT SUM(amount),COUNT(amount) FROM {a}"),
                    ("group", f"SELECT grp,COUNT(*) FROM {a} GROUP BY grp"),
                    ("join", f"SELECT a.id,b.value FROM {a} a JOIN {b} b ON a.id=b.id"),
                    ("empty_result", f"SELECT id,label FROM {a} WHERE id>100"),
                ]
                for name, query in queries:
                    cursor.execute("SET execution_backend='cpu'")
                    before = relay.queries
                    cursor.execute(query)
                    expected, metadata = cursor.fetchall(), cursor.description
                    assert relay.queries == before, "CPU query dispatched to GPU"
                    cursor.execute("SET execution_backend='gpu'")
                    cursor.execute(query)
                    actual = cursor.fetchall()
                    assert cursor.description == metadata, (name, cursor.description, metadata)
                    assert sorted(actual, key=repr) == sorted(expected, key=repr), (name, actual, expected)
                    assert relay.queries == before + 1, "GPU query did not use Sirius"
                    assert not list(args.export_root.iterdir()), "Query input files leaked"
                    passed(name)
                for bound in (2, 4, 2):
                    cursor.execute(f"SELECT id FROM {a} WHERE id>={bound} ORDER BY id")
                    assert cursor.fetchall() == tuple((i,) for i in range(bound, 5))
                passed("cached_plan_binds_each_execution")
                cursor.execute("SET execution_backend='cpu'")
                cursor.execute(f"UPDATE {b} SET value=99 WHERE id=1")
                cursor.execute("SET execution_backend='gpu'")
                cursor.execute(f"SELECT value FROM {b} WHERE id=1")
                assert cursor.fetchall() == ((99,),)
                passed("new_query_reads_current_data")

                cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                def concurrent_commit():
                    with connect() as writer, writer.cursor() as update:
                        update.execute(f"UPDATE {a} SET amount=99 WHERE id=1")
                        update.execute(f"UPDATE {b} SET value=101 WHERE id=1")
                relay.callback = concurrent_commit
                cursor.execute(f"SELECT a.amount,b.value FROM {a} a JOIN {b} b ON a.id=b.id WHERE a.id=1")
                assert cursor.fetchall() == ((1.25, 99),)
                assert relay.callback is None and not relay.errors, relay.errors
                passed("read_committed_inputs_share_outer_snapshot")

                connection.begin()
                cursor.execute(f"UPDATE {b} SET value=123 WHERE id=2")
                cursor.execute(f"SELECT value FROM {b} WHERE id=2")
                assert cursor.fetchall() == ((123,),)
                connection.rollback()
                cursor.execute(f"SELECT value FROM {b} WHERE id=2")
                assert cursor.fetchall() == ((20,),)
                passed("read_own_writes_and_rollback")

                large = prefix + "stream"
                cursor.execute(f"CREATE TABLE {large} (id BIGINT PRIMARY KEY, label VARCHAR(64) COLLATE utf8mb4_bin)")
                try:
                    expected = [(i, None if i % 3 == 0 else "中文\x00" + str(i)) for i in range(17000)]
                    for start in range(0, len(expected), 1000):
                        cursor.executemany(f"INSERT INTO {large} VALUES (%s,%s)", expected[start:start + 1000])
                    cursor.execute(f"SELECT id,label FROM {large} ORDER BY id")
                    assert cursor.fetchall() == tuple(expected)
                    assert not list(args.export_root.iterdir())
                    passed("streaming_17000_rows_across_batches")
                finally:
                    cursor.execute(f"DROP TABLE {large}")

                username, password = prefix + "u", secrets.token_hex(16)
                cursor.execute("CREATE USER %s IDENTIFIED BY %s", (username, password))
                try:
                    database = os.getenv("SEEKDB_DATABASE", "test").replace("`", "``")
                    cursor.execute(f"GRANT SELECT (id) ON `{database}`.{a} TO %s", (username,))
                    with connect(user=username, password=password) as limited, limited.cursor() as select:
                        select.execute("SET execution_backend='gpu'")
                        select.execute("SET ob_query_timeout=120000000")
                        select.execute(f"SELECT id FROM {a} ORDER BY id")
                        assert select.fetchall() == ((1,), (2,), (3,), (4,))
                        before = relay.queries
                        try:
                            select.execute(f"SELECT amount FROM {a}")
                        except pymysql.Error:
                            pass
                        else:
                            raise AssertionError("GPU query bypassed column privileges")
                        assert relay.queries == before
                    passed("column_privileges_without_file_privilege")
                finally:
                    cursor.execute("DROP USER %s", (username,))

                for query in (f"SELECT id FROM {a} FOR UPDATE", f"SELECT @gpu_test:=id FROM {a}"):
                    try:
                        cursor.execute(query)
                    except pymysql.Error:
                        pass
                    else:
                        raise AssertionError("Unsupported GPU query executed: " + query)
                passed("unsupported_queries_do_not_fall_back")

                def wait_worker_exit():
                    children = Path(f"/proc/{service.pid}/task/{service.pid}/children")
                    deadline = time.monotonic() + 10
                    while children.exists() and children.read_text().strip():
                        if time.monotonic() >= deadline:
                            raise AssertionError("Cancelled GPU worker was not reaped")
                        time.sleep(0.1)

                def kill_request():
                    with connect() as killer, killer.cursor() as kill:
                        kill.execute(f"KILL QUERY {connection.thread_id()}")
                relay.callback = kill_request
                try:
                    cursor.execute(f"SELECT id FROM {a}")
                except pymysql.Error:
                    pass
                else:
                    raise AssertionError("KILL QUERY did not interrupt the GPU query")
                assert relay.callback is None
                assert not list(args.export_root.iterdir())
                wait_worker_exit()
                cursor.execute("SELECT 1")
                assert cursor.fetchone() == (1,)
                passed("kill_query_cleans_inputs_and_preserves_connection")

                cursor.execute("SET ob_query_timeout=2000000")
                delay_done = threading.Event()
                def delay_input():
                    try:
                        time.sleep(3)
                    finally:
                        delay_done.set()
                relay.callback = delay_input
                try:
                    cursor.execute(f"SELECT id FROM {a}")
                except pymysql.Error:
                    pass
                else:
                    raise AssertionError("GPU query ignored its deadline")
                assert relay.callback is None
                assert not list(args.export_root.iterdir())
                assert delay_done.wait(5)
                wait_worker_exit()
                cursor.execute("SET ob_query_timeout=120000000")
                passed("timeout_cancels_worker_and_cleans_inputs")

                service.terminate()
                service.wait(timeout=10)
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {a}")
                except pymysql.Error:
                    pass
                else:
                    raise AssertionError("GPU query silently fell back after service shutdown")
                cursor.execute("SET execution_backend='cpu'")
                cursor.execute(f"SELECT COUNT(*) FROM {a}")
                assert cursor.fetchone() == (4,)
                assert not list(args.export_root.iterdir()), "Failure leaked query input files"
                passed("service_failure_preserves_cpu_path_and_cleans_inputs")
            finally:
                connection.rollback()
                cursor.execute("SET execution_backend='cpu'")
                cursor.execute(f"DROP TABLE IF EXISTS {a},{b}")
    finally:
        if relay:
            relay.close()
        if service.poll() is None:
            service.terminate()
            service.wait(timeout=10)
        log.close()
        assert not list(cache.iterdir()), "Compute-side statement inputs leaked"


if __name__ == "__main__":
    main()
