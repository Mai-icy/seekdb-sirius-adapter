"""Native seekdb IR -> Substrait -> public Sirius FFI.

Service workers reuse one Context serially; CLI execution owns a one-shot Context.
All FFI calls and Arrow cleanup stay on the Context owning thread.
"""
import ctypes
from contextlib import contextmanager
import json
from pathlib import Path
import time
import threading

import pyarrow as pa

from adapter_native_plan import encode_plan


class ArrowArrayStream(ctypes.Structure):
    _fields_ = [(name, ctypes.c_void_p) for name in
                ("get_schema", "get_next", "get_last_error", "release", "private_data")]


class NativeEngine:
    """One lazily created Context, used serially on its owning thread.

    Every result stream is released before another execution. Any failure after
    entering the engine poisons the instance; the service retires its process.
    """

    def __init__(self, library, config):
        self.library = str(Path(library).resolve(strict=True))
        self.config = str(Path(config).resolve(strict=True))
        self.thread = threading.get_ident()
        self.bridge = None
        self.context = None
        self.active = False
        self.poisoned = False

    def check_owner(self):
        if threading.get_ident() != self.thread:
            raise RuntimeError("Sirius Context must stay on its owning thread")

    def open(self):
        self.check_owner()
        if self.poisoned:
            raise RuntimeError("Failed Sirius Context cannot be reused")
        if self.context is not None:
            return
        bridge = ctypes.CDLL(self.library)
        bridge.compute_open.argtypes = [ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]
        bridge.compute_open.restype = ctypes.c_void_p
        bridge.compute_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
                                          ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
        bridge.compute_execute.restype = ctypes.c_int
        bridge.compute_close.argtypes = [ctypes.c_void_p]
        bridge.compute_close.restype = None
        self.bridge = bridge
        error = ctypes.create_string_buffer(4096)
        self.context = bridge.compute_open(self.config.encode(), error, len(error))
        if not self.context:
            self.context = None
            self.poisoned = True
            raise RuntimeError(error.value.decode(errors="replace"))

    def close(self):
        self.check_owner()
        if self.active:
            raise RuntimeError("Release the Arrow result before closing its Context")
        if self.context is not None:
            context, self.context = self.context, None
            self.bridge.compute_close(context)

    @contextmanager
    def result(self, template, tables):
        self.check_owner()
        if self.active or self.poisoned:
            raise RuntimeError("Sirius Context is busy or failed")
        started = time.monotonic()
        document = json.loads(template)
        plan, scans = encode_plan(template, document.get("parameters", []), tables)
        info = {"scans": scans, "plan_bytes": len(plan),
                "adapter_seconds": time.monotonic() - started,
                "context_reused": self.context is not None, "close_seconds": 0.0}
        # Unsupported plans fail before creating the native engine.
        started = time.monotonic()
        self.open()
        info['open_seconds'] = time.monotonic() - started
        self.active = True
        stream = ArrowArrayStream()
        reader = None
        try:
            error = ctypes.create_string_buffer(4096)
            started = time.monotonic()
            if self.bridge.compute_execute(self.context, plan, len(plan),
                                           ctypes.byref(stream), error, len(error)):
                raise RuntimeError(error.value.decode(errors="replace"))
            info['execute_seconds'] = time.monotonic() - started
            reader = pa.RecordBatchReader._import_from_c(ctypes.addressof(stream))
            yield reader, info
        except BaseException:
            self.poisoned = True
            raise
        finally:
            started = time.monotonic()
            try:
                try:
                    if reader is not None:
                        reader.close()
                        reader = None
                finally:
                    if stream.release:
                        ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArrayStream))(
                            stream.release)(ctypes.byref(stream))
            except BaseException:
                self.poisoned = True
                raise
            finally:
                self.active = False
                info['release_seconds'] = time.monotonic() - started


@contextmanager
def open_native_result(template, tables, library, config, engine=None):
    """Borrow a reusable engine, or own a one-shot Context for CLI callers."""
    owned = engine is None
    if owned:
        engine = NativeEngine(library, config)
    info = None
    try:
        with engine.result(template, tables) as (reader, info):
            yield reader, info
    finally:
        if owned:
            started = time.monotonic()
            engine.close()
            if info is not None:
                info['close_seconds'] = time.monotonic() - started


def execute_native(template, tables, library, config, output):
    rows = 0
    batch = None
    with open_native_result(template, tables, library, config) as (reader, info):
        try:
            with pa.OSFile(str(output), "wb") as sink:
                with pa.ipc.new_stream(sink, reader.schema) as writer:
                    for batch in reader:
                        writer.write_batch(batch)
                        rows += batch.num_rows
                        batch = None
        finally:
            batch = None
    return {"rows": rows, **info}


def main():
    import argparse
    from export_substrait import load_tables
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = execute_native(args.ir.read_text(), load_tables(args.manifest),
                            args.library, args.config, args.output)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
