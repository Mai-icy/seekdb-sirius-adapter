"""One-shot compute process: native seekdb IR -> Substrait -> public Sirius FFI.

This module owns no SQL planner or source-row transport. Parquet is scanned by
Sirius. Run one process per invocation; all FFI and Arrow cleanup stays on its
owning thread. Results are written as Arrow IPC batches, not collected in memory.
"""
import ctypes
from contextlib import contextmanager
import json
from pathlib import Path
import time

import pyarrow as pa

from adapter_native_plan import encode_plan


class ArrowArrayStream(ctypes.Structure):
    _fields_ = [(name, ctypes.c_void_p) for name in
                ("get_schema", "get_next", "get_last_error", "release", "private_data")]


@contextmanager
def open_native_result(template, tables, library, config):
    started = time.monotonic()
    document = json.loads(template)
    plan, scans = encode_plan(template, document.get("parameters", []), tables)
    info = {"scans": scans, "plan_bytes": len(plan), "adapter_seconds": time.monotonic() - started}
    # Fail unsupported plans before loading CUDA or creating an engine.
    bridge = ctypes.CDLL(str(Path(library).resolve(strict=True)))
    bridge.compute_open.argtypes = [ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]
    bridge.compute_open.restype = ctypes.c_void_p
    bridge.compute_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
                                       ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
    bridge.compute_execute.restype = ctypes.c_int
    bridge.compute_close.argtypes = [ctypes.c_void_p]
    bridge.compute_close.restype = None
    error = ctypes.create_string_buffer(4096)
    started = time.monotonic()
    context = bridge.compute_open(str(Path(config).resolve(strict=True)).encode(), error, len(error))
    if not context:
        raise RuntimeError(error.value.decode(errors="replace"))
    info['open_seconds'] = time.monotonic() - started
    stream = ArrowArrayStream()
    reader = None
    try:
        started = time.monotonic()
        if bridge.compute_execute(context, plan, len(plan), ctypes.byref(stream), error, len(error)):
            raise RuntimeError(error.value.decode(errors="replace"))
        info['execute_seconds'] = time.monotonic() - started
        reader = pa.RecordBatchReader._import_from_c(ctypes.addressof(stream))
        yield reader, info
    finally:
        started = time.monotonic()
        if reader is not None:
            reader.close()
            reader = None
        if stream.release:
            ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArrayStream))(stream.release)(ctypes.byref(stream))
        bridge.compute_close(context)
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
