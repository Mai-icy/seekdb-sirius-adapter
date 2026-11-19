"""Real Sirius worker reuse, changed inputs, error/crash/disconnect recovery.

Starts an isolated Unix-socket service. Requires Linux, the public FFI bridge,
and a GPU config. No database or existing service is modified.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'adapter'))
from query_service import receive, send, integer, string, Reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, mode=0o700, exist_ok=False)
    cache = args.output / 'cache'
    cache.mkdir(mode=0o700)
    address = args.output / 'query.sock'
    log_path = args.output / 'service.log'
    log = log_path.open('w')
    service = subprocess.Popen([sys.executable, str(ROOT/'adapter/query_service.py'),
        '--socket', str(address), '--cache-root', str(cache),
        '--library', args.library, '--config', args.config], stdout=log, stderr=subprocess.STDOUT)
    cases = []

    def mark(name, **info):
        cases.append(dict(name=name, passed=True, **info))
        (args.output/'results.json').write_text(json.dumps(cases, indent=2) + '\n')
        print('PASS', name, flush=True)

    def timings():
        return [item for line in log_path.read_text().splitlines() if line.startswith('{')
                and (item := json.loads(line)).get('event') == 'query_timing']

    def wait_until(predicate, message):
        deadline = time.monotonic() + 15
        while not predicate():
            if time.monotonic() >= deadline or service.poll() is not None:
                raise AssertionError(message)
            time.sleep(.05)

    def begin(timeout=10000, expression=None):
        conn = socket.socket(socket.AF_UNIX)
        conn.settimeout(15)
        conn.connect(str(address))
        root = dict(op='read', database='test', table='a', columns=['id'], types=['i64'])
        if expression is not None:
            root = dict(op='project', input=root, expressions=[expression])
        plan = json.dumps(dict(version=1, root=root))
        request = integer(2) + integer(timeout) + string(plan) + integer(1) + string('i64')
        request += b''.join(string(v) for v in ('', 'reuse-test', '1', 'test', '1')) + integer(0)
        send(conn, b'Q', request)
        kind, _ = receive(conn)
        assert kind == b'I', kind
        return conn

    def query(values, expression=None, error=False):
        table = pa.table({'id': pa.array(values, type=pa.int64())})
        output = pa.BufferOutputStream()
        pq.write_table(table, output)
        payload = output.getvalue().to_pybytes()
        started = time.monotonic()
        with begin(expression=expression) as conn:
            send(conn, b'B')
            send(conn, b'D', payload)
            send(conn, b'E', hashlib.sha256(payload).hexdigest().encode())
            assert receive(conn) == (b'A', b'')
            send(conn, b'F')
            rows = []
            while True:
                kind, data = receive(conn)
                if kind == b'X':
                    if not error:
                        raise AssertionError(data.decode())
                    return data.decode()
                if kind == b'M':
                    assert data == integer(1)
                elif kind == b'R':
                    reader = Reader(data)
                    for _ in range(reader.integer()):
                        length = reader.integer()
                        rows.append(None if length == 0xffffffff else reader.take(length).decode())
                    reader.end()
                elif kind == b'Z':
                    assert not data and not error
                    wait_until(lambda: not list(cache.glob('.upload-*')), 'Staging inputs leaked')
                    assert rows == [None if value is None else str(value) for value in values], rows
                    return time.monotonic() - started
                else:
                    raise AssertionError((kind, data))

    try:
        wait_until(address.exists, 'Service did not start')
        query([1, None, -2])
        first = timings()[-1]
        samples = []
        for i in range(12):
            samples.append(query([] if i % 3 == 0 else [i, None, -i]))
        later = timings()[1:]
        assert len(later) == 12 and all(t['worker_pid'] == first['worker_pid'] for t in later)
        assert not first['context_reused'] and all(t['context_reused'] for t in later)
        assert all(t['close_seconds'] == 0 for t in later)
        mark('same_context_changed_inputs_empty_null', worker_pid=first['worker_pid'], seconds=samples)

        # This public function fails during native binding/execution, not IR encoding.
        error = query([1], expression={'fn': 'factorial', 'type': 'i64',
                      'args': [{'literal': -1, 'type': 'i64'}]}, error=True)
        query([7])
        after = timings()[-1]
        assert after['worker_pid'] != first['worker_pid'] and not after['context_reused']
        mark('native_error_retires_context_and_recovers', error=error)

        conn = begin()
        conn.close()
        wait_until(lambda: not list(cache.glob('.upload-*')), 'Disconnected upload leaked')
        query([8])
        recovered = timings()[-1]
        assert recovered['worker_pid'] != after['worker_pid'] and not recovered['context_reused']
        mark('disconnect_retires_worker_and_recovers')

        with begin(timeout=100) as conn:
            try:
                kind, data = receive(conn)
                assert kind == b'X', (kind, data)
            except (ConnectionError, OSError):
                pass
        wait_until(lambda: not list(cache.glob('.upload-*')), 'Timed out input leaked')
        query([9])
        assert not timings()[-1]['context_reused']
        mark('deadline_retires_worker_and_recovers')

        crashed = timings()[-1]['worker_pid']
        os.kill(crashed, signal.SIGKILL)
        time.sleep(.1)
        query([10])
        assert timings()[-1]['worker_pid'] != crashed and not timings()[-1]['context_reused']
        mark('idle_crash_replaced')
        query([11])
        assert timings()[-1]['context_reused']
        mark('recovered_context_is_reused')
    finally:
        service.terminate()
        service.wait(timeout=15)
        log.close()
    assert not address.exists() and not list(cache.glob('.upload-*'))
    mark('shutdown_cleans_socket_and_inputs')


if __name__ == '__main__':
    main()
