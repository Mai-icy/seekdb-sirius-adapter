"""Real MySQL -> mutually authenticated TCP -> remote Parquet -> GPU checks.

The compute cache and source export directory must be different directories.
Run only against an isolated seekdb configured with the matching TLS endpoint,
source ID and small SEEKDB_SIRIUS_PART_BYTES (65536) to exercise multipart uploads.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import struct
import subprocess
import sys
import time

from query_path_integration import connect, REPO


def frame(conn):
    def exact(n):
        value = bytearray()
        while len(value) < n:
            part = conn.recv(n - len(value))
            if not part:
                raise EOFError()
            value.extend(part)
        return bytes(value)
    header = exact(5)
    return header[:1], exact(struct.unpack('!I', header[1:])[0])


def send(conn, kind, data=b''):
    conn.sendall(kind + struct.pack('!I', len(data)) + data)


def integer(n):
    return struct.pack('!I', n)


def string(value):
    value = value.encode()
    return integer(len(value)) + value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('cache-root', 'export-root', 'tls-ca', 'tls-cert', 'tls-key',
                 'client-cert', 'client-key', 'library', 'config', 'output-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--listen', required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(exist_ok=False)
    assert args.cache_root.resolve() != args.export_root.resolve()
    args.cache_root.mkdir(mode=0o700, exist_ok=True)
    database = 'network_gpu_' + secrets.token_hex(5)
    passed = []
    service = None
    log_path = args.output_dir / 'service.log'
    log = log_path.open('w')
    host, port = args.listen.rsplit(':', 1)
    endpoint = host, int(port)
    context = ssl.create_default_context(cafile=args.tls_ca)
    context.load_cert_chain(args.client_cert, args.client_key)
    def tls():
        return context.wrap_socket(socket.create_connection(endpoint, timeout=10), server_hostname=host)
    def start(**limits):
        nonlocal service
        command = [sys.executable, str(REPO / 'adapter/query_service.py'),
            '--listen', args.listen, '--cache-root', str(args.cache_root), '--workers', '2']
        for name in ('tls_ca', 'tls_cert', 'tls_key', 'library', 'config'):
            command += ['--' + name.replace('_', '-'), str(getattr(args, name))]
        for key, value in limits.items():
            command += ['--' + key.replace('_', '-'), str(value)]
        service = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while True:
            if service.poll() is not None:
                raise RuntimeError('Query service failed to start')
            try:
                with tls():
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.1)
    def stop():
        nonlocal service
        if service:
            service.terminate()
            service.wait(timeout=10)
            service = None
    def clean_uploads():
        deadline = time.monotonic() + 10
        while any(args.cache_root.glob('.upload-*')) and time.monotonic() < deadline:
            time.sleep(.05)
        assert not list(args.cache_root.glob('.upload-*')), 'Staging inputs leaked'
        assert not list(args.export_root.iterdir()), 'Source inputs leaked'
    def mark(name):
        clean_uploads()
        passed.append(name)
        (args.output_dir / 'results.json').write_text(json.dumps({'passed': passed}, indent=2) + '\n')
        print('PASS', name, flush=True)
    def events():
        result = []
        for line in log_path.read_text().splitlines():
            if line.startswith('{'):
                try:
                    value = json.loads(line)
                    if value.get('event') == 'parquet_inputs':
                        result.append(value)
                except ValueError:
                    pass
        return result
    def manifest(label):
        values = [json.loads(p.read_text()) | {'directory': str(p.parent)}
                  for p in args.cache_root.glob('*/manifest.json')]
        return next(value for value in values if value['label'] == label)
    def gpu(cursor, query, label=''):
        cursor.execute('SET execution_backend=\'gpu\'')
        cursor.execute('SET gpu_snapshot=%s', (label,))
        cursor.execute(query)
        return cursor.fetchall()
    def fails(cursor, query, label, contains=None):
        try:
            gpu(cursor, query, label)
        except Exception as error:
            if contains:
                assert contains.lower() in str(error).lower(), str(error)
        else:
            raise AssertionError('Query unexpectedly succeeded')
    def upload_probe(corrupt):
        # A real network request interrupted before Sirius execution must never
        # publish a reusable generation or retain a partially uploaded file.
        plan = json.dumps({'version': 1, 'root': {'op': 'read', 'database': database,
            'table': 'a', 'columns': ['id'], 'types': ['i64']}})
        request = integer(2) + integer(5000) + string(plan) + integer(1) + string('i64')
        request += string('') + b''.join(string(v) for v in ('probe', '1', database, '1')) + integer(0)
        with tls() as conn:
            send(conn, b'Q', request)
            assert frame(conn)[0] == b'I'
            send(conn, b'B')
            send(conn, b'D', b'incomplete parquet')
            if corrupt:
                send(conn, b'E', b'0' * 64)
                kind, error = frame(conn)
                assert kind == b'X' and b'checksum' in error
    try:
        start()
        with connect(database=None) as admin, admin.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        with connect(database=database) as connection, connection.cursor() as cursor:
            cursor.execute('SET ob_query_timeout=120000000')
            cursor.execute('SET parallel_degree_policy=\'MANUAL\'')
            cursor.execute('SET _force_parallel_query_dop=1')
            cursor.execute('CREATE TABLE a(id BIGINT PRIMARY KEY, label VARCHAR(128) COLLATE utf8mb4_bin, price DECIMAL(15,2))')
            cursor.execute('CREATE TABLE b(id BIGINT PRIMARY KEY, value BIGINT)')
            cursor.execute('CREATE TABLE empty_input(id BIGINT PRIMARY KEY)')
            values = [(i, None if i % 7 == 0 else '中文\x00' + str(i) * 10, str(i) + '.25') for i in range(5000)]
            for offset in range(0, len(values), 1000):
                cursor.executemany('INSERT INTO a VALUES (%s,%s,%s)', values[offset:offset+1000])
            cursor.execute('INSERT INTO b VALUES (1,10),(2,20)')
            query = 'SELECT id,label,price FROM a ORDER BY id'
            cursor.execute(query)
            expected, metadata = cursor.fetchall(), cursor.description
            assert gpu(cursor, query) == expected
            assert cursor.description == metadata
            mark('tls_multipart_values_and_mysql_metadata')
            assert gpu(cursor, query, 'v1') == expected
            first = manifest('v1')
            assert len(first['tables']) == 3
            assert sum(len(t['parts']) for t in first['tables']) > 3
            assert gpu(cursor, 'SELECT COUNT(*) FROM empty_input', 'v1') == ((0,),)
            mark('whole_database_snapshot_including_empty_and_unreferenced_tables')
            cursor.execute('UPDATE b SET value=99 WHERE id=1')
            cursor.execute('UPDATE a SET label=\'changed\' WHERE id=1')
            for _ in range(2):
                assert gpu(cursor, query, 'v1') == expected
                assert events()[-1]['cache_hit'] and events()[-1]['uploaded_bytes'] == 0
            assert gpu(cursor, 'SELECT value FROM b WHERE id=1', 'v1') == ((10,),)
            assert manifest('v1')['snapshot'] == first['snapshot']
            mark('repeated_queries_reuse_files_without_export_or_upload')
            assert gpu(cursor, 'SELECT value FROM b WHERE id=1', 'v2') == ((99,),)
            assert gpu(cursor, 'SELECT label FROM a WHERE id=1') == (('changed',),)
            mark('new_generation_refresh_and_statement_snapshot')
            connection.begin()
            fails(cursor, 'SELECT id FROM a LIMIT 1', 'v1', 'autocommit')
            connection.rollback()
            mark('named_snapshot_rejects_explicit_transactions')
            stop()
            start()
            assert gpu(cursor, query, 'v1') == expected
            assert events()[-1]['cache_hit']
            mark('compute_service_restart_reuses_published_snapshot')
            def parallel():
                with connect(database=database) as conn, conn.cursor() as cur:
                    cur.execute('SET ob_query_timeout=120000000')
                    return gpu(cur, 'SELECT COUNT(*) FROM a', 'concurrent')
            with ThreadPoolExecutor(max_workers=2) as pool:
                assert list(pool.map(lambda _: parallel(), range(2))) == [((5000,),), ((5000,),)]
            built = [v for v in events() if v['snapshot'] == 'concurrent']
            assert len(built) == 2 and sum(not v['cache_hit'] for v in built) == 1
            mark('concurrent_first_queries_publish_once')
            cursor.execute('ALTER TABLE b ADD COLUMN extra BIGINT')
            fails(cursor, 'SELECT COUNT(*) FROM a', 'v1', 'schema changed')
            assert gpu(cursor, 'SELECT COUNT(*) FROM a', 'schema_v2') == ((5000,),)
            mark('schema_changes_require_explicit_new_generation')
            upload_probe(True)
            mark('corrupt_upload_rejected_and_cleaned')
            upload_probe(False)
            mark('disconnected_upload_not_published_and_cleaned')
            no_cert = ssl.create_default_context(cafile=args.tls_ca)
            try:
                with no_cert.wrap_socket(socket.create_connection(endpoint, timeout=10), server_hostname=host) as conn:
                    conn.sendall(b'Q' + integer(0))
                    assert not conn.recv(1)
            except (ssl.SSLError, ConnectionError):
                pass
            mark('tls_rejects_clients_without_certificate')
            stop()
            start(cache_bytes=4096, snapshot_bytes=4096)
            fails(cursor, 'SELECT COUNT(*) FROM a', 'quota', 'byte limit')
            assert not any(json.loads(p.read_text())['label'] == 'quota' for p in args.cache_root.glob('*/manifest.json'))
            mark('quota_failure_keeps_generation_unpublished')
            stop()
            start()
            assert gpu(cursor, 'SELECT COUNT(*) FROM a', 'schema_v2') == ((5000,),)
            mark('existing_generation_survives_failed_upload')
        snapshot_id = Path(first['directory']).name
        result = subprocess.run([sys.executable, str(REPO/'adapter/snapshot_store.py'),
            '--root', str(args.cache_root), '--drop', snapshot_id], capture_output=True, text=True, check=True)
        assert not Path(first['directory']).exists()
        (args.output_dir/'drop.json').write_text(result.stdout)
        mark('explicit_release_removes_only_selected_generation')
    finally:
        stop()
        log.close()
        with connect(database=None) as admin, admin.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS {database}')
    print(json.dumps({'passed': passed}))


if __name__ == '__main__':
    main()
