"""Check an existing seekdb source against an existing remote GPU service.

Uses SEEKDB_HOST/PORT/USER/PASSWORD. Owns only a uniquely named test database;
the source, service, and their network configuration are managed externally.
"""
import argparse
import json
from pathlib import Path
import platform
import secrets
import time

from query_path_integration import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    database = 'two_host_' + secrets.token_hex(5)
    report = dict(database=database, client_platform=platform.platform(), cases=[])
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def record(name, elapsed=None):
        case = dict(name=name, passed=True)
        if elapsed is not None:
            case['seconds'] = elapsed
        report['cases'].append(case)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(case), flush=True)

    created = False
    try:
        with connect(database=None) as admin, admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
            created = True
        with connect(database=database, read_timeout=180) as conn, conn.cursor() as cur:
            cur.execute('SET ob_query_timeout=120000000')
            cur.execute('SET parallel_degree_policy=\'MANUAL\'')
            cur.execute('SET _force_parallel_query_dop=1')
            cur.execute('CREATE TABLE a(id BIGINT PRIMARY KEY, label VARCHAR(128) COLLATE utf8mb4_bin, price DECIMAL(15,2))')
            cur.execute('CREATE TABLE b(id BIGINT PRIMARY KEY, value BIGINT)')
            cur.execute('CREATE TABLE empty_input(id BIGINT PRIMARY KEY)')
            values = [(i, None if i % 7 == 0 else '中文\x00' + str(i) * 10,
                       str(i) + '.25') for i in range(5000)]
            for offset in range(0, len(values), 1000):
                cur.executemany('INSERT INTO a VALUES (%s,%s,%s)', values[offset:offset + 1000])
            cur.execute('INSERT INTO b VALUES (1,10),(2,20)')
            query = 'SELECT id,label,price FROM a ORDER BY id'
            cur.execute(query)
            expected, metadata = cur.fetchall(), cur.description

            def gpu(sql, label=''):
                cur.execute("SET execution_backend='gpu'")
                cur.execute('SET gpu_snapshot=%s', (label,))
                started = time.monotonic()
                cur.execute(sql)
                rows = cur.fetchall()
                return rows, time.monotonic() - started

            rows, elapsed = gpu(query)
            assert rows == expected and cur.description == metadata
            record('current_data_values_and_mysql_metadata', elapsed)
            rows, elapsed = gpu(query, 'v1')
            assert rows == expected and cur.description == metadata
            record('named_snapshot_first_query', elapsed)
            assert gpu('SELECT COUNT(*) FROM empty_input', 'v1')[0] == ((0,),)
            record('empty_table_in_whole_database_snapshot')
            cur.execute('UPDATE b SET value=99 WHERE id=1')
            cur.execute("UPDATE a SET label='changed' WHERE id=1")
            for index in range(2):
                rows, elapsed = gpu(query, 'v1')
                assert rows == expected and cur.description == metadata
                record(f'reused_snapshot_{index + 1}', elapsed)
            assert gpu('SELECT value FROM b WHERE id=1', 'v1')[0] == ((10,),)
            record('unreferenced_table_was_prepared')
            assert gpu('SELECT value FROM b WHERE id=1', 'v2')[0] == ((99,),)
            assert gpu('SELECT label FROM a WHERE id=1')[0] == (('changed',),)
            record('new_snapshot_and_current_data_see_updates')
            conn.begin()
            try:
                gpu('SELECT id FROM a LIMIT 1', 'v1')
            except Exception as error:
                assert 'autocommit' in str(error).lower(), str(error)
            else:
                raise AssertionError('Named snapshot accepted an explicit transaction')
            finally:
                conn.rollback()
            record('named_snapshot_rejects_transaction')
        report['passed'] = True
    except BaseException as error:
        report.update(passed=False, error=str(error))
        raise
    finally:
        if created:
            with connect(database=None) as admin, admin.cursor() as cur:
                cur.execute(f'DROP DATABASE {database}')
        args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
