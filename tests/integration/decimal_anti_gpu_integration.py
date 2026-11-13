"""Check native Decimal Parquet and anti joins through the public Sirius FFI."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pymysql

REPO = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    database = os.getenv('SEEKDB_DATABASE', 'test')
    name = 'decimal_anti_' + secrets.token_hex(5)
    report = {'source': 'seekdb-native-export', 'pin': False, 'cpu_fallback': False, 'cases': []}
    with pymysql.connect(host=os.getenv('SEEKDB_HOST', '127.0.0.1'),
                         port=int(os.getenv('SEEKDB_PORT', '2881')), user=os.getenv('SEEKDB_USER', 'root'),
                         password=os.getenv('SEEKDB_PASSWORD', ''), database=database,
                         autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute('SET NAMES utf8mb4 COLLATE utf8mb4_bin')
        cursor.execute("SET parallel_degree_policy='MANUAL'")
        cursor.execute('SET _force_parallel_query_dop=1')
        cursor.execute(f'CREATE TABLE {name} (id BIGINT PRIMARY KEY, grp BIGINT, amount DECIMAL(15,2), wide DECIMAL(38,2))')
        try:
            cursor.execute(f"INSERT INTO {name} VALUES "
                "(1,1,1.25,999999999999999999999999999999999999.99),"
                "(2,1,-2.50,-999999999999999999999999999999999999.99),"
                "(3,2,NULL,NULL),(4,NULL,9999999999999.99,0),(5,3,0,0),(6,NULL,-0.01,-0.01),(7,2,2.00,2)")
            file = out/'input.parquet'
            cursor.execute(f"SELECT id,grp,amount,wide INTO OUTFILE %s FORMAT=(TYPE='PARQUET') FROM {name}", (str(file),))
            assert pq.read_schema(file).types == [pa.int64(), pa.int64(), pa.decimal128(15,2), pa.decimal128(38,2)]
            manifest = out/'inputs.json'
            manifest.write_text(json.dumps({database+'.'+name: [str(file)]}))
            queries = [
                ('decimal_projection', f'SELECT id,amount,wide FROM {name}'),
                ('decimal_filter', f'SELECT id,amount FROM {name} WHERE amount>0'),
                ('decimal_min_max', f'SELECT MIN(amount),MAX(amount) FROM {name}'),
                ('decimal_multiply', f'SELECT id,amount*0.05 FROM {name} WHERE id<4'),
                ('native_not_exists', f'SELECT a.id,a.grp FROM {name} a WHERE NOT EXISTS (SELECT 1 FROM {name} b WHERE a.grp=b.grp AND b.id<=2)'),
            ]
            cases = []
            for label, sql in queries:
                cursor.execute(sql)
                expected = cursor.fetchall()
                cursor.execute('EXPLAIN FORMAT=SIRIUS_JSON '+sql)
                cases.append((label, sql, json.loads(cursor.fetchone()[0]), expected))
            # Exercise both anti directions regardless of the optimizer's choice.
            # Fixtures include duplicate keys, nullable keys, and an empty side.
            for kind in ('left_anti', 'right_anti'):
                for empty in (False, True):
                    read = {'op':'read', 'database':database, 'table':name,
                            'columns':['id','grp'], 'types':['i64','i64']}
                    restricted = {'op':'filter','input':read,'condition':{'fn':'lte','type':'bool',
                        'args':[{'field':0,'type':'i64'},{'literal':0 if empty else 2,'type':'i64'}]}}
                    left, right = (read, restricted) if kind=='left_anti' else (restricted, read)
                    join = {'op':'join','kind':kind,'left':left,'right':right,'conditions':[
                        {'fn':'equal','type':'bool','args':[{'field':1,'type':'i64'},{'field':3,'type':'i64'}]}]}
                    sql = f'SELECT a.id,a.grp FROM {name} a WHERE NOT EXISTS (SELECT 1 FROM {name} b WHERE a.grp=b.grp AND b.id<={0 if empty else 2})'
                    cursor.execute(sql)
                    cases.append((kind+('_empty' if empty else ''), sql, {'version':1,'root':join,'parameters':[]}, cursor.fetchall()))
            for label, sql, document, expected in cases:
                request = out/(label+'.ir.json')
                request.write_text(json.dumps(document))
                output = out/(label+'.arrow')
                start = time.monotonic()
                with (out/(label+'.log')).open('w') as log:
                    process = subprocess.run([sys.executable, str(REPO/'adapter/compute_node.py'),
                        '--ir', str(request), '--manifest', str(manifest), '--library', str(args.library.resolve()),
                        '--config', str(args.config.resolve()), '--output', str(output)],
                        stdout=log, stderr=subprocess.STDOUT, check=False, timeout=180)
                if process.returncode:
                    report['cases'].append({'name':label,'sql':sql,'passed':False,
                        'exit_code':process.returncode,'log':str(out/(label+'.log'))})
                    (out/'results.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
                    print('FAIL',label,flush=True)
                    continue
                with pa.memory_map(str(output), 'r') as source:
                    result = pa.ipc.open_stream(source).read_all()
                actual = list(zip(*(column.to_pylist() for column in result.columns)))
                assert sorted(actual,key=repr)==sorted(expected,key=repr), (label,actual,expected)
                if label=='decimal_projection':
                    assert result.schema.types == [pa.int64(),pa.decimal128(15,2),pa.decimal128(38,2)]
                report['cases'].append({'name':label,'sql':sql,'rows':actual,'schema':str(result.schema),
                                        'seconds':time.monotonic()-start,'passed':True})
                (out/'results.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
                print('PASS',label,flush=True)
        finally:
            cursor.execute(f'DROP TABLE {name}')
    assert all(case['passed'] for case in report['cases']), report


if __name__ == '__main__':
    main()
