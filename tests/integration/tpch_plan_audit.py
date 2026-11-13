"""Audit all 22 TPC-H native exports and Substrait encodings; does not run queries.

The target database must already contain all eight TPC-H schemas. Bindings use
schema-only placeholder file paths, so a passing entry is not GPU validation.
"""
import argparse
from collections import Counter
import json
import os
import re
from pathlib import Path
import sys

import pyarrow as pa
import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'adapter'))
from adapter_native_plan import ParquetTable, encode_plan


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def arrow_type(spec):
    if isinstance(spec, list):
        return pa.decimal128(spec[1], spec[2])
    return {'i8':pa.int8(), 'i16':pa.int16(), 'i32':pa.int32(), 'i64':pa.int64(),
            'date':pa.date32(), 'string':pa.string(), 'fp32':pa.float32(), 'fp64':pa.float64()}[spec]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--scale-factor', type=int, default=1)
    args = parser.parse_args()
    if args.scale_factor < 1:
        parser.error('scale-factor must be positive')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = {'scale_factor':args.scale_factor, 'gpu_executed':False, 'cases':[]}
    with pymysql.connect(host=os.getenv('SEEKDB_HOST','127.0.0.1'),
                         port=int(os.getenv('SEEKDB_PORT','2881')),user=os.getenv('SEEKDB_USER','root'),
                         password=os.getenv('SEEKDB_PASSWORD',''),database=os.environ['SEEKDB_DATABASE'],
                         autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute('SET NAMES utf8mb4 COLLATE utf8mb4_bin')
        cursor.execute("SET parallel_degree_policy='MANUAL'")
        cursor.execute('SET _force_parallel_query_dop=1')
        cursor.execute('SET ob_query_timeout=120000000')
        for file in sorted((Path(__file__).parent/'tpch_queries').glob('q*.sql')):
            sql = file.read_text()
            if file.stem=='q11':
                # Preserve exact SQL division instead of a Python binary float.
                sql = re.sub(r'\b0\.00010*\b',f'(0.0001 / {args.scale_factor})',sql)
            case = {'query':file.stem, 'exported':False, 'encoded':False}
            stage = 'export'
            try:
                cursor.execute('EXPLAIN FORMAT=SIRIUS_JSON '+sql)
                template = cursor.fetchone()[0]
                (args.output_dir/(file.stem+'.ir.json')).write_text(template)
                document = json.loads(template)
                case['exported'] = True
                nodes = list(walk(document))
                case['operators'] = dict(Counter(n['op'] for n in nodes if 'op' in n))
                case['wide_decimals'] = sorted({tuple(n['type']) for n in nodes
                    if isinstance(n.get('type'),list) and n['type'][0]=='decimal' and n['type'][1]>38})
                tables = {}
                schemas = {}
                for node in nodes:
                    if node.get('op')=='read':
                        name = node.get('database','')+'.'+node['table']
                        schema = schemas.setdefault(name,{})
                        for column, spec in zip(node['columns'],node['types']):
                            schema[column]=arrow_type(spec)
                for name, columns in schemas.items():
                    tables[name]=ParquetTable(pa.schema(list(columns.items())), ('/schema-only/'+name+'.parquet',))
                stage = 'encode'
                binary, scans = encode_plan(template,document.get('parameters',[]),tables)
                (args.output_dir/(file.stem+'.substrait')).write_bytes(binary)
                case.update(encoded=True, plan_bytes=len(binary), scans=len(scans))
            except Exception as error:
                case.update(failed_stage=stage,error=str(error))
            report['cases'].append(case)
            (args.output_dir/'results.json').write_text(json.dumps(report,indent=2)+'\n')
            print(file.stem, 'encoded' if case['encoded'] else case['error'],flush=True)
    print(json.dumps({'exported':sum(c['exported'] for c in report['cases']),
                      'encoded':sum(c['encoded'] for c in report['cases']), 'total':len(report['cases'])}))


if __name__=='__main__':
    main()
