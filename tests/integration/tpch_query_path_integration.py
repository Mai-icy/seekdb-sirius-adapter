"""Q1-Q22 parity through seekdb's CPU/GPU MySQL result paths.

Requires populated TPC-H tables and a running query_service. No SQL replanning
outside seekdb, no query rewrites except the scale-dependent Q11 threshold.
"""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import re
import time

import pymysql


def compare(query, expected, actual):
    if query == 11:
        for rows in (expected, actual):
            assert all(a[1] >= b[1] for a,b in zip(rows,rows[1:])), 'Q11 order violation'
        expected = sorted(expected,key=lambda row:(-row[1],row[0]))
        actual = sorted(actual,key=lambda row:(-row[1],row[0]))
    assert len(expected) == len(actual), ('row count',len(expected),len(actual))
    for i,(left,right) in enumerate(zip(expected,actual)):
        assert len(left)==len(right)
        for j,(a,b) in enumerate(zip(left,right)):
            if isinstance(a,Decimal):
                assert isinstance(b,Decimal), (i,j,'Decimal type lost',type(b))
            assert a==b, (i,j,a,b)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,required=True)
    parser.add_argument('--database',required=True)
    parser.add_argument('--scale-factor',type=Decimal,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--queries',default=','.join(str(i) for i in range(1,23)))
    parser.add_argument('--timeout',type=int,default=900)
    parser.add_argument('--gpu-snapshot',default='',
                        help='Reuse this named Parquet generation across all GPU queries')
    parser.add_argument('--cpu-dop',type=int,default=1,
                        help='CPU reference parallelism; GPU plan export remains serial')
    parser.add_argument('--gpu-first',action='store_true',
                        help='Execute GPU before the CPU reference; both backends are attempted independently')
    args=parser.parse_args()
    if args.cpu_dop < 1:
        parser.error('--cpu-dop must be positive')
    args.output.mkdir(parents=True,exist_ok=False)
    report={'database':args.database,'scale_factor':str(args.scale_factor),
            'entry':'seekdb-mysql','pin':False,'cpu_fallback':False,
            'cpu_dop':args.cpu_dop,'gpu_plan_dop':1,'gpu_snapshot':args.gpu_snapshot,
            'gpu_first':args.gpu_first,'cases':[]}
    with pymysql.connect(host='127.0.0.1',port=args.port,user='root',database=args.database,
                         autocommit=True,charset='utf8mb4',read_timeout=args.timeout+30) as connection, connection.cursor() as cursor:
        cursor.execute('SET NAMES utf8mb4 COLLATE utf8mb4_bin')
        cursor.execute("SET parallel_degree_policy='MANUAL'")
        cursor.execute('SET _force_parallel_query_dop=1')
        cursor.execute(f'SET ob_query_timeout={args.timeout*1000000}')
        if args.gpu_snapshot:
            cursor.execute('SET gpu_snapshot=%s',(args.gpu_snapshot,))
        for number in map(int,args.queries.split(',')):
            sql=(Path(__file__).parent/'tpch_queries'/f'q{number:02}.sql').read_text()
            if number==11:
                threshold=format(Decimal('0.0001')/args.scale_factor,'f')
                sql=re.sub(r'\b0\.00010*\b',threshold,sql)
            (args.output/f'q{number:02}.sql').write_text(sql)
            result={'query':number,'passed':False}
            outputs={}
            failures=[]
            print('START',number,flush=True)
            for backend in (('gpu','cpu') if args.gpu_first else ('cpu','gpu')):
                stage=backend
                started=time.monotonic()
                try:
                    cursor.execute("SET execution_backend='cpu'")
                    cursor.execute(f'SET _force_parallel_query_dop={args.cpu_dop if backend=="cpu" else 1}')
                    if backend=='gpu':
                        stage='plan'
                        cursor.execute('EXPLAIN FORMAT=SIRIUS_JSON '+sql)
                        (args.output/f'q{number:02}.ir.json').write_text(cursor.fetchone()[0])
                        cursor.execute("SET execution_backend='gpu'")
                    stage=backend
                    started=time.monotonic();cursor.execute(sql);rows=cursor.fetchall()
                    result[backend+'_seconds']=time.monotonic()-started
                    result[backend+'_metadata']=cursor.description
                    outputs[backend]=(rows,cursor.description)
                    (args.output/f'q{number:02}.{backend}.json').write_text(json.dumps(rows,default=str)+'\n')
                except Exception as error:
                    failures.append(dict(stage=stage,error=str(error),elapsed_seconds=time.monotonic()-started))
            if not failures:
                try:
                    expected,metadata=outputs['cpu']
                    actual,gpu_metadata=outputs['gpu']
                    assert metadata==gpu_metadata, ('MySQL metadata',metadata,gpu_metadata)
                    compare(number,expected,actual)
                    result.update(passed=True,rows=len(actual))
                except Exception as error:
                    failures.append(dict(stage='compare',error=str(error)))
            if failures:
                result.update(failures[0],failures=failures)
            report['cases'].append(result)
            (args.output/'results.json').write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps(result),flush=True)
    assert all(c['passed'] for c in report['cases']), 'TPC-H parity failed'


if __name__=='__main__':
    main()
