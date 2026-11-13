"""Exact arithmetic edges through the seekdb CPU/GPU MySQL result paths."""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import secrets

import pymysql


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    table='decimal_edges_'+secrets.token_hex(5)
    cases=[]
    with pymysql.connect(host='127.0.0.1',port=args.port,user='root',database='test',autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute('SET NAMES utf8mb4 COLLATE utf8mb4_bin')
        cursor.execute("SET parallel_degree_policy='MANUAL'")
        cursor.execute('SET _force_parallel_query_dop=1')
        cursor.execute('SET ob_query_timeout=120000000')
        cursor.execute(f'CREATE TABLE {table}(id BIGINT PRIMARY KEY,n DECIMAL(20,6),d DECIMAL(15,6))')
        try:
            rows=[(1,'90071992547409.001234','3'),(2,'-90071992547409.001234','3'),
                  (3,'1.005','1'),(4,'-1.005','1'),(5,'1','6'),(6,'1','-6'),
                  (7,'-1','-6'),(8,'1','0'),(9,None,'1'),(10,'1',None),
                  (11,'0','1'),(12,'0','0'),(13,'0.000001','0.000002'),
                  (14,'123.456789','0.000001')]
            cursor.executemany(f'INSERT INTO {table} VALUES(%s,%s,%s)',rows)
            queries=[('dynamic_division',f'SELECT id,n/d FROM {table} ORDER BY id'),
                     ('half_away_rounding',f'SELECT id,CAST(n/d AS DECIMAL(38,2)) FROM {table} ORDER BY id'),
                     ('literal_fraction',f'SELECT id,n/0.2 FROM {table} WHERE id>2 ORDER BY id'),
                     ('wide_sum',f'SELECT SUM(n),SUM(n*d),AVG(n) FROM {table}')]
            for name,sql in queries:
                cursor.execute("SET execution_backend='cpu'");cursor.execute(sql)
                expected=cursor.fetchall();metadata=cursor.description
                cursor.execute("SET execution_backend='gpu'");cursor.execute(sql)
                actual=cursor.fetchall()
                assert metadata==cursor.description,(name,'metadata')
                assert expected==actual,(name,expected,actual)
                for row in actual:
                    assert all(v is None or isinstance(v,(int,Decimal)) for v in row)
                cases.append({'name':name,'passed':True,'rows':actual})
                (args.output/'results.json').write_text(json.dumps(cases,default=str,indent=2)+'\n')
                print('PASS',name,flush=True)
        finally:
            cursor.execute("SET execution_backend='cpu'")
            cursor.execute(f'DROP TABLE {table}')


if __name__=='__main__':
    main()
