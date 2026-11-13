"""Snapshot-backed precision and join-cardinality correctness coverage."""
from decimal import Decimal
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from substrait.plan_pb2 import Plan

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'adapter'))
from adapter_native_plan import ParquetTable, encode_plan
from decimal_bounds import Bounds, exact_operand, narrow_decimals
import parquet_statistics


class DecimalBoundsTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path=Path(self.directory.name)/'input.parquet'
        data=pa.table({'id':[1,2,3], 'v':pa.array([Decimal('-12.34'),Decimal('2.01'),None],type=pa.decimal128(15,2))})
        pq.write_table(data,self.path,row_group_size=2)
        self.tables={'test.a':ParquetTable(data.schema,(str(self.path),))}
        self.read={'op':'read','database':'test','table':'a','columns':['id','v'],
                   'types':['i64',['decimal',15,2]],'unique_keys':[[0]]}

    def test_wide_sum_is_proved_then_narrowed(self):
        root={'op':'aggregate','input':self.read,'groups':[],
              'measures':[{'fn':'sum','args':[{'field':1,'type':['decimal',15,2]}],
                           'distinct':False,'type':['decimal',53,2]}]}
        document={'version':1,'root':root}
        binary,_=encode_plan(json.dumps(document),[],self.tables)
        self.assertTrue(Plan.FromString(binary).relations)
        self.assertEqual(root['measures'][0]['type'],['decimal',53,2])
        narrow_decimals(document,[],self.tables)
        self.assertEqual(root['measures'][0]['type'],['decimal',38,2])

    def test_large_coefficient_is_rejected(self):
        root={'op':'project','input':self.read,'expressions':[{'literal':'1'+'0'*38,'type':['decimal',53,0]}]}
        with self.assertRaisesRegex(ValueError,'cannot be proved'):
            encode_plan(json.dumps({'version':1,'root':root}),[],self.tables)

    def test_primary_key_bounds_join_multiplicity(self):
        condition={'fn':'equal','args':[{'field':0,'type':'i64'},{'field':2,'type':'i64'}],'type':'bool'}
        node={'op':'join','kind':'inner','left':self.read,'right':self.read,'conditions':[condition]}
        proof=Bounds(self.tables,[]).relation(node)
        self.assertEqual(proof.rows,3)
        self.assertEqual(proof.keys,[frozenset([0]),frozenset([2])])
        node['conditions']=[]
        self.assertEqual(Bounds(self.tables,[]).relation(node).rows,9)

    def test_missing_statistics_uses_declared_domain(self):
        data=pq.read_table(self.path)
        pq.write_table(data,self.path,write_statistics=False)
        proof=Bounds(self.tables,[]).relation(self.read)
        self.assertEqual(proof.values[1].magnitude,Decimal('9999999999999.99'))

    def test_empty_outer_side_preserves_rows(self):
        pq.write_table(pa.table({'id':pa.array([],type=pa.int64()),'v':pa.array([],type=pa.decimal128(15,2))}),Path(self.directory.name)/'empty.parquet')
        self.tables['test.empty']=ParquetTable(self.tables['test.a'].schema,(str(Path(self.directory.name)/'empty.parquet'),))
        other={**self.read,'table':'empty'}
        node={'op':'join','kind':'left','left':self.read,'right':other,'conditions':[]}
        self.assertEqual(Bounds(self.tables,[]).relation(node).rows,3)

    def test_only_injective_scale_casts_can_be_removed(self):
        value={'field':0,'type':['decimal',20,6]}
        self.assertIs(exact_operand({'cast':value,'type':['decimal',34,20]}),value)
        for target in (['decimal',24,20],['decimal',20,2]):
            node={'cast':value,'type':target}
            self.assertIs(exact_operand(node),node)

    def test_fractional_number_literal_keeps_its_quantum(self):
        value=Bounds(self.tables,['0.0001']).expr({'param':0,'type':'number'},[])
        self.assertEqual(value.spec,['decimal',4,4])
        self.assertEqual(value.magnitude,Decimal('0.0001'))

    def test_widening_cast_checks_values_not_only_type_hints(self):
        value={'fn':'add','args':[{'literal':60,'type':'i64'}, {'literal':40,'type':'i64'}],
               'type':['decimal',2,0]}
        expression={'fn':'divide','args':[{'cast':value,'type':['decimal',20,18]},
                    {'literal':1,'type':'i64'}],'type':['decimal',38,0]}
        with self.assertRaisesRegex(ValueError,'intermediate'):
            Bounds(self.tables,[]).expr(expression,[])

    def cached_table(self):
        return replace(self.tables['test.a'], statistics_path=str(Path(self.directory.name)/'stats.json'))

    def test_immutable_statistics_survive_a_new_proof(self):
        table=self.cached_table()
        expected=Bounds({'test.a':table},[]).source('test.a')
        with mock.patch.object(parquet_statistics,'scan_statistics',side_effect=AssertionError('Unexpected metadata scan')):
            self.assertEqual(Bounds({'test.a':table},[]).source('test.a'),expected)

    def test_changed_input_invalidates_statistics(self):
        table=self.cached_table()
        self.assertEqual(parquet_statistics.table_statistics(table)[1]['v'],Decimal('12.34'))
        data=pa.table({'id':[1,2,3], 'v':pa.array([Decimal('-98.76'),None,None],type=pa.decimal128(15,2))})
        pq.write_table(data,self.path,row_group_size=2)
        self.assertEqual(parquet_statistics.table_statistics(table)[1]['v'],Decimal('98.76'))

    def test_corrupt_cache_cannot_reduce_a_bound(self):
        table=self.cached_table()
        expected=parquet_statistics.table_statistics(table)
        path=Path(table.statistics_path);record=json.loads(path.read_text())
        record['bounds']['v']=['1','1'];path.write_text(json.dumps(record))
        self.assertEqual(parquet_statistics.table_statistics(table),expected)

    def test_concurrent_proofs_collect_statistics_once(self):
        table=self.cached_table()
        with mock.patch.object(parquet_statistics,'scan_statistics',wraps=parquet_statistics.scan_statistics) as scan:
            with ThreadPoolExecutor(max_workers=4) as executor:
                results=list(executor.map(parquet_statistics.table_statistics,[table]*4))
            self.assertEqual(scan.call_count,1)
        self.assertTrue(all(result==results[0] for result in results))

    def test_mutation_during_collection_is_not_published(self):
        table=self.cached_table();scan=parquet_statistics.scan_statistics
        def change(table):
            result=scan(table)
            data=pq.read_table(self.path);pq.write_table(data,self.path)
            return result
        with mock.patch.object(parquet_statistics,'scan_statistics',side_effect=change):
            with self.assertRaisesRegex(ValueError,'changed during'):
                parquet_statistics.table_statistics(table)
        self.assertFalse(Path(table.statistics_path).exists())

    def test_statistics_do_not_round_decimal38(self):
        value=Decimal('-99999999999999999999999999999999999999')
        schema=pa.schema([('v',pa.decimal128(38,0))])
        pq.write_table(pa.table({'v':pa.array([value,None],type=schema[0].type)}),self.path,row_group_size=1)
        table=ParquetTable(schema,(str(self.path),),str(Path(self.directory.name)/'stats.json'))
        expected=value.copy_abs()
        self.assertEqual(parquet_statistics.table_statistics(table)[1]['v'],expected)
        self.assertEqual(parquet_statistics.table_statistics(table)[1]['v'],expected)


if __name__=='__main__':
    unittest.main()
