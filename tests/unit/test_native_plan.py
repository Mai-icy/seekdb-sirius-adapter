"""Regression coverage for the native-plan/Parquet Substrait boundary."""
import json
from pathlib import Path
import sys
import unittest
import tempfile
import pyarrow.parquet as pq

import pyarrow as pa
from substrait.plan_pb2 import Plan

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "adapter"))
from adapter_native_plan import ParquetTable, encode_plan


def field(index, kind="i64"):
    return {"field": index, "type": kind}


def literal(value, kind="i64"):
    return {"literal": value, "type": kind}


def read(columns=("id",), types=("i64",)):
    return {"op": "read", "database": "test", "table": "a",
            "columns": list(columns), "types": list(types)}


def project(source, *expressions):
    return {"op": "project", "input": source, "expressions": list(expressions)}


class NativePlanTest(unittest.TestCase):
    def setUp(self):
        self.schema = pa.schema([("id", pa.int64()), ("v", pa.decimal128(12, 2)),
                                 ("label", pa.string())])
        self.tables = {"test.a": ParquetTable(self.schema, ("/data/a-1.parquet", "/data/a-2.parquet"))}

    def encode(self, root, parameters=(), **extra):
        data, scans = encode_plan(json.dumps({"version": 1, "root": root, **extra}),
                                  parameters, self.tables)
        return Plan.FromString(data), scans

    def test_native_parquet_read_preserves_reordered_columns(self):
        plan, scans = self.encode(read(("label", "id"), ("string", "i64")))
        rel = plan.relations[0].root.input.read
        self.assertFalse(rel.HasField("named_table"))
        self.assertFalse(rel.HasField("projection"))
        self.assertEqual(list(rel.base_schema.names), ["label", "id"])
        self.assertEqual([f.uri_file for f in rel.local_files.items], list(self.tables["test.a"].files))
        self.assertTrue(all(f.HasField("parquet") for f in rel.local_files.items))
        self.assertEqual(scans[0]["columns"], ["label", "id"])

    def test_binds_each_execution_parameters(self):
        root = project(read(), {"param": 0, "type": "i64"})
        for value in (3, 9):
            plan, _ = self.encode(root, (value,))
            self.assertEqual(plan.relations[0].root.input.project.expressions[0].literal.i64, value)
        with self.assertRaisesRegex(ValueError, "Unbound"):
            self.encode(root)

    def test_limit_uses_fields_supported_by_upstream_reader(self):
        for count, expected in ((literal(0), 0), (literal(10), 10), (None, -1)):
            plan, _ = self.encode({"op": "fetch", "input": read(), "count": count, "offset": literal(2)})
            fetch = plan.relations[0].root.input.fetch
            self.assertEqual((fetch.count, fetch.offset), (expected, 2))
            self.assertFalse(fetch.HasField("count_expr"))
            self.assertFalse(fetch.HasField("offset_expr"))

    def test_count_star_has_a_physical_row_count_carrier(self):
        plan, _ = self.encode({"op": "aggregate", "input": read((), ()), "groups": [],
                               "measures": [{"fn": "count", "type": "i64", "args": [], "distinct": False}]})
        agg = plan.relations[0].root.input.project.input.aggregate
        self.assertEqual(list(agg.input.read.base_schema.names), ["id"])
        self.assertEqual(len(agg.groupings), 1)
        self.assertEqual(len(agg.measures[0].measure.arguments), 0)

    def test_schema_mismatch_and_unbound_columns_rejected(self):
        for source in (read(("missing",)), read(("id",), ("i32",)),
                       read(("id", "id"), ("i64", "i64")),
                       {"op": "read", "database": "other", "table": "a", "columns": ["id"], "types": ["i64"]}):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.encode(source)
        with self.assertRaisesRegex(ValueError, "outside input"):
            self.encode(project(read(), field(1)))

    def test_exact_decimal_literal_and_null(self):
        plan, _ = self.encode(project(read(), literal("-12.34", ["decimal", 12, 2]), literal(None)))
        exprs = plan.relations[0].root.input.project.expressions
        self.assertEqual(int.from_bytes(exprs[0].literal.decimal.value, "little", signed=True), -1234)
        self.assertTrue(exprs[1].literal.HasField("null"))
        for value in ("1.234", "10000000000.00"):
            with self.assertRaisesRegex(ValueError, "precision"):
                self.encode(project(read(), literal(value, ["decimal", 12, 2])))

    def test_exact_division_and_replay_need_no_private_extensions(self):
        division = {"fn": "divide", "type": ["decimal", 12, 2],
                    "args": [field(0), literal(2)]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"input.parquet"
            data = pa.table({"id": pa.array([1, 2], type=pa.int64())})
            pq.write_table(data, path)
            self.tables = {"test.a": ParquetTable(data.schema, (str(path),))}
            plan, _ = self.encode(project(read(), division))
        names = {entry.extension_function.name for entry in plan.extensions}
        self.assertTrue({'//', '%', 'cast_to_type', 'factorial'} <= names)
        self.assertFalse(any(name.startswith('sirius_') for name in names))
        plan, _ = self.encode({"op": "reference", "id": 0}, materialized=[read()])
        self.assertEqual(plan.relations[-1].root.input.reference.subtree_ordinal, 0)
        with self.assertRaisesRegex(ValueError, "Invalid native materialized reference"):
            self.encode({"op": "reference", "id": 1}, materialized=[read()])

    def test_anti_join_uses_nullable_marker_and_rebases_right_anti(self):
        condition = {"fn": "equal", "type": "bool", "args": [field(0), field(1)]}
        for kind, expected in (("left_anti", [0, 1]), ("right_anti", [1, 0])):
            plan, _ = self.encode({"op": "join", "kind": kind, "left": read(),
                "right": read(), "conditions": [condition]})
            output = plan.relations[0].root.input.project
            self.assertEqual(list(output.common.emit.output_mapping), [0])
            self.assertEqual(output.input.filter.condition.scalar_function.arguments[0]
                .value.selection.direct_reference.struct_field.field, 2)
            join = output.input.filter.input.join
            self.assertEqual(join.type, 3)
            self.assertEqual(join.right.project.expressions[0].literal.i8, 1)
            self.assertEqual([a.value.selection.direct_reference.struct_field.field
                for a in join.expression.scalar_function.arguments], expected)

    def test_inner_join_keeps_bound_input_ordinals(self):
        condition = {"fn": "equal", "type": "bool", "args": [field(0), field(1)]}
        plan, scans = self.encode({"op": "join", "kind": "inner", "left": read(),
                                   "right": read(), "conditions": [condition]})
        join = plan.relations[0].root.input.join
        self.assertEqual(len(scans), 2)
        self.assertEqual([a.value.selection.direct_reference.struct_field.field
                          for a in join.expression.scalar_function.arguments], [0, 1])

    def test_sort_preserves_null_order(self):
        plan, _ = self.encode({"op": "sort", "input": read(),
                               "keys": [{"expr": field(0), "direction": i} for i in range(4)]})
        self.assertEqual([s.direction for s in plan.relations[0].root.input.sort.sorts], [1, 2, 3, 4])

    def test_bounds_and_version(self):
        for count in (-1, 2**63):
            with self.assertRaises(ValueError):
                self.encode({"op": "fetch", "input": read(), "count": literal(count), "offset": None})
        with self.assertRaisesRegex(ValueError, "version"):
            self.encode(read(), version=2)
        with self.assertRaises(ValueError):
            self.encode(project(read(), literal("NaN", "fp64")))


if __name__ == "__main__":
    unittest.main()
