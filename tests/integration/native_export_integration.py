"""Exercise the native optimizer export through a running disposable seekdb."""
import json
import os
from pathlib import Path
import secrets
import sys

import pyarrow as pa
import pymysql
from substrait.plan_pb2 import Plan

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "adapter"))
from adapter_native_plan import ParquetTable, encode_plan


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def main():
    database = os.environ.get("SEEKDB_DATABASE", "test")
    table = "native_export_" + secrets.token_hex(5)
    schema = pa.schema([("id", pa.int64()), ("amount", pa.int64())])
    inputs = {database + "." + table: ParquetTable(schema, ("/data/fixture.parquet",))}
    with pymysql.connect(host=os.environ.get("SEEKDB_HOST", "127.0.0.1"),
                         port=int(os.environ.get("SEEKDB_PORT", "2881")),
                         user=os.environ.get("SEEKDB_USER", "root"),
                         password=os.environ.get("SEEKDB_PASSWORD", ""),
                         database=database, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET parallel_degree_policy='MANUAL'")
            cursor.execute("SET _force_parallel_query_dop=1")
            cursor.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, amount BIGINT)")
            try:
                cursor.execute(f"INSERT INTO {table} VALUES (1, 10), (2, 20), (3, NULL)")
                queries = [
                    f"SELECT amount, id FROM {table} WHERE id >= 2 ORDER BY id DESC LIMIT 1",
                    f"SELECT COUNT(*) FROM {table}",
                    f"SELECT id, COUNT(amount) FROM {table} GROUP BY id",
                    f"SELECT a.id FROM {table} a JOIN {table} b ON a.id=b.id WHERE b.amount > 10",
                ]
                for sql in queries:
                    cursor.execute("EXPLAIN FORMAT=SIRIUS_JSON " + sql)
                    rows = cursor.fetchall()
                    assert len(rows) == 1
                    document = json.loads(rows[0][0])
                    assert document["version"] == 1
                    assert any(n.get("op") == "read" for n in walk(document))
                    binary, scans = encode_plan(rows[0][0], document["parameters"], inputs)
                    assert Plan.FromString(binary).relations[-1].HasField("root")
                    assert scans
                cursor.execute(f"EXPLAIN FORMAT=SIRIUS_JSON SELECT SUM(amount) FROM {table}")
                wide = json.loads(cursor.fetchone()[0])
                assert any(isinstance(n.get("type"), list) and n["type"][0] == "decimal"
                           and n["type"][1] > 38 for n in walk(wide))
                try:
                    encode_plan(json.dumps(wide), wide["parameters"], inputs)
                except ValueError as error:
                    assert "Decimal128" in str(error), error
                else:
                    raise AssertionError("Wide Decimal was silently narrowed")
                try:
                    cursor.execute(f"EXPLAIN FORMAT=SIRIUS_JSON SELECT a.id FROM {table} a "
                                   f"WHERE a.amount NOT IN (SELECT b.amount FROM {table} b)")
                except pymysql.MySQLError as error:
                    assert "NULL-aware anti join" in str(error), error
                else:
                    raise AssertionError("NULL-aware anti join was exported as ordinary anti join")
                # Repeated literal-bearing statements must not reuse a stale value.
                plans = []
                for bound in (1, 3):
                    cursor.execute(f"EXPLAIN FORMAT=SIRIUS_JSON SELECT id FROM {table} WHERE id >= {bound}")
                    document = json.loads(cursor.fetchone()[0])
                    binary, _ = encode_plan(json.dumps(document), document["parameters"], inputs)
                    plans.append(binary)
                assert plans[0] != plans[1]
                cursor.execute(f"EXPLAIN SELECT id FROM {table}")
                assert cursor.fetchall()
                try:
                    cursor.execute(f"EXPLAIN FORMAT=SIRIUS_JSON DELETE FROM {table}")
                except pymysql.MySQLError:
                    pass
                else:
                    raise AssertionError("DML export was not rejected")
                print("PASS: four native SQL plans, wide Decimal preservation, NULL-aware anti rejection, fresh constants, ordinary EXPLAIN and DML rejection")
            finally:
                cursor.execute(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
