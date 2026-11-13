"""Validate seekdb-native plans against unmodified Sirius reading external Parquet.

By default the seekdb table is empty and Parquet fixtures carry the data. With
--export-from-seekdb, data is inserted into seekdb and exported by native SQL.
Both modes invoke the same unmodified Sirius GPU interface.
"""
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
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--export-from-seekdb", action="store_true",
                        help="Populate seekdb and use native Parquet export instead of synthetic files")
    args = parser.parse_args()
    directory = args.output_dir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    database = os.environ.get("SEEKDB_DATABASE", "test")
    table = "parquet_gpu_" + secrets.token_hex(5)
    # Physical order deliberately differs from seekdb's declared table order.
    data = pa.table({"amount": pa.array([1.25, 2.5, None, 4.0], type=pa.float64()),
                     "grp": pa.array([1, 1, 2, None], type=pa.int64()),
                     "id": pa.array([1, 2, 3, 4], type=pa.int64())})
    paths = []
    for index in range(2):
        path = directory / f"part-{index}.parquet"
        if not args.export_from_seekdb:
            pq.write_table(data.slice(index * 2, 2), path)
        paths.append(str(path))
    manifest = directory / "inputs.json"
    manifest.write_text(json.dumps({database + "." + table: paths}))
    queries = [
        ("projection_filter_limit", f"SELECT amount, id FROM {table} WHERE id >= 2 ORDER BY id DESC LIMIT 2",
         [(4.0, 4), (None, 3)]),
        ("count", f"SELECT COUNT(*) FROM {table}", [(4,)]),
        ("sum", f"SELECT SUM(amount), COUNT(amount) FROM {table}", [(7.75, 3)]),
        ("group_null", f"SELECT grp, COUNT(*) FROM {table} GROUP BY grp", [(1, 2), (2, 1), (None, 1)]),
        ("self_join", f"SELECT a.id, b.amount FROM {table} a JOIN {table} b ON a.id=b.id WHERE b.id >= 3",
         [(3, None), (4, 4.0)]),
        ("empty_filter", f"SELECT id FROM {table} WHERE id > 100", []),
    ]
    output_types = {
        "projection_filter_limit": [pa.float64(), pa.int64()],
        "count": [pa.int64()],
        "sum": [pa.float64(), pa.int64()],
        "group_null": [pa.int64(), pa.int64()],
        "self_join": [pa.int64(), pa.float64()],
        "empty_filter": [pa.int64()],
    }
    report = {"table": database + "." + table, "seekdb_rows": 4 if args.export_from_seekdb else 0,
              "source": "seekdb-native-export" if args.export_from_seekdb else "fixture", "parquet_rows": 4,
              "input_order": data.schema.names, "cases": []}
    with pymysql.connect(host=os.environ.get("SEEKDB_HOST", "127.0.0.1"),
                         port=int(os.environ.get("SEEKDB_PORT", "2881")),
                         user=os.environ.get("SEEKDB_USER", "root"),
                         password=os.environ.get("SEEKDB_PASSWORD", ""),
                         database=database, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET parallel_degree_policy='MANUAL'")
            cursor.execute("SET _force_parallel_query_dop=1")
            cursor.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, grp BIGINT, amount DOUBLE)")
            try:
                if args.export_from_seekdb:
                    cursor.execute(f"INSERT INTO {table} VALUES (1,1,1.25),(2,1,2.5),(3,2,NULL),(4,NULL,4.0)")
                    for index, path in enumerate(paths):
                        cursor.execute(f"SELECT amount, grp, id INTO OUTFILE %s FORMAT=(TYPE='PARQUET') "
                                       f"FROM {table} WHERE id BETWEEN {index*2+1} AND {index*2+2} ORDER BY id", (path,))
                        exported = pq.read_table(path)
                        assert exported.schema.names == data.schema.names
                        assert exported.equals(data.slice(index*2, 2))
                for name, sql, expected in queries:
                    cursor.execute("EXPLAIN FORMAT=SIRIUS_JSON " + sql)
                    rows = cursor.fetchall()
                    assert len(rows) == 1
                    request = directory / (name + ".ir.json")
                    request.write_text(rows[0][0])
                    output = directory / (name + ".arrow")
                    start = time.monotonic()
                    with (directory / (name + ".log")).open("w") as log:
                        subprocess.run([sys.executable, str(REPO / "adapter/compute_node.py"),
                                        "--ir", str(request), "--manifest", str(manifest),
                                        "--library", str(args.library.resolve()),
                                        "--config", str(args.config.resolve()), "--output", str(output)],
                                       check=True, stdout=log, stderr=subprocess.STDOUT, timeout=120)
                    with pa.memory_map(str(output), "r") as source:
                        result = pa.ipc.open_stream(source).read_all()
                    assert result.num_columns == (len(expected[0]) if expected else 1)
                    assert result.schema.types == output_types[name], (name, result.schema)
                    actual = list(zip(*(column.to_pylist() for column in result.columns)))
                    if name == "projection_filter_limit":
                        assert actual == expected, (name, actual, expected)
                    else:
                        assert sorted(actual, key=repr) == sorted(expected, key=repr), (name, actual, expected)
                    report["cases"].append({"name": name, "sql": sql, "rows": actual,
                                             "schema": str(result.schema),
                                             "seconds": time.monotonic() - start, "passed": True})
                    # Arrow decimal values stay exact in the machine-readable report.
                    (directory / "results.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
                    print("PASS", name, flush=True)
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                assert cursor.fetchone()[0] == (4 if args.export_from_seekdb else 0)
            finally:
                cursor.execute(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
