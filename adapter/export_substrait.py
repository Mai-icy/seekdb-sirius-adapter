#!/usr/bin/env python3
"""Export a seekdb native plan with explicit Parquet inputs; never plan SQL here."""
import argparse
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import pymysql
from google.protobuf.json_format import MessageToJson
from substrait.plan_pb2 import Plan

from adapter_native_plan import ParquetTable, encode_plan


def load_tables(manifest):
    manifest = Path(manifest).resolve(strict=True)
    entries = json.loads(manifest.read_text())
    if not isinstance(entries, dict) or not entries:
        raise ValueError("Manifest must map database.table names to lists of Parquet files")
    tables = {}
    for name, paths in entries.items():
        if not isinstance(name, str) or not isinstance(paths, list) or not paths:
            raise ValueError("Each input requires a name and a nonempty file list")
        files, schema = [], None
        for path in paths:
            path = (manifest.parent / path).resolve(strict=True)
            if not path.is_file():
                raise ValueError(f"Not a regular Parquet file: {path}")
            current = pq.read_schema(path)
            if schema is not None and not schema.equals(current, check_metadata=False):
                raise ValueError(f"Parquet files have different schemas: {name}")
            schema = current
            if str(path) in files:
                raise ValueError(f"Duplicate Parquet file: {path}")
            files.append(str(path))
        tables[name] = ParquetTable(schema, tuple(files))
    return tables


def explain(sql, args):
    with pymysql.connect(host=args.host, port=args.port, user=args.user,
                         password=os.environ.get("SEEKDB_PASSWORD", ""),
                         database=args.database, charset="utf8mb4", autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET parallel_degree_policy='MANUAL'")
            cursor.execute("SET _force_parallel_query_dop=1")
            cursor.execute("EXPLAIN FORMAT=SIRIUS_JSON " + sql)
            rows = cursor.fetchall()
            if len(rows) != 1 or len(rows[0]) != 1:
                raise ValueError("Expected one native plan document from seekdb")
            return rows[0][0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql", type=Path, help="SELECT file to plan inside seekdb")
    source.add_argument("--ir", type=Path, help="Previously exported SIRIUS_JSON document")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="Binary Substrait plan")
    parser.add_argument("--json-output", type=Path, help="Optional standard Substrait JSON")
    parser.add_argument("--ir-output", type=Path, help="Native plan request for the compute process")
    parser.add_argument("--host", default=os.environ.get("SEEKDB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SEEKDB_PORT", "2881")))
    parser.add_argument("--user", default=os.environ.get("SEEKDB_USER", "root"))
    parser.add_argument("--database", default=os.environ.get("SEEKDB_DATABASE", "test"))
    args = parser.parse_args()
    template = args.ir.read_text() if args.ir else explain(args.sql.read_text(), args)
    document = json.loads(template)
    plan, scans = encode_plan(template, document.get("parameters", []), load_tables(args.manifest))
    args.output.write_bytes(plan)
    if args.json_output:
        args.json_output.write_text(MessageToJson(Plan.FromString(plan)) + "\n")
    if args.ir_output:
        args.ir_output.write_text(template + "\n")
    print(json.dumps({"output": str(args.output), "bytes": len(plan), "scans": scans}))


if __name__ == "__main__":
    main()
