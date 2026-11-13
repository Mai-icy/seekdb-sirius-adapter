"""Exact, reusable statistics for private immutable Parquet generations.

Only trusted snapshot owners may supply statistics_path. File identities bind
the cache to its generation; changes invalidate it instead of narrowing Decimal
types using stale bounds. No row data is read or converted here.
"""
from decimal import Decimal
from fractions import Fraction
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _identity(table):
    files = []
    for path in table.files:
        info = os.stat(path)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Parquet input must be a regular file')
        files.append([str(Path(path).absolute()), info.st_dev, info.st_ino,
                      info.st_size, info.st_mtime_ns, info.st_ctime_ns])
    return {'files': files, 'schema': hashlib.sha256(table.schema.serialize().to_pybytes()).hexdigest()}


def scan_statistics(table):
    rows = 0
    numeric = [(i, field.name) for i, field in enumerate(table.schema)
               if pa.types.is_signed_integer(field.type) or pa.types.is_decimal(field.type)]
    bounds = {field.name: None for field in table.schema}
    for _, name in numeric:
        bounds[name] = 0
    for path in table.files:
        with pq.ParquetFile(path) as parquet:
            if not table.schema.equals(parquet.schema_arrow, check_metadata=False):
                raise ValueError('Parquet schema changed during Decimal128 proof')
            metadata = parquet.metadata
            rows += metadata.num_rows
            for index in range(metadata.num_row_groups):
                group = metadata.row_group(index)
                for column, name in numeric:
                    if bounds[name] is None:
                        continue
                    stats = group.column(column).statistics
                    if stats is not None and stats.has_null_count and stats.null_count == group.num_rows:
                        continue
                    if stats is None or not stats.has_min_max:
                        bounds[name] = None
                    else:
                        # Decimal.copy_abs does not round through the caller's
                        # decimal context (ordinary abs can round 38-digit data).
                        low, high = stats.min, stats.max
                        low = low.copy_abs() if isinstance(low, Decimal) else abs(low)
                        high = high.copy_abs() if isinstance(high, Decimal) else abs(high)
                        bounds[name] = max(bounds[name], low, high)
    # Convert once per column, rather than twice per row group and column.
    return rows, {name: None if value is None else Fraction(value) for name, value in bounds.items()}


def _load(path, identity, names):
    try:
        value = json.loads(path.read_text())
        checksum = value.pop('checksum')
        if checksum != hashlib.sha256(_canonical(value).encode()).hexdigest():
            return None
        if value['version'] != 1 or value['identity'] != identity:
            return None
        if type(value['rows']) is not int or value['rows'] < 0 or set(value['bounds']) != set(names):
            return None
        bounds = {}
        for name, raw in value['bounds'].items():
            if raw is None:
                bounds[name] = None
            else:
                if not isinstance(raw, list) or len(raw) != 2 or not all(isinstance(x, str) for x in raw):
                    return None
                numerator, denominator = map(int, raw)
                if numerator < 0 or denominator <= 0:
                    return None
                bounds[name] = Fraction(numerator, denominator)
        return value['rows'], bounds
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None


def table_statistics(table):
    path = getattr(table, 'statistics_path', None)
    if path is None:
        return scan_statistics(table)
    path = Path(path)
    parent = path.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise ValueError('Statistics cache requires a private snapshot directory')
    # Lock files live inside the generation and disappear when it is dropped.
    # The query service holds the generation lease while this lock is held.
    fd = os.open(str(path) + '.lock', os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, 'a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        identity = _identity(table)
        cached = _load(path, identity, table.schema.names)
        if cached is not None:
            if _identity(table) != identity:
                raise ValueError('Parquet changed while loading statistics')
            return cached
        rows, bounds = scan_statistics(table)
        if _identity(table) != identity:
            raise ValueError('Parquet changed during statistics collection')
        value = dict(version=1, identity=identity, rows=rows,
                     bounds={name: None if bound is None else [str(bound.numerator), str(bound.denominator)]
                             for name, bound in bounds.items()})
        value['checksum'] = hashlib.sha256(_canonical(value).encode()).hexdigest()
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix='.statistics-', delete=False) as file:
                temporary = file.name
                file.write(_canonical(value) + '\n')
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, path)
        finally:
            if temporary is not None and os.path.exists(temporary):
                os.unlink(temporary)
        return rows, bounds
