#!/usr/bin/env python3
"""Run repeated CPU/GPU TPC-H parity suites through seekdb's MySQL endpoint."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import uuid

from summarize import summarize, write_summary

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'tests/integration/tpch_query_path_integration.py'


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temporary.replace(path)


def revision(path):
    if not (path / '.git').exists():
        return {'commit': None, 'dirty': None, 'initialized': False}
    def git(*args):
        return subprocess.check_output(['git', '-C', str(path), *args], text=True).strip()
    try:
        return {'commit': git('rev-parse', 'HEAD'),
                'dirty': bool(git('status', '--porcelain')), 'initialized': True}
    except subprocess.CalledProcessError:
        return {'commit': None, 'dirty': None, 'initialized': False}


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default=os.getenv('SEEKDB_HOST', '127.0.0.1'))
    parser.add_argument('--port', type=int, default=int(os.getenv('SEEKDB_PORT', '2881')))
    parser.add_argument('--user', default=os.getenv('SEEKDB_USER', 'root'))
    parser.add_argument('--database', required=True)
    parser.add_argument('--scale-factor', type=Decimal, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--queries', default=','.join(map(str, range(1, 23))))
    parser.add_argument('--mode', choices=['snapshot', 'statement'], default='snapshot')
    parser.add_argument('--gpu-snapshot', help='Existing immutable version; default creates a unique label')
    parser.add_argument('--cpu-dop', type=int, default=1)
    parser.add_argument('--timeout', type=int, default=900, help='Per SQL timeout in seconds')
    parser.add_argument('--warmups', type=int, default=1, help='Full parity suites excluded from measurements')
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--gpu-first', action='store_true')
    parser.add_argument('--environment', type=Path, help='Operator-provided JSON describing actual deployed binaries and hardware')
    parser.add_argument('--dry-run', action='store_true', help='Write commands and manifest without connecting to seekdb')
    args = parser.parse_args()
    if not args.scale_factor.is_finite() or args.scale_factor <= 0:
        parser.error('scale-factor must be finite and positive')
    if min(args.cpu_dop, args.timeout, args.rounds) < 1 or args.warmups < 0:
        parser.error('cpu-dop/timeout/rounds must be positive; warmups must be nonnegative')
    if not 1 <= args.port <= 65535:
        parser.error('port must be in 1..65535')
    try:
        args.query_ids = list(map(int, args.queries.split(',')))
    except ValueError:
        parser.error('queries must be comma-separated integers')
    if not args.query_ids or len(set(args.query_ids)) != len(args.query_ids) or any(q not in range(1, 23) for q in args.query_ids):
        parser.error('queries must be distinct numbers in 1..22')
    if args.mode == 'statement' and args.gpu_snapshot is not None:
        parser.error('statement mode does not accept gpu-snapshot')
    if args.mode == 'snapshot' and args.warmups < 1:
        parser.error('snapshot measurements require at least one warmup suite; initial costs stay in warmup results')
    if args.gpu_snapshot is not None and not 1 <= len(args.gpu_snapshot) <= 128:
        parser.error('gpu-snapshot must contain 1..128 characters')
    if args.environment:
        args.environment_data = json.loads(args.environment.read_text())
        if not isinstance(args.environment_data, dict):
            parser.error('environment must contain a JSON object')
    else:
        args.environment_data = None
    return args


def main():
    args = arguments()
    snapshot = '' if args.mode == 'statement' else (args.gpu_snapshot or 'bench-' + uuid.uuid4().hex)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    settings = dict(database=args.database, scale_factor=str(args.scale_factor),
                    queries=args.query_ids, mode=args.mode, gpu_snapshot=snapshot,
                    cpu_dop=args.cpu_dop, gpu_plan_dop=1, gpu_first=args.gpu_first,
                    timeout=args.timeout, warmups=args.warmups, rounds=args.rounds)
    hashes = {}
    for path in [RUNNER, Path(__file__).resolve(), Path(__file__).with_name('summarize.py'),
                 *sorted((ROOT / 'adapter').glob('*.py')),
                 *(ROOT / f'tests/integration/tpch_queries/q{q:02}.sql' for q in args.query_ids)]:
        hashes[str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = dict(format_version=1, status='planned', created=datetime.now(timezone.utc).isoformat(),
                    settings=settings, connection=dict(host=args.host, port=args.port, user=args.user),
                    client=dict(python=platform.python_version(), platform=platform.platform()),
                    checkout={name: revision(path) for name, path in
                              [('adapter', ROOT), ('seekdb', ROOT / 'deps/seekdb'), ('sirius', ROOT / 'deps/sirius')]},
                    source_sha256=hashes, deployed_environment=args.environment_data, runs=[])
    for phase, count in [('warmup', args.warmups), ('measurement', args.rounds)]:
        for index in range(1, count + 1):
            directory = f'{phase}-{index:02}'
            command = [sys.executable, str(RUNNER), '--host', args.host, '--port', str(args.port),
                       '--user', args.user, '--database', args.database, '--scale-factor', str(args.scale_factor),
                       '--output', str(output / directory), '--queries', args.queries,
                       '--timeout', str(args.timeout), '--cpu-dop', str(args.cpu_dop), '--gpu-snapshot', snapshot]
            if args.gpu_first:
                command.append('--gpu-first')
            manifest['runs'].append(dict(phase=phase, index=index, directory=directory, command=command, status='planned'))
    save(output / 'run.json', manifest)
    if args.dry_run:
        print(f'Plan only: {output / "run.json"}; no database connection or GPU execution.')
        return 0
    manifest['status'] = 'running'
    try:
        for run in manifest['runs']:
            run['status'] = 'running'
            save(output / 'run.json', manifest)
            print(f'{run["directory"]}: log {output / (run["directory"] + ".log")}', flush=True)
            start = time.monotonic()
            try:
                with (output / (run['directory'] + '.log')).open('w') as log:
                    result = subprocess.run(run['command'], stdout=log, stderr=subprocess.STDOUT)
                run['returncode'] = result.returncode
                run['status'] = 'passed' if result.returncode == 0 else 'failed'
            finally:
                run['wall_seconds'] = time.monotonic() - start
            if run['status'] != 'passed':
                manifest['status'] = 'failed'
                break
        else:
            manifest['status'] = 'complete'
    except KeyboardInterrupt:
        manifest['status'] = 'interrupted'
        print('Interrupted; retained partial reports. Named snapshot is not deleted.', file=sys.stderr)
    except Exception as error:
        manifest['status'] = 'failed'
        manifest['error'] = str(error)
    finally:
        manifest['finished'] = datetime.now(timezone.utc).isoformat()
        save(output / 'run.json', manifest)
    summary = summarize(output)
    if not summary['complete'] and manifest['status'] == 'complete':
        manifest['status'] = 'failed'
        save(output / 'run.json', manifest)
    write_summary(output, summary)
    print(f'Result: {output / "summary.md"}', flush=True)
    return 0 if summary['complete'] else 1


if __name__ == '__main__':
    sys.exit(main())
