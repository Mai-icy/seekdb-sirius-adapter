#!/usr/bin/env python3
"""Summarize a benchmark run; never combine partial or failed suites into a speedup."""
import argparse
import json
import math
from pathlib import Path
import statistics
import sys


def summarize(root):
    root = Path(root)
    manifest = json.loads((root / 'run.json').read_text())
    if manifest.get('format_version') != 1:
        raise ValueError('Unsupported benchmark manifest version')
    settings = manifest['settings']
    queries = settings['queries']
    if (not queries or any(type(q) is not int or q not in range(1, 23) for q in queries)
            or len(set(queries)) != len(queries)
            or type(settings['rounds']) is not int or settings['rounds'] < 1
            or type(settings['warmups']) is not int or settings['warmups'] < 0):
        raise ValueError('Invalid query selection or round counts')
    if settings['mode'] not in ('snapshot', 'statement'):
        raise ValueError('Unknown input mode')
    if settings['mode'] == 'snapshot' and (not settings['gpu_snapshot'] or settings['warmups'] < 1):
        raise ValueError('Snapshot mode requires a version and warmup')
    if settings['mode'] == 'statement' and settings['gpu_snapshot']:
        raise ValueError('Statement mode must use an empty snapshot')
    expected = [(phase, i) for phase, count in [('warmup', settings['warmups']), ('measurement', settings['rounds'])]
                for i in range(1, count + 1)]
    errors = []
    if manifest.get('status') != 'complete':
        errors.append('Run status is ' + str(manifest.get('status')))
    runs = manifest['runs']
    if [(r['phase'], r['index']) for r in runs] != expected:
        errors.append('Missing, duplicated or reordered rounds')
    measurements = []
    for run in runs:
        label = f'{run["phase"]}-{run["index"]:02}'
        if run.get('status') != 'passed' or run.get('returncode') != 0:
            errors.append(f'{label}: not completed successfully')
            continue
        try:
            if run['directory'] != label:
                raise ValueError('Unexpected results directory')
            report = json.loads((root / label / 'results.json').read_text())
            for key in ('database', 'scale_factor', 'cpu_dop', 'gpu_plan_dop', 'gpu_snapshot', 'gpu_first', 'queries'):
                if report.get(key) != settings[key]:
                    raise ValueError(f'Mismatched setting: {key}')
            if report.get('entry') != 'seekdb-mysql' or report.get('pin') is not False or report.get('cpu_fallback') is not False:
                raise ValueError('Unexpected execution path')
            cases = report['cases']
            if [c['query'] for c in cases] != settings['queries']:
                raise ValueError('Missing, duplicated or reordered query cases')
            for case in cases:
                if case.get('passed') is not True or case.get('failures') or case.get('error'):
                    raise ValueError(f'Q{case["query"]}: parity failed')
                if not case.get('cpu_metadata') or case.get('cpu_metadata') != case.get('gpu_metadata'):
                    raise ValueError(f'Q{case["query"]}: metadata missing or different')
                for backend in ('cpu', 'gpu'):
                    value = case[backend + '_seconds']
                    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                        raise ValueError('Timing must be finite and positive')
            if run['phase'] == 'measurement':
                measurements.append(cases)
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append(f'{label}: {error}')
    if len(measurements) != settings['rounds'] or not measurements:
        errors.append('Not all measurement rounds are valid')
    result = dict(format_version=1, complete=not errors, full_tpch=set(settings['queries']) == set(range(1, 23)),
                  settings=settings, errors=errors, query_statistics=[], suite=None)
    if errors:
        return result
    for index, query in enumerate(settings['queries']):
        row = {'query': query}
        for backend in ('cpu', 'gpu'):
            values = [cases[index][backend + '_seconds'] for cases in measurements]
            row[backend] = dict(samples=values, median_seconds=statistics.median(values),
                                min_seconds=min(values), max_seconds=max(values))
        row['cpu_over_gpu'] = row['cpu']['median_seconds'] / row['gpu']['median_seconds']
        result['query_statistics'].append(row)
    totals = {backend: [sum(c[backend + '_seconds'] for c in cases) for cases in measurements]
              for backend in ('cpu', 'gpu')}
    result['suite'] = {backend: dict(samples=values, median_seconds=statistics.median(values))
                       for backend, values in totals.items()}
    result['suite']['cpu_over_gpu'] = result['suite']['cpu']['median_seconds'] / result['suite']['gpu']['median_seconds']
    return result


def write_summary(root, result):
    root = Path(root)
    (root / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    lines = ['# TPC-H benchmark', '', f'Complete: {result["complete"]}; full Q1–Q22: {result["full_tpch"]}.', '']
    if not result['complete']:
        lines += ['Incomplete or invalid run; no aggregate speedup is reported.', '']
        lines += ['- ' + error for error in result['errors']]
    else:
        lines += ['Times include execute + fetchall; exclude warmup suites and separate EXPLAIN.',
                  'CPU/GPU ratios describe this run only. This is not an audited TPC-H result.', '',
                  '| Query | CPU median s | GPU median s | CPU/GPU |', '| --- | ---: | ---: | ---: |']
        for row in result['query_statistics']:
            lines.append(f'| Q{row["query"]} | {row["cpu"]["median_seconds"]:.3f} | {row["gpu"]["median_seconds"]:.3f} | {row["cpu_over_gpu"]:.3f} |')
        suite = result['suite']
        lines += ['', f'Suite median (sum within each measured round): CPU {suite["cpu"]["median_seconds"]:.3f}s; '
                  f'GPU {suite["gpu"]["median_seconds"]:.3f}s; ratio {suite["cpu_over_gpu"]:.3f}.',
                  '', 'Min/max and all measured samples are in summary.json. The suite median is not the sum of per-query medians.']
    (root / 'summary.md').write_text('\n'.join(lines) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    result = summarize(args.directory)
    write_summary(args.directory, result)
    print(args.directory / 'summary.md')
    return 0 if result['complete'] else 1


if __name__ == '__main__':
    sys.exit(main())
