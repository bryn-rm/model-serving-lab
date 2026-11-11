#!/usr/bin/env python3
"""Tabulate saved pilot runs and suggest an open-loop overload rate.

Reads only manifest.json and summary.json from run directories (searched
recursively under each argument). One row per run and repeat.

  python3 scripts/pilot_table.py SESSION/runs
  python3 scripts/pilot_table.py --suggest-rate SESSION/runs/closed-direct

Units: latencies in ms (first text = dispatch to first nonempty text event,
success-only population unless marked penalised); throughput and goodput in
successful requests per second over the repeat's measurement window.
The suggested rate is 1.25 x the highest closed-loop success throughput among
points with success rate >= 99%, no drops and success first-text p95 within the
run's objective (BUILD_PLAN.md section 6). It is a pilot starting point only.
"""
import argparse
import json
import math
import sys
from pathlib import Path


def value(number):
    if number is None:
        return None
    if number.get('unbounded'):
        return math.inf
    v = number.get('value')
    return None if v is None else float(v)


def rows(paths):
    found = []
    for root in paths:
        for manifest_path in sorted(Path(root).rglob('manifest.json')):
            run = manifest_path.parent
            summary_path = run / 'summary.json'
            if not summary_path.exists():
                continue
            manifest = json.loads(manifest_path.read_text())
            summary = json.loads(summary_path.read_text())
            config = manifest['config']
            for repeat in summary['repeats']:
                counts, rates, metrics = repeat['counts'], repeat['rates'], repeat['offered']['metrics_ms']
                window_s = repeat['measurement_window_ms'] / 1000 if repeat['measurement_window_ms'] else None
                found.append({
                    'run': run.name, 'target': config['target'], 'mode': config['mode'],
                    'workers': config['workers'] if config['mode'] == 'closed' else None,
                    'rate': counts['offered'] / window_s if config['mode'] == 'open' and window_s else None,
                    'cap': config.get('gateway_admission_cap'), 'repeat': repeat['repeat'],
                    'complete': repeat['complete'], 'state': summary['state'],
                    'offered': counts['offered'], 'success': counts['success'], 'rejected': counts['rejected'],
                    'failed': counts['failed'], 'dropped': counts['dropped'],
                    'success_rate': value(rates['success_rate']),
                    'throughput': counts['success'] / window_s if window_s else None,
                    'goodput': value(rates['goodput_requests_per_second']),
                    'ft_p50': value(metrics['success_first_text']['p50']),
                    'ft_p95': value(metrics['success_first_text']['p95']),
                    'pen_ft_p95': value(metrics['penalised_scheduled_first_text']['p95']),
                    'done_p95': value(metrics['success_completion']['p95']),
                    'lag_p95': value(repeat['diagnostics']['scheduling_delay_ms']['p95']),
                    'out_tokens_p50': value(repeat['diagnostics']['completion_tokens']['p50']),
                    'length_deviations': counts['output_length_deviation'],
                    'objective_ms': config['first_text_objective_ms'],
                })
    return found


def fmt(v, digits=1):
    if v is None:
        return '-'
    if isinstance(v, float):
        return '∞' if math.isinf(v) else f'{v:.{digits}f}'
    return str(v)


def table(found):
    columns = [('target', 'target'), ('mode', 'mode'), ('workers', 'workers'), ('rate', 'rate/s'), ('cap', 'cap'),
               ('repeat', 'rep'), ('offered', 'offered'), ('success', 'ok'), ('rejected', 'rej'),
               ('failed', 'fail'), ('dropped', 'drop'), ('throughput', 'ok/s'), ('goodput', 'goodput/s'),
               ('ft_p50', 'first-text p50'), ('ft_p95', 'first-text p95'), ('pen_ft_p95', 'penalised sched. p95'),
               ('done_p95', 'completion p95'), ('lag_p95', 'dispatch lag p95'), ('out_tokens_p50', 'out tokens p50'),
               ('length_deviations', 'len dev'), ('state', 'state')]
    lines = ['| ' + ' | '.join(title for _, title in columns) + ' |', '|' + '---|' * len(columns)]
    for row in sorted(found, key=lambda r: (r['target'], r['mode'], r['workers'] or 0, r['rate'] or 0, r['repeat'])):
        digits = {'success_rate': 3, 'rate': 2, 'throughput': 2, 'goodput': 2}
        lines.append('| ' + ' | '.join(fmt(row[key], digits.get(key, 1)) for key, _ in columns) + ' |')
    return '\n'.join(lines)


def suggest_rate(found):
    healthy = [r for r in found if r['mode'] == 'closed' and r['complete'] and r['dropped'] == 0
               and (r['success_rate'] or 0) >= 0.99 and r['ft_p95'] is not None and r['ft_p95'] <= r['objective_ms']
               and r['throughput']]
    if not healthy:
        return None, None
    best = max(healthy, key=lambda r: r['throughput'])
    return round(1.25 * best['throughput'], 2), best


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('paths', nargs='+')
    parser.add_argument('--suggest-rate', action='store_true')
    args = parser.parse_args()
    found = rows(args.paths)
    if not found:
        print('no summarised runs found', file=sys.stderr)
        return 1
    if args.suggest_rate:
        rate, best = suggest_rate(found)
        if rate is None:
            print('no closed-loop point met the objective with >=99% success; choose a rate manually', file=sys.stderr)
            return 1
        print(f"basis: {best['target']} workers={best['workers']} throughput={best['throughput']:.2f}/s "
              f"first-text p95={best['ft_p95']:.1f} ms", file=sys.stderr)
        print(rate)
        return 0
    print(table(found))
    return 0


if __name__ == '__main__':
    sys.exit(main())
