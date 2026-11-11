#!/usr/bin/env python3
"""Regenerate the synthetic E1 report from durable run evidence."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--report', default='docs/e1-mock.md')
    parser.add_argument('--extract', default='docs/e1-mock-summary.json')
    args = parser.parse_args()
    root = Path(args.root)
    index = json.loads((root / 'e1-index.json').read_text())
    report = {'synthetic': True, 'source': str(root / 'e1-index.json'),
              'platform': platform.platform(), 'logical_cpus': os.cpu_count(),
              'affinity_cpus': len(os.sched_getaffinity(0)), 'conditions': []}
    for item in index['runs']:
        path = Path(item['run'])
        summary = json.loads((path / 'summary.json').read_text())
        offline = json.loads((path / 'summary.offline.json').read_text())
        if summary != offline:
            raise ValueError('offline summary does not match final summary')
        manifest = json.loads((path / 'manifest.json').read_text())
        workload = json.loads((path / 'workload.json').read_text())
        condition = {**item, 'run_id': manifest['run_id'], 'git_sha': manifest['git_sha'],
                     'git_dirty': manifest['git_dirty'], 'started_utc': manifest['started_utc'], 'body_sha256': workload['prompts'][0]['sha256'],
                     'transport': manifest['transport'], 'repeats': [], 'aggregate': {}}
        for repeat in summary['repeats']:
            entry = {'repeat': repeat['repeat'], 'counts': repeat['counts']}
            for metric in ('first_byte', 'first_text', 'completion'):
                for percentile in ('p50', 'p95'):
                    entry[metric + '_' + percentile + '_ms'] = repeat['offered']['metrics_ms']['success_' + metric][percentile]['value']
            entry['client_reuse_fraction'] = repeat['rates']['connection_reuse_fraction']['value']
            entry['scheduling_delay_p95_ms'] = repeat['diagnostics']['scheduling_delay_ms']['p95']['value']
            if item['target'] == 'mock-gateway':
                def counts(which):
                    text = (path / f'repeat-{repeat["repeat"]}-{which}.prom').read_text()
                    return dict((key, float(value)) for key, value in re.findall(
                        r'lab_gateway_upstream_connections_total\{reused="(true|false)"\} ([\d.e+]+)', text))
                before, after = counts('before'), counts('after')
                entry['upstream_connection_reused'] = int(after.get('true', 0) - before.get('true', 0))
                entry['upstream_connection_new'] = int(after.get('false', 0) - before.get('false', 0))
            condition['repeats'].append(entry)
        for metric in ('first_byte_p50_ms', 'first_byte_p95_ms', 'first_text_p50_ms', 'first_text_p95_ms', 'completion_p95_ms'):
            values = [repeat[metric] for repeat in condition['repeats']]
            condition['aggregate'][metric] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
        report['conditions'].append(condition)
    if len({condition['body_sha256'] for condition in report['conditions']}) != 1:
        raise ValueError('request bodies differ across arms')
    if any(repeat['counts']['success'] != repeat['counts']['offered'] for condition in report['conditions'] for repeat in condition['repeats']):
        raise ValueError('this success-only E1 report template requires every attempt to succeed; inspect failures')
    try:
        report['cgroup_cpu_max'] = Path('/sys/fs/cgroup/cpu.max').read_text().strip()
    except OSError:
        report['cgroup_cpu_max'] = None
    Path(args.extract).write_text(json.dumps(report, indent=2) + '\n')
    total = sum(repeat['counts']['offered'] for condition in report['conditions'] for repeat in condition['repeats'])
    rows = []
    for condition in report['conditions']:
        cells = []
        for metric in ('first_byte_p50_ms', 'first_byte_p95_ms', 'first_text_p50_ms', 'first_text_p95_ms'):
            aggregate = condition['aggregate'][metric]
            cells.append(f'{aggregate["median"]:.3f} [{aggregate["min"]:.3f}, {aggregate["max"]:.3f}]')
        rows.append(f'| {condition["target"]} | {condition["workers"]} | ' + ' | '.join(cells) + ' |')
    connections = []
    for condition in report['conditions']:
        reuse = ', '.join(f'{repeat["client_reuse_fraction"] * 100:.2f}%' for repeat in condition['repeats'])
        upstream = ', '.join(f'{repeat["upstream_connection_reused"]}/{repeat["upstream_connection_new"]}' for repeat in condition['repeats']) if condition['target'] == 'mock-gateway' else 'not applicable'
        connections.append(f'| {condition["target"]} | {condition["workers"]} | {reuse} | {upstream} | {condition["cpu_seconds_mock"]:.2f} | {condition["cpu_seconds_gateway"]:.2f} | {condition["cpu_seconds_generator_and_offline_summary"]:.2f} |')
    differences = []
    for workers in (1, 32):
        direct = next(condition for condition in report['conditions'] if condition['target'] == 'mock-direct' and condition['workers'] == workers)
        gateway = next(condition for condition in report['conditions'] if condition['target'] == 'mock-gateway' and condition['workers'] == workers)
        delta = {metric: gateway['aggregate'][metric]['median'] - direct['aggregate'][metric]['median'] for metric in ('first_byte_p50_ms', 'first_text_p50_ms')}
        tail = gateway['aggregate']['first_text_p95_ms']
        direct_tail = direct['aggregate']['first_text_p95_ms']
        differences.append(f'At concurrency {workers}, the difference between arm medians of repeat first-text p50 was {delta["first_text_p50_ms"]:+.3f} ms; first-byte p50 differed by {delta["first_byte_p50_ms"]:+.3f} ms. Gateway first-text p95 ranged {tail["min"]:.3f}–{tail["max"]:.3f} ms, versus {direct_tail["min"]:.3f}–{direct_tail["max"]:.3f} ms direct.')
    attempts = report['conditions'][0]['repeats'][0]['counts']['offered']
    executed_date = report['conditions'][0]['started_utc'][:10]
    report_text = f'''# E1: synthetic mock direct versus gateway

Executed {executed_date} on the native CPU Codespace; no inference or GPU
measurements. Raw records, manifests, snapshots, metric captures and offline
summaries are in `{root}/` (intentionally ignored). The repository evidence
extract is [e1-mock-summary.json](e1-mock-summary.json).

## Conditions

- {attempts:,} offered attempts per repeat; three repeats per target/concurrency:
  {total:,} measured attempts, plus 32 sequential warm-ups before each repeat.
- Identical body SHA-256: `{report['conditions'][0]['body_sha256']}`.
  Temperature 0, top_p 1, seed 42, ignore_eos true, requested 128 output tokens.
- `synthetic-success.sse`, 1 ms delay before each fixture event. Usage is invented
  (12 prompt/6 completion tokens); all attempts flag output-length deviation.
- Generator HTTP/1.1, 512 total/per-host idle connections, unlimited transport
  active connections; safety ceiling 256; gateway cap 0. Both endpoints require
  separate ephemeral bearer credentials. Defaults: 120 s first text/idle, 300 s
  total; drain 10 s, dispatch limit 10 min. No retries/redirects.
- Native loopback on one shared CPU host. Gateway logs enabled; each request
  record is synchronously persisted. No agent builds or tests ran alongside
  the final measured run; external host activity remains uncontrolled.
- Run order: direct/1, gateway/1, gateway/32, direct/32. Sequential warm-ups do
  not prewarm a 32-connection pool; repeats retain each process transport pool.
- The working tree was dirty; manifests record its base source SHA and settings.
  No native engine metrics exist on this mock; status files say so. Gateway
  `/metrics` captures supply the upstream connection deltas below.

## Observations

All {total:,} attempts succeeded, with no rejection, drop, failure or missing record.
Usage coverage was 100%; gateway admission was true for every gateway response.
Direct admission remained unknown (no gateway admission evidence). All successes
met the 1,000 ms first-text objective. Timing values below are milliseconds from
dispatch to the first response-header byte or first nonempty text event.

Each cell is **median of three repeat statistics [min, max]**; attempts are not pooled.

| Target | Workers | First-byte p50 | First-byte p95 | First-text p50 | First-text p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
{chr(10).join(rows)}

{(chr(10) + chr(10)).join(differences)}

These are differences of statistics from sequential arms, not paired per-request
causal effects. Repeat spread and concurrent tails matter; a single median is
not a sufficient description of gateway overhead. The small-overhead prediction
must be assessed alongside this variation; there is no GPU confirmation yet.

## Connection and CPU evidence

| Target | Workers | Client reuse per repeat | Upstream reused/new per repeat | Mock CPU s | Gateway CPU s | Generator + offline summary CPU s |
| --- | ---: | --- | --- | ---: | ---: | ---: |
{chr(10).join(connections)}

CPU values are accumulated process user+system seconds over all three repeats,
warm-up, metric captures and summary work, not CPU percentages or per-request
latencies. Mock/gateway use Linux `/proc` ticks; generator/summary use child
resource usage. Gateway rows include validation, relay, logs and metrics work.
No model-efficiency claim follows from mock CPU differences.

Sequential warm-up means the first concurrent repeat grows its connection pool.
Finite 5 ms client EOF cleanup can close late responses. Inspect both client
reuse and gateway upstream reuse rather than assuming the pools eliminate churn.
Transport/retry settings match across arms. Closed-loop workers also wait on the
bounded record writer between attempts: observed throughput is not engine capacity.

This is a synthetic harness regression/overhead observation on a shared CPU
host. It has no real tokenizer, scheduler, cache, fixed-length generation or GPU
evidence. Arm order, host noise, timer granularity, fsync backpressure and
sequential warm-up limit causal interpretation. GPU confirmation and calibrated
output lengths remain Phase 4 work. All offline summaries matched final summaries.

## Reproduce

```sh
make build
python3 scripts/run-e1.py
# Supply the root printed by the runner to regenerate this report:
python3 scripts/report-e1.py --root {root}
```

The runner saves a unique root with per-run evidence, CPU counters and
`e1-index.json`, invokes offline summarisation, and stops its servers. The
included extract belongs to the recorded run; a future rerun produces new data.
'''
    Path(args.report).write_text(report_text)


if __name__ == '__main__':
    main()
