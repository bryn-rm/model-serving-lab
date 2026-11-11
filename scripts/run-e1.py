#!/usr/bin/env python3
"""CPU-only synthetic E1; raw run evidence stays under results/ (ignored)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import time
import urllib.request


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def cpu(pid):
    # Linux /proc ticks: coarse process user+system CPU, not utilisation.
    fields = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def ready(url):
    for _ in range(100):
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(.05)
    raise RuntimeError(f'not ready: {url}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='results/e1-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))
    parser.add_argument('--attempts', type=int, default=2000)
    args = parser.parse_args()
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    # Generate ephemeral credentials; never store their values.
    env['LAB_GATEWAY_API_KEY'] = os.urandom(24).hex()
    env['LAB_UPSTREAM_API_KEY'] = os.urandom(24).hex()
    mock, public, admin = port(), port(), port()
    upstream = f'http://127.0.0.1:{mock}'
    gateway = f'http://127.0.0.1:{public}'
    admin_url = f'http://127.0.0.1:{admin}'
    subprocess.run(['./bin/lab', 'prepare', '--synthetic', '--output', str(root / 'inputs'),
                    '--attempts', str(args.attempts), '--rate', '100'], check=True, env=env)
    metadata = {'synthetic': True, 'topology': 'native loopback; generator, mock and gateway on same CPU host',
                'mock_delay': '1ms per fixture event', 'fixture': 'testdata/streams/synthetic-success.sse',
                'fixture_sha256': hashlib.sha256(Path('testdata/streams/synthetic-success.sse').read_bytes()).hexdigest(),
                'model': 'synthetic fixture; no inference', 'generation_config': 'not applicable',
                'output_tokens': 'invented 6, regardless of requested 128'}
    (root / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    commands = [
        ['./bin/mockllm', '--addr', f'127.0.0.1:{mock}', '--delay', '1ms'],
        ['./bin/gateway', '--addr', f'127.0.0.1:{public}', '--admin-addr', f'127.0.0.1:{admin}',
         '--upstream', upstream, '--admission-cap', '0'],
    ]
    processes = []
    logs = []
    runs = []
    try:
        for name, command in zip(('mock', 'gateway'), commands):
            log = (root / (name + '.log')).open('w')
            logs.append(log)
            processes.append(subprocess.Popen(command, stdout=log, stderr=log, env=env))
        ready(upstream + '/health')
        ready(admin_url + '/readyz')
        for workers, target in ((1, 'mock-direct'), (1, 'mock-gateway'), (32, 'mock-gateway'), (32, 'mock-direct')):
            direct = target == 'mock-direct'
            before = [cpu(p.pid) for p in processes]
            child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            command = ['./bin/lab', 'bench', '--mode', 'closed', '--target', target,
                       '--url', (upstream if direct else gateway) + '/v1/chat/completions',
                       '--api-key-env', 'LAB_UPSTREAM_API_KEY' if direct else 'LAB_GATEWAY_API_KEY',
                       '--workload', str(root / 'inputs/workload.json'), '--workers', str(workers),
                       '--max-inflight', '256', '--attempts', str(args.attempts), '--repeats', '3',
                       '--warmup', '32', '--output', str(root / 'runs'), '--gateway-admission-cap', '0',
                       '--environment', str(root / 'environment.json')]
            if not direct:
                command += ['--metrics-url', admin_url + '/metrics']
            print(f'Running synthetic E1: {target} concurrency={workers}', flush=True)
            began = time.monotonic()
            result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, env=env)
            elapsed = time.monotonic() - began
            run = result.stdout.strip()
            subprocess.run(['./bin/lab', 'summarize', '--run', run, '--output', run + '/summary.offline.json'], check=True)
            after = [cpu(p.pid) for p in processes]
            child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
            runs.append({'workers': workers, 'target': target, 'run': run, 'elapsed_seconds': elapsed,
                         'cpu_seconds_mock': after[0] - before[0], 'cpu_seconds_gateway': after[1] - before[1],
                         'cpu_seconds_generator_and_offline_summary': (child_after.ru_utime + child_after.ru_stime -
                                                                      child_before.ru_utime - child_before.ru_stime)})
            (root / 'e1-index.json').write_text(json.dumps({'synthetic': True, 'runs': runs}, indent=2) + '\n')
    finally:
        for process in reversed(processes):
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=22)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
    print(root)


if __name__ == '__main__':
    main()
