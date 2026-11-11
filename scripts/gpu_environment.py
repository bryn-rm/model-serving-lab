#!/usr/bin/env python3
"""Snapshot the effective GPU-stack settings for `lab bench --environment`.

Combines the pinned/requested settings (configs/gpu-pilot.env) with evidence
from the running stack: container image digest, GPU/driver, vLLM version,
/v1/models max_model_len and the vllm:cache_config_info metric labels. Exits
nonzero on missing evidence or when running arguments/metrics contradict the
declared condition, so a run is never labelled
with settings it did not use. Credentials are read from the environment and
never written.

  python3 scripts/gpu_environment.py --output SESSION/environment.json
"""
import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request

# Keys that `lab bench` pre-populates as null; all must be filled here.
BENCH_KEYS = ('image_tag', 'image_digest', 'model_revision', 'tokenizer_revision', 'gpu', 'driver',
              'topology', 'max_num_seqs', 'max_num_batched_tokens', 'gpu_memory_utilization',
              'max_model_len', 'dtype', 'kv_cache_dtype', 'cache_block_settings', 'chunked_prefill',
              'prefix_caching', 'native_rejection_controls')
TOPOLOGY = ('generator on GPU host; direct arm -> 127.0.0.1:8000 (Docker-published vLLM); '
            'gateway arm -> 127.0.0.1:8080 (Docker-published gateway) -> http://vllm:8000 '
            '(Compose bridge network); HTTP/1.1 throughout')


def read_env_file(path):
    """Parse the plain KEY=VALUE pin file (comments and blank lines ignored)."""
    values = {}
    with open(path, encoding='utf-8') as handle:
        for number, line in enumerate(handle, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, sep, value = line.partition('=')
            if not sep or not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
                raise ValueError(f'{path}:{number}: expected KEY=VALUE')
            values[key] = value
    return values


def metric_labels(text, name):
    """Return the label dict of the first sample of metric `name`, or None."""
    for line in text.splitlines():
        if line.startswith(name + '{'):
            body = line[len(name) + 1:line.rindex('}')]
            return dict(re.findall(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"', body))
    return None


def expected_commands(pins):
    """Explicit deployment contract; never inspect or persist container env."""
    engine = [pins['MODEL_ID']]
    for flag, key in (
        ('revision', 'MODEL_REVISION'), ('tokenizer-revision', 'MODEL_REVISION'),
        ('served-model-name', 'SERVED_MODEL_NAME'), ('dtype', 'DTYPE'),
        ('kv-cache-dtype', 'KV_CACHE_DTYPE'), ('max-model-len', 'MAX_MODEL_LEN'),
        ('gpu-memory-utilization', 'GPU_MEMORY_UTILIZATION'), ('max-num-seqs', 'MAX_NUM_SEQS'),
        ('max-num-batched-tokens', 'MAX_NUM_BATCHED_TOKENS'), ('generation-config', 'GENERATION_CONFIG')):
        engine.append(f'--{flag}={pins[key]}')
    engine += [pins['CHUNKED_PREFILL_FLAG'], pins['PREFIX_CACHING_FLAG'], '--host=0.0.0.0', '--port=8000']
    gateway = ['--addr=0.0.0.0:8080', '--admin-addr=0.0.0.0:8081', '--upstream=http://vllm:8000']
    for flag, key in (
        ('model', 'SERVED_MODEL_NAME'), ('admission-cap', 'LAB_ADMISSION_CAP'),
        ('first-text-timeout', 'GATEWAY_FIRST_TEXT_TIMEOUT'), ('idle-timeout', 'GATEWAY_IDLE_TIMEOUT'),
        ('timeout', 'GATEWAY_TOTAL_TIMEOUT'), ('max-output-tokens', 'GATEWAY_MAX_OUTPUT_TOKENS')):
        gateway.append(f'--{flag}={pins[key]}')
    return {'vllm': (['vllm', 'serve'], engine), 'gateway': (['/gateway'], gateway)}


def command_problems(service, entrypoint, command, expected):
    """Compare exact arguments, allowing order changes; redact all actual values.

    Extra and duplicate arguments fail too, since later flags can override pins.
    """
    expected_entrypoint, expected_command = expected
    problems = []
    if entrypoint != expected_entrypoint:
        problems.append(f'{service}: running entrypoint differs from deployment contract')
    if not isinstance(command, list) or sorted(command) != sorted(expected_command):
        problems.append(f'{service}: running arguments differ from requested settings')
    return problems


def metric_value(text, name):
    samples = [line.split()[1] for line in text.splitlines()
               if line.startswith(name + ' ')]
    if len(samples) != 1:
        return None
    return float(samples[0])


def build(pins, reported):
    """Combine pins with reported evidence; return (environment, mismatches)."""
    cache = reported.get('cache_config_info') or {}
    env = {
        'synthetic': False,
        'model': pins['MODEL_ID'],
        'served_model_name': pins['SERVED_MODEL_NAME'],
        'image_tag': f"{pins['VLLM_IMAGE_REPO']}:{pins['VLLM_IMAGE_TAG']}",
        'image_digest': pins['VLLM_IMAGE_DIGEST'] or None,
        'model_revision': pins['MODEL_REVISION'] or None,
        'tokenizer_revision': pins['MODEL_REVISION'] or None,
        'gpu': reported.get('gpu'),
        'driver': reported.get('driver'),
        'topology': TOPOLOGY,
        'max_num_seqs': int(pins['MAX_NUM_SEQS']),
        'max_num_batched_tokens': int(pins['MAX_NUM_BATCHED_TOKENS']),
        'gpu_memory_utilization': float(pins['GPU_MEMORY_UTILIZATION']),
        'max_model_len': int(pins['MAX_MODEL_LEN']),
        'dtype': pins['DTYPE'],
        'kv_cache_dtype': pins['KV_CACHE_DTYPE'],
        'cache_block_settings': {k: cache.get(k) for k in ('block_size', 'num_gpu_blocks', 'cache_dtype')} if cache else None,
        'chunked_prefill': pins['CHUNKED_PREFILL_FLAG'] == '--enable-chunked-prefill',
        'prefix_caching': False if pins['PREFIX_CACHING_FLAG'] in ('--no-enable-prefix-caching', '--disable-prefix-caching') else None,
        'native_rejection_controls': 'none configured by this deployment; engine queues beyond max_num_seqs',
        'generation_config': pins['GENERATION_CONFIG'],
        'engine_flags': {'chunked_prefill': pins['CHUNKED_PREFILL_FLAG'], 'prefix_caching': pins['PREFIX_CACHING_FLAG']},
        'gateway': {'admission_cap': int(pins['LAB_ADMISSION_CAP']),
                    'first_text_timeout': pins['GATEWAY_FIRST_TEXT_TIMEOUT'],
                    'idle_timeout': pins['GATEWAY_IDLE_TIMEOUT'],
                    'total_timeout': pins['GATEWAY_TOTAL_TIMEOUT'],
                    'max_output_tokens': int(pins['GATEWAY_MAX_OUTPUT_TOKENS'])},
        'engine_reported': reported,
    }
    mismatches = []

    def check(name, expected, actual):
        if actual is None:
            mismatches.append(f'{name}: running evidence unavailable')
        elif str(actual) != str(expected):
            mismatches.append(f'{name}: pinned {expected!r}, running {actual!r}')

    for key in ('image_digest', 'model_revision'):
        if env[key] is None:
            mismatches.append(f'{key} is not pinned; run scripts/gpu-preflight.sh --write')
    check('image_digest', env['image_digest'], reported.get('running_image_digest'))
    check('max_model_len', env['max_model_len'], reported.get('models_max_model_len'))
    check('served_model_name', env['served_model_name'], reported.get('models_id'))
    check('enable_prefix_caching', 'false', str(cache.get('enable_prefix_caching', '')).lower() or None)
    memory = cache.get('gpu_memory_utilization')
    check('gpu_memory_utilization', env['gpu_memory_utilization'], float(memory) if memory is not None else None)
    check('gateway admission_cap', float(env['gateway']['admission_cap']), reported.get('gateway_admission_cap'))
    for key in ('block_size', 'num_gpu_blocks', 'cache_dtype'):
        if cache.get(key) is None:
            mismatches.append(f'cache_config_info.{key}: running evidence unavailable')
    if reported.get('command_checks') is None:
        mismatches.append('running container argument checks unavailable')
    else:
        mismatches.extend(reported['command_checks'])
    return env, mismatches


def fetch(url, key=None):
    request = urllib.request.Request(url)
    if key:
        request.add_header('Authorization', 'Bearer ' + key)
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode()


def run(command):
    return subprocess.run(command, check=True, text=True, capture_output=True).stdout.strip()


def collect(pins, engine, admin, compose):
    reported = {}
    gpu = run(['nvidia-smi', '--query-gpu=name,driver_version,memory.total', '--format=csv,noheader']).splitlines()[0]
    name, driver, memory = (part.strip() for part in gpu.split(','))
    reported.update(gpu=f'{name} ({memory})', driver=driver)
    container = run(compose + ['ps', '-q', 'vllm'])
    if not container:
        raise RuntimeError('vllm container is not running')
    image_id = run(['docker', 'inspect', '--format', '{{.Image}}', container])
    digests = json.loads(run(['docker', 'image', 'inspect', '--format', '{{json .RepoDigests}}', image_id]))
    repo = pins['VLLM_IMAGE_REPO'] + '@'
    reported['running_image_digest'] = next((d[len(repo):] for d in digests if d.startswith(repo)), None)
    try:
        reported['vllm_version'] = json.loads(fetch(engine + '/version')).get('version')
    except OSError:
        reported['vllm_version'] = None
    models = json.loads(fetch(engine + '/v1/models', os.environ.get('LAB_UPSTREAM_API_KEY')))['data']
    reported['models_id'] = models[0].get('id') if models else None
    reported['models_root'] = models[0].get('root') if models else None
    reported['models_max_model_len'] = models[0].get('max_model_len') if models else None
    reported['cache_config_info'] = metric_labels(fetch(engine + '/metrics'), 'vllm:cache_config_info')
    reported['gateway_admission_cap'] = metric_value(fetch(admin + '/metrics'), 'lab_gateway_admission_cap')
    reported['command_checks'] = []
    for service, expected in expected_commands(pins).items():
        service_container = run(compose + ['ps', '-q', service])
        if not service_container:
            raise RuntimeError(f'{service} container is not running')
        # Read only entrypoint and argv, never Config.Env (contains credentials).
        entrypoint = json.loads(run(['docker', 'inspect', '--format', '{{json .Config.Entrypoint}}', service_container]))
        command = json.loads(run(['docker', 'inspect', '--format', '{{json .Config.Cmd}}', service_container]))
        reported['command_checks'].extend(command_problems(service, entrypoint, command, expected))
    return reported


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--env-file', default='configs/gpu-pilot.env')
    parser.add_argument('--engine', default='http://127.0.0.1:8000')
    parser.add_argument('--admin', default='http://127.0.0.1:8081')
    parser.add_argument('--admission-cap', type=int, help='expected gateway cap override for this condition')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    pins = read_env_file(args.env_file)
    if args.admission_cap is not None:
        if args.admission_cap < 0:
            parser.error('--admission-cap must be nonnegative')
        pins['LAB_ADMISSION_CAP'] = str(args.admission_cap)
    compose = ['docker', 'compose', '--env-file', args.env_file, '-f', 'deploy/compose.gpu.yaml']
    env, mismatches = build(pins, collect(pins, args.engine.rstrip('/'), args.admin.rstrip('/'), compose))
    env['consistency_mismatches'] = mismatches
    with open(args.output, 'w', encoding='utf-8') as handle:
        json.dump(env, handle, indent=2)
        handle.write('\n')
    missing = [key for key in BENCH_KEYS if env.get(key) is None]
    for problem in mismatches + [f'{key} unavailable' for key in missing]:
        print('environment:', problem, file=sys.stderr)
    if mismatches or missing:
        return 1
    print(args.output)
    return 0


if __name__ == '__main__':
    sys.exit(main())
