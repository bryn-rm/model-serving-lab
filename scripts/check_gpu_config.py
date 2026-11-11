#!/usr/bin/env python3
"""Validate the GPU deployment config without a GPU.

Checks configs/gpu-pilot.env for valid pilot settings and consistency with
configs/environment.json, then renders deploy/compose.gpu.yaml with placeholder
credentials/pins and checks the resolved engine and gateway arguments, loopback
port bindings and credential handling. Exits nonzero on any problem.
"""
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gpu_environment import read_env_file  # noqa: E402

ENV_FILE = 'configs/gpu-pilot.env'
COMPOSE = 'deploy/compose.gpu.yaml'
PLAN_DEFAULTS = {'MODEL_ID': 'Qwen/Qwen2.5-7B-Instruct', 'SERVED_MODEL_NAME': 'lab-model', 'DTYPE': 'auto',
                 'MAX_MODEL_LEN': '4096', 'GPU_MEMORY_UTILIZATION': '0.90', 'MAX_NUM_SEQS': '64',
                 'MAX_NUM_BATCHED_TOKENS': '4096', 'CHUNKED_PREFILL_FLAG': '--enable-chunked-prefill',
                 'GENERATION_CONFIG': 'vllm', 'GATEWAY_FIRST_TEXT_TIMEOUT': '120s',
                 'GATEWAY_IDLE_TIMEOUT': '120s', 'GATEWAY_TOTAL_TIMEOUT': '300s'}
PLACEHOLDERS = {'LAB_UPSTREAM_API_KEY': 'placeholder-upstream-credential',
                'LAB_GATEWAY_API_KEY': 'placeholder-gateway-credential'}


def check_pins(pins, candidate):
    adjustable = {'MAX_NUM_SEQS', 'MAX_NUM_BATCHED_TOKENS', 'GPU_MEMORY_UTILIZATION'}
    problems = [f'{k}={pins.get(k)!r}, required {v!r}' for k, v in PLAN_DEFAULTS.items()
                if k not in adjustable and pins.get(k) != v]
    for key in ('MAX_NUM_SEQS', 'MAX_NUM_BATCHED_TOKENS'):
        if not re.fullmatch(r'[1-9][0-9]*', pins.get(key, '')):
            problems.append(f'{key} must be a positive integer')
    try:
        memory = float(pins.get('GPU_MEMORY_UTILIZATION', ''))
        if not 0 < memory <= 1:
            raise ValueError()
    except ValueError:
        problems.append('GPU_MEMORY_UTILIZATION must be in (0, 1]')
    if problems:
        return problems

    if pins.get('PREFIX_CACHING_FLAG') not in ('--no-enable-prefix-caching', '--disable-prefix-caching'):
        problems.append('PREFIX_CACHING_FLAG must disable prefix caching')
    if pins.get('VLLM_IMAGE_DIGEST') and not re.fullmatch(r'sha256:[0-9a-f]{64}', pins['VLLM_IMAGE_DIGEST']):
        problems.append('VLLM_IMAGE_DIGEST must be sha256:<64 hex>')
    if pins.get('MODEL_REVISION') and not re.fullmatch(r'[0-9a-f]{40}', pins['MODEL_REVISION']):
        problems.append('MODEL_REVISION must be a 40-hex commit')
    if not re.fullmatch(r'\d+', pins.get('LAB_ADMISSION_CAP', '')):
        problems.append('LAB_ADMISSION_CAP must be a nonnegative integer')
    expected = {'dtype': pins.get('DTYPE'), 'max_model_len': int(pins.get('MAX_MODEL_LEN', 0)),
                'gpu_memory_utilization': float(pins.get('GPU_MEMORY_UTILIZATION', 0)),
                'max_num_seqs': int(pins.get('MAX_NUM_SEQS', 0)),
                'max_num_batched_tokens': int(pins.get('MAX_NUM_BATCHED_TOKENS', 0)),
                'chunked_prefill': True, 'prefix_caching': False, 'generation_config': pins.get('GENERATION_CONFIG')}
    problems += [f'configs/environment.json scheduler_candidate.{k}={candidate.get(k)!r} != {v!r}'
                 for k, v in expected.items() if candidate.get(k) != v]
    if candidate.get('model') not in (None, pins.get('MODEL_ID')):
        problems.append('configs/environment.json model differs from the pin file')
    return problems


def check_compose(rendered, pins, compose_text):
    problems = []
    services = rendered['services']
    vllm, gateway = services['vllm'], services['gateway']
    command = vllm['command']
    expected = [pins['MODEL_ID'], f"--revision={pins['MODEL_REVISION']}", f"--tokenizer-revision={pins['MODEL_REVISION']}",
                f"--served-model-name={pins['SERVED_MODEL_NAME']}", f"--dtype={pins['DTYPE']}",
                f"--kv-cache-dtype={pins['KV_CACHE_DTYPE']}", f"--max-model-len={pins['MAX_MODEL_LEN']}",
                f"--gpu-memory-utilization={pins['GPU_MEMORY_UTILIZATION']}", f"--max-num-seqs={pins['MAX_NUM_SEQS']}",
                f"--max-num-batched-tokens={pins['MAX_NUM_BATCHED_TOKENS']}", pins['CHUNKED_PREFILL_FLAG'],
                pins['PREFIX_CACHING_FLAG'], f"--generation-config={pins['GENERATION_CONFIG']}"]
    problems += [f'vllm command missing {arg}' for arg in expected if arg not in command]
    if command[0] != pins['MODEL_ID']:
        problems.append('vllm command must start with the model')
    if vllm.get('entrypoint') != ['vllm', 'serve']:
        problems.append('vllm entrypoint must be vllm serve')
    if not vllm['image'].endswith('@' + pins['VLLM_IMAGE_DIGEST']):
        problems.append('vllm image must be pinned by digest')
    if vllm['environment'].get('VLLM_API_KEY') != PLACEHOLDERS['LAB_UPSTREAM_API_KEY']:
        problems.append('vllm must take VLLM_API_KEY from LAB_UPSTREAM_API_KEY')
    if any('api-key' in arg or 'api_key' in arg for arg in command):
        problems.append('credentials must not be passed in vllm argv')
    gateway_command = gateway['command']
    for arg in ('--upstream=http://vllm:8000', f"--model={pins['SERVED_MODEL_NAME']}",
                f"--admission-cap={pins['LAB_ADMISSION_CAP']}",
                f"--first-text-timeout={pins['GATEWAY_FIRST_TEXT_TIMEOUT']}",
                f"--idle-timeout={pins['GATEWAY_IDLE_TIMEOUT']}", f"--timeout={pins['GATEWAY_TOTAL_TIMEOUT']}"):
        if arg not in gateway_command:
            problems.append(f'gateway command missing {arg}')
    for name, key in (('gateway', 'LAB_GATEWAY_API_KEY'), ('gateway', 'LAB_UPSTREAM_API_KEY')):
        if services[name]['environment'].get(key) != PLACEHOLDERS[key]:
            problems.append(f'{name} must read {key} from the shell environment')
    published = {}
    for name, service in services.items():
        for port in service.get('ports', []):
            published[int(port['published'])] = name
            if port.get('host_ip') != '127.0.0.1':
                problems.append(f'{name} port {port["published"]} is not bound to 127.0.0.1')
    if published != {8000: 'vllm', 8080: 'gateway', 8081: 'gateway'}:
        problems.append(f'unexpected published ports {published}')
    if any(value in compose_text for value in PLACEHOLDERS.values()):
        problems.append('compose file contains a literal credential')
    return problems


def main():
    pins = read_env_file(ENV_FILE)
    candidate = json.loads(Path('configs/environment.json').read_text()).get('scheduler_candidate', {})
    problems = check_pins(pins, candidate)
    render_pins = dict(pins)
    render_pins['VLLM_IMAGE_DIGEST'] = pins['VLLM_IMAGE_DIGEST'] or 'sha256:' + '0' * 64
    render_pins['MODEL_REVISION'] = pins['MODEL_REVISION'] or '0' * 40
    env = {**PLACEHOLDERS, 'VLLM_IMAGE_DIGEST': render_pins['VLLM_IMAGE_DIGEST'],
           'MODEL_REVISION': render_pins['MODEL_REVISION'], 'PATH': '/usr/bin:/bin:/usr/local/bin', 'HOME': str(Path.home())}
    try:
        result = subprocess.run(['docker', 'compose', '--env-file', ENV_FILE, '-f', COMPOSE, 'config', '--format', 'json'],
                                env=env, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        print('BLOCKED: docker compose is not installed; compose rendering not checked', file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr)
        return 1
    problems += check_compose(json.loads(result.stdout), render_pins, Path(COMPOSE).read_text())
    for problem in problems:
        print('CONFIG PROBLEM:', problem, file=sys.stderr)
    if problems:
        return 1
    pinned = 'pinned' if pins['VLLM_IMAGE_DIGEST'] and pins['MODEL_REVISION'] else 'NOT YET PINNED (run gpu-preflight.sh --write on the GPU host)'
    print(f'GPU config valid: resolved settings, loopback ports, env-only credentials; image/model {pinned}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
