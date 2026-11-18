"""Offline tests for the Phase 4 GPU helper scripts (synthetic inputs only).

Run: python3 -I -m unittest discover -s scripts -p 'test_*.py'
"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import check_gpu_config
import gpu_capture
import gpu_environment
import pilot_table

# vLLM-shaped (synthetic) stream: role-only delta, CRLF framing, a comment,
# text, finish reason, usage-only chunk and [DONE].
STREAM = (b'data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1762862400,'
          b'"model":"lab-model","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}\r\n\r\n'
          b': keep-alive\r\n\r\n'
          b'data: {"id":"chatcmpl-abc123","created":1762862400,"choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}\r\n\r\n'
          b'data: {"id":"chatcmpl-abc123","created":1762862400,"choices":[{"index":0,"delta":{"content":" there"},"finish_reason":"length"}]}\r\n\r\n'
          b'data: {"id":"chatcmpl-abc123","created":1762862400,"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":2,"total_tokens":22}}\r\n\r\n'
          b'data: [DONE]\r\n\r\n')


class CaptureTests(unittest.TestCase):
    def test_verify_success_and_usage(self):
        summary, problems = gpu_capture.verify(STREAM, 2)
        self.assertEqual(problems, [])
        self.assertEqual((summary['text_events'], summary['prompt_tokens'], summary['finish_reason']), (2, 20, 'length'))
        _, problems = gpu_capture.verify(STREAM, 128)
        self.assertIn('completion_tokens 2 != requested 128', problems)

    def test_verify_failures(self):
        truncated = STREAM.replace(b'data: [DONE]\r\n\r\n', b'')
        self.assertIn('no [DONE] (truncated stream)', gpu_capture.verify(truncated, None)[1])
        no_usage = b'data: {"choices":[{"delta":{"content":"x"}}]}\n\ndata: [DONE]\n\n'
        self.assertIn('no usage chunk (stream_options.include_usage)', gpu_capture.verify(no_usage, None)[1])
        for error in (b'data: {"error":{"message":"bad","code":500}}\n\n', b'data: {"object":"error","message":"bad"}\n\n',
                      b'event: error\ndata: boom\n\n'):
            self.assertIn('stream reported an error', gpu_capture.verify(error + b'data: [DONE]\n\n', None)[1])

    def test_sanitize_preserves_framing_and_text(self):
        clean, rules = gpu_capture.sanitize_bytes('direct-success.body', STREAM)
        self.assertNotIn(b'abc123', clean)
        self.assertNotIn(b'1762862400', clean)
        self.assertEqual(clean.count(b'\r\n\r\n'), STREAM.count(b'\r\n\r\n'))
        self.assertIn(b': keep-alive\r\n', clean)
        self.assertEqual(gpu_capture.inspect(clean), gpu_capture.inspect(STREAM))
        self.assertTrue(any('response id' in rule for rule in rules))
        headers = b'HTTP/1.1 200 OK\r\ndate: Tue, 11 Nov 2026 12:00:00 GMT\r\nx-request-id: abc\r\ncontent-type: text/event-stream\r\n\r\n'
        clean, _ = gpu_capture.sanitize_bytes('direct-success.headers', headers)
        self.assertIn(b'date: sanitized\r\n', clean)
        self.assertIn(b'x-request-id: sanitized\r\n', clean)
        self.assertIn(b'content-type: text/event-stream\r\n', clean)

    def test_sanitize_refuses_raw_secret_and_scan_finds_it(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, {'LAB_UPSTREAM_API_KEY': 'synthetic-secret-value'}):
            raw, out = Path(root, 'raw'), Path(root, 'out')
            raw.mkdir()
            (raw / 'ok.body').write_bytes(STREAM)
            provenance = gpu_capture.sanitize(raw, out, gpu_capture.secrets())
            self.assertEqual(provenance['files'][0]['path'], 'ok.body')
            self.assertEqual(gpu_capture.find_secrets([out], gpu_capture.secrets()), [])
            (raw / 'leak.headers').write_bytes(b'x-echo: synthetic-secret-value\n')
            with self.assertRaises(SystemExit):
                gpu_capture.sanitize(raw, Path(root, 'out2'), gpu_capture.secrets())
            self.assertTrue(gpu_capture.find_secrets([root], gpu_capture.secrets()))


PINS = gpu_environment.read_env_file('configs/gpu-pilot.env')


class EnvironmentTests(unittest.TestCase):
    def test_env_file_parses_plan_defaults(self):
        self.assertEqual(PINS['MAX_MODEL_LEN'], '4096')
        self.assertEqual(PINS['SERVED_MODEL_NAME'], 'lab-model')
        self.assertFalse(any('KEY' in key and key.startswith('LAB_') and 'API' in key for key in PINS))

    def test_metric_labels(self):
        text = ('# HELP vllm:cache_config_info info\nvllm:cache_config_info{block_size="16",cache_dtype="auto",'
                'enable_prefix_caching="False",gpu_memory_utilization="0.9",num_gpu_blocks="1234"} 1.0\n')
        labels = gpu_environment.metric_labels(text, 'vllm:cache_config_info')
        self.assertEqual(labels['enable_prefix_caching'], 'False')
        self.assertEqual(labels['num_gpu_blocks'], '1234')
        self.assertIsNone(gpu_environment.metric_labels(text, 'vllm:missing'))

    def test_build_fills_bench_keys_and_detects_contradictions(self):
        pins = dict(PINS, VLLM_IMAGE_DIGEST='sha256:' + 'a' * 64, MODEL_REVISION='b' * 40)
        reported = {'gpu': 'Synthetic GPU (24564 MiB)', 'driver': '999.1', 'running_image_digest': pins['VLLM_IMAGE_DIGEST'],
                    'models_id': 'lab-model', 'models_max_model_len': 4096,
                    'command_checks': [], 'gateway_admission_cap': 0.0,
                    'cache_config_info': {'block_size': '16', 'cache_dtype': 'auto', 'num_gpu_blocks': '1',
                                          'enable_prefix_caching': 'False', 'gpu_memory_utilization': '0.9'}}
        env, mismatches = gpu_environment.build(pins, reported)
        self.assertEqual(mismatches, [])
        self.assertEqual([k for k in gpu_environment.BENCH_KEYS if env[k] is None], [])
        self.assertIs(env['prefix_caching'], False)
        bad = dict(reported, models_max_model_len=32768, running_image_digest='sha256:' + 'c' * 64,
                   cache_config_info=dict(reported['cache_config_info'], enable_prefix_caching='True'))
        _, mismatches = gpu_environment.build(pins, bad)
        self.assertEqual(len(mismatches), 3)
        _, mismatches = gpu_environment.build(dict(PINS, VLLM_IMAGE_DIGEST='', MODEL_REVISION=''), {})
        self.assertTrue(any('not pinned' in item for item in mismatches))
        self.assertTrue(any('unavailable' in item for item in mismatches))

    def test_missing_runtime_evidence_and_cap_override(self):
        pins = dict(PINS, VLLM_IMAGE_DIGEST='sha256:' + 'a' * 64, MODEL_REVISION='b' * 40)
        reported = {'gpu': 'Synthetic GPU', 'driver': '999.1', 'running_image_digest': pins['VLLM_IMAGE_DIGEST'],
                    'models_id': 'lab-model', 'models_max_model_len': 4096, 'command_checks': [],
                    'gateway_admission_cap': 8.0,
                    'cache_config_info': {'block_size': '16', 'num_gpu_blocks': '1', 'cache_dtype': 'auto',
                                          'enable_prefix_caching': 'False', 'gpu_memory_utilization': '0.9'}}
        self.assertTrue(any('admission_cap' in p for p in gpu_environment.build(pins, reported)[1]))
        pins['LAB_ADMISSION_CAP'] = '8'
        env, problems = gpu_environment.build(pins, reported)
        self.assertEqual(problems, [])
        self.assertEqual(env['gateway']['admission_cap'], 8)
        for key in ('running_image_digest', 'models_id', 'models_max_model_len', 'cache_config_info',
                    'command_checks', 'gateway_admission_cap'):
            with self.subTest(missing=key):
                missing = dict(reported)
                del missing[key]
                self.assertTrue(gpu_environment.build(pins, missing)[1])
        for key in reported['cache_config_info']:
            missing = dict(reported, cache_config_info=dict(reported['cache_config_info']))
            del missing['cache_config_info'][key]
            self.assertTrue(gpu_environment.build(pins, missing)[1])

    def test_stale_container_arguments_and_unexpected_flags_fail(self):
        for service, expected in gpu_environment.expected_commands(PINS).items():
            entrypoint, command = expected
            self.assertEqual(gpu_environment.command_problems(service, entrypoint, command, expected), [])
            for index in range(len(command)):
                stale = command.copy()
                stale[index] += '-stale'
                with self.subTest(service=service, argument=index):
                    self.assertTrue(gpu_environment.command_problems(service, entrypoint, stale, expected))
            for extra in ('--max-num-seqs=1', '--api-key=synthetic-secret'):
                problems = gpu_environment.command_problems(service, entrypoint, command + [extra], expected)
                self.assertTrue(problems)
                self.assertNotIn('synthetic-secret', str(problems))
            self.assertTrue(gpu_environment.command_problems(service, ['other'], command, expected))

    def test_main_rejects_incomplete_snapshot_and_accepts_explicit_cap(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as root:
            output = str(Path(root, 'environment.json'))
            pins = dict(PINS, VLLM_IMAGE_DIGEST='sha256:' + 'a' * 64, MODEL_REVISION='b' * 40)
            Path(output).write_text('{"consistency_mismatches": []}\n')  # stale earlier snapshot
            with mock.patch.object(gpu_environment, 'read_env_file', return_value=pins), \
                    mock.patch.object(gpu_environment, 'collect', return_value={}), \
                    mock.patch('sys.argv', ['gpu_environment.py', '--output', output, '--admission-cap', '8']), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(gpu_environment.main(), 1)
            # A rejected snapshot never occupies --output, and a stale one is removed.
            self.assertFalse(Path(output).exists())
            saved = json.loads(Path(output + '.rejected.json').read_text())
            self.assertEqual(saved['gateway']['admission_cap'], 8)
            self.assertTrue(saved['consistency_mismatches'])
            self.assertIn('gpu unavailable', saved['consistency_mismatches'])

    def test_main_writes_consistent_snapshot_and_clears_rejection(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as root:
            output = str(Path(root, 'environment.json'))
            env = {key: 'evidence' for key in gpu_environment.BENCH_KEYS}
            Path(output + '.rejected.json').write_text('{}\n')  # earlier failed attempt
            with mock.patch.object(gpu_environment, 'read_env_file', return_value=dict(PINS)), \
                    mock.patch.object(gpu_environment, 'collect', return_value={}), \
                    mock.patch.object(gpu_environment, 'build', return_value=(env, [])), \
                    mock.patch('sys.argv', ['gpu_environment.py', '--output', output]), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(gpu_environment.main(), 0)
            self.assertEqual(json.loads(Path(output).read_text())['consistency_mismatches'], [])
            self.assertFalse(Path(output + '.rejected.json').exists())

    def test_collect_checks_running_containers_without_reading_secrets(self):
        pins = dict(PINS, VLLM_IMAGE_DIGEST='sha256:' + 'a' * 64, MODEL_REVISION='b' * 40)
        engine_entry, engine_cmd = gpu_environment.expected_commands(pins)['vllm']
        gateway_entry, gateway_cmd = gpu_environment.expected_commands(pins)['gateway']
        # A stale revision and a cap changed only at launch must both be caught.
        engine_cmd = [arg.replace(pins['MODEL_REVISION'], 'c' * 40) for arg in engine_cmd]
        gateway_cmd = [arg.replace('--admission-cap=0', '--admission-cap=8') for arg in gateway_cmd]
        def run(command):
            if command[0] == 'nvidia-smi':
                return 'Synthetic GPU, 999.1, 24564 MiB'
            if command[-3:-1] == ['ps', '-q']:
                return command[-1] + '-container'
            if command[:2] == ['docker', 'image']:
                return json.dumps([pins['VLLM_IMAGE_REPO'] + '@' + pins['VLLM_IMAGE_DIGEST']])
            self.assertNotIn('Env', str(command))
            if command[3] == '{{.Image}}':
                return 'image-id'
            service = command[-1].removesuffix('-container')
            entry, args = (engine_entry, engine_cmd) if service == 'vllm' else (gateway_entry, gateway_cmd)
            return json.dumps(entry if 'Entrypoint' in command[3] else args)
        def fetch(url, key=None):
            if url.endswith('/version'):
                return '{"version":"synthetic"}'
            if url.endswith('/v1/models'):
                return '{"data":[{"id":"lab-model","max_model_len":4096}]}'
            if url.startswith('http://admin'):
                return 'lab_gateway_admission_cap 8\n'
            return ('vllm:cache_config_info{block_size="16",num_gpu_blocks="1",cache_dtype="auto",'
                    'enable_prefix_caching="False",gpu_memory_utilization="0.9"} 1\n')
        with mock.patch.object(gpu_environment, 'run', side_effect=run), \
                mock.patch.object(gpu_environment, 'fetch', side_effect=fetch):
            reported = gpu_environment.collect(pins, 'http://engine', 'http://admin', ['docker', 'compose'])
        self.assertEqual(len(reported['command_checks']), 2)
        self.assertEqual(reported['gateway_admission_cap'], 8)
        self.assertNotIn('c' * 40, json.dumps(reported))
        self.assertTrue(gpu_environment.build(pins, reported)[1])

    def test_cap_metric_requires_exactly_one_sample(self):
        self.assertEqual(gpu_environment.metric_value('lab_gateway_admission_cap 8\n', 'lab_gateway_admission_cap'), 8)
        self.assertIsNone(gpu_environment.metric_value('# no gauge\n', 'lab_gateway_admission_cap'))



class ConfigTests(unittest.TestCase):
    def test_pins_match_plan_and_candidate(self):
        candidate = json.loads(Path('configs/environment.json').read_text())['scheduler_candidate']
        self.assertEqual(check_gpu_config.check_pins(PINS, candidate), [])
        drifted = dict(PINS, MAX_NUM_SEQS='128', PREFIX_CACHING_FLAG='--enable-prefix-caching')
        self.assertTrue(check_gpu_config.check_pins(drifted, candidate))

    def test_memory_fit_adjustments_require_matching_candidate(self):
        candidate = json.loads(Path('configs/environment.json').read_text())['scheduler_candidate']
        pins = dict(PINS, MAX_NUM_SEQS='32', MAX_NUM_BATCHED_TOKENS='2048', GPU_MEMORY_UTILIZATION='0.85')
        self.assertTrue(check_gpu_config.check_pins(pins, candidate))
        candidate.update(max_num_seqs=32, max_num_batched_tokens=2048, gpu_memory_utilization=0.85)
        self.assertEqual(check_gpu_config.check_pins(pins, candidate), [])
        for key, value in (('MAX_NUM_SEQS', '0'), ('MAX_NUM_BATCHED_TOKENS', '-1'),
                           ('MAX_NUM_SEQS', 'bad'), ('GPU_MEMORY_UTILIZATION', '0'),
                           ('GPU_MEMORY_UTILIZATION', '1.1'), ('GPU_MEMORY_UTILIZATION', 'nan')):
            with self.subTest(key=key, value=value):
                self.assertTrue(check_gpu_config.check_pins(dict(pins, **{key: value}), candidate))



def summary(target, mode, workers, success, offered, window_ms, p95):
    number = lambda v: {'value': v, 'unbounded': False, 'undefined': False}
    dist = lambda v: {'count': 1, 'p50': number(v), 'p95': number(v), 'p99': number(None), 'min': number(v), 'max': number(v)}
    repeat = {'repeat': 1, 'complete': True, 'measurement_window_ms': window_ms,
              'counts': {'offered': offered, 'success': success, 'rejected': 0, 'failed': offered - success,
                         'dropped': 0, 'output_length_deviation': 0},
              'rates': {'success_rate': number(success / offered), 'goodput_requests_per_second': number(success / window_ms * 1000)},
              'offered': {'metrics_ms': {'success_first_text': dist(p95), 'penalised_scheduled_first_text': dist(p95),
                                         'success_completion': dist(p95 * 4)}},
              'diagnostics': {'scheduling_delay_ms': dist(0.1), 'completion_tokens': dist(128)}}
    manifest = {'config': {'target': target, 'mode': mode, 'workers': workers, 'gateway_admission_cap': None,
                           'first_text_objective_ms': 1000}}
    return manifest, {'state': 'complete', 'repeats': [repeat]}


class PilotTableTests(unittest.TestCase):
    def test_table_and_suggested_rate(self):
        with tempfile.TemporaryDirectory() as root:
            for name, args in {'c1': ('gpu-direct', 'closed', 1, 40, 40, 20000, 150.0),
                               'c16': ('gpu-direct', 'closed', 16, 64, 64, 8000, 600.0),
                               'c64': ('gpu-direct', 'closed', 64, 256, 256, 16000, 2500.0)}.items():
                run = Path(root, name)
                run.mkdir()
                manifest, data = summary(*args)
                (run / 'manifest.json').write_text(json.dumps(manifest))
                (run / 'summary.json').write_text(json.dumps(data))
            found = pilot_table.rows([root])
            self.assertEqual(len(found), 3)
            self.assertIn('| gpu-direct | closed | 16 |', pilot_table.table(found))
            rate, best = pilot_table.suggest_rate(found)
            # c64 breaches the 1,000 ms objective; c16 (8 req/s) is the basis.
            self.assertEqual((best['workers'], rate), (16, 10.0))


if __name__ == '__main__':
    unittest.main()
