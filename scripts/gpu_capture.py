#!/usr/bin/env python3
"""Verify, compare, sanitise and secret-scan raw GPU protocol captures.

  verify  STREAM.sse --expect-completion-tokens N   check one successful stream
  compare A.sse B.sse                               same usage on both paths
  sanitize RAW_DIR OUT_DIR                          fixture-safe copies + provenance
  scan DIR...                                       fail if a credential value appears

Secret values come from LAB_UPSTREAM_API_KEY, LAB_GATEWAY_API_KEY and HF_TOKEN
in the environment; they are compared, never printed. Sanitising rewrites only
volatile identifiers and timestamps. SSE framing (line endings, blank-line
separators, comments) and generated text are preserved byte-for-byte.
"""
import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

SECRET_VARS = ('LAB_UPSTREAM_API_KEY', 'LAB_GATEWAY_API_KEY', 'HF_TOKEN')
VOLATILE_HEADERS = ('date', 'x-request-id', 'x-lab-request-id', 'set-cookie')
ID_PATTERN = re.compile(rb'"id"(\s*):(\s*)"(chatcmpl|cmpl)-[^"]*"')
CREATED_PATTERN = re.compile(rb'"created"(\s*):(\s*)\d+')


def events(raw):
    """Yield (event_type, data) for each SSE event, tolerating CRLF/CR."""
    text = raw.decode('utf-8').replace('\r\n', '\n').replace('\r', '\n')
    for block in text.split('\n\n'):
        kind, data = 'message', []
        for line in block.split('\n'):
            if not line or line.startswith(':'):
                continue
            field, _, value = line.partition(':')
            value = value[1:] if value.startswith(' ') else value
            if field == 'event':
                kind = value
            elif field == 'data':
                data.append(value)
        if data:
            yield kind, '\n'.join(data)


def inspect(raw):
    """Summarise a captured stream the way the lab client classifies it."""
    result = {'events': 0, 'text_events': 0, 'text_bytes': 0, 'done': False, 'error': None,
              'prompt_tokens': None, 'completion_tokens': None, 'finish_reason': None, 'after_done': 0}
    for kind, data in events(raw):
        result['events'] += 1
        if result['done']:
            result['after_done'] += 1
        if kind == 'error':
            result['error'] = data or 'error event'
            continue
        if data == '[DONE]':
            result['done'] = True
            continue
        chunk = json.loads(data)
        if chunk.get('error') or chunk.get('object') == 'error':
            result['error'] = data
        usage = chunk.get('usage')
        if usage:
            result['prompt_tokens'] = usage.get('prompt_tokens')
            result['completion_tokens'] = usage.get('completion_tokens')
        for choice in chunk.get('choices') or []:
            if choice.get('finish_reason'):
                result['finish_reason'] = choice['finish_reason']
            content = (choice.get('delta') or {}).get('content')
            if content:
                result['text_events'] += 1
                result['text_bytes'] += len(content.encode())
    return result


def verify(raw, expected_tokens):
    """Return (summary, problems) for a stream that should have succeeded."""
    summary = inspect(raw)
    problems = []
    if summary['error']:
        problems.append('stream reported an error')
    if not summary['done']:
        problems.append('no [DONE] (truncated stream)')
    if summary['after_done']:
        problems.append('events after [DONE]')
    if not summary['text_events']:
        problems.append('no text events')
    if summary['completion_tokens'] is None or summary['prompt_tokens'] is None:
        problems.append('no usage chunk (stream_options.include_usage)')
    elif expected_tokens is not None and summary['completion_tokens'] != expected_tokens:
        problems.append(f"completion_tokens {summary['completion_tokens']} != requested {expected_tokens}")
    return summary, problems


def secrets():
    return {name: os.environ[name].encode() for name in SECRET_VARS if os.environ.get(name)}


def find_secrets(paths, values):
    hits = []
    for root in paths:
        root = Path(root)
        files = [root] if root.is_file() else sorted(p for p in root.rglob('*') if p.is_file())
        for path in files:
            data = path.read_bytes()
            hits += [f'{path}: contains {name}' for name, value in values.items() if value in data]
    return hits


def sanitize_bytes(name, raw):
    """Return (sanitised bytes, list of applied rules)."""
    rules = []
    out = raw
    if name.endswith('.headers'):
        lines = []
        for line in raw.split(b'\n'):
            key = line.split(b':', 1)[0].strip().lower().decode('latin-1')
            if b':' in line and key in VOLATILE_HEADERS:
                ending = b'\r' if line.endswith(b'\r') else b''
                line = line.split(b':', 1)[0] + b': sanitized' + ending
                rules.append(f'header {key}')
            lines.append(line)
        out = b'\n'.join(lines)
    else:
        out, ids = ID_PATTERN.subn(lambda m: b'"id"' + m.group(1) + b':' + m.group(2) + b'"' + m.group(3) + b'-sanitized"', out)
        out, created = CREATED_PATTERN.subn(lambda m: b'"created"' + m.group(1) + b':' + m.group(2) + b'0', out)
        if ids:
            rules.append(f'{ids} response id(s)')
        if created:
            rules.append(f'{created} created timestamp(s)')
    return out, sorted(set(rules))


def sanitize(raw_dir, out_dir, values):
    raw_dir, out_dir = Path(raw_dir), Path(out_dir)
    hits = find_secrets([raw_dir], values)
    if hits:
        raise SystemExit('refusing to sanitise: credential found in raw capture\n' + '\n'.join(hits))
    out_dir.mkdir(parents=True, exist_ok=True)
    provenance = {'sanitizer': 'scripts/gpu_capture.py', 'secrets_checked': sorted(values), 'files': []}
    for path in sorted(p for p in raw_dir.rglob('*') if p.is_file()):
        relative = path.relative_to(raw_dir)
        raw = path.read_bytes()
        clean, rules = sanitize_bytes(path.name, raw)
        target = out_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(clean)
        provenance['files'].append({'path': str(relative), 'raw_sha256': hashlib.sha256(raw).hexdigest(),
                                    'sanitized_sha256': hashlib.sha256(clean).hexdigest(), 'rules': rules})
    (out_dir / 'sanitize-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    return provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('verify')
    p.add_argument('stream')
    p.add_argument('--expect-completion-tokens', type=int)
    p = sub.add_parser('compare')
    p.add_argument('a')
    p.add_argument('b')
    p = sub.add_parser('sanitize')
    p.add_argument('raw_dir')
    p.add_argument('out_dir')
    p = sub.add_parser('scan')
    p.add_argument('paths', nargs='+')
    args = parser.parse_args()

    if args.command == 'verify':
        summary, problems = verify(Path(args.stream).read_bytes(), args.expect_completion_tokens)
        print(json.dumps({'stream': args.stream, **summary, 'problems': problems}))
        return 1 if problems else 0
    if args.command == 'compare':
        a, b = inspect(Path(args.a).read_bytes()), inspect(Path(args.b).read_bytes())
        keys = ('prompt_tokens', 'completion_tokens', 'finish_reason', 'done')
        diff = {k: [a[k], b[k]] for k in keys if a[k] != b[k]}
        print(json.dumps({'a': args.a, 'b': args.b, 'differences': diff,
                          'note': 'text and chunking may differ across paths; usage must not'}))
        return 1 if diff else 0
    values = secrets()
    if not values:
        print('warning: no credential variables set; secret scan is vacuous', file=sys.stderr)
    if args.command == 'sanitize':
        provenance = sanitize(args.raw_dir, args.out_dir, values)
        print(f"sanitised {len(provenance['files'])} files into {args.out_dir}")
        return 0
    hits = find_secrets(args.paths, values)
    for hit in hits:
        print('SECRET FOUND:', hit, file=sys.stderr)
    if not hits:
        print('secret scan clean:', ' '.join(args.paths))
    return 1 if hits else 0


if __name__ == '__main__':
    sys.exit(main())
