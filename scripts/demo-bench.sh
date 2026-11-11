#!/usr/bin/env bash
# CPU-only synthetic examples for both modes, with isolated loopback ports.
set -euo pipefail
example_root=${1:-results/bench-demo-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$example_root"
export LAB_GATEWAY_API_KEY
export LAB_UPSTREAM_API_KEY
LAB_GATEWAY_API_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
LAB_UPSTREAM_API_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
./bin/mockllm --addr 127.0.0.1:18000 --delay 1ms >"$example_root/mock.log" 2>&1 &
example_mock_pid=$!
./bin/gateway --addr 127.0.0.1:18080 --admin-addr 127.0.0.1:18081 --upstream http://127.0.0.1:18000 --admission-cap 8 >"$example_root/gateway.log" 2>&1 &
example_gateway_pid=$!
trap 'kill "$example_mock_pid" "$example_gateway_pid" 2>/dev/null || true; wait "$example_mock_pid" "$example_gateway_pid" 2>/dev/null || true' EXIT
python3 - <<'PY'
import time, urllib.request
for attempt in range(100):
    try:
        with urllib.request.urlopen('http://127.0.0.1:18081/readyz', timeout=1) as response:
            assert response.status == 200
        break
    except OSError:
        time.sleep(.05)
else:
    raise SystemExit('mock/gateway failed readiness; inspect logs')
PY
./bin/lab prepare --synthetic --output "$example_root/inputs" --attempts 200 --rate 100
example_closed_run=$(./bin/lab bench --mode closed --target mock-direct \
  --url http://127.0.0.1:18000/v1/chat/completions --api-key-env LAB_UPSTREAM_API_KEY \
  --workload "$example_root/inputs/workload.json" --workers 4 --max-inflight 32 \
  --attempts 200 --repeats 3 --warmup 10 --output "$example_root/runs")
./bin/lab summarize --run "$example_closed_run" --output "$example_closed_run/summary.offline.json"
example_open_run=$(./bin/lab bench --mode open --target mock-gateway \
  --url http://127.0.0.1:18080/v1/chat/completions --api-key-env LAB_GATEWAY_API_KEY \
  --workload "$example_root/inputs/workload.json" --schedule "$example_root/inputs/schedule.json" \
  --max-inflight 32 --repeats 3 --warmup 10 --gateway-admission-cap 8 \
  --output "$example_root/runs")
./bin/lab summarize --run "$example_open_run" --output "$example_open_run/summary.offline.json"
printf 'Closed run: %s\nOpen run: %s\n' "$example_closed_run" "$example_open_run"
