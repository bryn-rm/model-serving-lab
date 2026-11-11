#!/usr/bin/env bash
# Phase 4 protocol capture against the running GPU stack.
#
#   scripts/gpu-capture.sh SESSION_DIR
#
# Needs SESSION_DIR/inputs/workload.json (from `lab prepare`), the stack up and
# LAB_UPSTREAM_API_KEY / LAB_GATEWAY_API_KEY exported. Sends the identical saved
# body bytes to vLLM directly and through the gateway, captures known error
# envelopes, engine/gateway metrics and model info, verifies usage and byte
# parity, then writes sanitised copies. Credentials go to curl via a header
# file descriptor, never argv, and are scanned for before sanitising.
set -euo pipefail
session=${1:?usage: scripts/gpu-capture.sh SESSION_DIR}
: "${LAB_UPSTREAM_API_KEY:?export LAB_UPSTREAM_API_KEY}" "${LAB_GATEWAY_API_KEY:?export LAB_GATEWAY_API_KEY}"
# Endpoint/log overrides exist only for offline rehearsal against a fake engine.
engine=${ENGINE_URL:-http://127.0.0.1:8000}
gateway=${GATEWAY_URL:-http://127.0.0.1:8080}
admin=${GATEWAY_ADMIN_URL:-http://127.0.0.1:8081}
compose=(docker compose --env-file "${GPU_ENV:-configs/gpu-pilot.env}" -f deploy/compose.gpu.yaml)
gateway_logs() { if [ -n "${GATEWAY_LOG_FILE:-}" ]; then cat "$GATEWAY_LOG_FILE"; else "${compose[@]}" logs --no-color gateway; fi; }
raw=$session/capture/raw
mkdir -p "$raw"
INVALID_CAPTURE_KEY=invalid-key-for-error-capture

# post NAME URL KEY_VARIABLE BODY_FILE: response headers, body and status.
post() {
  local name=$1 url=$2 key_var=$3 body=$4
  curl -sS -N --http1.1 --max-time 300 -D "$raw/$name.headers" -o "$raw/$name.body" \
    -w '%{http_code} first_byte_s=%{time_starttransfer} total_s=%{time_total}\n' \
    -H 'Content-Type: application/json' -H 'Accept: text/event-stream' -H "X-Request-ID: capture-$name" \
    -H @<(printf 'Authorization: Bearer %s\n' "${!key_var}") \
    --data-binary @"$body" "$url/v1/chat/completions" >"$raw/$name.status"
  echo "capture: $name -> $(cat "$raw/$name.status")"
}

# The exact benchmark body bytes (not re-serialised).
jq -j '.prompts[0].body' "$session/inputs/workload.json" >"$raw/request-body.json"
expected_tokens=$(jq -r '.max_tokens // .max_completion_tokens' "$raw/request-body.json")
body_sha=$(sha256sum "$raw/request-body.json" | cut -d' ' -f1)

curl -fsS "$engine/metrics" >"$raw/vllm-metrics-before.prom"
curl -fsS "$admin/metrics" >"$raw/gateway-metrics-before.prom"
post direct-success "$engine" LAB_UPSTREAM_API_KEY "$raw/request-body.json"
post gateway-success "$gateway" LAB_GATEWAY_API_KEY "$raw/request-body.json"

# Error envelopes. Mid-stream engine errors cannot be provoked reliably and are
# recorded opportunistically from benchmark failures instead.
jq -c '.model = "not-lab-model"' "$raw/request-body.json" >"$raw/request-unknown-model.json"
python3 - "$raw/request-body.json" "$raw/request-context-overflow.json" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
body['messages'] = [{'role': 'user', 'content': 'overflow ' * 6000}]  # > 4,096-token context
json.dump(body, open(sys.argv[2], 'w'))
PY
post direct-unauthorized "$engine" INVALID_CAPTURE_KEY "$raw/request-body.json"
post direct-unknown-model "$engine" LAB_UPSTREAM_API_KEY "$raw/request-unknown-model.json"
post direct-context-overflow "$engine" LAB_UPSTREAM_API_KEY "$raw/request-context-overflow.json"
post gateway-context-overflow "$gateway" LAB_GATEWAY_API_KEY "$raw/request-context-overflow.json"
post gateway-unauthorized "$gateway" INVALID_CAPTURE_KEY "$raw/request-body.json"

curl -fsS "$engine/metrics" >"$raw/vllm-metrics-after.prom"
curl -fsS "$admin/metrics" >"$raw/gateway-metrics-after.prom"
curl -fsS "$engine/version" >"$raw/vllm-version.json" || echo '{}' >"$raw/vllm-version.json"
curl -fsS -H @<(printf 'Authorization: Bearer %s\n' "$LAB_UPSTREAM_API_KEY") "$engine/v1/models" >"$raw/vllm-models.json"
curl -sS -D "$raw/vllm-health.headers" -o /dev/null "$engine/health"

# Checks: usage and [DONE] on both paths, matched usage, byte-identical body
# at the gateway (its admitted log records the forwarded body's SHA-256), and
# the error statuses that the lab classifies.
checks=$session/capture/checks.txt
failed=0
check() { "$@" 2>&1 | tee -a "$checks" || failed=1; }
: >"$checks"
check python3 scripts/gpu_capture.py verify "$raw/direct-success.body" --expect-completion-tokens "$expected_tokens"
check python3 scripts/gpu_capture.py verify "$raw/gateway-success.body" --expect-completion-tokens "$expected_tokens"
check python3 scripts/gpu_capture.py compare "$raw/direct-success.body" "$raw/gateway-success.body"
gateway_sha=$(gateway_logs 2>/dev/null |
  grep -o '{.*"client_request_id":"capture-gateway-success".*}' |
  jq -r 'select(.msg == "admitted") | .body_sha256' | tail -1 || true)
if [ "$gateway_sha" = "$body_sha" ]; then
  echo "body parity: gateway forwarded sha256 $body_sha" | tee -a "$checks"
else
  echo "body parity FAILED: sent $body_sha, gateway logged ${gateway_sha:-nothing}" | tee -a "$checks"; failed=1
fi
for pair in direct-unauthorized:401 gateway-unauthorized:401 direct-unknown-model:404 \
  direct-context-overflow:400 gateway-context-overflow:400; do
  name=${pair%%:*} want=${pair##*:} got=$(cut -d' ' -f1 "$raw/$name.status")
  echo "status $name: $got (expected $want)" | tee -a "$checks"
  # Unexpected error statuses are evidence to record, not a capture failure.
  [ "$got" = "$want" ] || echo "  NOTE: record the actual envelope rather than assuming" | tee -a "$checks"
done
grep -i '^x-lab-upstream-status' "$raw/gateway-context-overflow.headers" | tee -a "$checks" ||
  echo "NOTE: gateway overflow response was not an upstream relay" | tee -a "$checks"

python3 scripts/gpu_capture.py sanitize "$raw" "$session/capture/sanitized"
echo "capture: raw (keep off-repo) $raw; sanitised fixtures $session/capture/sanitized; checks $checks"
[ "$failed" -eq 0 ] || { echo "capture: CHECKS FAILED (see $checks)" >&2; exit 1; }
