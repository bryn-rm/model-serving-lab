#!/usr/bin/env bash
# Verify that a client disconnect releases engine work on both paths.
#
#   scripts/gpu-cancel-check.sh SESSION_DIR
#
# Needs an idle stack (no other traffic), SESSION_DIR/inputs/workload.json and
# exported credentials. For each path it starts a long ignore_eos stream, waits
# for the first text, disconnects the client, then polls vLLM's running/waiting
# gauges (and the gateway's in-flight gauge) until they return to zero.
set -euo pipefail
session=${1:?usage: scripts/gpu-cancel-check.sh SESSION_DIR}
: "${LAB_UPSTREAM_API_KEY:?}" "${LAB_GATEWAY_API_KEY:?}"
# Endpoint/log overrides exist only for offline rehearsal against a fake engine.
engine=${ENGINE_URL:-http://127.0.0.1:8000}
gateway=${GATEWAY_URL:-http://127.0.0.1:8080}
admin=${GATEWAY_ADMIN_URL:-http://127.0.0.1:8081}
compose=(docker compose --env-file "${GPU_ENV:-configs/gpu-pilot.env}" -f deploy/compose.gpu.yaml)
gateway_logs() { if [ -n "${GATEWAY_LOG_FILE:-}" ]; then cat "$GATEWAY_LOG_FILE"; else "${compose[@]}" logs --no-color gateway; fi; }
out=$session/cancel
mkdir -p "$out"
release_limit=${CANCEL_RELEASE_LIMIT_SECONDS:-15}

# Sum a gauge over all label sets (vLLM labels by engine/model).
gauge() { curl -fsS "$1/metrics" | awk -v name="$2" '$1 == name || index($1, name "{") == 1 { s += $NF } END { print s + 0 }'; }

# A long generation so the disconnect lands mid-stream (not a benchmark body).
jq -c '.max_tokens = 2048 | .ignore_eos = true' <(jq -j '.prompts[0].body' "$session/inputs/workload.json") >"$out/long-body.json"

failed=0
for path in direct gateway; do
  if [ "$path" = direct ]; then url=$engine key_var=LAB_UPSTREAM_API_KEY; else url=$gateway key_var=LAB_GATEWAY_API_KEY; fi
  [ "$(gauge "$engine" vllm:num_requests_running)" = 0 ] || { echo "cancel: engine busy before $path test; stop other traffic" >&2; exit 1; }
  curl -fsS "$engine/metrics" >"$out/$path-vllm-before.prom"
  curl -sS -N --http1.1 -o "$out/$path-stream.partial" -H 'Content-Type: application/json' \
    -H "X-Request-ID: cancel-check-$path" -H @<(printf 'Authorization: Bearer %s\n' "${!key_var}") \
    --data-binary @"$out/long-body.json" "$url/v1/chat/completions" &
  client=$!
  for _ in $(seq 1 1200); do grep -qE '"content": ?"[^"]' "$out/$path-stream.partial" 2>/dev/null && break; sleep 0.1; done
  grep -qE '"content": ?"[^"]' "$out/$path-stream.partial" || { echo "cancel: no text within 120 s on $path" >&2; kill "$client"; exit 1; }
  sleep 1
  running_before=$(gauge "$engine" vllm:num_requests_running)
  kill "$client"; wait "$client" 2>/dev/null || true
  disconnected=$(date +%s.%N)
  released=
  for _ in $(seq 1 $((release_limit * 10))); do
    running=$(gauge "$engine" vllm:num_requests_running); waiting=$(gauge "$engine" vllm:num_requests_waiting)
    if [ "$running" = 0 ] && [ "$waiting" = 0 ]; then released=$(date +%s.%N); break; fi
    sleep 0.1
  done
  curl -fsS "$engine/metrics" >"$out/$path-vllm-after.prom"
  text_bytes=$(wc -c <"$out/$path-stream.partial")
  if [ -n "$released" ]; then
    printf 'cancel %s: running=%s before disconnect; engine released after %.2f s; %s stream bytes received\n' \
      "$path" "$running_before" "$(awk -v a="$released" -v b="$disconnected" 'BEGIN { print a - b }')" "$text_bytes" | tee -a "$out/result.txt"
  else
    echo "cancel $path: FAILED engine still running/waiting after ${release_limit} s" | tee -a "$out/result.txt"; failed=1
  fi
  [ "$running_before" != 0 ] || { echo "cancel $path: FAILED request was not running at disconnect" | tee -a "$out/result.txt"; failed=1; }
  if [ "$path" = gateway ]; then
    inflight=$(gauge "$admin" lab_gateway_inflight_requests)
    outcome=$(gateway_logs 2>/dev/null | grep -o '{.*"client_request_id":"cancel-check-gateway".*}' |
      jq -r 'select(.msg == "request finished") | .outcome' | tail -1 || true)
    echo "cancel gateway: in-flight gauge $inflight; logged outcome ${outcome:-not found}" | tee -a "$out/result.txt"
    [ "$inflight" = 0 ] || failed=1
  fi
  # Abort counters differ across releases; save their deltas as evidence.
  diff <(grep -E '^vllm:request_success_total' "$out/$path-vllm-before.prom") \
    <(grep -E '^vllm:request_success_total' "$out/$path-vllm-after.prom") >"$out/$path-finished-reason.diff" || true
done
[ "$failed" -eq 0 ] && echo "cancel: PASS (evidence in $out)" || { echo "cancel: FAILED (evidence in $out)" >&2; exit 1; }
