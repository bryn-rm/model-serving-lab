# GPU pilot runbook (Phase 4, first session)

Serves **Qwen/Qwen2.5-7B-Instruct** as `lab-model` with a 4,096-token context
on one NVIDIA GPU (24 GB class) with vLLM, behind the Go gateway. The session
pins the image digest and model/tokenizer revision, captures sanitised real
streams, error envelopes and metrics, and checks usage, byte parity and
cancellation. It then runs a short, exploratory closed-loop and open-loop pilot.
Pilot results select the final settings. They are not final E1–E5 results.

Assumptions: a Linux VM with Docker Engine, Compose v2, the NVIDIA driver and
NVIDIA Container Toolkit, `git`, `curl`, `jq` and `python3`, with roughly 60 GB
free disk. Container-only GPU rentals need a provider-specific launch recipe
and are not covered here. Use `tmux` so an SSH drop does not kill a run.
Run every command from the repository root. Each block continues the
same shell, so keep it open (or re-run step 2's "resume" lines).

| What | Where |
| --- | --- |
| Pins and engine settings (no secrets) | `configs/gpu-pilot.env` |
| Stack | `deploy/compose.gpu.yaml`: vLLM `127.0.0.1:8000`, gateway `127.0.0.1:8080`, gateway admin `127.0.0.1:8081` |
| Session evidence (ignored by git) | `results/gpu-<UTC>/` |

Engine flags match BUILD_PLAN.md section 2:
- `--dtype auto`, `--kv-cache-dtype auto` and `--max-model-len 4096`.
- `--gpu-memory-utilization 0.90`, `--max-num-seqs 64` and `--max-num-batched-tokens 4096`.
- `--enable-chunked-prefill` and prefix caching explicitly disabled.
- `--generation-config vllm`, so repository generation defaults cannot change the workload.
- `--revision` and `--tokenizer-revision` both set to the pinned commit.

vLLM reads its API key from `VLLM_API_KEY`, which Compose takes from
`LAB_UPSTREAM_API_KEY`, so it never appears in argv. Every port is bound to
host loopback. Use SSH forwarding to view anything remotely.

## 0. Before the session (Codespace)

Commit and push the Phase 3/4 work, so the GPU host runs a known SHA:

```sh
make check && make build && make gpu-config-check && git diff --check
git status --short    # commit and push before renting the GPU
```

## 1. Host setup

```sh
git clone <your-repo-url> model-serving-lab && cd model-serving-lab
git checkout <commit-sha-from-step-0>
# Build bin/lab (Go not required on the host; uses the pinned golang:1.27.3 image):
make lab-docker
./bin/lab 2>&1 | head -1    # prints usage

# Credentials: generated once, kept outside the repository, never echoed.
umask 077
[ -f ~/.lab-keys ] || python3 -c 'import secrets; print(f"export LAB_UPSTREAM_API_KEY={secrets.token_hex(24)}\nexport LAB_GATEWAY_API_KEY={secrets.token_hex(24)}")' > ~/.lab-keys
. ~/.lab-keys
# Optional, only for gated/rate-limited downloads: export HF_TOKEN=... (do not write it into the repo)
```

## 2. Session directory and shell helpers

```sh
S=results/gpu-$(date -u +%Y%m%dT%H%M%SZ); mkdir -p "$S/logs"; echo "$S" > results/CURRENT_SESSION
date -u +%FT%TZ > "$S/session-start-utc.txt"     # billing/cost record
set -a; . configs/gpu-pilot.env; set +a           # non-secret pins only
wait_idle() { until [ "$(curl -fsS http://127.0.0.1:8000/metrics | awk '/^vllm:num_requests_(running|waiting)[{ ]/ {s+=$NF} END {print s+0}')" = 0 ]; do sleep 2; done; }
```

To resume in a new shell:

```sh
cd model-serving-lab && . ~/.lab-keys && S=$(cat results/CURRENT_SESSION)
set -a; . configs/gpu-pilot.env; set +a
wait_idle() { until [ "$(curl -fsS http://127.0.0.1:8000/metrics | awk '/^vllm:num_requests_(running|waiting)[{ ]/ {s+=$NF} END {print s+0}')" = 0 ]; do sleep 2; done; }
```

## 3. Compatibility preflight and pins

```sh
scripts/gpu-preflight.sh "$S/preflight" --write
git diff configs/gpu-pilot.env       # digest, 40-hex revision, prefix-cache flag spelling
make gpu-config-check
set -a; . configs/gpu-pilot.env; set +a
git commit -am "Pin vLLM image digest and Qwen2.5-7B-Instruct revision"   # push when possible
```

The preflight fails on any of the following. All evidence goes to `$S/preflight/`:
- A missing tool, or no GPU visible.
- An image that cannot start with `--gpus all`. This points to the NVIDIA Container Toolkit or a driver/CUDA mismatch.
- `torch` in the image being unable to run a BF16 matmul on the GPU.
- A `vllm serve` flag that the pinned release does not support.
- A tag that has moved away from an existing digest pin.
- A model revision without `config.json` or `tokenizer_config.json`.

It records several values:
- The `nvidia-smi` output, including the newest CUDA version the driver supports.
- The image's vLLM, torch and CUDA versions.
- The registry digest of `vllm/vllm-openai:v0.33.0`.
- The `main` commit of the model, used for both the model and the tokenizer/chat template.
- The prefix-cache flag spelling that the pinned release accepts.

**If the image is incompatible** (v0.33.0 defaults to CUDA 13 artifacts), pick
a documented stable vLLM image that runs on this driver. Set `VLLM_IMAGE_TAG`,
clear `VLLM_IMAGE_DIGEST`, re-run the preflight, and record the substitution
and reason once in BUILD_STATUS.md. Freeze that image for the whole matrix.

## 4. Start the stack and verify memory fit

```sh
make gpu-up
timeout 45m bash -c 'until curl -fsS http://127.0.0.1:8000/health >/dev/null; do
  docker compose --env-file configs/gpu-pilot.env -f deploy/compose.gpu.yaml ps --status exited -q vllm | grep -q . && exit 1
  sleep 15; done' && echo "vLLM healthy" || echo "vLLM FAILED: inspect logs below"
curl -fsS http://127.0.0.1:8081/readyz && echo " gateway ready"
docker compose --env-file configs/gpu-pilot.env -f deploy/compose.gpu.yaml logs --no-color vllm > "$S/logs/vllm-startup.log"
grep -iE 'non-default args|KV cache|maximum concurrency|CUDA graph|out of memory|error' "$S/logs/vllm-startup.log" | tail -20
nvidia-smi --query-gpu=memory.used,memory.total --format=csv | tee "$S/logs/gpu-memory-after-start.csv"
```

The first start downloads about 15 GB of weights into `~/.cache/huggingface`.
Watch it with `docker compose --env-file configs/gpu-pilot.env -f deploy/compose.gpu.yaml logs -f vllm`.

**If startup fails on memory:** change only what is needed. Lower
`MAX_NUM_SEQS`/`MAX_NUM_BATCHED_TOKENS` first, then `GPU_MEMORY_UTILIZATION`.
Update the same fields in `configs/environment.json`'s `scheduler_candidate`
and record the change and reason in BUILD_STATUS.md. Reload exported settings
before restarting, since shell exports take precedence over the Compose env file:

```sh
set -a; . configs/gpu-pilot.env; set +a
make gpu-config-check
make gpu-down && make gpu-up
```

The resolved settings are frozen after the pilot.

## 5. Effective-settings snapshot (used by every benchmark run)

```sh
python3 scripts/gpu_environment.py --output "$S/environment.json"
jq '{image_digest, model_revision, max_model_len, prefix_caching, cache_block_settings, engine_reported}' "$S/environment.json"
```

The snapshot verifies the declared condition against the running stack.
Both containers' entrypoints and complete argument lists must match the
requested settings, including model/tokenizer revision, scheduler limits,
cache flags and gateway deadlines. Unexpected or duplicate arguments fail.
Only the comparison result is saved; container environment variables and raw
arguments are never written into the snapshot. It also checks:

- The running container's image digest.
- The gateway's `lab_gateway_admission_cap` metric.
- The GPU and driver.
- The vLLM version, when the release exposes `/version`.
- `max_model_len` from `/v1/models`.
- The `vllm:cache_config_info` labels: block size, GPU blocks, prefix caching and memory utilisation.

It exits nonzero on contradictions or missing required evidence. `lab bench
--environment` saves it into every run's `manifest.json`, alongside the bench's
own request settings, body hashes, deadlines, transport settings, git SHA and
dirty state, and before/after `/metrics` scrapes. Re-run this step after any
restart or setting change.

## 6. Natural-completion smoke (both paths)

```sh
LAB_API_KEY=$LAB_UPSTREAM_API_KEY ./bin/lab smoke --url http://127.0.0.1:8000/v1/chat/completions --output "$S/smoke-direct.json"
LAB_API_KEY=$LAB_GATEWAY_API_KEY ./bin/lab smoke --url http://127.0.0.1:8080/v1/chat/completions --output "$S/smoke-gateway.json"
jq -c '{outcome, admission, first_text_ms, prompt_tokens, completion_tokens, finish_reason}' "$S"/smoke-*.json
```

## 7. Pilot workload: one body, saved once

```sh
./bin/lab prepare --model "$SERVED_MODEL_NAME" --max-tokens 128 --attempts 1 --rate 1 --output "$S/inputs"
jq -r '.prompts[0].sha256, .prompts[0].body' "$S/inputs/workload.json"
```

The body uses `temperature=0`, `top_p=1`, `seed=42`, `ignore_eos=true`,
`max_tokens=128` and `stream_options.include_usage=true`. It is synthetic,
fixed-length generation with one short prompt, so prompt-length calibration
(E3) and prefix workloads (E4) remain to be done.

## 8. Same request through each path, usage and byte parity, protocol capture

```sh
scripts/gpu-capture.sh "$S"
cat "$S/capture/checks.txt"
```

The script sends the **identical saved body bytes** to vLLM and to the gateway.
It then checks four things:
- Both streams end in `[DONE]` with text, a usage chunk and `completion_tokens == 128`.
- Usage is identical on both paths.
- The gateway logged the same body SHA-256 it forwarded.
- The error statuses for a bad key, an unknown model and a context overflow, both direct and relayed through the gateway.

It saves headers and bodies, `/metrics` before and after, `/version`,
`/v1/models` and `/health`. Raw files stay in `$S/capture/raw`. Sanitised
copies go to `$S/capture/sanitized`, where response IDs, `created` and
`date`/request-ID headers are replaced while framing and text are kept. The
sanitiser refuses to run if a credential value appears in any raw file.
Mid-stream engine errors cannot be provoked reliably. Record any that occur
during the pilot (`error_code` `sse_error` in `requests.jsonl`).

## 9. Cancellation (both paths)

```sh
wait_idle
scripts/gpu-cancel-check.sh "$S"
cat "$S/cancel/result.txt"
```

For each path the script starts a 2,048-token `ignore_eos` stream and waits
for text. It then disconnects the client and requires vLLM's
running/waiting gauges to return to zero within 15 s. For the gateway path it
also requires the in-flight gauge to reach zero and records the logged
outcome, expected to be `client_canceled`. The finished-reason counter diffs
are saved as evidence.

## 10. Closed-loop pilot (exploratory)

```sh
common=(--workload "$S/inputs/workload.json" --environment "$S/environment.json"
  --metrics-url http://127.0.0.1:8000/metrics --repeats 1 --warmup 4 --max-inflight 256)
direct=(--target gpu-direct --url http://127.0.0.1:8000/v1/chat/completions --api-key-env LAB_UPSTREAM_API_KEY)
gateway=(--target gpu-gateway --url http://127.0.0.1:8080/v1/chat/completions --api-key-env LAB_GATEWAY_API_KEY
  --gateway-admission-cap "$LAB_ADMISSION_CAP")

for c in 1 4 8 16 32 64 128; do
  wait_idle
  ./bin/lab bench --mode closed "${direct[@]}" "${common[@]}" --workers "$c" \
    --attempts $(( c * 4 > 40 ? c * 4 : 40 )) --output "$S/runs/closed-direct" || break
  python3 scripts/pilot_table.py "$S/runs/closed-direct" | tail -1
done
# Matched gateway spot checks (same body, same client settings, cap disabled):
for c in 1 16; do
  wait_idle
  ./bin/lab bench --mode closed "${gateway[@]}" "${common[@]}" --workers "$c" \
    --attempts $(( c * 4 > 40 ? c * 4 : 40 )) --output "$S/runs/closed-gateway" || break
done
python3 scripts/pilot_table.py "$S/runs" | tee "$S/pilot-closed.md"
```

Stop early with Ctrl-C when a point shows a clear latency breach, failures,
a throughput plateau or memory/preemption trouble. The first Ctrl-C drains
in-flight requests and a second one aborts. Request failures are recorded
data and do not stop the loop. Generation is 128 tokens, so 128 workers
exceed `max-num-seqs=64` and should show queueing.

## 11. Open-loop pilot (60 s of arrivals, both arms)

```sh
RATE=$(python3 scripts/pilot_table.py --suggest-rate "$S/runs/closed-direct"); echo "rate=$RATE/s"
ATTEMPTS=$(python3 -c 'import math, sys; print(math.ceil(60 * float(sys.argv[1])))' "$RATE")
./bin/lab prepare --model "$SERVED_MODEL_NAME" --max-tokens 128 --attempts "$ATTEMPTS" --rate "$RATE" --output "$S/inputs-open-$RATE"
# Identical body bytes to the closed-loop runs:
[ "$(jq -r '.prompts[0].sha256' "$S/inputs/workload.json")" = "$(jq -r '.prompts[0].sha256' "$S/inputs-open-$RATE/workload.json")" ] && echo "same body"
open=(--mode open --workload "$S/inputs-open-$RATE/workload.json" --schedule "$S/inputs-open-$RATE/schedule.json"
  --environment "$S/environment.json" --metrics-url http://127.0.0.1:8000/metrics --repeats 1 --warmup 4 --max-inflight 256)
wait_idle; ./bin/lab bench "${open[@]}" "${direct[@]}" --output "$S/runs/open-direct"
wait_idle; ./bin/lab bench "${open[@]}" "${gateway[@]}" --output "$S/runs/open-gateway"
python3 scripts/pilot_table.py "$S/runs/open-direct" "$S/runs/open-gateway" | tee "$S/pilot-open.md"
```

`--suggest-rate` returns 1.25 × the best closed-loop success throughput among
points with at least 99% success, no drops and success first-text p95 within
1,000 ms. If it finds none, set `RATE` by hand. Interpretation guide:
- Drops or a large dispatch-lag p95 mean the generator, not the model, limited
  the run. Repeat with more `--max-inflight`, or treat the point as invalid.
- Aim for a rate where queueing is visible without most attempts timing out.
- Freeze the open-loop rate, the first-text objective and the engine settings
  in BUILD_STATUS.md before any final run.

## 12. Export evidence

```sh
dc() { docker compose --env-file configs/gpu-pilot.env -f deploy/compose.gpu.yaml "$@"; }
dc logs --no-color vllm > "$S/logs/vllm.log"; dc logs --no-color gateway > "$S/logs/gateway.log"
curl -fsS http://127.0.0.1:8000/metrics > "$S/logs/vllm-metrics-final.prom"
curl -fsS http://127.0.0.1:8081/metrics > "$S/logs/gateway-metrics-final.prom"
cp configs/gpu-pilot.env "$S/"; git rev-parse HEAD > "$S/git-sha.txt"; git status --porcelain > "$S/git-status.txt"
nvidia-smi > "$S/logs/nvidia-smi-final.txt"
python3 scripts/gpu_capture.py scan "$S"          # must print "secret scan clean"
tar -C results -czf "$S.tar.gz" "$(basename "$S")"
(cd results && sha256sum "$(basename "$S").tar.gz") | tee "$S.tar.gz.sha256"
```

From your laptop or Codespace, save the archive off-host and verify it:

```sh
rsync -avP <user>@<gpu-host>:model-serving-lab/results/gpu-*.tar.gz* results/
cd results && sha256sum -c gpu-*.tar.gz.sha256
```

To propose fixtures for the repository, copy the sanitised captures. Review
them before committing.

```sh
F=testdata/streams/vllm-$VLLM_IMAGE_TAG; mkdir -p "$F"
cp "$S"/capture/sanitized/*-success.{headers,body} "$S"/capture/sanitized/*-{unauthorized,unknown-model,context-overflow}.{headers,body} "$F"/
cp "$S/capture/sanitized/vllm-metrics-after.prom" "$S/capture/sanitized/sanitize-provenance.json" "$S/preflight/preflight.json" "$F"/
```

## 13. Stop the stack and the billing

```sh
make gpu-down
docker ps --filter label=com.docker.compose.project=model-serving-lab-gpu -q | grep -q . && echo "STILL RUNNING" || echo "stack stopped"
nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader   # expect no output
date -u +%FT%TZ | tee results/last-gpu-session-stopped-utc.txt
```

Then stop or terminate the instance in the provider console, according to its
billing rules. Retained disks, including the HF cache, can still cost money.
Record the GPU type, total session hours (including startup and idle time)
and spend in BUILD_STATUS.md.

## Notes

- **Admission caps for later E5 arms.** Only the gateway restarts:
  `LAB_ADMISSION_CAP=8 make gpu-up`. Snapshot that condition with
  `python3 scripts/gpu_environment.py --admission-cap 8 --output "$S/environment-cap8.json"`.
  Pass that file to `lab bench --environment` along with `--gateway-admission-cap 8`.
  Omitting the snapshot override deliberately fails if the running cap differs
  from the pin file. Keep the cap disabled (`0`) for the pilot and E1.
- **Never save `docker compose config` output.** It expands credentials.
  `make gpu-config-check` renders it with placeholders only.
- **Native vLLM rejection.** The deployment configures none. Requests beyond
  `max-num-seqs` wait in the engine queue, which the snapshot records as
  `native_rejection_controls`.
