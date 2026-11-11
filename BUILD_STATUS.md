# Build status — 11 November 2026

## Completed
- **Phases 0 (CPU fallback), 1, 2 and 3 complete.** Go 1.27.3; the only
  direct Go dependency remains the Prometheus client. Fixtures, usage counts
  and all current benchmark timings are **synthetic**.
- Gateway: auth, strict byte-preserving validation, immediate admission,
  deadlines, cancellation, bounded shutdown, private admin metrics.
- Phase 3: `lab prepare`, `lab bench` (closed/open, `--config` plus flag
  overrides), `lab summarize`. The contract is in `docs/benchmark-contract.md`.
  - Saved body bytes and schedules; per-attempt records, including drops and
    dispatch lag; bounded draining. The drain defaults to the total deadline.
  - Failure-aware nearest-rank summaries with infinity penalties, separate
    gateway/upstream-429 rates and scheduled-arrival goodput.
  - Review fixes (drain censoring, idle pool sizing, rejection rates,
    synthetic evidence) are covered by regression tests.
- **Phase 4 preparation (no GPU used yet):**
  - `deploy/compose.gpu.yaml` runs vLLM (`vllm serve`, image pinned by
    digest) serving Qwen/Qwen2.5-7B-Instruct as `lab-model`. Settings come
    from the plan defaults: 4,096 context, `dtype auto`, 0.90 memory, 64 seqs,
    4,096 batched tokens, chunked prefill on, prefix caching off,
    `--generation-config vllm`, and the model/tokenizer revision pinned.
  - The gateway runs with cap 0 (disabled), 120/120/300 s deadlines and
    loopback-only ports 8000/8080/8081.
  - Credentials are environment-only; vLLM takes `VLLM_API_KEY`.
  - Pins and settings live in `configs/gpu-pilot.env`. Empty digest/revision
    pins make Compose refuse to start.
  - `scripts/gpu-preflight.sh` checks:
    - host tools and the GPU;
    - an in-container torch BF16 matmul with `--gpus all`;
    - support for every serve flag in the pinned release.

    It also resolves and pins the registry digest, the HF revision (checking
    tokenizer/config files) and the prefix-cache flag spelling.
  - `scripts/gpu_environment.py` snapshots pins plus running-stack evidence
    (image digest, GPU/driver, `/v1/models`, `vllm:cache_config_info`) for
    `lab bench --environment`. It fails when the stack contradicts the pins.
  - `scripts/gpu-capture.sh` sends identical body bytes to both paths.
    - It verifies `[DONE]`, usage, 128 completion tokens, matched usage and
      gateway body-SHA parity.
    - It captures 401/404/400 envelopes (direct and relayed), metrics,
      `/version` and `/v1/models`.
    - It sanitises (IDs, timestamps, volatile headers) after a secret scan.
  - `scripts/gpu-cancel-check.sh` disconnects mid-stream on both paths. It
    requires the engine gauges and the gateway in-flight gauge to return to
    zero and records the gateway outcome.
  - `scripts/pilot_table.py` tabulates runs and suggests an open-loop rate
    (1.25 × best healthy closed-loop throughput).
  - `make gpu-up/gpu-down/gpu-config-check/lab-docker`. The procedure is in
    `docs/gpu-runbook.md`.

## Phase 4 review fixes
- Confirmed all three review findings with synthetic regression cases.
- Environment snapshots now require runtime evidence and compare both running
  containers' entrypoints/arguments with the declared condition. Missing data,
  stale revisions/settings and extra flags fail; raw argv/env are not saved.
- Gateway admission is checked against its metric. `--admission-cap` declares
  an override explicitly, preventing cap-8 runs from being labelled cap-0.
- Config checks allow positive sequence/batch limits and memory utilisation in
  (0, 1], requiring consistency with `scheduler_candidate`. The runbook records
  adjustments and reloads shell exports before restarting.
- These checks protect measurement labels; they do not establish real-engine
  behaviour. No GPU run or result comparison was performed.

## Verification (11 November)
- Passed: `make check`, `make build`, `git diff --check`,
  `make gpu-config-check` (15 Python tests, the compose render, resolved-setting and
  `configs/environment.json` consistency).
- Negative config checks pass. A mutated compose file (public port, drifted
  `max-num-seqs`, literal key) is rejected, and empty pins are refused.
- `make lab-docker` built a static `bin/lab` in `golang:1.27.3`.
- Offline rehearsal used a throwaway **fake** vLLM plus the real gateway
  binary. These are script-logic checks only, with no engine behaviour:
  - `gpu-capture.sh`: all checks pass;
  - `gpu-cancel-check.sh`: PASS (outcome `client_canceled`);
  - runbook steps 10–11 (closed sweep, gateway spot check, rate suggestion,
    same-body check, open loop on both arms, metric captures, pilot table);
  - session secret scan: clean.
- Earlier Phase 3: repeated race tests, native demo, signal drain/abort, and
  synthetic E1 (`docs/e1-mock.md`; 24,000 successes; first-text p50 +0.427 ms
  at concurrency 1 and +1.729 ms at 32). E1 extract is from a dirty tree.

## Pending live checks (need authorised GPU access)
- Host/toolkit compatibility of `vllm/vllm-openai:v0.33.0` (CUDA 13 default).
  Substitute a documented compatible image if needed and record why, once.
- Pin and commit `VLLM_IMAGE_DIGEST` and `MODEL_REVISION`, and confirm the
  pinned release's flag spellings, including for prefix caching.
- 7B memory fit at the plan defaults. Record any adjustment, then freeze.
- Engine-reported settings match the pins (`gpu_environment.py`).
- Real streams: usage/128-token length on both paths, body parity, real error
  envelopes, real `/metrics` names; sanitised fixtures in `testdata/`.
- Engine release on client cancellation (direct and gateway).
- Closed-loop sweep and 60 s open-loop pilot. Then freeze the rate, the
  objective and the engine settings before E2–E5; GPU E1 confirmation; native
  benchmark cross-check.

## Next action
Commit Phase 4 prep, then run `docs/gpu-runbook.md` on the GPU host. Record
GPU type, session hours and spend here afterwards. Still to build after the
pilot: calibrated prompt buckets (E3), prefix workloads (E4) and
Prometheus/Grafana.
