# Build status — 11 November 2026

## Completed
- **Phases 0 (CPU fallback), 1, 2 and 3 complete.** Go 1.27.3; the only
  direct dependency remains the Prometheus Go client. Fixtures, usage counts
  and all current benchmark timings are **synthetic**.
- Gateway auth, strict byte-preserving validation, immediate admission,
  deadlines, cancellation, bounded shutdown and private admin metrics remain
  covered by the Phase 2 suite. Its review fixes (duplicate keys, terminal
  capacity release, empty errors and upstream configuration) are preserved.
- **Phase 3:** `lab prepare`, `lab bench` (closed/open, JSON `--config`
  plus flag overrides), `lab summarize`;
  saved exact request bodies/hashes and arrival schedules; fixed workers,
  bounded in-flight work, explicit drops/dispatch lag, per-request deadlines,
  interruption and bounded draining, no inference retries.
- Unique run directories save manifests, workload/schedule snapshots,
  per-repeat identities, synced JSONL observations, separate warm-up records,
  summaries and metric capture/status evidence. Disk errors stop the run;
  partial evidence remains readable. Credentials are environment-only.
- Offline summaries retain failed text timings, distinguish gateway rejection
  from admitted upstream 429, account for missing/unknown records, use
  nearest-rank success-only and infinity-penalised populations, and report
  scheduled-arrival goodput, rates, usage/output lengths and repeat spread.
  The contract and rationale are in `docs/benchmark-contract.md`.

## Phase 3 review fixes (11 November)
- **Drain default now tracks the total request deadline** (`--drain 0`,
  resolved into the manifest). The old 10 s default would cut late open-loop
  arrivals as `drain_timeout` long before their 120/300 s deadlines, biasing
  overload results by arrival position. A first signal drains; a second
  aborts (synced records remain summarisable). Rationale recorded in
  `docs/benchmark-contract.md`; E1 (closed loop) is unaffected.
- Generator idle pool is `max(512, max-inflight)`; the manifest records the
  transport's actual settings instead of duplicated constants.
- Summaries add `gateway_rejection_rate` and `upstream_429_rate` alongside the
  combined `rejection_rate` (E5 needs the gateway-only 429 fraction).
- Summary `synthetic` is also set by any `X-Lab-Synthetic` response evidence.
- Regression tests added for each fix.

## Verification
- 11 Nov review: `make check`, `make build`, `git diff --check` pass;
  `go test -race -count=10 ./internal/bench ./cmd/lab` pass;
  `scripts/demo-bench.sh` pass; native SIGTERM drain/abort check pass
  (manifest left `running`, in-flight attempts counted as missing failures).
  Second-signal abort cannot work for SIGINT when the process inherits SIGINT
  as ignored (e.g. a background job in a non-interactive script); use SIGTERM.
  E1 was not rerun (its closed-loop results do not depend on these fixes).
- 10 Nov (Phase 3 build): `make check`: pass (formatting, vet, all race tests); `make build`: pass;
  `git diff --check`: pass. HTTP tests need local-listener access, which the
  initial sandbox run blocked; the authorised rerun passed.
- Repeated `go test -race -count=10 ./internal/bench ./cmd/lab`: pass using the
  Makefile's Clang/CGO/cache settings. Tests cover accounting, concurrency,
  drops/lag, post-text failures, all deadlines, 429 admission, EOF/reuse,
  bounded draining, interrupted/missing/torn records, infinity, disk errors,
  config precedence and disabled transport replay.
- Native `scripts/demo-bench.sh`: pass for both modes and offline summaries.
  `docs/benchmarking.md` contains usage and copy-pasteable script commands.
- **E1 mock:** 2,000 attempts × three repeats × direct/gateway × concurrency
  1/32 = 24,000 successes; identical bodies and matched client transports.
  Offline and end-of-run summaries agree. Raw evidence:
  `results/e1-phase3-final/` (ignored); repository extract/report:
  `docs/e1-mock-summary.json`, `docs/e1-mock.md`.
- E1 median first-text p50 differences: +0.427 ms at concurrency 1, +1.729 ms
  at 32. Gateway concurrency-32 repeat p95s varied 18.110–20.329 ms; client
  connection reuse was 97.75–98.85%. CPU and upstream reuse are in the report.
  Six invented output tokens differ from the requested 128 in every replay.
  These results describe CPU harness behaviour, not real-engine capacity.
- Docker was not rerun: deployment files were unchanged in Phase 3.

## Pending live checks and next action
Commit Phase 3, then optionally rerun `scripts/run-e1.py` from the clean
commit so E1 manifests record `git_dirty=false` (the current extract is from
a dirty tree). Phase 4 workload prep still needs calibrated multi-prompt
buckets (E3) and shared/unique-prefix workloads (E4); `lab prepare` currently
emits one constant-rate prompt.

Phase 4 requires authorised GPU access: real sanitised streams/error shapes,
headers, native metrics, image digest/compatibility, model/tokenizer revisions,
GPU deployment, output-length calibration and the GPU E1 confirmation. Freeze
resolved scheduler/cache controls after the pilot before E2–E5. Candidate
settings in `configs/environment.json` remain unverified. No GPU spend incurred.
