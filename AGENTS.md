# Model Serving Lab

## Project and task context

This is a Go gateway and benchmarking toolkit for a single-GPU vLLM deployment.
Its purpose is to measure streaming latency, capacity and admission-control
trade-offs.

Read `BUILD_STATUS.md` for current progress and verification evidence.

Use `BUILD_PLAN.md`, when present, for the requested phase's specification.
It is an intentionally ignored local file. Keep its existing ignore rule.

Inspect the current code, tests and `Makefile` before editing. Check these
instructions against the checkout and correct outdated paths or commands.

Follow the user's current task scope. Record material changes to the design
or measurement contract, including their rationale, before comparing results.

## Code map

- `cmd/lab` contains the `smoke`, `prepare`, `bench` and `summarize` CLI entry
  points. `internal/smoke` implements the smoke client; `internal/bench` owns
  benchmark scheduling, records, persistence and offline summaries.
- `cmd/mockllm` and `internal/mockllm` contain the simulated inference server.
- `cmd/gateway` and `internal/gateway` contain the gateway.
- `internal/sse` contains SSE framing and decoding. `internal/stream` contains
  shared event inspection, transport and deadline behaviour.
- `deploy/` contains container configuration (`compose.gpu.yaml` for vLLM plus
  gateway). `configs/` contains experiment settings; `configs/gpu-pilot.env`
  holds the GPU pins and engine flags.
- `scripts/` contains the CPU demo/E1 runners and GPU-session helpers
  (preflight, environment snapshot, capture/sanitise, cancellation, pilot
  table), tested by `make gpu-config-check`. `docs/gpu-runbook.md` is the GPU
  session procedure.
- `testdata/` contains fixtures. `docs/` contains explanations and experiment
  reports.

## Implementation approach

Reuse shared streaming and transport code across clients and the gateway.

Keep code idiomatic and readable, with explicit ownership of response bodies,
cancellation, admission permits and terminal result records.

Add targeted tests for changed behaviour and specific regressions. Extend
existing tests where practical.

Keep changes focused. Explain new dependencies and preserve the configured
toolchain unless the task requires a change.

Read credentials from environment settings and keep them out of logs,
command examples with real values, fixtures and result manifests.

## Streaming and gateway invariants

Validate requests, then forward their original body bytes unchanged.
Direct/gateway comparisons use identical payloads and matched settings.

Flush incremental SSE events. Treat explicit SSE errors and EOF without
`[DONE]` as failure, preserving any text timings already observed.

Support the tested current and legacy vLLM error envelopes. Keep transport
errors, deadlines, client cancellation and downstream write failures distinct.

Preserve upstream cancellation and per-write deadlines. Release admission
capacity on every terminal path. Keep inference retries disabled.

Retain separate gateway and upstream credentials and private admin endpoints.

Distinguish gateway admission rejection from upstream errors after admission,
including upstream 429s. Preserve true/false/unknown admission evidence.

## Measurement invariants

Label simulated streams, invented token counts and mock timings as synthetic.
Establish real-vLLM claims with actual engine captures or runs.

Keep client chunk-gap measurements distinct from token-level metrics.
State the population, timing boundary and units for every statistic.

Account for scheduled arrivals, generator drops, scheduling delay and failed
attempts. Use scheduled-arrival timing for open-loop goodput.

Report success-only and failure-penalised latency summaries separately.
Preserve infinite penalties explicitly and report admission-unknown counts.

Save raw observations and experiment settings before summarising. Preserve
partial evidence on interruption and surface result-writing failures.

Keep cache, scheduler, output-length and transport controls consistent within
comparisons. Record configuration changes as separate experimental conditions.

## Verification and handoff

Run the following after implementation changes, using the Makefile's configured
compiler and cache settings:

```sh
make check
make build
git diff --check
```

Use relevant native smoke or container checks when changing the request path or
deployment. Run repeated race tests when concurrency changes or flaky behaviour
justify them. Report each check as passed, failed or blocked based on actual
execution.

Keep `BUILD_STATUS.md` concise, with completed work, verification, pending live
checks and the next action. Update usage documentation when commands change.
Finish with a short explanation of the change, checks and material limitations.

Use paid GPU resources within the user's authorised access and spend allowance.
Save results off-host and end rented sessions according to provider billing rules.
Commit or push changes when requested by the user.
