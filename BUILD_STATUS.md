# Build status — 10 November 2026

## Completed
- **Phase 0 (CPU fallback), Phase 1 and Phase 2 complete.** Go 1.27.3;
  one direct dependency, `github.com/prometheus/client_golang v1.25.0`. Fixtures and token counts remain **synthetic**.
- **Phase 2 gateway** (`cmd/gateway`, `internal/gateway`): bearer auth
  (`LAB_GATEWAY_API_KEY`) separate from the upstream credential
  (`LAB_UPSTREAM_API_KEY`); strict whitelist validation (no nulls, integral
  integers, one output limit ≤ `--max-output-tokens`, text-only messages);
  original body bytes forwarded unchanged; immediate semaphore admission
  (`--admission-cap`, 0 disables) with JSON 429 + `Retry-After`; slot held
  from dispatch to terminal outcome and released by `defer`; early flushed
  streaming; shared first-text/idle/total deadlines; per-write downstream
  deadlines; JSON errors before headers, SSE error (no `[DONE]`) after;
  upstream error events (current and legacy vLLM shapes) forwarded and
  recorded as failures; no retries; `X-Lab-Request-ID`, validated
  `X-Request-ID` echo and `X-Lab-Admitted`; JSON logs carry body length and
  SHA-256, never keys or prompts; admin listener `:8081` serves `/healthz`,
  `/readyz` (upstream `/health` + drain state) and `/metrics`; bounded
  shutdown (grace, then SSE `shutdown` error, ≤ grace + write timeout + 1s).
- **Shared code:** `internal/stream` now holds deadlines, event inspection and
  the transport, used by both `lab smoke` and the gateway. Phase 1 tests pass
  unchanged, so the Phase 1 review fixes are preserved. The gateway transport
  sets no per-host connection cap (admission is the only limiter) and keeps
  512 idle connections (`--upstream-idle-conns`).
- The mock now also serves `/health` and enforces `LAB_UPSTREAM_API_KEY` when set.

## Phase 2 review alterations (10 Nov)
- Release admission immediately after forwarding `[DONE]`; an upstream that
  keeps its response open can no longer retain a slot until a deadline.
  Connections already read through EOF remain reusable; otherwise the body
  is closed rather than blocking terminal completion to drain it.
- Reject duplicate JSON keys, including nested and escaped duplicate keys,
  before dispatch. Validated request bytes are still forwarded unchanged.
- Treat `event: error` with empty data as a failure in the shared inspector
  used by the gateway and smoke client.
- Reject upstream URLs containing credentials, queries or fragments and
  malformed readiness paths at startup, preventing ambiguous routing,
  credential logging and readiness-handler failures.
- Correct README dependency guidance: the gateway requires the pinned
  Prometheus Go client and its transitive dependencies.
- Add regression coverage for terminal streams without EOF, duplicate-key
  rejection before dispatch, invalid configuration and empty error events.

## Review verification (10 Nov)
- `make check`: pass (formatting, vet and `go test -race ./...`). The initial
  sandbox run could not open local TCP test listeners; the full check passed
  when rerun with the required access.
- Focused gateway tests, `make build` and `git diff --check`: pass.
- Existing tests cover auth, body parity, early streaming, overload/recovery,
  cancellation, deadlines, failure-path slot release, stalled clients,
  readiness/metrics and bounded shutdown. New regression tests also pass.
- Docker and native smoke/SIGTERM demos were reported passing by the original
  implementer; they were not repeated in this review.

## Pending GPU access
Real sanitized stream captures (including vLLM's real mid-stream error
shape), headers, `/metrics`, startup evidence, vLLM v0.33.0 image digest and
compatibility, model/tokenizer revisions. `configs/environment.json` holds
unverified candidate settings. GPU Compose (`make gpu-up`) is not written.

## Next action
Phase 3: `lab bench` (closed/open-loop), per-attempt JSONL, manifest,
failure-aware summaries, then E1 on the mock.
