# Build status — 10 November 2026

- **Phase 0 CPU fallback complete; Phase 1 complete.** Go 1.27.3 is pinned in
  `go.mod`, CI and the CPU Dockerfile. Standard library only; Clang 18.1.3
  supports race checks. No prior implementation or Python scaffolding existed.
- Added `lab smoke`, a simulated SSE server with configurable event delay and
  flushed fragmentation, bounded decoding, streamed stdout, separate measurement
  JSON, deadlines, cancellation, usage and failure reporting. Fixtures and mock
  measurements are explicitly synthetic; token counts are invented.
- **Verified:** `make check` (gofmt check, `go vet ./...`,
  `CC=clang CGO_ENABLED=1 go test -race ./...`), `make build`,
  `docker compose -f deploy/compose.cpu.yaml config --quiet`, `git diff --check`.
  HTTP tests required execution outside the agent sandbox, which blocks listeners.
  Native binary smoke checks passed with 3-byte flushed fragments: success exits
  0; truncation exits 1 and preserves text timings in failure JSON.
- Tests cover arbitrary fragmentation, multiline/CRLF, comments, empty/role
  deltas, finish reason, usage, SSE/JSON/HTTP errors, oversized events/lines,
  truncation, early text, output errors, cancellation and all three deadlines.
- Decisions: the `go 1.27.3` directive supplies the initial toolchain pin;
  `go mod tidy` removes an identical redundant `toolchain` directive. Make uses
  `/tmp/model-serving-lab-go-cache` because the default home cache is read-only
  here. Smoke JSON is diagnostic; Phase 3 run artifacts are not implemented.
- **Pending GPU access:** real sanitized successful/usage/error stream captures,
  headers, `/metrics`, startup evidence, vLLM v0.33.0 image compatibility/digest,
  and model/tokenizer commit pins. Candidate settings are recorded in
  `configs/environment.json`; none have been GPU-verified. CPU Compose validates
  but container startup was not tested: this session cannot access the Docker
  daemon. The native CPU path is verified and needs no Docker or model download.
- `BUILD_PLAN.md` is unchanged and its existing `.gitignore` rule is preserved.
- **Review (10 Nov, Claude):** Phases 0 (CPU fallback) and 1 meet their gates.
  Fixes applied: (1) the legacy flat vLLM error body (`{"object":"error",…}`)
  was silently skipped and could report success when followed by `[DONE]`;
  it is now an `sse_error`. (2) The transport's fixed 120s
  `ResponseHeaderTimeout` was removed: header waits are covered by the
  configurable first-text deadline and are now classified `timeout`, not
  `transport_error`. (3) New tests: error-after-text then `[DONE]` (keeps
  first-text timing), legacy error shape, first-text deadline while awaiting
  headers. Re-verified `make check`, `go test -race -count=5`, `make build`,
  `git diff --check` and native smoke (success exit 0; error exit 1).
- **Watch at first GPU capture:** confirm vLLM's real mid-stream error shape
  (the synthetic fixture's `event: error` is not vLLM's format); confirm
  `usage` arrives as a separate `choices: []` chunk.
- **Next action:** Phase 2 gateway — validated byte-preserving forwarding,
  auth, admission (429 + `Retry-After`), deadlines, cancellation, metrics.
  Reuse `internal/sse` and the smoke transport settings.

From the repository root, terminal 1:

```sh
make build
./bin/mockllm --addr 127.0.0.1:8000
```

Terminal 2, also from the repository root:

```sh
./bin/lab smoke --output /tmp/measurement.json
cat /tmp/measurement.json
```

Stop the server with Ctrl-C. Optional CPU containers: `make mock-up` /
`make mock-down`. Verification: `make check`.
