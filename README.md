# model-serving-lab

A Go inference streaming client and simulated server, developed toward an
admission-control experiment around vLLM. Phases 0–1 use CPU-only Codespaces;
all current fixtures and token counts are **synthetic**. No GPU performance
results or real vLLM protocol captures are claimed.

Requires Go **1.27.3**. Race checks use Clang (tested with 18.1.3).
No model downloads or third-party Go dependencies are needed.

```sh
export GOCACHE=/tmp/model-serving-lab-go-cache
make check
make build
# Terminal 1, from the repository root:
go run ./cmd/mockllm --addr 127.0.0.1:8000
# Terminal 2:
go run ./cmd/lab smoke --output /tmp/measurement.json
cat /tmp/measurement.json
```

Text streams to stdout; the measurement is saved separately even on stream
failure, and the CLI exits nonzero on failure. `--url`, `--model`, `--prompt`,
`--api-key` (or `LAB_API_KEY`), `--timeout`, `--first-text-timeout`,
`--idle-timeout` and `--max-event-bytes` configure the client. No inference
retries or redirects are performed. Defaults: 300s total, 120s first text and
read idle, 1 MiB events. First byte uses `httptrace`; text timing tracks nonempty
content events, not tokens. Null timings mean unobserved events. Smoke JSON is
an individual diagnostic observation, not the Phase 3 benchmark/run format.

Server options: `--delay 40ms` before each event, `--fragment-bytes 1` to flush
small fragments, and `--fixture testdata/streams/synthetic-error.sse` (or
`synthetic-truncated.sse`) for failure replay. The server is a fixture source,
not a request validator or inference engine. It binds loopback by default,
limits request bodies, flushes events and stops on cancellation.

Optional container path: `make mock-up` / `make mock-down` (Docker Compose).
Use only one server on port 8000 at a time. Local commands are the primary
Codespaces path. `make gpu-up` reports the unresolved GPU prerequisites.

Layout: `cmd/` entry points, `internal/` protocol/client/replay code,
`testdata/streams/` synthetic fixtures and provenance, `configs/` environment
pins, `deploy/` CPU container setup, `docs/` experiment hypotheses. Gateway,
load generation, summaries and GPU deployment belong to later phases.
See [BUILD_STATUS.md](BUILD_STATUS.md) for verification and pending work.
