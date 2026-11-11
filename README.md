# model-serving-lab

A Go streaming gateway, client and simulated server, developed toward an
admission-control experiment around vLLM. Phases 0–3 use CPU-only Codespaces;
all current fixtures and token counts are **synthetic**. No GPU performance
results or real vLLM protocol captures are claimed.

Requires Go **1.27.3**. Race checks use Clang (tested with 18.1.3).
No model downloads are needed. The gateway uses the Prometheus Go client;
Go downloads the pinned dependencies on the first build.

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

Gateway (Phase 2): `LAB_GATEWAY_API_KEY` is required from clients;
`LAB_UPSTREAM_API_KEY` (optional) is sent upstream and enforced by the mock.
Public API on `127.0.0.1:8080`, admin (`/healthz`, `/readyz`, `/metrics`) on
`127.0.0.1:8081`. `--admission-cap N` (0 disables) returns 429 with
`Retry-After` when full. Run `./bin/gateway -h` for deadlines and limits.

```sh
export LAB_GATEWAY_API_KEY=dev-gateway-key
./bin/mockllm &                     # upstream on :8000
./bin/gateway --admission-cap 64 &  # gateway on :8080, admin on :8081
LAB_API_KEY=$LAB_GATEWAY_API_KEY ./bin/lab smoke --url http://127.0.0.1:8080/v1/chat/completions
```

Optional container path: `make mock-up` / `make mock-down` (Docker Compose;
mock, gateway and admin ports; requires `LAB_GATEWAY_API_KEY`).
Use only one server on port 8000 at a time. Local commands are the primary
Codespaces path. `make gpu-up` reports the unresolved GPU prerequisites.

Layout: `cmd/` entry points, `internal/` protocol/client/replay code,
`testdata/streams/` synthetic fixtures and provenance, `configs/` environment
pins, `deploy/` CPU container setup, `docs/` experiment hypotheses and results.
Phase 3 includes closed/open-loop benchmarks and offline summaries. GPU
deployment and real-engine experiments belong to Phase 4.
See [BUILD_STATUS.md](BUILD_STATUS.md) for verification and pending work.


Benchmark quick start (synthetic, native servers are started and cleaned up):

```sh
make build
bash scripts/demo-bench.sh
python3 scripts/run-e1.py
```

The demo runs and summarises both modes. E1 uses 2,000 attempts per repeat,
three repeats, direct/gateway targets at concurrency 1 and 32. Each benchmark
prints its unique run directory under `results/`; raw records are synced to
`requests.jsonl` before offline summarisation. See
[benchmarking.md](docs/benchmarking.md) for tested commands and configuration,
[benchmark-contract.md](docs/benchmark-contract.md) for failure/partial-run
semantics, and [e1-mock.md](docs/e1-mock.md) for measured synthetic results.
