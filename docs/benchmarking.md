# CPU benchmark usage

Run from the repository root with Go 1.27.3, Clang and Python 3 available.
These examples use synthetic replay, not model inference. No GPU access is used.
The prepared body requests 128 tokens; the mock fixture reports an invented six.

```sh
make check
make build
# Starts isolated native servers, runs both modes, summarises each, cleans up:
bash scripts/demo-bench.sh
# E1: 2,000 attempts/repeat, three repeats, concurrency 1 and 32, both arms:
python3 scripts/run-e1.py
```

The commands inside `scripts/demo-bench.sh` were executed against native mock
and gateway processes. For an already-running stack, the equivalent commands
below assume `LAB_UPSTREAM_API_KEY` and `LAB_GATEWAY_API_KEY` are set to the
credentials used by those servers. The script creates ephemeral credentials
without saving them. Choose a new output directory for each example session.

```sh
./bin/lab prepare --synthetic --output /tmp/lab-inputs --attempts 200 --rate 100

# Closed loop: fixed four workers, same body across all attempts and repeats.
./bin/lab bench --mode closed --target mock-direct \
  --url http://127.0.0.1:8000/v1/chat/completions \
  --api-key-env LAB_UPSTREAM_API_KEY \
  --workload /tmp/lab-inputs/workload.json \
  --workers 4 --max-inflight 32 --attempts 200 --repeats 3 --warmup 10

# Open loop: 200 saved arrivals over two seconds, independent of completions.
# Set the recorded cap to the actual gateway setting (8 in the demo script).
./bin/lab bench --mode open --target mock-gateway \
  --url http://127.0.0.1:8080/v1/chat/completions \
  --api-key-env LAB_GATEWAY_API_KEY \
  --workload /tmp/lab-inputs/workload.json \
  --schedule /tmp/lab-inputs/schedule.json \
  --max-inflight 32 --repeats 3 --warmup 10 --gateway-admission-cap 8

# Each bench command prints its unique run directory. Supply that path here:
./bin/lab summarize --run results/REPLACE_WITH_RUN_DIRECTORY \
  --output /tmp/summary.json
```

For fully copy-pasteable commands that capture the generated directories, use
`scripts/demo-bench.sh`; its two `lab summarize` invocations require no editing.
`lab summarize --run DIR` also writes JSON to stdout without touching evidence.
The saved run can be summarised after a failed or interrupted command.

`--output` on `bench` is the parent directory, not a file. Each unique run saves
`manifest.json`, `workload.json`, `schedule.json`, `warmup.jsonl`,
`requests.jsonl`, `summary.json` and per-repeat metric capture/status files.
The manifest records source SHA/dirty state, effective deadlines (nanoseconds),
transport settings, generator bounds, warm-up policy, input hashes and supplied
environment metadata, per-prompt body hashes and request settings (model,
sampling and output limits). Unknown engine/revision/GPU settings remain null.
The exact original request bodies are preserved in the workload snapshot. The mock
has no native engine metrics; status files explicitly record unavailability.
E1 saves gateway metrics separately for upstream connection diagnostics.

Custom workload JSON has `schema_version: 1`, `synthetic: true/false`, and a
`prompts` array of `{ "prompt_id": "...", "body": "HTTP JSON as a string",
"sha256": "..." }`. Hashes are checked; an omitted hash is computed on load.
Strings preserve exact HTTP body bytes across snapshots/arms. A schedule has
`schema_version: 1`, `window_ms`, and ordered `arrivals` with `offset_ms` and
`prompt_id`. Reuse those same files to compare targets. Closed-loop cycles
through workload order; open-loop uses the saved prompt order. A prepared
schedule is constant-rate; custom schedules can encode other arrival patterns.

`--timeout`, `--first-text-timeout`, `--idle-timeout` and `--max-event-bytes`
control each request using `internal/stream`. `--max-duration` bounds dispatch
per repeat; `--drain` bounds remaining in-flight work after arrivals or a stop
(default `0` = the total request deadline, so late arrivals are not cut short).
SIGINT/SIGTERM stops dispatch, records unstarted attempts as explicit drops in
that repeat, drains, and exits nonzero with partial evidence. A second signal
aborts the drain immediately; synced records remain summarisable. Disk errors also
exit nonzero and stop dispatch. Request failures are data: an otherwise complete
benchmark exits zero even if some attempts fail. Warm-up failure aborts the run.
Use `--warmup 0` deliberately when testing failure streams or overload handling.

`--environment FILE` saves supplied experiment settings, including resolved
engine controls, model/tokenizer revisions and topology. Do not include secrets.
A snapshot with a nonempty `consistency_mismatches` list (as written by a
failed `scripts/gpu_environment.py` check) is refused before any dispatch.
`--metrics-url URL` saves bounded private metric scrapes before/after repeats
(`repeat-N-{before,after}.prom`). `--gateway-metrics-url URL` does the same for
the gateway's private admin metrics (`repeat-N-{before,after}.gateway.prom`).
Both record a status file and a manifest `metric_captures` entry with its
`source`, including explicit unavailability when the URL is empty.

Error records keep `server_error_type` and `server_error_code` when the
target's error envelope (HTTP error body or SSE error event, current or legacy
vLLM shape, or a gateway error) carries short identifier values. Messages are
never saved, since they can echo prompt text; free-text values become
`unrecognised`.
Credentials are read only from the variable named by `--api-key-env`; they are
not stored in manifests. Redirects and inference retries stay disabled.

See [benchmark-contract.md](benchmark-contract.md) for exact populations,
infinity encoding and partial-run semantics, and [e1-mock.md](e1-mock.md) for
observations and limitations of the synthetic E1 run.


## Saved benchmark configuration

`lab bench --config FILE --output RESULTS` loads a JSON configuration first;
explicit flags override it. Paths are relative to the current working directory.
For example, with the prepared workload and native upstream above:

```sh
cat > /tmp/lab-bench.json <<'JSON'
{
  "mode": "closed",
  "url": "http://127.0.0.1:8000/v1/chat/completions",
  "target": "mock-direct",
  "workload": "/tmp/lab-inputs/workload.json",
  "workers": 4,
  "max_inflight": 32,
  "attempts_per_repeat": 200,
  "repeats": 3,
  "warmup_requests_per_repeat": 10,
  "api_key_env": "LAB_UPSTREAM_API_KEY"
}
JSON
./bin/lab bench --config /tmp/lab-bench.json --output results
```

Config keys match `manifest.config`, plus `workload`, `schedule`,
`environment_file`, `api_key_env` and `output`. Duration fields use integer
nanoseconds (`max_duration_ns`, `drain_ns`, `deadlines_ns` with `Total`,
`FirstText`, `Idle`); CLI duration flags accept Go duration strings instead.
Omitted fields retain defaults. `environment` can contain an inline metadata
object; `--environment FILE` supplies additional overrides. Unknown config keys
and literal credential fields are rejected; put only the environment variable
name in `api_key_env`.
