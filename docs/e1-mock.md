# E1: synthetic mock direct versus gateway

Executed 2026-11-10 on the native CPU Codespace; no inference or GPU
measurements. Raw records, manifests, snapshots, metric captures and offline
summaries are in `results/e1-phase3-final/` (intentionally ignored). The repository evidence
extract is [e1-mock-summary.json](e1-mock-summary.json).

## Conditions

- 2,000 offered attempts per repeat; three repeats per target/concurrency:
  24,000 measured attempts, plus 32 sequential warm-ups before each repeat.
- Identical body SHA-256: `63e7d0b9e72d9a922f833b4fb695637839fbc99684e8bee269788c3392a84fcd`.
  Temperature 0, top_p 1, seed 42, ignore_eos true, requested 128 output tokens.
- `synthetic-success.sse`, 1 ms delay before each fixture event. Usage is invented
  (12 prompt/6 completion tokens); all attempts flag output-length deviation.
- Generator HTTP/1.1, 512 total/per-host idle connections, unlimited transport
  active connections; safety ceiling 256; gateway cap 0. Both endpoints require
  separate ephemeral bearer credentials. Defaults: 120 s first text/idle, 300 s
  total; drain 10 s, dispatch limit 10 min. No retries/redirects.
- Native loopback on one shared CPU host. Gateway logs enabled; each request
  record is synchronously persisted. No agent builds or tests ran alongside
  the final measured run; external host activity remains uncontrolled.
- Run order: direct/1, gateway/1, gateway/32, direct/32. Sequential warm-ups do
  not prewarm a 32-connection pool; repeats retain each process transport pool.
- The working tree was dirty; manifests record its base source SHA and settings.
  No native engine metrics exist on this mock; status files say so. Gateway
  `/metrics` captures supply the upstream connection deltas below.

## Observations

All 24,000 attempts succeeded, with no rejection, drop, failure or missing record.
Usage coverage was 100%; gateway admission was true for every gateway response.
Direct admission remained unknown (no gateway admission evidence). All successes
met the 1,000 ms first-text objective. Timing values below are milliseconds from
dispatch to the first response-header byte or first nonempty text event.

Each cell is **median of three repeat statistics [min, max]**; attempts are not pooled.

| Target | Workers | First-byte p50 | First-byte p95 | First-text p50 | First-text p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| mock-direct | 1 | 1.306 [1.303, 1.319] | 1.978 [1.854, 1.987] | 3.579 [3.571, 3.588] | 4.625 [4.520, 4.634] |
| mock-gateway | 1 | 1.732 [1.710, 1.740] | 2.669 [2.570, 2.782] | 4.005 [3.961, 4.011] | 5.224 [5.073, 5.265] |
| mock-gateway | 32 | 3.126 [2.820, 3.389] | 14.681 [13.327, 15.756] | 6.295 [5.946, 6.617] | 18.362 [18.110, 20.329] |
| mock-direct | 32 | 1.699 [1.697, 1.733] | 3.148 [3.057, 3.262] | 4.566 [4.564, 4.612] | 6.343 [6.244, 6.434] |

At concurrency 1, the difference between arm medians of repeat first-text p50 was +0.427 ms; first-byte p50 differed by +0.426 ms. Gateway first-text p95 ranged 5.073–5.265 ms, versus 4.520–4.634 ms direct.

At concurrency 32, the difference between arm medians of repeat first-text p50 was +1.729 ms; first-byte p50 differed by +1.428 ms. Gateway first-text p95 ranged 18.110–20.329 ms, versus 6.244–6.434 ms direct.

These are differences of statistics from sequential arms, not paired per-request
causal effects. Repeat spread and concurrent tails matter; a single median is
not a sufficient description of gateway overhead. The small-overhead prediction
must be assessed alongside this variation; there is no GPU confirmation yet.

## Connection and CPU evidence

| Target | Workers | Client reuse per repeat | Upstream reused/new per repeat | Mock CPU s | Gateway CPU s | Generator + offline summary CPU s |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| mock-direct | 1 | 100.00%, 100.00%, 100.00% | not applicable | 3.82 | 0.00 | 4.08 |
| mock-gateway | 1 | 100.00%, 100.00%, 100.00% | 2000/0, 2000/0, 2000/0 | 4.06 | 5.69 | 4.34 |
| mock-gateway | 32 | 97.75%, 98.45%, 98.85% | 1972/28, 2000/0, 2000/0 | 1.36 | 3.32 | 2.47 |
| mock-direct | 32 | 98.45%, 99.95%, 100.00% | not applicable | 1.20 | 0.00 | 2.11 |

CPU values are accumulated process user+system seconds over all three repeats,
warm-up, metric captures and summary work, not CPU percentages or per-request
latencies. Mock/gateway use Linux `/proc` ticks; generator/summary use child
resource usage. Gateway rows include validation, relay, logs and metrics work.
No model-efficiency claim follows from mock CPU differences.

Sequential warm-up means the first concurrent repeat grows its connection pool.
Finite 5 ms client EOF cleanup can close late responses. Inspect both client
reuse and gateway upstream reuse rather than assuming the pools eliminate churn.
Transport/retry settings match across arms. Closed-loop workers also wait on the
bounded record writer between attempts: observed throughput is not engine capacity.

This is a synthetic harness regression/overhead observation on a shared CPU
host. It has no real tokenizer, scheduler, cache, fixed-length generation or GPU
evidence. Arm order, host noise, timer granularity, fsync backpressure and
sequential warm-up limit causal interpretation. GPU confirmation and calibrated
output lengths remain Phase 4 work. All offline summaries matched final summaries.

## Reproduce

```sh
make build
python3 scripts/run-e1.py
# Supply the root printed by the runner to regenerate this report:
python3 scripts/report-e1.py --root results/e1-phase3-final
```

The runner saves a unique root with per-run evidence, CPU counters and
`e1-index.json`, invokes offline summarisation, and stops its servers. The
included extract belongs to the recorded run; a future rerun produces new data.
