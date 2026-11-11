# Phase 3 measurement contract

These decisions precede E1 measurements. All current mock observations, usage
counts and timings are synthetic; they establish harness behaviour, not GPU
capacity or token latency.

- A prepared workload stores each HTTP JSON body as a string plus SHA-256.
  This preserves whitespace and omitted defaults exactly when snapshots are
  reloaded. Manifest workload/schedule hashes identify their compact JSON
  representation (rather than the pretty-printed snapshot bytes). Open-loop
  schedules store offsets, prompt IDs and an explicit
  measurement-window length; the same file is replayed for each repeat/arm.
- Closed-loop attempts are assigned to a fixed worker pool, with the configured
  attempt count as the denominator. Open-loop arrivals never wait for a permit:
  a full generator records a drop. Writer backpressure can cause dispatch lag
  and is measured rather than hidden by an unbounded queue.
- The drain bound defaults to the effective total request deadline
  (`--drain 0`, resolved and saved in the manifest). Review change, made before
  any open-loop GPU comparison: the earlier 10 s default was far shorter than
  the 120 s first-text and 300 s total allowances, so under overload, arrivals
  late in the window would be cut as `drain_timeout` while earlier arrivals got
  the full deadline. That censoring depended on arrival position, not service.
  A shorter explicit drain remains available and is recorded. A first
  SIGINT/SIGTERM starts the drain; a second aborts immediately, leaving synced
  records and a `running` manifest for offline summarisation. The synthetic E1
  runs used the old 10 s value; it applies to closed loops only on interruption,
  so their results are unaffected.
- Interruption/run limits stop dispatch and allow only bounded draining.
  Unstarted attempts in the interrupted repeat are recorded as drops with an
  explicit reason. Thus its offered population is the frozen planned set,
  including canceled future arrivals. Interrupted repeats are identified and
  excluded from across-repeat aggregates; their individual counts remain visible.
- A single writer syncs every JSONL record; bounded channel buffers absorb
  only limited pending work. Startup snapshots and manifests use atomic replacement and directory sync. Disk errors
  stop dispatch/cancel requests and return nonzero. A final unterminated JSONL
  line is reported as partial evidence by the offline reader; interior corruption
  fails. Raw completed lines remain available even without a final manifest.
- Direct responses have admission unknown unless explicit evidence exists.
  Generator drops are admission=false because they were never dispatched.
  HTTP 429 is rejection, with separate gateway-rejected and upstream-429 counts
  and rates (`gateway_rejection_rate`, `upstream_429_rate`); `rejection_rate`
  and `rejection_latency_ms` include both kinds. Use the gateway-only rate for
  E5's admission 429 fraction.
  A gateway rejection requires admission=false and no upstream-status header;
  admission=true plus HTTP 429 is an admitted upstream rejection, penalised in
  both offered and admitted populations. Lost responses remain unknown.
- First byte is dispatch to first response-header byte (httptrace). First/last
  text and chunk gaps refer to nonempty text events, not tokens. Completion is
  dispatch to terminal outcome; [DONE] timing is separate. Failed requests keep
  observed text timings but receive infinity in service-penalised distributions.
- Percentiles use nearest rank. Missing observations have value=null,
  unbounded=false, undefined=true; infinity has value=null, unbounded=true,
  undefined=false. p99 is reported only with at least 1,000 population members.
  Aggregate median/min/max applies to each repeat statistic without pooling raw
  attempts. Infinity participates in sorting and remains explicit.
- Open-loop goodput is successful completions meeting the scheduled-arrival
  first-text objective divided by the saved arrival window, including bounded
  drain outcomes. Closed-loop uses dispatch first-text latency and elapsed repeat
  duration. Success rate and objective compliance use separate explicit counts.
- Usage coverage is reported, along with engine-reported output lengths and
  deviations from requested output length. Mock fixtures do not honour requested
  generation lengths: their invented six-token usage must not be treated as
  calibration. All E1 arms use the same fixture and request bytes.
- Transport is HTTP/1.1 with max(512, max-inflight) idle connections (total
  and per host; the manifest records the transport's actual values), no hidden per-host active
  connection cap, no redirects or inference retries. Connection gets/reuse and
  idle reuse are recorded per attempt. Terminal success, HTTP errors and SSE
  errors get at most a 5 ms EOF drain (64 KiB);
  timing ends before that cleanup. This allows client connection reuse without
  an unbounded EOF wait. Gateway upstream reuse is observed separately, not assumed.
- Environment settings are supplied as a saved JSON object. Unknown engine,
  GPU and revision fields remain explicitly null. Metric captures have status
  evidence; the fixture mock has no native engine metrics. Warm-up is sequential,
  excluded from measured counts, saved separately, and fully completed before
  each repeat. CPU E1 also saves host process CPU counters externally.

## Summary populations and units

`counts` are attempts, except connection counters, which count transport
connection gets. Rates are fractions of the offered population, except
`usage_coverage_dispatched` (usage-bearing/dispatched),
`connection_reuse_fraction` (reused/gets), the gateway/upstream-429 split of
`rejection_rate`, `known_admitted_success_rate`
(successes/admitted), and goodput (requests/second). `failure_rate` counts
terminal failures; rejections and generator drops have separate rates. Their
sum with success rate is one for a fully accounted repeat. Explicit unknown
admission counts include dispatched requests without evidence and missing
records after a crash, but exclude recorded generator drops.

`offered` includes every frozen attempt in a started repeat; `known_admitted`
contains only admission=true records. Their `success_*` timing distributions
include observed timings from successful requests only; `penalised_*` use the
whole population, assigning unsuccessful or unobserved timings infinity.
All these timing values use milliseconds. `completion` is terminal elapsed
time, not time to last text. Undefined statistics remain explicit JSON objects.

Diagnostics use all observed records, including failures: dispatch delay and
rejection terminal latency are milliseconds; chunk gaps are milliseconds
between text events in the same request; prompt/completion lengths are reported
tokens; text lengths are bytes. Usage is never inferred from text. Output-length
deviations compare observed usage against the saved body's output limit.
A summary missing a final manifest/window marks its repeat incomplete; a
closed-loop partial window is inferred from the latest durable terminal record.
Unstarted repeats are counted separately and have no invented observations.
The summary `synthetic` flag is true if the workload was synthetic or any saved
response carried `X-Lab-Synthetic: true`.
