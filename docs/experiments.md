# Predictions to test

Synthetic E1 replay results are recorded in [e1-mock.md](e1-mock.md).
They describe harness overhead and variability, not GPU performance.

| Question | Prediction |
| --- | --- |
| Q1: gateway overhead | Small absolute overhead, potentially obscured by GPU variability. |
| Q2: saturation | Queueing eventually increases first-text latency and failure risk. |
| Q3: prompt length | Longer input increases prefill work; scheduling affects magnitude. |
| Q4: prefix reuse | Warm repeated prefixes help prefill, with little benefit for unique prefixes. |
| Q5: admission | Some caps may preserve admitted latency through fast rejection; overly small caps may reduce goodput. |

Record disagreements with measurements. GPU pilot, frozen controls, final runs and charts are pending.


E1 mock observations: the difference in median first-text p50 between arms was
0.427 ms at concurrency 1 and 1.729 ms at concurrency 32. Concurrent gateway
first-text p95 varied substantially across repeats (18.110–20.329 ms). The
small-overhead prediction describes the serial median more closely than the
concurrent tail. Three repeats, 2,000 attempts each, succeeded in all four
conditions. All usage and timings remain synthetic; GPU confirmation is pending.
