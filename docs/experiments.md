# Predictions to test

No performance conclusions have been drawn from synthetic replay.

| Question | Prediction |
| --- | --- |
| Q1: gateway overhead | Small absolute overhead, potentially obscured by GPU variability. |
| Q2: saturation | Queueing eventually increases first-text latency and failure risk. |
| Q3: prompt length | Longer input increases prefill work; scheduling affects magnitude. |
| Q4: prefix reuse | Warm repeated prefixes help prefill, with little benefit for unique prefixes. |
| Q5: admission | Some caps may preserve admitted latency through fast rejection; overly small caps may reduce goodput. |

Record disagreements with measurements. GPU pilot, frozen controls, final runs and charts are pending.
