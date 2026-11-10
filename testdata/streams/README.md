# Synthetic fixtures

Every `.sse` file here is handwritten synthetic data, not a real vLLM capture.
`synthetic-success.sse` includes role-only/empty deltas, text, a finish reason,
a usage-only event and `[DONE]`. Token counts are invented. Error and truncated
variants exercise terminal failures. Multiline, CRLF and arbitrary fragmentation
are constructed in parser tests. Replay changes timing and network framing;
text chunks are not token timings.

Real capture is **pending until a GPU environment is available**. At the first
authorised GPU session capture sanitized successful, usage and error streams,
response headers, raw `/metrics`, startup settings, model/tokenizer revision,
image tag/digest, GPU/driver and capture commands. Keep real captures and their
provenance distinct from these files. No real metric scrape exists yet.
