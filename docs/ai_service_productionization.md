# AI Service Productionization

Status: design proposal, not yet implemented.
Scope: `ai_service/` (Kafka consumer → trigger detection → retrieval → RAG generation → publish).

Baseline observed in the codebase today (see file references throughout):
- One `AiServiceConsumer` process, one Kafka consumer group (`ai-service-group`), no partition/scaling config, no Dockerfile/replica setup.
- Retrieval and generation run fully inline per message: embed → pgvector search → build prompt → curl subprocess to NVIDIA NIM → publish → commit offset. No concurrency within a process.
- No rate limiting anywhere toward NIM or per-user/per-channel.
- No idempotency/dedup for AI replies — acknowledged and deliberately unsolved in `ai_service/consumer.py`'s docstring.
- No Redis usage in `ai_service/` at all (Redis exists in `app/` for chat-ingest dedup and presence).
- No structured logging, metrics, or tracing in `ai_service/` — only `logging.basicConfig` text logs. `app/core/metrics.py` and `app/core/structred_log.py` are the only existing observability patterns in the repo, both scoped to the ingest path.
- No dedicated connection/thread pools for embedding or NIM calls — CPU-bound embedding runs on asyncio's default executor; NIM calls are one `curl` subprocess per request with no cap.

---

## 1. Rate Limiting

### Goal
Bound both (a) how much AI traffic a single user or channel can generate, and (b) total outbound calls to NVIDIA NIM, without adding a new dependency — reuse the Redis instance and `SET NX`-style pattern already used in `app/core/dedup.py`.

### Design: token bucket in Redis, checked before generation

Two independent limits, checked in the consumer's message handler (`ai_service/consumer.py`) right after `pipeline.detect()` returns a trigger match and before any embedding/NIM work happens — reject cheaply, before spending CPU or a NIM call:

```
rate:user:{sender_id}   → N requests / 60s   (default: 5/min)
rate:channel:{channel_id} → M requests / 60s (default: 20/min)
```

Implementation: Redis `INCR` + `EXPIRE NX` (fixed window) is enough here — token bucket's burst-smoothing isn't worth the extra Lua script complexity for a feature that just needs "stop abuse," not precise pacing. Sketch:

```python
async def check_rate_limit(key: str, limit: int, window_s: int) -> bool:
    count = await redis_client.redis.incr(key)
    if count == 1:
        await redis_client.redis.expire(key, window_s)
    return count <= limit
```

On limit exceeded: log at INFO (not ERROR — this is expected/working-as-intended), increment a `rate_limited_total` counter (see Observability), and **do not publish a "rate limited" reply** — silently drop, matching the existing philosophy in `consumer.py` that a dropped AI reply is preferable to noisy/duplicate output. Still commit the Kafka offset (message was handled, just declined).

Failure mode: if Redis is down, `check_rate_limit` must fail **open** (allow the request) — same policy as `DedupService.check()` already does, for the same reason: Redis being down shouldn't take down the AI feature, and NIM's own capacity is a secondary backstop.

### Concurrent-call cap toward NIM (the second, distinct limit)

Per-user/channel limits don't bound total load if you later run multiple consumer instances. Add a process-local `asyncio.Semaphore(NVIDIA_MAX_CONCURRENT_CALLS)` (default 4–8, tunable) around `NvidiaChatClient.chat_completion` in `ai_service/rag/generator.py`. This is not a "rate limit" in the abuse-prevention sense — it's a concurrency cap protecting NIM and your own cost/latency, and it composes with whatever consumer-count scaling you do (see §2).

---

## 2. Concurrency

### How multiple AI workers would process requests

Today: one process, one partition-consumer, fully serial (embed→retrieve→generate→publish→commit, then next `poll()`). To scale, run N processes with the **same** `AI_KAFKA_GROUP_ID` — Kafka rebalances partitions across them automatically, no code change needed for that part. Producer already keys messages by `channelId` (`app/core/kafka_producer.py`), so **all messages for a given channel land on the same partition and are processed by the same consumer instance, in order** — this is what makes it safe to add workers without extra locking for per-channel ordering.

Within a single process, add intra-process concurrency instead of scaling processes first (cheaper, no new infra): replace the fully-serial `poll → handle → commit` with a bounded pool, e.g. `asyncio.Semaphore(WORKER_CONCURRENCY)` (default 4) wrapping the per-message handler, with messages dispatched as tasks and offsets committed **per-partition, in offset order**, not per-message-completion order (async completion can reorder). This requires tracking the lowest uncommitted offset per partition — a small queue/watermark structure, not a large rewrite, but worth calling out as the trickiest correctness piece of this whole plan.

Given the current single-consumer, single-topic-partition-per-channel design, recommend starting with **multi-process horizontal scaling** (simpler, ordering is free) and only add intra-process concurrency later if per-channel throughput (not aggregate throughput) becomes the bottleneck — intra-process concurrency only helps when one channel alone is generating more `/ask` traffic than one worker can serially handle.

### Avoiding duplicate responses

Three independent mechanisms, layered (defense in depth — any one of them failing shouldn't produce a duplicate):

1. **Idempotent commit ordering** (already exists): offsets commit synchronously after publish, so a crash before commit causes reprocessing of at most the in-flight message — not a wave of duplicates.
2. **New: reply idempotency key in Redis**, same `SET NX` + TTL pattern as `DedupService`. Key on the *triggering* message id, not a new random id: `ai-reply:{triggering_message_id}` → `SET NX` before calling `publish_answer`, TTL ~10 min (covers rebalance-driven reprocessing windows). If `SET NX` fails (key exists), skip publish — this message was already answered. This directly closes the gap `consumer.py`'s docstring flags as deliberately unsolved.
3. **Multi-process safety**: because Kafka guarantees one partition is owned by exactly one consumer in a group at a time, two processes can't concurrently process the *same* message — the Redis key above is protecting against **reprocessing** (rebalance, crash-restart), not concurrent processing across workers.

Fail-open policy for step 2 matches §1: if Redis is unavailable, proceed with publish anyway (accept possible duplicate over dropped reply — consistent with existing `DedupService` philosophy) and log loudly so it's visible in metrics.

---

## 3. Observability

Reuse `app/core/metrics.py`'s rolling-window-in-memory pattern and `app/core/structred_log.py`'s conventions rather than introducing Prometheus/OTel in this pass — the codebase's own comment in `metrics.py` explains the rationale ("get the numbers first"); apply the same reasoning here, but note the AI service's latencies (NIM calls, embedding) are much larger and more variable than ingest, so percentile tracking matters even more.

### Logs
Structured (JSON) logs per pipeline stage, each carrying a shared `trace_id` = the triggering message's id, so one request's full path (detect → embed → retrieve → generate → publish) can be grepped as a unit:
- `ai.trigger.matched` / `ai.trigger.rate_limited`
- `ai.retrieval.completed` (chunk count, top score, latency_ms)
- `ai.generation.completed` / `ai.generation.degraded` (had_error path in `generator.py`)
- `ai.publish.completed` / `ai.publish.duplicate_skipped`

### Metrics (extend `IngestMetrics` pattern into a new `AiServiceMetrics`)
- `ai_requests_total` (counter, labeled by outcome: answered / rate_limited / degraded / duplicate_skipped)
- `ai_latency_ms` — end-to-end, as a `LatencyStats` rolling window (reuse the existing class as-is)
- `retrieval_latency_ms` (embed + pgvector search combined, and split if useful)
- `nim_latency_ms`
- `ai_failures_total` (labeled by stage: kafka / embed / pgvector / nim / redis / publish)
- `kafka_consumer_lag` — **not derivable from in-process state**; must be scraped externally via `kafka-consumer-groups.sh --describe --group ai-service-group` or the Kafka AdminClient's `list_consumer_group_offsets`, run on an interval from a small sidecar/cron rather than the consumer itself (a stuck consumer can't reliably report its own lag).

### Tracing
Given curl-subprocess NIM calls and asyncio thread-pool embedding calls, at minimum wrap each stage in a span with the shared `trace_id` from logs even without a real tracing backend — the log-based approach above already gives 80% of the value. If OpenTelemetry is added later, the natural span boundaries are exactly the four pipeline stages above; this log design doesn't need to change, just gets exported instead of/alongside logged.

### What to alert on first
`ai_failures_total{stage=nim}` rate and `kafka_consumer_lag` are the two highest-signal metrics — NIM failures directly cause user-visible `GENERATION_UNAVAILABLE_MESSAGE` degradation, and lag indicates the whole feature is falling behind regardless of per-message success rate.

---

## 4. Failure Scenarios

| Component down | Current behavior | Recommended handling |
|---|---|---|
| **Kafka unavailable** | Consumer's `poll()` raises/blocks; existing circuit breaker in `app/core/kafka_producer.py` already protects the *publish* side. Consume side has no equivalent. | Let the consumer loop retry with backoff (librdkafka handles reconnect internally for consume); surface via `ai_failures_total{stage=kafka}` and lag metric spiking. No message loss risk — Kafka retains the topic; AI replies just fall behind until reconnect. |
| **OpenAI/NVIDIA NIM unavailable** | Already handled: `NvidiaChatClient` retries 3x with backoff on 5xx/429/curl failure; `RagGenerator.answer()` catches `NvidiaClientError` and returns `GENERATION_UNAVAILABLE_MESSAGE` gracefully rather than crashing the consumer. | Keep as-is; add the semaphore from §1 so a NIM outage doesn't pile up unbounded concurrent retries against it. Ensure the degraded-path reply is still subject to the dedup key in §2 (a degraded reply still counts as "answered" — don't let a later retry double-answer). |
| **PGVector (Postgres) unavailable** | `semantic_search` would raise; not currently caught anywhere in `generator.py`'s call path — this likely crashes the message handler today. | Wrap retrieval in a try/except that degrades to "answer from prompt alone, no retrieved context" (or a canned "context temporarily unavailable" reply) rather than failing the whole message — same graceful-degradation philosophy already used for NIM outages. Log/metric as `stage=pgvector`. |
| **Redis unavailable** | N/A today (no Redis usage in ai_service). Under this design: rate limiting and dedup both depend on it. | Both fail open, per §1/§2 — Redis being down should degrade *protection*, not availability. This is a deliberate trade-off: a burst of duplicate/unthrottled AI replies during a Redis outage is preferable to the AI feature going fully dark. |
| **Consumer crash** | Uncommitted offset means the in-flight message (and only that message, given serial processing) is reprocessed by the rebalanced partition. Existing docstring in `consumer.py` already reasons through this for the *no-dedup-key* case. | With the §2 Redis dedup key added, reprocessing after a crash no longer risks a duplicate *reply* — worst case is redundant embed/retrieval/NIM work for one message, which is a cost/latency concern, not a correctness one. |

---

## 5. Final Architecture Review

### Bottlenecks
- **Fully serial per-message pipeline** is the dominant bottleneck: one `/ask` message ties up its consumer for the sum of embed + pgvector + NIM latency (NIM alone has a 20s timeout budget) before the next message on that channel's partition can even start. A single busy channel starves itself — and today, since there's exactly one consumer instance, starves the entire feature.
- **Default asyncio executor for embedding** (`workers/embedder.py`) is shared with anything else that happens to use `run_in_executor` in the same process — not isolated, and its default size (`min(32, cpu+4)`) is a coincidence, not a tuned value.
- **Curl subprocess per NIM call** (`ai_service/rag/nvidia_client.py`) has real per-call overhead (process spawn) compared to a persistent HTTP client/connection pool — worth revisiting once the "curl was a timeout-misdiagnosis workaround" rationale in that file's docstring is re-verified; a modern async HTTP client with explicit timeout config may now work fine and would remove this cost entirely.

### Scalability risks
- No horizontal scaling is wired up (no Dockerfile/replica config for `ai_service/`) — scaling today means manually starting a second process with the same group id, untested.
- Per-process DB connection pools (`pool_size=5, max_overflow=2` in both `ai_service/consumer.py` and `workers/document_worker.py`) are not shared — scaling consumer count multiplies total Postgres connections linearly with no central cap, which can exhaust Postgres `max_connections` before it exhausts anything else.
- `RETRIEVAL_CONTEXT_CHAR_BUDGET` and chunk over-fetch (`limit*4` in `chunk_repository.py`) scale pgvector query cost with corpus size per-channel; no observed index-tuning discussion (HNSW/IVFFlat params) — worth a follow-up if channels accumulate large document sets.

### Hidden race conditions
- **The one this whole plan exists to fix**: no dedup key on AI replies today — crash-after-publish-before-commit, or any future intra-process concurrency (§2), can produce two AI replies to one trigger message. Addressed by §2's Redis `SET NX` key.
- **Offset-commit-after-async-completion** if intra-process concurrency (§2) is added without care: committing per-message-completion rather than tracking a per-partition low-watermark can commit offset N+1 while offset N's task is still in flight — a crash then skips N's message entirely. Called out explicitly in §2 as the trickiest piece; don't add concurrency without the watermark structure.
- **Rate-limit check-then-act race**: two rapid messages from the same user, processed near-simultaneously (only possible with intra-process concurrency or multiple consumers on different partitions somehow both touching one user), could both pass the `INCR`-based check before either's count is visible to the other — actually fine here, since Redis `INCR` is atomic and both increments land correctly even if the *check* of the returned count happens "simultaneously" from the app's perspective; flagging only to confirm this is *not* a race, given INCR's atomicity, unlike a naive GET-then-SET pattern would be.

### Cost optimization opportunities
- **Semaphore-bounded NIM concurrency (§1)** doubles as cost control, not just reliability — without it, a traffic burst produces a matching burst of billed NIM calls with no smoothing.
- **Prompt context budget** (`RETRIEVAL_CONTEXT_CHAR_BUDGET=16000` chars) is a direct cost lever — every retrieved chunk sent to NIM is billed input tokens; the existing `min_score=0.6` + `max_per_document=3` caps are good levers to revisit if per-request cost becomes visible via the new metrics.
- **Rate limiting itself (§1)** is a cost control as much as an abuse control — capping requests/user/channel directly caps worst-case NIM spend per unit time, which is worth stating explicitly when presenting this plan since "rate limiting" often gets framed as purely a security feature.
- **Degraded-path replies** (NIM/pgvector unavailable) should be cheap by construction — confirm the degraded path never still pays for an embedding call it can't use (currently, embedding happens before the NIM call, so an NIM-outage degraded reply still pays retrieval cost; a pgvector-outage degraded reply saves nothing either way since it's what triggers the degradation). Minor, but worth noting the ordering (embed → retrieve → generate) means failures late in the pipeline never save the cost incurred earlier in it.

---

## Suggested implementation order

1. Redis reply-dedup key (§2) — closes the correctness gap the current code explicitly flags as unsolved; highest value, lowest risk, no new infra.
2. Rate limiting (§1) — reuses the same Redis, same `SET`-based pattern, small diff.
3. NIM semaphore (§1/§2) — one-line-ish change, immediate cost/reliability benefit.
4. PGVector failure handling (§4) — closes a real crash path that's currently untested/unhandled.
5. Observability module (§3) — `AiServiceMetrics` mirroring `IngestMetrics`, plus structured logs with `trace_id`.
6. Horizontal scaling (§2) — Dockerfile/replica config once the above are in place and provide the visibility to know if/when it's needed.
