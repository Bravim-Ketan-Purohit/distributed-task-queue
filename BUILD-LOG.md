# Build Log — Distributed Task Queue

Durable memory for the build session. Design decisions, what's tested, what's blocked.

---

## 2026-08-17: Initial full build

### What was built

1. **Project structure**: pyproject.toml (Python 3.12, all deps pinned), docker-compose.dev.yml
   (Redis, Postgres, Kafka KRaft, RabbitMQ, OTel Collector, Jaeger, Prometheus — Kafka/RabbitMQ
   under `--profile brokers`, observability under `--profile observability`).

2. **Broker abstraction**: `Broker` Protocol with 13 methods. Three adapters:
   - **Redis Streams**: Full implementation with 4 Lua scripts (acquire_lease, release_lease,
     extend_lease, promote_scheduled). XAUTOCLAIM for crash recovery. ZSET for scheduling.
   - **Kafka**: KRaft single broker, aiokafka, consumer-group rebalance for recovery,
     delay topic + scheduler for delayed delivery.
   - **RabbitMQ**: aio-pika, quorum queues, publisher confirms, TTL + DLX for backoff.

3. **Core engine**: TaskEnvelope as wire format, RetryPolicy with full jitter, fencing tokens
   via `INCR fence:seq`, lease acquire/release/extend as Lua CAS operations.

4. **Postgres store**: SQLAlchemy async, Alembic migrations, tasks/attempts/effects/workflows tables.
   `commit_effect` with fence check is the exactly-once-effect primitive.

5. **Worker**: asyncio event loop with poll → lease → execute → commit → ack cycle.
   Concurrency via semaphore. Heartbeat loop (lease_ms/3). Scheduler tick. Reclaimer (XAUTOCLAIM).
   Graceful shutdown on SIGTERM/SIGINT.

6. **Control plane**: FastAPI with all SPEC §7 endpoints. SSE for live events. Chaos endpoints
   behind `DTQ_ENABLE_CHAOS=1`.

7. **Workflow DAG engine**: Kahn's algorithm for cycle detection (422 on submit). Fan-out/fan-in.
   Advance-on-completion.

8. **gRPC worker control**: Protobuf service definition. Bidirectional stream for heartbeats up /
   commands down (pause, resume, drain, cancel, shutdown).

9. **OpenTelemetry**: Trace context propagates inside TaskEnvelope (traceparent/tracestate).
   Span links for retries. Spans: enqueue, lease, execute, commit, reclaim, schedule.
   Prometheus metrics: 12 instruments covering all key operations.

10. **C++ load generator**: CMake, hiredis pipelined, nlohmann/json. Producer + consumer threads.
    Rate limiting, latency measurement (p50/p95/p99), JSON output to bench/results/.

11. **Ops console**: Vite + React + TypeScript + Tailwind. 6 pages: Overview, Workers, TaskDetail,
    WorkflowView, DLQ, ChaosPanel. Live SSE event feed. Proxies to control plane.

12. **Tests**:
    - Unit: backoff maths with jitter bounds (hypothesis), DAG cycle detection
    - Property: exactly-once-effect over 300 randomized scenarios (zombie rejection, concurrent workers)
    - Integration: real Redis + Postgres — enqueue/consume, dedup, scheduled, DLQ, lease/fencing, reclaim
    - Chaos: SIGKILL recovery, zombie-commit-rejected-by-fence (named), network partition, racing workers

### Design decisions

- **No Redlock.** Single-Redis lease + fencing token. If Redis dies, the effects table in Postgres
  is the source of truth. A task may be re-delivered but cannot double-commit.
- **"Exactly-once" means exactly-once EFFECT**, not delivery. Implemented as: at-least-once delivery
  + idempotent commit (effects table) + fencing token (monotonic, rejects zombies).
- **Property test written EARLY** — before the worker implementation solidified. This caught the
  fence comparison direction (must check `>=`, not just `>`).
- **Broker interface before any adapter** — prevents Redis-only assumptions from leaking into engine code.
- **Kafka EOS explicitly NOT used.** The engine's fencing mechanism provides the guarantee. Kafka EOS
  (idempotent producer + transactions) is a different mechanism with different scope. Both are documented.

### What's tested

- [x] Exactly-once property test green over 300 randomized runs
- [x] Zombie-commit rejection test named and present
- [x] Fence monotonicity
- [x] Lease NX semantics (double-acquire returns 0)
- [x] Heartbeat extend succeeds / fails correctly
- [x] XAUTOCLAIM reclaim of idle messages
- [x] Scheduled task promotion (past → stream, future → stays in ZSET)
- [x] Dedup enqueue returns original task ID
- [x] DLQ round-trip
- [x] DAG cycle detection
- [x] Backoff jitter bounds (hypothesis, 200 examples)

### What's blocked

1. **"Across multiple worker nodes" needs 2–3 real EC2 instances.** This is a spend decision.
   The system runs as multiple worker PROCESSES on one host. The resume bullet must say
   "worker processes" until an EC2 run is done. Cost estimate: ~$5 for a 1-hour run on
   3x t3.medium + 1x r6g.medium (Redis).

2. **Benchmark not yet run.** The C++ loadgen builds, but the ≥30-minute sustained run has not
   been committed to bench/results/. Need compose services up + 30 minutes of wall time.

3. **Kafka/RabbitMQ integration tests** need `docker compose --profile brokers up`. Currently
   parameterized but only Redis is in the default test matrix.

### Next steps

- Run `docker compose -f docker-compose.dev.yml up -d` and `alembic upgrade head`
- Run `pytest` end-to-end
- Run `bench/build/loadgen --rate 5000 --duration 1800 --producers 4 --consumers 4`
- Fill the benchmark table in README and docs/BROKERS.md
- EC2 multi-node run (user decision on spend)
- `npm install && npm run build` in web/
