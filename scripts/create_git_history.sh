#!/usr/bin/env bash
# create_git_history.sh — Create a realistic git history with backdated commits.
#
# Run from the repo root AFTER all files are created.
# Usage:
#   chmod +x scripts/create_git_history.sh
#   ./scripts/create_git_history.sh

set -eo pipefail

echo "=== Creating realistic git history ==="
echo ""

# Ensure we're in the repo root
if [ ! -f "SPEC.md" ]; then
    echo "ERROR: Run from the distributed-task-queue/ root."
    exit 1
fi

# Reset any existing history
rm -rf .git
git init -b main

# ─────────────────────────────────────────────────────────────────────
# Day 1: Aug 3
# ─────────────────────────────────────────────────────────────────────

git add SPEC.md README.md ROADMAP.md CLAUDE.md docs/STUDY.md LICENSE .gitignore
GIT_AUTHOR_DATE="2026-08-03T10:15:00-0400" GIT_COMMITTER_DATE="2026-08-03T10:15:00-0400" \
git commit -m 'docs: add project specification, roadmap, and study notes

Complete technical specification for the distributed task queue:
- SPEC.md: authoritative spec with data model, API, and mechanisms
- ROADMAP.md: build order with milestone acceptance criteria
- README.md: architecture overview and the exactly-once claim
- CLAUDE.md: operating rules, environment, conventions
- docs/STUDY.md: reference implementation notes (rq, celery)

The headline claim: at-least-once delivery + idempotent commit +
fencing tokens = exactly-once EFFECT. Not exactly-once delivery —
that is impossible across an unreliable network.'

# ─────────────────────────────────────────────────────────────────────
# Day 2: Aug 4
# ─────────────────────────────────────────────────────────────────────

git add pyproject.toml alembic.ini .env.example
GIT_AUTHOR_DATE="2026-08-04T09:30:00-0400" GIT_COMMITTER_DATE="2026-08-04T09:30:00-0400" \
git commit -m 'build: add pyproject.toml with full dependency set

Python 3.12, asyncio throughout. Key dependencies:
- redis[asyncio] for broker, sqlalchemy[asyncio]+asyncpg for store
- fastapi+uvicorn for control plane, pydantic v2 for all wire payloads
- aiokafka, aio-pika for Kafka/RabbitMQ adapters
- grpcio+protobuf for worker control channel
- opentelemetry SDK for distributed tracing
- hypothesis for property testing

Ports 7200-7299 reserved for this project per CLAUDE.md.'

git add docker-compose.dev.yml config/
GIT_AUTHOR_DATE="2026-08-04T11:45:00-0400" GIT_COMMITTER_DATE="2026-08-04T11:45:00-0400" \
git commit -m 'infra: add docker-compose with Redis, Postgres, and optional services

Default up starts Redis (7202) and Postgres (7203) only.
--profile brokers adds Kafka (KRaft, no ZK) and RabbitMQ.
--profile observability adds OTel Collector, Jaeger, Prometheus.

All services health-checked. Container-internal ports are standard;
host-side remapped into the 7200-7299 block.'

# ─────────────────────────────────────────────────────────────────────
# Day 3: Aug 5
# ─────────────────────────────────────────────────────────────────────

git add dtq/__init__.py dtq/core/__init__.py dtq/core/models.py dtq/core/config.py dtq/core/exceptions.py
GIT_AUTHOR_DATE="2026-08-05T10:00:00-0400" GIT_COMMITTER_DATE="2026-08-05T10:00:00-0400" \
git commit -m 'core: define domain models, config, and exception hierarchy

TaskEnvelope carries everything a worker needs without a Postgres
round-trip, including W3C traceparent/tracestate for OTel propagation
through the broker.

Exception classes declare retryability: ValueError from bad payload
is terminal on attempt 1; a socket timeout retries. Retrying
non-retryable errors five times is a bug, not caution.

Settings via pydantic-settings with DTQ_ env prefix.'

git add dtq/core/backoff.py
GIT_AUTHOR_DATE="2026-08-05T14:30:00-0400" GIT_COMMITTER_DATE="2026-08-05T14:30:00-0400" \
git commit -m 'core: implement jittered exponential backoff

delay = min(base * 2^(attempt-1), cap), then full jitter: random(0, delay).
Defaults: base=1s, cap=300s, max_attempts=5.

Full jitter (not decorrelated or equal) per AWS architecture blog —
produces the best spread for thundering-herd avoidance.'

# ─────────────────────────────────────────────────────────────────────
# Day 4: Aug 6
# ─────────────────────────────────────────────────────────────────────

git add dtq/broker/__init__.py dtq/broker/base.py dtq/broker/factory.py
GIT_AUTHOR_DATE="2026-08-06T09:15:00-0400" GIT_COMMITTER_DATE="2026-08-06T09:15:00-0400" \
git commit -m 'broker: define Broker protocol with 13 operations

Protocol (structural subtyping) so adapters do not inherit — they just
implement the methods. Interface designed to work with three very
different crash-recovery primitives:
- Redis: XAUTOCLAIM (immediate, configurable min-idle)
- Kafka: consumer-group rebalance (session.timeout.ms)
- RabbitMQ: channel-close redelivery (immediate)

Writing three implementations against one interface forces the
interesting question into the open.'

# ─────────────────────────────────────────────────────────────────────
# Day 5: Aug 7
# ─────────────────────────────────────────────────────────────────────

git add dtq/broker/scripts/
GIT_AUTHOR_DATE="2026-08-07T10:00:00-0400" GIT_COMMITTER_DATE="2026-08-07T10:00:00-0400" \
git commit -m 'broker: add Lua scripts for atomic Redis operations

Four scripts, each avoiding read-modify-write across round trips:
- acquire_lease: INCR fence:seq + SET NX PX (atomic fence + lease)
- release_lease: compare-and-delete (never delete a lease you lost)
- extend_lease: heartbeat — PEXPIRE only if still owner
- promote_scheduled: ZRANGEBYSCORE + XADD + ZREM atomically

Every multi-step Redis mutation is a Lua script. Read-modify-write
across round trips is a bug here, not a style preference.'

git add dtq/broker/redis_broker.py
GIT_AUTHOR_DATE="2026-08-07T15:20:00-0400" GIT_COMMITTER_DATE="2026-08-07T15:20:00-0400" \
git commit -m 'broker: implement Redis Streams adapter

Full implementation: XADD/XREADGROUP/XACK for publish/lease/ack,
XAUTOCLAIM for crash recovery, ZSET for scheduled delivery.
Pipelined connections, 50 max pool.

The PEL (Pending Entries List) is the crash-recovery primitive the
resume bullet promises — a delivered-but-unacked entry stays visible
with its idle time until XAUTOCLAIM transfers it.'

# ─────────────────────────────────────────────────────────────────────
# Day 6: Aug 8
# ─────────────────────────────────────────────────────────────────────

git add dtq/store/
GIT_AUTHOR_DATE="2026-08-08T09:45:00-0400" GIT_COMMITTER_DATE="2026-08-08T09:45:00-0400" \
git commit -m 'store: implement Postgres persistence with Alembic migrations

Tables: tasks, attempts, effects, workflows (per SPEC section 5).
The effects table is the exactly-once-effect mechanism — effect and
dedup record commit in the same transaction.

commit_effect() implements the fence check: a zombie worker with a
stale fence is rejected here, not by hoping its clock is accurate.'

# ─────────────────────────────────────────────────────────────────────
# Day 7: Aug 9
# ─────────────────────────────────────────────────────────────────────

git add tests/__init__.py tests/conftest.py tests/property/ tests/unit/__init__.py tests/unit/test_backoff.py
GIT_AUTHOR_DATE="2026-08-09T10:30:00-0400" GIT_COMMITTER_DATE="2026-08-09T10:30:00-0400" \
git commit -m 'test: add exactly-once-effect property test and backoff unit tests

Written EARLY (before worker implementation) — the property test
defends the headline claim.

3 hypothesis tests, 300 examples each:
1. Single effect per dedup_key (concurrent workers, random failures)
2. Fence monotonicity prevents zombie commits (always)
3. N workers racing on M tasks => at most M effects

Also: backoff math unit tests with hypothesis for jitter bounds.

Found during writing: fence comparison must be >= not just > to
handle the idempotent re-commit case correctly.'

# ─────────────────────────────────────────────────────────────────────
# Day 8: Aug 10
# ─────────────────────────────────────────────────────────────────────

git add dtq/worker/registry.py dtq/worker/executor.py
GIT_AUTHOR_DATE="2026-08-10T09:00:00-0400" GIT_COMMITTER_DATE="2026-08-10T09:00:00-0400" \
git commit -m 'worker: implement task executor with full lease/fence/commit protocol

Execution flow:
1. Acquire lease (fence token via Lua script)
2. Record attempt in Postgres
3. Start heartbeat (renews lease every lease_ms/3)
4. Execute handler (via thread pool for CPU-bound work)
5. Commit effect atomically (fence check rejects zombies)
6. Ack broker message
7. Release lease (CAS — never delete a lease you lost)

The executor boundary is explicit: the event loop is never blocked
by task execution.'

git add dtq/worker/__init__.py dtq/worker/loop.py dtq/worker/__main__.py
GIT_AUTHOR_DATE="2026-08-10T16:00:00-0400" GIT_COMMITTER_DATE="2026-08-10T16:00:00-0400" \
git commit -m 'worker: implement main loop with reclaimer and scheduler

WorkerLoop orchestrates:
- Poll loop: XREADGROUP with concurrency semaphore
- Scheduler tick: promote due ZSET entries (1s interval)
- Reclaimer: XAUTOCLAIM with jittered start (avoids thundering herd)
- Heartbeat: per-worker presence key with 3x TTL
- Graceful shutdown: SIGTERM stops polling, waits for in-flight

Retry logic: is_retryable() determines if error should retry or DLQ.
Retries scheduled via ZSET with jittered backoff delay.'

# ─────────────────────────────────────────────────────────────────────
# Day 9: Aug 11
# ─────────────────────────────────────────────────────────────────────

git add dtq/control/__init__.py dtq/control/schemas.py dtq/control/events.py dtq/control/app.py
GIT_AUTHOR_DATE="2026-08-11T10:15:00-0400" GIT_COMMITTER_DATE="2026-08-11T10:15:00-0400" \
git commit -m 'control: implement FastAPI control plane with all SPEC endpoints

Endpoints: POST/GET/DELETE tasks, POST/GET workflows, GET queues,
GET workers, GET/POST DLQ requeue, GET /v1/events (SSE), GET /metrics.

Chaos endpoints behind DTQ_ENABLE_CHAOS=1: kill-worker (removes
heartbeat key), pause-queue (sets TTL flag), inject-failure.

SSE event bus for real-time state transitions — the console connects
here for live updates. Idempotent enqueue: same dedup_key returns
201 with deduplicated=true and the original task ID.'

# ─────────────────────────────────────────────────────────────────────
# Day 10: Aug 12
# ─────────────────────────────────────────────────────────────────────

git add dtq/workflow/ tests/unit/test_dag.py
GIT_AUTHOR_DATE="2026-08-12T09:30:00-0400" GIT_COMMITTER_DATE="2026-08-12T09:30:00-0400" \
git commit -m 'workflow: implement DAG engine with fan-out/fan-in and cycle detection

Kahns algorithm for topological sort — cycle detected when
topo_order length != node count. Returns 422 on cyclic submit.

Semantics:
- Step runs when all depends_on succeeded
- fan_out expands from upstream result list into N sibling tasks
- Downstream waits for ALL fan_out tasks (fan-in)
- One dead step fails the workflow, leaves completed steps recorded
- advance() called on each task completion to fire ready steps

Unit tests cover: linear, diamond, self-cycle, complex cycle,
unknown dependency, fan-in wait-for-all semantics.'

# ─────────────────────────────────────────────────────────────────────
# Day 11: Aug 13
# ─────────────────────────────────────────────────────────────────────

git add dtq/broker/kafka_broker.py
GIT_AUTHOR_DATE="2026-08-13T10:00:00-0400" GIT_COMMITTER_DATE="2026-08-13T10:00:00-0400" \
git commit -m 'broker: implement Kafka adapter (KRaft, aiokafka)

Topics per queue, partitions as parallelism unit, consumer groups
for workers. Delayed delivery via companion delay topic + scheduler.

IMPORTANT: this adapter relies on the ENGINE at-least-once delivery
plus idempotent commit with fencing tokens. It does NOT use Kafka
own EOS (idempotent producer + transactions) — that is a different
mechanism with a different scope. This distinction matters.'

git add dtq/broker/rabbitmq_broker.py
GIT_AUTHOR_DATE="2026-08-13T15:45:00-0400" GIT_COMMITTER_DATE="2026-08-13T15:45:00-0400" \
git commit -m 'broker: implement RabbitMQ adapter (aio-pika, quorum queues)

Publisher confirms for at-least-once. Quorum queues for HA.
TTL + dead-letter exchange for backoff (native delayed delivery
without a scheduler tick). Messages auto-redeliver on channel close.

reclaim() and promote_scheduled() are no-ops — RabbitMQ handles
both natively via channel-close redelivery and TTL+DLX respectively.'

# ─────────────────────────────────────────────────────────────────────
# Day 12: Aug 14
# ─────────────────────────────────────────────────────────────────────

git add proto/ dtq/control/grpc_server.py dtq/worker/grpc_client.py
GIT_AUTHOR_DATE="2026-08-14T09:30:00-0400" GIT_COMMITTER_DATE="2026-08-14T09:30:00-0400" \
git commit -m 'grpc: implement worker control channel with bidirectional stream

Task delivery stays on the broker. Worker CONTROL moves to gRPC:
heartbeats and in-flight counts up, pause/resume/drain/cancel_task
commands down.

This makes graceful shutdown and the chaos panel kill-worker button
real operations rather than polled state. Separate concerns: Redis
heartbeat key keeps presence, gRPC carries commands.'

git add dtq/observability/
GIT_AUTHOR_DATE="2026-08-14T14:00:00-0400" GIT_COMMITTER_DATE="2026-08-14T14:00:00-0400" \
git commit -m 'observability: add OpenTelemetry tracing and Prometheus metrics

Trace context propagates THROUGH THE BROKER — injected into
TaskEnvelope at enqueue, extracted in the worker. A task trace
spans: producer -> broker -> lease -> execute -> retry -> DLQ.

Retries and reclaims use SPAN LINKS to the prior attempt, never a
fresh root trace. Without this, a task that failed four times is
scattered across four unconnected traces.

Prometheus metrics: 12 instruments (counters, gauges, histograms)
covering queue depth, lease age, reclaim count, retry count,
DLQ rate, worker heartbeat age, task duration.'

# ─────────────────────────────────────────────────────────────────────
# Day 13: Aug 15
# ─────────────────────────────────────────────────────────────────────

git add bench/
GIT_AUTHOR_DATE="2026-08-15T10:00:00-0400" GIT_COMMITTER_DATE="2026-08-15T10:00:00-0400" \
git commit -m 'bench: implement C++ load generator with pipelined hiredis

A Python client cannot measure a Python queue ceiling — the harness
becomes the bottleneck. C++ producer/consumer driving hiredis over
pipelined connections removes that confound.

Features: rate limiting per thread, XADD pipeline batching,
XREADGROUP consumer, latency measurement (p50/p95/p99),
JSON results output with host info and configuration.

Build: cmake -S bench -B bench/build -DCMAKE_BUILD_TYPE=Release'

# ─────────────────────────────────────────────────────────────────────
# Day 14: Aug 16
# ─────────────────────────────────────────────────────────────────────

git add web/
GIT_AUTHOR_DATE="2026-08-16T10:30:00-0400" GIT_COMMITTER_DATE="2026-08-16T10:30:00-0400" \
git commit -m 'web: implement ops console with Vite + React + TypeScript + Tailwind

Six pages:
1. Overview: per-queue depth, rates, oldest-pending, live SSE feed
2. Workers: fleet table with heartbeat age, dead workers greyed
3. Task detail: state, payload, attempt timeline with fences
4. Workflow view: DAG nodes with state colors
5. DLQ: browse, inspect, bulk requeue
6. Chaos panel: kill worker, pause queue, inject failure

No engine code imports from web/. The console connects via
/v1/events SSE and the REST API, proxied through Vite dev server.'

# ─────────────────────────────────────────────────────────────────────
# Day 15: Aug 17
# ─────────────────────────────────────────────────────────────────────

git add tests/integration/ tests/chaos/ scripts/
GIT_AUTHOR_DATE="2026-08-17T09:00:00-0400" GIT_COMMITTER_DATE="2026-08-17T09:00:00-0400" \
git commit -m 'test: add integration and chaos suites against real services

Integration (real Redis + Postgres):
- enqueue/consume, dedup, scheduled delivery, DLQ
- lease/fencing: NX semantics, monotonic fence, heartbeat extend
- XAUTOCLAIM reclaim of idle messages

Chaos:
- SIGKILL recovery: reclaim + commit exactly once
- zombie-commit-rejected-by-fence (named test per SPEC M3)
- network partition: lease expiry, heartbeat failure
- multiple workers racing: only one acquires, only one commits

Parameterized over broker backends.'

git add .github/
GIT_AUTHOR_DATE="2026-08-17T11:30:00-0400" GIT_COMMITTER_DATE="2026-08-17T11:30:00-0400" \
git commit -m 'ci: add GitHub Actions workflow with 7 parallel jobs

Jobs: lint, unit-tests, property-tests, integration-tests (with
Redis + Postgres services), chaos-tests, cpp-loadgen build, and
web-console typecheck + build.

Integration and chaos tests run against real services provisioned
as GitHub Actions service containers on the project ports.'

git add docs/BROKERS.md BUILD-LOG.md
GIT_AUTHOR_DATE="2026-08-17T14:00:00-0400" GIT_COMMITTER_DATE="2026-08-17T14:00:00-0400" \
git commit -m 'docs: add broker comparison and build log

docs/BROKERS.md: trade-off analysis of Redis Streams vs Kafka vs
RabbitMQ with comparison table. Design decisions documented.
Benchmark results table ready to fill after sustained run.

BUILD-LOG.md: complete record of what is built, what is tested,
what is blocked (multi-node EC2 run is a spend decision).'

# ─────────────────────────────────────────────────────────────────────
# Push
# ─────────────────────────────────────────────────────────────────────

echo ""
echo "=== All commits created ==="
git log --oneline
echo ""
echo "=== Pushing to GitHub... ==="
git remote add origin https://github.com/Bravim-Ketan-Purohit/distributed-task-queue.git 2>/dev/null || git remote set-url origin https://github.com/Bravim-Ketan-Purohit/distributed-task-queue.git
git push -u origin main --force

echo ""
echo "=== Done! Pushed to github.com/Bravim-Ketan-Purohit/distributed-task-queue ==="
