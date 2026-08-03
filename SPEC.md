# SPEC — Distributed Task Queue & Workflow Engine

**Authoritative technical specification.** `ROADMAP.md` gives the build order; this gives the contents.
Where they disagree, this wins. If a requirement here looks wrong, say so and stop — don't silently
redesign.

---

## 1. The claim

> Asynchronous background-job engine with distributed locking, worker heartbeats, exponential-backoff
> retries, and mid-task crash recovery for **exactly-once execution semantics**. Load-tested to
> **[X]M+** events per day across multiple worker nodes.

Resume stack string the build must match: *Python (asyncio), C++, Redis, AWS EC2*
(`Bravim_Purohit_SDE.tex:138`).

### Read this before writing a line of code

"Exactly-once execution" is the load-bearing phrase and, taken literally across a network, it is
impossible — a worker can always die in the window between doing the work and recording that it did.
What is achievable, and what this repo must actually implement, is:

> **at-least-once delivery + idempotent commit = exactly-once *effect*.**

Concretely: a task may be *delivered* more than once; its *effect* lands exactly once, because the effect
and the dedup record commit in the same database transaction, and a fencing token stops a resurrected
zombie worker from committing stale work.

You will be asked about this in an interview, and it is a gift of a question if the code is right. So:
implement effect-idempotency properly, and make the README state the mechanism in one sentence rather
than repeating the marketing phrase. Do **not** quietly redefine the term and move on.

## 2. Non-goals

- Not a Celery/RQ replacement, and not API-compatible with either.
- No cron/periodic scheduling beyond delayed execution (`run_at`). No calendar semantics, no timezones.
- No multi-tenant auth, no per-tenant quotas.
- **Amended 2026-08-17:** Redis Streams remains the **default** broker and the one the resume bullet
  describes. Kafka and RabbitMQ arrive in M7 as *adapters behind a broker interface*, so the comparison
  becomes a deliverable — see §13. No SQS (that's the sibling `agentic-orchestrator` project).
- No autoscaling controller. Worker count is set by the operator.
- No task-code sandboxing. Workers run trusted code from the same repo.

## 3. Architecture

```
   producers ──HTTP──► control plane (FastAPI :7201)
                              │
                              ├── Postgres :7203   task records, attempts, dedup, DAG state
                              │
                              └── Redis :7202
                                    ├─ STREAM  q:<name>          ready tasks + consumer groups
                                    ├─ ZSET    sched:<name>      delayed / backoff retries
                                    ├─ HASH    lease:<task_id>   owner, fence, expires_at
                                    ├─ STRING  fence:seq         monotonic fencing counter
                                    └─ STREAM  dlq:<name>        exhausted tasks
                                          ▲
              ┌───────────────────────────┼───────────────────────────┐
        worker-1 (asyncio)          worker-2 (asyncio)          bench/loadgen (C++)
        XREADGROUP → lease          heartbeat → extend          drives the broker at
        execute → commit            reclaim via XAUTOCLAIM      rates Python can't
              │
              └── metrics :7206+          web/ ops console :7200
```

### Why Redis Streams and not a list

`LPUSH`/`BRPOP` loses the in-flight task when a worker dies. Streams give a per-group **Pending Entries
List**: a delivered-but-unacked entry stays visible, with its idle time, and `XAUTOCLAIM` transfers it to
a live worker. That is the crash-recovery primitive the resume bullet promises — don't rebuild it by hand
on top of lists.

### Why C++ is in this project

Two legitimate reasons, both worth stating in the README:

1. **`bench/loadgen` is C++.** A Python client cannot measure a Python queue's ceiling — the harness
   becomes the bottleneck and the reported number describes the harness. A C++ producer/consumer driving
   `hiredis` over pipelined connections removes that confound.
2. **`workers/cpp/` is a second worker runtime** speaking the same wire protocol. It proves the queue's
   contract is a documented protocol rather than pickled Python objects, and gives a CPU-bound execution
   path that isn't GIL-limited.

(2) is M6 and optional. (1) is required, because the headline number depends on it.

## 4. Module layout

```
dtq/
  broker/         Redis access, Lua scripts, stream + zset operations
  core/           Task, Attempt, RetryPolicy, states, serialisation
  worker/         asyncio worker loop, lease heartbeat, executor, reclaimer
  workflow/       DAG definition, dependency resolution, fan-out / fan-in
  control/        FastAPI app: enqueue, query, admin, DLQ
  store/          Postgres access, migrations, dedup + fencing
  observability/  Prometheus metrics, structured logging
bench/            C++ loadgen (CMake), results/*.json
workers/cpp/      optional second runtime (M6)
web/              ops console (Vite + React + TS)
tests/            unit, integration (real Redis + Postgres), chaos
scripts/          compose up, chaos scenarios, migration runner
```

## 5. Data model (Postgres)

```sql
CREATE TYPE task_state AS ENUM
  ('pending','scheduled','leased','succeeded','failed','dead','cancelled');

CREATE TABLE tasks (
  id             UUID PRIMARY KEY,
  queue          TEXT        NOT NULL,
  task_name      TEXT        NOT NULL,
  payload        JSONB       NOT NULL,
  state          task_state  NOT NULL DEFAULT 'pending',
  priority       SMALLINT    NOT NULL DEFAULT 0,
  attempt        INT         NOT NULL DEFAULT 0,
  max_attempts   INT         NOT NULL DEFAULT 5,
  dedup_key      TEXT,                          -- caller-supplied idempotency key
  run_at         TIMESTAMPTZ,
  workflow_id    UUID REFERENCES workflows(id),
  step_name      TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX tasks_dedup ON tasks (queue, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX tasks_state_queue ON tasks (state, queue, priority DESC, created_at);

CREATE TABLE attempts (
  id           BIGSERIAL PRIMARY KEY,
  task_id      UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  attempt_no   INT  NOT NULL,
  worker_id    TEXT NOT NULL,
  fence        BIGINT NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ,
  outcome      TEXT,                            -- succeeded | failed | lease_lost | timeout
  error_type   TEXT,
  error_repr   TEXT,
  UNIQUE (task_id, attempt_no)
);

-- The exactly-once-effect table. Effect + this row commit together, or neither commits.
CREATE TABLE effects (
  dedup_key   TEXT PRIMARY KEY,
  task_id     UUID NOT NULL,
  fence       BIGINT NOT NULL,
  result      JSONB,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workflows (
  id UUID PRIMARY KEY, name TEXT NOT NULL, spec JSONB NOT NULL,
  state TEXT NOT NULL DEFAULT 'running', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## 6. The four mechanisms in the bullet

### Distributed locking with fencing tokens

Do **not** implement Redlock. Single-Redis lock plus a fencing token, and be able to explain why:

1. `fence = INCR fence:seq` — monotonic, global.
2. `SET lease:<task_id> <worker_id>:<fence> NX PX <lease_ms>` — atomic acquire.
3. The worker passes `fence` into every write it makes.
4. The commit transaction is conditional on the fence:
   `INSERT INTO effects … WHERE NOT EXISTS (SELECT 1 FROM effects WHERE dedup_key = $1 AND fence > $2)`.

A worker that stalls past its lease, loses it to a reclaimer, then wakes up and tries to commit is
rejected by the fence comparison — not by hoping its clock is accurate. Lease release is a Lua
compare-and-delete so a worker can never delete a lease it no longer owns.

### Worker heartbeats

Each worker renews `lease:<task_id>` every `lease_ms / 3` while executing, and separately writes
`worker:<id>` with `PX heartbeat_ttl` carrying `{queues, in_flight, started_at, version}`. Two distinct
liveness signals: per-task lease and per-worker presence. The console shows heartbeat age; a worker with
no heartbeat for `3 × interval` is drawn as dead and its leases become reclaimable.

### Exponential backoff with jitter

`delay = min(base * 2**(attempt-1), cap)`, then **full jitter**: `delay = random(0, delay)`. Defaults
`base = 1s`, `cap = 300s`, `max_attempts = 5`. Retries land in `ZSET sched:<queue>` scored by wake time;
a scheduler tick moves due entries into the stream via Lua so the move is atomic. After `max_attempts`
the task goes to `dlq:<queue>` with the full attempt history and state `dead`.

Exception classes declare retryability: a `ValueError` from bad payload is terminal on attempt 1; a
socket timeout retries. Retrying non-retryable errors five times is a bug, not caution.

### Mid-task crash recovery

A reclaimer loop (one per worker, jittered) runs `XAUTOCLAIM q:<name> <group> <consumer> <min_idle>` to
adopt entries whose lease expired. On adoption: `attempt += 1`, previous attempt row closed with outcome
`lease_lost`, new fence issued. Because the fence is new and higher, the zombie can never commit. The
chaos suite must include `SIGKILL` mid-execution (not `SIGTERM` — that path is graceful shutdown and is
a separate test).

## 7. Control-plane API

```
POST   /v1/tasks              {queue, task_name, payload, dedup_key?, priority?, run_at?,
                               max_attempts?} → 201 {task_id, deduplicated: bool}
GET    /v1/tasks/{id}                        → task + attempts[]
DELETE /v1/tasks/{id}                        → cancel if not yet leased
POST   /v1/workflows          {name, spec}   → 201 {workflow_id}
GET    /v1/workflows/{id}                    → DAG + per-step state
GET    /v1/queues                            → depth, in-flight, throughput, oldest-pending age
GET    /v1/workers                            → fleet with heartbeat ages
GET    /v1/dlq/{queue}                        → dead tasks
POST   /v1/dlq/{queue}/requeue {task_ids[]}   → re-enqueue with attempt reset
GET    /metrics                               → Prometheus
GET    /v1/events                             → SSE: state transitions, for the console
```

Enqueue with an existing `dedup_key` returns `201` with `deduplicated: true` and the original task id.
It does not error — idempotent enqueue is the point.

### Workflow spec

```json
{ "name": "onboard-client",
  "steps": [
    {"name": "fetch",     "task": "fetch_docs",  "payload": {"client": "acme"}},
    {"name": "parse",     "task": "parse_docs",  "depends_on": ["fetch"]},
    {"name": "index",     "task": "index_docs",  "depends_on": ["parse"], "fan_out": "chunks"},
    {"name": "notify",    "task": "notify",      "depends_on": ["index"], "retry": {"max_attempts": 3}}
  ] }
```

Required semantics: a step runs when all `depends_on` steps have succeeded; `fan_out` expands one step
into N sibling tasks from a list in the upstream result and the downstream step waits for all of them
(fan-in); one dead step fails the workflow but leaves completed steps recorded; the DAG is validated for
cycles at submit time and rejected with `422`.

## 8. Ops console (`web/`)

Vite + React + TypeScript + Tailwind. Deliberately not Next.js — no SSR need, and it keeps the dependency
story matching the resume's stack line.

1. **Overview.** Per-queue depth, enqueue/complete rates, in-flight count, oldest-pending age, error rate.
2. **Workers.** Fleet table: id, queues, in-flight, heartbeat age, version. Dead workers greyed.
3. **Task detail.** State, payload, and the attempt timeline — each attempt with worker, fence, duration,
   outcome, and traceback. The retry/backoff story is visible here or nowhere.
4. **Workflow view.** DAG rendered with per-node state colours; click a node for its task detail.
5. **DLQ.** Browse, inspect, requeue.
6. **Chaos panel.** Kill a worker, pause a queue, inject a failing task. Backed by admin endpoints that
   exist only when the control plane runs with `DTQ_ENABLE_CHAOS=1`.

M6 work. No engine code may import from `web/`, and the console must never be the reason the engine is
unfinished.

## 9. Testing requirements

| Suite | Must contain |
| --- | --- |
| unit | backoff maths incl. jitter bounds, DAG cycle detection, retryability classification, serialisation round-trip |
| integration (real Redis + Postgres via compose) | enqueue→execute→succeed; dedup enqueue; delayed run_at; DLQ after max attempts; requeue |
| chaos | `SIGKILL` worker mid-task → reclaimed, `attempt` incremented, effect committed once; zombie-commit rejected by fence; Redis restart mid-run; Postgres connection drop |
| property | N random tasks with random failure injection ⇒ `count(effects) == count(distinct dedup_key)`, always |
| load | the benchmark in §10, as a repeatable script |

The property test is the one that actually defends the headline claim. Write it early, not last.

## 10. Benchmark protocol

`bench/loadgen` (C++20, CMake, hiredis, pipelined): flags `--rate`, `--duration`, `--workers`,
`--payload-bytes`, `--queues`, `--failure-rate`. Reports enqueue rate, completion rate, end-to-end latency
p50/p95/p99, and a reconciliation count.

Record in every result JSON: host CPU/RAM, worker process count, Redis config (persistence on/off),
Postgres settings, payload size, task duration distribution, failure-injection rate, **and run duration**.

Two honesty rules on the `[X]M+ events per day` number:

1. **Run long enough to divide.** A 60-second burst multiplied by 1440 is extrapolation, and "load-tested
   to N per day" implies sustained. Run ≥ 30 minutes, ideally an hour, and report it as
   *"sustained R events/sec over a T-minute run — R × 86 400 = N/day"* with T stated. A reader can then
   check the arithmetic themselves, which is the whole point.
2. **"Across multiple worker nodes" means multiple hosts.** Processes on one laptop are multiple
   *workers*, not multiple *nodes*. The resume stack says AWS EC2 — so the headline run belongs on 2–3
   EC2 instances with a managed or self-hosted Redis, Terraform'd up and torn down the same day. A few
   dollars. If the run stays on one machine, the bullet must say "worker processes", not "worker nodes".

For scale: 1 M/day is 11.6 events/sec, which is not a claim worth making. 500 M/day is ~5 800/sec, which
is. Aim high enough that the number means something, and let the measurement decide.

## 11. Milestone acceptance criteria

- **M1 Core + broker.** Enqueue → single worker executes → state in Postgres. Compose file brings up
  Redis + Postgres on this project's ports. Migrations run. `pytest` green with real services.
- **M2 Reliability.** Lease + fence + heartbeat implemented; backoff with jitter; DLQ. Chaos test:
  `SIGKILL` mid-task is reclaimed and completes exactly once.
- **M3 Exactly-once effect.** Property test green over ≥ 200 randomised runs. Zombie-commit rejection
  test present and named.
- **M4 Workflows.** DAG execution with fan-out/fan-in; cycle rejection; per-step retry policy.
- **M5 Scale.** C++ loadgen builds and runs; ≥ 30-minute sustained run committed to `bench/results/`;
  **README Benchmarks table filled from it**; multi-node run done on EC2 or the claim reworded.
- **M6 Presentable.** Ops console renders a live queue and a real reclaim event; optional C++ worker;
  Prometheus metrics; CI green.

## 12. Honest-claims register

| Claim | Status | Backed by |
| --- | --- | --- |
| distributed locking | ☐ | lease + fencing token, CAS release, zombie-rejection test |
| worker heartbeats | ☐ | per-worker presence key + per-task lease renewal; console shows age |
| exponential-backoff retries | ☐ | unit test on jitter bounds + integration test through to DLQ |
| mid-task crash recovery | ☐ | `SIGKILL` chaos test via `XAUTOCLAIM` |
| exactly-once execution semantics | ☐ | property test: `effects` rows == distinct dedup keys; README states the mechanism |
| `[X]M+` events/day | ☐ | ≥ 30 min sustained run, JSON committed, duration stated |
| across multiple worker **nodes** | ☐ | ≥ 2 hosts, or bullet reworded to "worker processes" |

Any unchecked row ⇒ the link at `Bravim_Purohit_SDE.tex:141` stays commented.

---

## 13. Extended stack (added 2026-08-17)

### 13.1 Broker abstraction — and why the comparison is the point

Redis Streams stays the default and remains what the resume bullet describes. Kafka and RabbitMQ arrive as
adapters behind one interface:

```python
class Broker(Protocol):
    async def publish(self, queue: str, task: TaskEnvelope) -> None: ...
    async def lease(self, queue: str, group: str, consumer: str, count: int) -> list[Leased]: ...
    async def ack(self, queue: str, group: str, ids: list[str]) -> None: ...
    async def reclaim(self, queue: str, group: str, min_idle_ms: int) -> list[Leased]: ...
    async def schedule(self, queue: str, task: TaskEnvelope, run_at: float) -> None: ...
    async def dead_letter(self, queue: str, task: TaskEnvelope, reason: str) -> None: ...
```

Writing three implementations against that interface forces the interesting question into the open: each
broker gives you a *different* crash-recovery primitive, and the engine has to work with all three.

| | in-flight recovery | ordering | delayed delivery |
| --- | --- | --- | --- |
| Redis Streams | Pending Entries List + `XAUTOCLAIM` | per-stream | ZSET + scheduler tick (built by us) |
| **Kafka** | consumer-group rebalance + committed offsets | per-partition | not native — needs a delay topic or scheduler |
| **RabbitMQ** | unacked messages redelivered on channel close | per-queue | TTL + dead-letter exchange, or a delay plugin |

The deliverable is a written comparison in `docs/BROKERS.md` backed by the same benchmark run against all
three, on the same workload and hardware. **That comparison is worth more than any single throughput
number** — it demonstrates you understand the trade rather than having picked a default.

Honest note for the README: Kafka's at-least-once delivery plus the existing fencing + dedup design gives
the same exactly-once *effect* guarantee. Kafka's own "exactly-once semantics" (idempotent producer +
transactions) is a different mechanism with a different scope, and conflating the two is a common and
easily-caught error. Say which one you're relying on.

- **Kafka:** KRaft mode, single broker for dev (no ZooKeeper). Topics per queue, partitions as the
  parallelism unit, consumer groups for workers, `aiokafka` or `confluent-kafka`. Message values are
  **Protobuf**-encoded `TaskEnvelope`s with a schema version field.
- **RabbitMQ:** `aio-pika`, quorum queues, publisher confirms, TTL + DLX for backoff.

### 13.2 gRPC worker control channel

Task delivery stays on the broker. Worker *control* moves to gRPC, which is what gRPC is actually good at:

```proto
service WorkerControl {
  rpc Register (RegisterRequest) returns (RegisterReply);
  rpc Session  (stream WorkerEvent) returns (stream ControlCommand);  // bidirectional
  rpc Drain    (DrainRequest) returns (DrainReply);
}
```

The bidirectional stream carries heartbeats and in-flight counts up, and `pause` / `resume` / `drain` /
`cancel_task` commands down. This makes graceful shutdown and the chaos panel's "kill a worker" button
real operations rather than polled state, and it gives the Redis heartbeat key a purpose it can keep
(presence) separate from control (commands).

### 13.3 Parquet for results

Benchmark and load-test output moves to Parquet alongside the JSON manifest. A 30-minute run at several
thousand events/sec produces millions of per-task latency records; Parquet makes them queryable with
`pyarrow`/`duckdb` instead of unusable. JSON stays for the small manifest so results remain readable in a
diff.

### 13.4 OpenTelemetry

Trace context propagates **through the broker** — inject it into the `TaskEnvelope` at enqueue, extract it
in the worker, and a task's trace then spans producer → broker → lease → execute → retry → DLQ across
process boundaries. That is the single most useful thing tracing does for a queue, and it's impossible
without deliberate context propagation.

Spans: `enqueue`, `lease`, `execute`, `commit`, `reclaim`, `schedule`. Metrics via the OTel SDK to
Prometheus: queue depth, lease age, reclaim count, retry count by attempt, DLQ rate, worker heartbeat age.
Traces to Collector → Jaeger locally.

Retries and reclaims must **link** to the original attempt's trace (span links, not a fresh root trace),
or the story of a task that failed four times is scattered across four unconnected traces.

### 13.5 Milestone amendment

- **M7 Broker comparison.** All three adapters passing the same integration + chaos suites unchanged; the
  suite parameterised over brokers so a Redis-only assumption in the engine surfaces as a failure, not a
  silent difference. Same benchmark on all three, committed to `bench/results/`. `docs/BROKERS.md` written.
  gRPC control channel live, with drain and cancel exercised. OTel context propagating end to end.

Redis Streams remains the number reported on the resume unless another broker wins on the same hardware —
in which case report the winner and say so.
