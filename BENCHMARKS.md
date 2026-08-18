# Benchmark Results — Distributed Task Queue

Recorded 2026-08-18. Host: Apple M3 Pro, 11 cores, 18 GB, macOS 27.0 arm64. **Single host — these are worker *processes*, not worker nodes.**

## Two very different numbers. Use the right one.

| Path | What it exercises | Sustained | Events/day | Run |
| --- | --- | --- | --- | --- |
| **Broker** | Redis Streams `XADD` → `XREADGROUP` → `XACK` | **118 831 /s** | 10.27 B | 31 min |
| **Engine** | control plane → Redis → worker → lease/fence → **Postgres commit** | **224.8 /s** | 19.4 M | 5 min |

The gap is ~500×, and it is the whole story: the broker figure measures Redis, while the
engine figure measures the thing the resume bullet actually describes — durable,
idempotent, fenced task execution with a Postgres commit per task.

**Quoting the broker number as the engine's throughput would be wrong.** If the resume
says "background-job engine … load-tested to [X]M+ events per day", the honest figure is
the engine one.

### Broker run — `bench/results/sustained-31min.json`

```
Duration:      1860 s (31 min)      Enqueued:  221 819 500
Complete rate: 118 831 /s           Completed: 221 026 592
Failed:        0                    Latency:   p50 3 ms · p95 7 ms · p99 17 ms · max 4371 ms
```

Enqueue and completion rates tracked within 0.4 % for the full 31 minutes with zero
backlog growth — a genuine steady state, not a burst extrapolated to a day.

### Engine run — `bench/results/engine-path.json`

```
Duration: 301 s   Enqueued: 67 699   Completed: 67 682   Rate: 224.8 /s
Workers:  4 processes × concurrency 32
```

**Caveat:** enqueued and completed are nearly 1:1, so the 24 async HTTP producers were
likely the limit rather than the workers. A cleaner engine ceiling would pre-load the
queue and measure drain rate with no HTTP in the loop. 224.8/s is therefore a *floor*,
not the engine's maximum.

## Four bugs found by running this

Every one of these blocked a measurement, and all four predate this session.

### 1. Broker benchmark stalled at ~583 000 messages, silently

`XADD` was issued without `MAXLEN`, so the stream grew unbounded until Redis hit its
256 MB `maxmemory` under `noeviction` and began rejecting writes. Consumers drained what
existed (`pending: 0`, `lag: 0`) and then had nothing left.

Worse, the producer counted **every reply as a success**, including `REDIS_REPLY_ERROR`.
So the harness reported 2 700 056 "enqueued" while the stream held 582 832 — phantom
throughput. Fixed by adding `MAXLEN ~ 200000` and counting error replies as failures.

Three separate runs stalled at 583 093 / 582 850 / 582 832 before this was found.

### 2. The database schema had never been created

`alembic upgrade head` aborted with `DuplicateObjectError: type "task_state" already
exists`. The migration called `task_state.create(..., checkfirst=True)` and then passed
the same `sa.Enum` to `op.create_table`, which emits a second `CREATE TYPE`. Fixed by
referencing the column type as `postgresql.ENUM(..., create_type=False)`.

### 3. Every control-plane request returned HTTP 500

`AttemptRow.task_id` had no `ForeignKey` in the ORM (the migration had it, the model did
not), so `TaskRow.attempts` could not resolve its join and **every** SQLAlchemy mapper
failed to configure. Fixed by declaring the FK on the model.

### 4. `UnboundLocalError` on every enqueue

`dtq/control/events.py:publish_event` ended with `_subscribers -= dead`, which rebinds the
module-level name and therefore makes `_subscribers` a local for the whole function —
raising `UnboundLocalError` on the read above it. Fixed with `difference_update`.

## What did work, first try

Once the engine ran, the reliability machinery behaved exactly as specified: leases with
fencing tokens, worker heartbeats, **exponential backoff with jitter** (observed retry
delays 3.68 s / 1.38 s / 2.31 s on successive attempts), and dead-lettering after
`max_attempts`. Those were visible in the worker logs without any fixing.

## Reproducing

```bash
docker compose -f docker-compose.dev.yml up -d redis postgres
alembic upgrade head
cmake -S bench -B bench/build -DCMAKE_BUILD_TYPE=Release && cmake --build bench/build -j
./bench/build/loadgen --duration 1860 --rate 0 --producers 4 --consumers 8   # broker

uvicorn dtq.control.app:app --port 7201 &
for i in 1 2 3 4; do python bench/bench_worker.py & done                      # engine
python bench/engine_bench.py 300
```
