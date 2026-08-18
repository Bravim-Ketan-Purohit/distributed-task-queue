# Distributed Task Queue & Workflow Engine

Asynchronous background-job engine with **distributed locking, worker heartbeats, exponential-backoff
retries, and mid-task crash recovery** for exactly-once execution semantics.

**Stack:** Python (asyncio) · C++ · Redis · AWS EC2
**Resume target:** `Bravim_Purohit_SDE.tex` → Projects & Publications
**Role:** Software Development Engineer

---

## The claim this repo must prove

> Asynchronous background-job engine with distributed locking, worker heartbeats, exponential-backoff
> retries, and mid-task crash recovery for exactly-once execution semantics. Load-tested to **[X]M+**
> events per day across multiple worker nodes.

### A note on "exactly-once"

This is the phrase an interviewer will attack, and they should. Exactly-once *delivery* is impossible
across an unreliable network. What is achievable is exactly-once *effect*: at-least-once delivery plus
idempotent execution, fenced by a lock with a monotonic token so a resurrected zombie worker cannot
commit stale work.

Decide now which one this repo implements, implement it, and describe it in those words. Getting this
distinction right is worth more in an interview than the throughput number.

## Benchmarks this repo owes the resume

| Metric | Resume placeholder | Measured | Method |
| --- | --- | --- | --- |
| Sustained volume — **engine path** | `[X]M+` events/day | **19.4 M/day** (224.8/s) | durable path w/ Postgres commit — [BENCHMARKS.md](BENCHMARKS.md) |
| Sustained volume — broker path | *(not the bullet)* | 10.27 B/day (118 831/s) | Redis Streams only, 31 min sustained |
| Latency (broker) | — | p50 3 ms · p95 7 ms · p99 17 ms | 31-min run, 221 M events, 0 failed |
| Topology | — | **4 worker processes, 1 host** | not "worker nodes" |

Record: worker-node count, payload size, task duration distribution, Redis instance class, and whether
the number is measured sustained throughput or extrapolated from a shorter window. State the
extrapolation explicitly if that's what it is.

**Do not uncomment** the GitHub link at `Bravim_Purohit_SDE.tex:141` until this is filled and the repo
is public.

## Architecture

```
  producers                                     workers (N nodes)
     │                                                │
     ▼                                                ▼
 ┌────────────────── Redis ──────────────────┐   ┌──────────────┐
 │  queues (priority)   scheduled (ZSET)     │◄──│ poll / claim │
 │  locks + fence tokens   heartbeats        │   │  fork exec   │
 │  dead-letter queue      results           │   │  heartbeat   │
 └───────────────────────────────────────────┘   └──────┬───────┘
                    ▲                                   │
                    │        reaper: expired lease ──────┘
                    └──── requeue with backoff, or → DLQ
```

The interesting part is not the happy path — it's the reaper. When a worker dies mid-task, its lease
expires, and the task must return to the queue exactly once without racing another reaper.

## Why Python *and* C++

The resume claims both. Justify it or drop one: the orchestration and scheduling layer in Python
(asyncio, fast to iterate) and a C++ worker for hot-path execution where per-task overhead dominates,
with a measured comparison between the two. An unexplained language in a bullet is a liability.

## Getting started

```bash
python -m venv .venv && source .venv/bin/activate   # needs Python 3.11+
pip install -r requirements.txt
docker run -p 6379:6379 redis:7                     # local broker
pytest -q
```

## Layout

```
queue/         enqueue API, serialization, priorities
worker/        poll loop, fork/exec, heartbeat, failure capture
scheduler/     delayed + periodic tasks (Redis ZSET)
reaper/        lease expiry detection and safe requeue
cpp_worker/    C++ execution path
bench/         load generator
docs/STUDY.md  notes from rq and celery
```

## Documents

| File | What it's for |
| --- | --- |
| [SPEC.md](SPEC.md) | **Authoritative** technical specification — what to build, the data model, the measurement protocol, and the honest-claims register |
| [ROADMAP.md](ROADMAP.md) | Build order, milestone by milestone |
| [CLAUDE.md](CLAUDE.md) | Operating rules for a coding session here: environment, ports, conventions, when to stop and ask |
| [docs/STUDY.md](docs/STUDY.md) | What to read in the reference implementations before writing code |

Where `SPEC.md` and any other document disagree, `SPEC.md` wins.

## Status

**Implemented and benchmarked.** Engine path measured at 224.8 tasks/sec (19.4 M/day) with a
durable Postgres commit per task; the Redis broker path sustains 118 831/s over 31 minutes with
zero failures. See [BENCHMARKS.md](BENCHMARKS.md) — including four bugs found by running it, one of
which meant the database schema had never been created.

This repo reserves ports **7200–7299**; up to eight sibling
projects may run at the same time, so nothing here binds outside that block.
