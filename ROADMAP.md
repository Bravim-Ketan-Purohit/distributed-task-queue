# Roadmap — Distributed Task Queue

Build the single-worker happy path first, then break it on purpose. Most of this project's interview
value lives in the failure handling, so the failure tests are the deliverable — not an afterthought.

## M1 — Minimum viable queue

- [ ] Enqueue API: task name, args, serialization format (pick one, justify it)
- [ ] Single worker poll loop against Redis
- [ ] Synchronous execution, result written back
- [ ] Traceback capture on failure — the full traceback, not just the exception message
- [ ] CI green

## M2 — Process isolation

- [ ] Fork per task so a segfault or OOM in a task can't take down the worker
- [ ] Task timeouts with hard kill
- [ ] Structured logging of task lifecycle: claimed → started → finished/failed

## M3 — Reliability primitives

- [ ] Distributed lock per task with a **fence token** (monotonic), not just a mutex
- [ ] Worker heartbeats with a lease TTL
- [ ] Reaper: detect expired leases, requeue exactly once
- [ ] Exponential backoff with jitter; max-attempt cap
- [ ] Dead-letter queue for terminally failed tasks
- [ ] **Write down the exactly-once story** in the README, precisely

## M4 — Break it deliberately

Each of these is a test, not a manual experiment:

- [ ] `kill -9` a worker mid-task → task runs again, effect applied once
- [ ] Redis restart mid-flight → no task silently lost
- [ ] Network partition between worker and Redis → lease expires, no double-commit
- [ ] Zombie worker resumes after its lease expired → fence token rejects its write
- [ ] Duplicate delivery → idempotency key prevents a second effect

## M5 — Scheduling and workflows

- [ ] Delayed tasks via Redis ZSET
- [ ] Periodic / cron-style tasks
- [ ] Task chaining: sequential dependencies, fan-out/fan-in
- [ ] Priority queues

## M6 — C++ worker + measurement

- [ ] C++ execution path sharing the same Redis protocol
- [ ] Head-to-head benchmark: Python vs C++ worker, per-task overhead
- [ ] Multi-node load test on EC2; **fill the Benchmarks table**
- [ ] Record node count, payload size, and task-duration distribution

## M7 — Presentable

- [ ] README architecture diagram matches reality
- [ ] Failure-mode tests documented and passing in CI
- [ ] Flip repo public, then uncomment `Bravim_Purohit_SDE.tex:141`

## Gate before the resume link goes live

`[X]M+` replaced with a measured number · every failure mode in M4 covered by a passing test ·
the exactly-once claim stated precisely enough to survive follow-up questions.
