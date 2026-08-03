# Study notes — Distributed Task Queue

Reference material, carried over from `projects-ref.md`.

## References

### [`rq/rq`](https://github.com/rq/rq) — start here

Redis Queue. Much simpler than Celery and readable end to end.

**What to study:** open `worker.py` and follow the loop:

1. How it continuously polls Redis for new jobs
2. How it forks a new process to execute the job
3. How it handles a job failing — traceback capture

That loop is the thing being rebuilt here. Read it until you could write it from memory, then close
the tab and write your own.

Also worth reading in `rq`: the job state model (queued → started → finished/failed), and how the
registries track in-flight work so a dead worker's jobs are discoverable.

### [`celery/celery`](https://github.com/celery/celery)

The Python standard, but the codebase is incredibly dense. Use it as a reference for *what features
exist and why*, not as a model for structure.

**What to study:** the concepts — `acks_late`, visibility timeout, prefetch multiplier, task
idempotency, and the documented caveats around them. Celery's docs are honest about where guarantees
break down, which is exactly the material an interviewer probes.

## The distinction to get right

Celery's own documentation is careful here, and so should this repo be:

- **at-most-once** — ack before executing; work can be lost
- **at-least-once** — ack after executing; work can run twice
- **exactly-once effect** — at-least-once delivery + idempotent execution + fencing

There is no exactly-once *delivery* over an unreliable network. Read up on fencing tokens
(Kleppmann's critique of distributed locking is the canonical write-up) and make sure the lock design
here survives a paused-then-resumed worker.

## Questions to answer before coding

1. When is a job acked — before or after execution? What does each choice cost?
2. A worker is `SIGSTOP`ed for longer than its lease, then resumed mid-write. What stops it from
   corrupting state?
3. Why does exponential backoff need jitter?
4. What makes a task idempotent, and whose responsibility is it — the queue's or the task author's?
5. Fork vs. thread vs. subprocess for execution — what does fork buy, and what does it break?

## Deliberate divergences from the references

| Area | rq / celery does | This repo does | Why |
| --- | --- | --- | --- |
| | | | |
