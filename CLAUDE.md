# CLAUDE.md — Distributed Task Queue & Workflow Engine

Operating instructions for a Claude Code session in this repo. Read `SPEC.md` first — especially §1,
which explains what "exactly-once" is allowed to mean here. Follow `ROADMAP.md` for order.

## What this is

An asyncio background-job engine on Redis Streams with distributed leases, worker heartbeats, jittered
exponential backoff, and crash recovery — plus a C++ load generator, because a Python harness can't
measure a Python queue's ceiling. It exists to prove one resume bullet, quoted in `SPEC.md` §1.

## Hard rules

1. **Stay inside this directory.** Independent git repo; the parent is deliberately not a repo and seven
   sibling projects sit beside it. Never read, write, or `git` above `distributed-task-queue/`.
2. **Never invent a measurement.** The `[X]M+ events/day` number comes from a committed run in
   `bench/results/` of ≥ 30 minutes. No extrapolating a 60-second burst.
3. **Never touch the resume.** Different repo. Don't edit the `.tex`, don't uncomment the GitHub link.
4. **"Exactly-once" is a technical claim, not a slogan.** Implement at-least-once delivery + idempotent
   commit + fencing tokens, and describe it that way. Never write a docstring asserting exactly-once
   delivery — it isn't true and the code doesn't do it.
5. **No Celery, RQ, Dramatiq, or arq.** Reading them is encouraged (`docs/STUDY.md`); importing them
   deletes the project's reason to exist. Redis client, FastAPI, Pydantic, SQLAlchemy/asyncpg are fine.
6. **No Redlock.** Single-Redis lease plus fencing token — `SPEC.md` §6. If you think Redlock is needed,
   stop and ask.
7. **Don't weaken a test to make it pass.** A failing exactly-once property test means a real
   double-commit path exists. Find it.

## Environment (this machine: arm64 macOS, 11 cores, 18 GB)

`python3` on the PATH is **3.8.10 and unusable here**. Don't fight pyenv — `uv` 0.12 is installed:

```bash
uv venv --python 3.12
source .venv/bin/activate
uv pip install -e ".[dev]"
```

Pin `requires-python = ">=3.11"` in `pyproject.toml`. CI uses 3.12; keep them matched.

Services (Docker 28 + compose v2.33 installed):

```bash
docker compose -f docker-compose.dev.yml up -d      # Redis + Postgres on this project's ports
alembic upgrade head                                 # or your migration runner
```

C++ loadgen: Apple clang 17 and CMake 4.2 are present; `ninja` is **not** (`brew install ninja`, or use
the default Makefiles generator).

```bash
cmake -S bench -B bench/build -DCMAKE_BUILD_TYPE=Release && cmake --build bench/build --parallel 10
```

Console (`web/`, M6): Node 22 / npm 10 installed.

## Ports — this project owns 7200–7299

Up to eight sibling projects may run at once. Never bind outside this block, and never bind :3000,
:5432, :6379, or :8000 — three sibling projects also want Redis and Postgres.

| Port | Use |
| --- | --- |
| 7200 | `web/` ops console dev server |
| 7201 | control plane (FastAPI) |
| 7202 | Redis |
| 7203 | Postgres |
| 7204 | Prometheus (optional) |
| 7205 | Grafana (optional) |
| 7206–7215 | worker metrics endpoints |

Put these in `docker-compose.dev.yml` as `"7202:6379"` / `"7203:5432"` — remap on the host side, keep
container-internal ports standard.

## Commands

```bash
uvicorn dtq.control.app:app --reload --port 7201
python -m dtq.worker --queues default,high --concurrency 8 --metrics-port 7206
pytest -q                                   # unit
pytest -q -m integration                    # needs compose services up
pytest -q -m chaos                          # SIGKILL / restart scenarios
./scripts/chaos_kill_worker.sh              # the demo: kill mid-task, watch reclaim
./bench/build/loadgen --rate 5000 --duration 30m --workers 4
```

## Conventions

- Python 3.12, `async`/`await` throughout. No blocking I/O in the event loop — CPU-bound task bodies go
  to a thread or process pool, and the executor boundary is explicit.
- Full type hints; `mypy --strict` on `dtq/core`, `dtq/broker`, `dtq/store`. Ruff for lint + format.
- Pydantic v2 for all wire payloads. Redis and Postgres access is confined to `dtq/broker` and
  `dtq/store` — no raw client calls leaking into `worker/` or `control/`.
- Every multi-step Redis mutation is a Lua script in `dtq/broker/scripts/`. Read-modify-write across
  round trips is a bug here, not a style preference.
- Structured logging (JSON) with `task_id`, `worker_id`, `attempt`, `fence` on every task-scoped line.
  Tracing this system without those four fields is impossible.
- Tests: pytest + pytest-asyncio. Integration and chaos tests are marked and run against real services —
  no mocked Redis. `fakeredis` will hide exactly the bugs this project is about.
- Commits: imperative, ≤ 72 chars, scoped — `broker: move due retries with a Lua script`.
- Git identity is already set for this repo (`bravimpurohit1305@gmail.com`). Leave it.

## Definition of done, and when to stop

Milestones per `SPEC.md` §11. CI must be green; the workflow currently tolerates an empty scaffold
(`pytest` exit 5 passes) — once real tests exist that tolerance is dead weight, so tighten it.

**Stop and ask the user** when:

- The `[X]M+ events/day` claim needs real EC2 instances to be honest about "multiple worker nodes" — that
  is a spend decision (a few dollars) and a resume-wording decision, not yours to make silently.
- A `SPEC.md` requirement looks wrong or unimplementable as written.
- You want a dependency not named in `SPEC.md`.
- The exactly-once property test fails in a way that suggests the design, not the code, is wrong.

Report honestly: "property test green over 200 seeds, chaos suite green, sustained 4 200/sec over
35 minutes on one host" is a real status. "Load-tested to 5 M/day" with no committed result file is not.

---

## Extended stack additions (2026-08-17)

See `SPEC.md` §13. Gains: a **broker abstraction** with Kafka and RabbitMQ adapters, a **gRPC worker control
channel**, **Protobuf** envelopes, **Parquet** results, **OpenTelemetry**.

**New ports** (same 7200–7299 block): `7216` Kafka (KRaft) · `7217` RabbitMQ · `7218` RabbitMQ management ·
`7219` gRPC worker control · `7220` Jaeger UI · `7221` OTel Collector gRPC.

**New prerequisites:** `aiokafka` or `confluent-kafka`, `aio-pika`, `grpcio` + `grpcio-tools`, `protobuf`,
`pyarrow`. Kafka in **KRaft mode, single broker** — no ZooKeeper. Add both brokers to compose under a
`--profile brokers` so the default `up` stays light.

**New hard rules:**

8. **Redis Streams stays the default and stays the resume number.** Kafka and RabbitMQ are adapters. Don't
   promote one to default because it benchmarked better without telling the user — that's a resume change.
9. **The integration and chaos suites are parameterised over brokers** and must pass unchanged on all three.
   A Redis-only assumption leaking into the engine has to surface as a test failure, not a silent difference.
10. **Never confuse the two "exactly-once"s.** Kafka's EOS (idempotent producer + transactions) is a different
    mechanism from this project's at-least-once-delivery-plus-idempotent-commit. State which one a given path
    relies on. Conflating them is the single most catchable error in this repo.
11. **Trace context propagates inside the `TaskEnvelope`**, not just in-process. A task's trace must span
    producer → broker → lease → execute → retry → DLQ. Retries and reclaims use **span links** to the prior
    attempt, never a fresh root trace.
12. **Benchmarks run with exporters off**, and the result manifest records it.

**New stop-and-ask:** if a non-Redis broker wins materially on the same hardware, report it and let the user
decide — it affects the resume bullet, not just the default.
