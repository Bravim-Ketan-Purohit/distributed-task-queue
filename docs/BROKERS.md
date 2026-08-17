# Broker Comparison — Redis Streams vs Kafka vs RabbitMQ

This document records the trade-offs between the three broker adapters. Writing three
implementations against one interface forces the interesting question into the open:
each broker gives a *different* crash-recovery primitive, and the engine must work with all three.

## Summary

| | Redis Streams | Kafka (KRaft) | RabbitMQ |
| --- | --- | --- | --- |
| **In-flight recovery** | Pending Entries List + `XAUTOCLAIM` | Consumer-group rebalance + committed offsets | Unacked messages redelivered on channel close |
| **Ordering** | Per-stream | Per-partition | Per-queue |
| **Delayed delivery** | ZSET + scheduler tick (built by us) | Not native — delay topic + scheduler | TTL + dead-letter exchange |
| **Exactly-once mechanism** | Engine's fencing + dedup (at-least-once delivery) | Engine's fencing + dedup (NOT Kafka EOS) | Engine's fencing + dedup (at-least-once via publisher confirms) |
| **Crash recovery speed** | Immediate (XAUTOCLAIM, configurable min-idle) | Rebalance timeout (session.timeout.ms, default 45s) | Immediate on channel close |
| **Message ordering under failure** | Preserved within stream | Preserved within partition | Preserved within queue |
| **Operational complexity** | Low (single binary) | Medium (KRaft eliminates ZK, but still heavier) | Medium (quorum queues need ≥3 nodes for HA) |

## The critical distinction

All three adapters rely on the ENGINE's mechanism for exactly-once effect:

> **at-least-once delivery + idempotent commit (effects table) + fencing tokens = exactly-once EFFECT**

This is NOT:
- Kafka's "exactly-once semantics" (idempotent producer + transactions) — different mechanism, different scope
- RabbitMQ's "at-most-once" with auto-ack — we use manual ack
- Redis's lack of durability guarantees — we use AOF + the effects table in Postgres as the source of truth

The fencing token is what actually prevents double-commits across ALL THREE brokers. The broker just provides
the at-least-once delivery primitive; the engine provides the idempotent commit.

## Recovery characteristics

### Redis Streams
**Best for:** Fast crash recovery, low-latency reclaim.

`XAUTOCLAIM` transfers idle entries to a live consumer. The reclaimer runs as a loop within each worker
(jittered to avoid thundering herd). Recovery time = `min_idle_ms` + jitter ≈ seconds.

Limitation: single Redis is a SPOF. For production, Redis Cluster or Sentinel. The fencing tokens
survive Redis restarts because the effects table (Postgres) is the source of truth, not Redis.

### Kafka
**Best for:** High throughput, strong ordering guarantees per partition.

Recovery is via consumer-group rebalance: when a consumer dies, its partitions are reassigned to living
consumers after `session.timeout.ms`. This is slower than Redis XAUTOCLAIM (tens of seconds vs seconds)
but scales better to very high message rates.

Limitation: delayed delivery requires a workaround (delay topic or external scheduler). Partition count
is set at topic creation and bounds parallelism.

### RabbitMQ
**Best for:** Flexible routing, TTL-based backoff without external scheduler.

Recovery is automatic: when a consumer's channel closes (intentionally or via TCP death), unacked messages
are redelivered to other consumers. With quorum queues, this survives broker node failures too.

The TTL + DLX pattern gives native delayed delivery without a scheduler tick — messages expire and route
back to the main queue automatically.

Limitation: message rate ceiling is lower than Kafka and Redis Streams for single-queue throughput.

## Benchmark results

*(To be filled after ≥30-minute sustained runs against all three brokers on the same hardware.)*

| Broker | Rate (events/sec) | p50 latency | p99 latency | Duration | Hardware |
| --- | --- | --- | --- | --- | --- |
| Redis Streams | — | — | — | — | — |
| Kafka | — | — | — | — | — |
| RabbitMQ | — | — | — | — | — |

**Note:** Benchmarks run with OTel exporters OFF. The result manifest records this.

Redis Streams remains the number reported on the resume unless another broker wins on the same hardware —
in which case, report the winner and say so.

## Design decisions

1. **Broker is an interface, not a base class.** Protocol (structural subtyping) so adapters don't need
   to inherit — they just implement the methods.

2. **Lease management stays in Redis regardless of broker.** Even when Kafka or RabbitMQ delivers messages,
   the fencing token and lease are still in Redis (or could be moved to Postgres). The lease is not a broker
   concern — it's an engine concern.

3. **Integration and chaos suites are parameterized over brokers.** A Redis-only assumption in the engine
   surfaces as a test failure, not a silent behavioral difference.

4. **Kafka's reclaim() is a no-op.** Recovery happens via rebalance, which is external to our code.
   The engine still acquires fencing tokens and commits via the effects table — the same zombie rejection
   path applies regardless.

5. **RabbitMQ's promote_scheduled() is a no-op.** TTL + DLX handles delayed delivery natively.
