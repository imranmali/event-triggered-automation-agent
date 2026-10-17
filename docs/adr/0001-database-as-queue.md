# ADR-0001: Use the relational database as the work queue

* Status: accepted
* Date: 2026-10-17

## Context

Ingesting an event has to (a) persist the event, (b) decide which workflows it
triggers and (c) enqueue that work, and do all three atomically. If the work went to
a separate broker (Redis, RabbitMQ, SQS), we'd face the dual-write problem: a crash
between the DB commit and the broker publish loses work, and the other order creates
work for events that don't exist. We'd need an outbox table plus a relay to fix that.
We also need per-run state (attempts, checkpoints, leases) that a broker can't hold.

## Decision

The `runs` table is the queue. Ingest writes the event and its runs in one
transaction. Workers claim runs with `UPDATE … RETURNING`, using
`FOR UPDATE SKIP LOCKED` inside a `MATERIALIZED` CTE on Postgres. Delays and retries
are expressed through `next_attempt_at`.

## Consequences

* ✅ Exactly one durable source of truth. A webhook is acknowledged only after the
  work exists.
* ✅ Retry scheduling, visibility (`/api/runs`) and the DLQ are just queries.
* ✅ SQLite works for development and tests, with the same code path minus row locks.
* ⚠️ Polling adds up to `poll_interval` of latency for cross-process work (runs
  created in-process wake their worker immediately). Postgres `LISTEN/NOTIFY` can fix
  this later.
* ⚠️ Throughput is bounded by the database. That's acceptable for automation
  workloads (hundreds to low thousands of runs per second on Postgres). The
  `Store` boundary allows swapping the claim mechanism later.
* Queue *ingress* (Redis Streams) remains supported. Those messages are converted to
  events through the same transactional ingest path.
