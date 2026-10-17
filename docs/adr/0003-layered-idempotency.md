# ADR-0003: Layered idempotency instead of "exactly-once delivery"

* Status: accepted
* Date: 2026-10-17

## Context

Exactly-once *delivery* is impossible across a network boundary. Providers redeliver
webhooks, queues redeliver messages, workers crash after calling an API but before
recording it. What we can build is exactly-once *effect*.

## Decision

Deduplicate at every boundary:

1. **Ingress**: `UNIQUE(source, idempotency_key)`, with the key taken from the
   provider's delivery id or a payload id, falling back to the body hash.
2. **Fan-out**: `UNIQUE(event_id, workflow_name)`.
3. **Execution**: per-step checkpoints. Succeeded, skipped and continued steps are
   never re-executed when a run is retried or resumed.
4. **Downstream**: each step gets `idempotency_key = run_id:generation:step_id`.
   `http_request` sends it as `Idempotency-Key`, and `emit_event` uses it as the
   child event's key. It changes only on an explicit *restart* replay (new
   generation).

## Consequences

* ✅ Duplicate webhooks, duplicate queue messages, retries and DLQ resumes cause no
  repeated side effects, provided downstreams honour idempotency keys (Stripe, most
  payment and CRM APIs do).
* ✅ Operators can choose between *resume* (no repeats) and *restart* (deliberate
  re-execution).
* ⚠️ A downstream that ignores idempotency keys can see a duplicate call in one
  window only: a crash after the call but before the checkpoint. This is documented.
