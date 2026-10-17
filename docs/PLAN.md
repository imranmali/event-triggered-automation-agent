# Project Plan — Event-Triggered Automation Agent

## 1. Goal

Build a production-grade automation engine that **listens to webhooks and
queues, matches incoming events against declarative triggers, and executes
multi-step workflows** with the guarantees real systems need:

| Requirement            | What "done" means                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------------------- |
| Webhook ingestion      | Authenticated (HMAC/GitHub/Stripe signatures), size-limited, durable before ACK, `202` in ms        |
| Queue ingestion        | Redis Streams consumer groups, at-least-once, crash recovery of pending messages, poison handling   |
| Trigger matching       | Source + event type (glob) + payload filters; one event can fan out to many workflows                |
| Workflow execution     | Ordered steps, templated inputs, conditional steps, step timeouts, pluggable actions                |
| Idempotent execution   | Dedupe at ingress, at run creation, at step level (checkpoints) and downstream (idempotency keys)   |
| Retry logic            | Durable exponential backoff with jitter, `Retry-After`, retryable vs permanent error classification  |
| Dead-letter handling   | Exhausted/permanent failures parked with full context; inspect, replay (resume or restart), discard  |
| Operability            | Leases + heartbeats + fencing, graceful shutdown, metrics, structured logs, health checks, admin API |
| Deployability          | Single binary-ish CLI, Docker image, docker-compose stack, migrations, CI                            |

Non-goals (v1): a visual workflow editor, long-running durable timers
(hours/days), cross-region active-active.

## 2. Architecture decisions

* **Python 3.11+, asyncio** — I/O bound workload; FastAPI for HTTP, httpx for
  outbound calls.
* **The database is the queue** (ADR-0001). Runs live in a `runs` table and
  workers claim them with `UPDATE … RETURNING` (+ `FOR UPDATE SKIP LOCKED` on
  Postgres). This gives transactional ingest (event + runs written atomically),
  no dual-write problem and one less moving part. SQLite for dev/tests,
  Postgres in production.
* **Leases with fencing** (ADR-0002) — a claimed run carries
  `lease_owner`/`lease_token`; every state transition is conditional on the
  token, so a zombie worker can never overwrite a run that was reclaimed.
* **Layered idempotency** (ADR-0003) — see table above.
* **Declarative YAML workflows** with sandboxed Jinja2 templating; secrets are
  only reachable through an explicit `secrets.*` namespace.
* **Workflow versions are pinned** — every run references an immutable
  snapshot of the workflow definition, so retries and replays are deterministic
  even after a config change.

## 3. Milestones

| #  | Milestone                                                        | Status |
| -- | ---------------------------------------------------------------- | ------ |
| M0 | Plan, repo skeleton, tooling (ruff, mypy, pytest, CI)            | ⏳     |
| M1 | Domain model, schema, store with leasing/fencing, migrations     | ⏳     |
| M2 | Workflow DSL: spec, loader, trigger matcher, templating          | ⏳     |
| M3 | Actions: http_request, transform, log, emit_event, delay, fail, python plugins | ⏳ |
| M4 | Execution engine: executor w/ checkpoints, retry policy, worker (concurrency, heartbeats, reaper, graceful shutdown) | ⏳ |
| M5 | Ingress: webhook endpoint + signature verification, ingest service, Redis Streams consumer | ⏳ |
| M6 | Dead-letter queue: park, inspect, replay (resume/restart), discard, bulk replay | ⏳ |
| M7 | Admin API, metrics, structured logging, health/readiness, CLI    | ⏳     |
| M8 | Test suite: unit + integration + failure-injection (crash, zombie worker, concurrency, duplicate delivery) | ⏳ |
| M9 | Packaging: Dockerfile, docker-compose (Postgres + Redis), example configs, demo script | ⏳ |
| M10| Documentation: README, architecture, workflow reference, operations runbook, ADRs | ⏳ |

## 4. Delivery semantics (contract)

* **Ingress**: a webhook gets `202` only after the event *and* its runs are
  committed. Duplicate deliveries (same idempotency key) get `200` with the
  original event id and create nothing.
* **Execution**: at-least-once per step, effectively-once per step for actions
  that honour the idempotency key the engine passes them (`run_id:step_id`,
  stable across retries).
* **Failure**: a run ends `succeeded`, `dead_lettered` or `cancelled` — never
  silently lost. Worker crashes are recovered when the lease expires.

## 5. Test strategy

* Unit tests for pure logic (backoff, matcher, templating, signatures).
* Store tests run against SQLite always and Postgres when
  `ETA_TEST_DATABASE_URL` is set (CI does this).
* Engine integration tests with a fake clock-free design (short real delays).
* Failure-injection: lease expiry mid-step, zombie worker write rejected,
  N workers × M runs executed exactly once, duplicate webhook/queue delivery,
  poison queue messages.
* API tests via FastAPI `TestClient`/httpx ASGI transport.
* Redis consumer tests against a real `redis-server` (skipped if absent).

## 6. Risks & mitigations

| Risk                                   | Mitigation                                                                   |
| -------------------------------------- | ---------------------------------------------------------------------------- |
| DB as queue becomes a bottleneck       | Indexed claim query, SKIP LOCKED, batch claims; adapter boundary allows swap |
| Clock skew between workers             | Leases are generous vs heartbeat interval; NTP required (documented)         |
| Non-idempotent downstream APIs         | Engine passes stable idempotency keys; docs call out the contract            |
| Template injection / secret leakage    | Immutable sandboxed Jinja2, explicit `secrets` namespace, secrets redacted in logs |
| Retry storms                           | Full jitter, max delay cap, `Retry-After`, per-step policies                 |

## 7. Future work

* SQS / Kafka / RabbitMQ ingress adapters (interface already exists).
* Postgres `LISTEN/NOTIFY` wake-ups to cut idle polling latency.
* Durable timers for long `delay` steps; parallel step groups (DAGs).
* Per-workflow concurrency limits and rate limiting.
* OpenTelemetry tracing across ingress → run → outbound HTTP.
