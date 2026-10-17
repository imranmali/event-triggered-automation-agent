# Event-Triggered Automation Agent

A production-grade automation engine: it **listens to webhooks and queues**, matches
events against declarative **triggers**, and executes multi-step **workflows** with
**idempotent execution**, durable **retries** and a **dead-letter queue** you can
inspect and replay.

```
 GitHub / Stripe / any HTTP ──► POST /webhooks/{source} ─┐   verify signature, dedupe,
 Redis Streams (consumer group) ─► stream consumer ──────┤   persist event + runs atomically
 Workflows (emit_event) ─────────► internal events ──────┘
                                                         ▼
                                    ┌────────── runs table (= durable queue) ──────────┐
                                    │ queued ─► running ─► succeeded                   │
                                    │   ▲         │                                    │
                                    │   └─ retry ◄┤ (backoff + jitter, Retry-After)    │
                                    │             └► dead_lettered ─► replay / discard │
                                    └──────────────────────────────────────────────────┘
                                                         ▲
                       N workers: claim (SKIP LOCKED) · lease + heartbeat · fencing tokens
                                  step checkpoints · reaper recovers crashed workers
```

## Why this is "production", not a demo

| Concern | How it's handled |
| --- | --- |
| **Never lose an event** | Webhooks get `202` only *after* the event and its runs are committed in one transaction. Queue messages are `XACK`ed only after commit. |
| **Never double-execute** | Four layers: ingress dedupe on `(source, idempotency_key)`; unique run per `(event, workflow)`; per-step checkpoints so retries skip completed steps; stable `Idempotency-Key` (`run:generation:step`) sent downstream. |
| **Survive crashes** | Runs are leased with a heartbeat. If a worker dies, the reaper requeues its run when the lease expires. The crash counts as a step attempt, so crash loops end in the DLQ. |
| **Zombie workers** | Every state transition is fenced on a lease token. A worker that stalls past its lease is rejected (`LeaseLostError`) and can't overwrite the new owner. |
| **Retry correctly** | Exponential backoff with full jitter, a delay cap and `Retry-After` support. Errors are classified: 5xx/408/429/timeouts/network errors are retried, other 4xx and template errors go straight to the DLQ. Policies can be set per workflow and overridden per step. |
| **Operate failures** | The dead-letter queue keeps the full context (event, step outputs, error, history). Replay can *resume* from the failed step or *restart* with fresh idempotency keys, optionally re-pinned to a fixed workflow version. Poison queue messages are dead-lettered too. |
| **Deterministic retries** | Runs are pinned to an immutable, content-hashed workflow version, so a config deploy never changes an in-flight run. |
| **Security** | GitHub, Stripe (with replay window) and generic HMAC signatures with constant-time compares. Signature/auth headers are never persisted. Templates run in a sandboxed Jinja2 environment. Secrets are exposed only via the explicit `ETA_SECRET_*` namespace. Admin API uses a bearer token. Body size is limited. |
| **Observability** | Structured JSON logs with `run_id`/`event_id` context, Prometheus metrics (queue depth/lag, DLQ size, step latency, retries…), per-run history timeline, `/healthz` and `/readyz`. |
| **Graceful shutdown** | On SIGTERM, workers stop claiming, drain in-flight runs within a grace period, then release the rest back to the queue *without* charging an attempt. |

Verified by **217 tests**, run against SQLite *and* Postgres plus a real Redis. They
include failure injection for crashed workers, zombie writes, stolen leases, 10
concurrent duplicate deliveries, 4 competing workers × 60 runs executed exactly
once, and poison messages.

## Quick start

### Local (SQLite, no dependencies)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env               # webhook secrets + downstream URLs
python scripts/mock_downstream.py --port 9000 &   # flaky fake API (FAIL_RATE=0.3)
ETA_DATABASE_URL=sqlite+aiosqlite:///./eta.db eta serve --worker --migrate
```

```bash
python scripts/send_webhook.py github-push --id delivery-1   # 202, runs workflow
python scripts/send_webhook.py github-push --id delivery-1   # 200 duplicate, no new run
python scripts/send_webhook.py stripe-payment                # fulfil + chained workflow
python scripts/send_webhook.py generic demo.flaky            # fails twice, then succeeds
python scripts/send_webhook.py generic demo.poison           # permanent failure → DLQ

curl -s -H "Authorization: Bearer change-me" localhost:8080/api/dlq | jq
eta dlq replay <dlq_id> --mode resume
```

### Full stack (Postgres + Redis + 2 workers + consumer)

```bash
docker compose up --build
python scripts/send_webhook.py redis-order --order-id o-1   # via Redis Streams
open http://localhost:8080/docs                              # OpenAPI UI
```

## A workflow

```yaml
# config/workflows/stripe_payment_fulfillment.yaml
name: stripe_payment_fulfillment
trigger:
  source: stripe
  event: payment_intent.succeeded
  filter:
    - { path: payload.data.object.amount, op: gt, value: 0 }
retry: { max_attempts: 8, initial_delay: 1s, max_delay: 10m }
steps:
  - id: fulfil
    action: http_request          # sends Idempotency-Key: <run>:<gen>:fulfil
    with:
      method: POST
      url: "{{ secrets.FULFILLMENT_URL }}"
      json:
        payment_intent: "{{ event.payload.data.object.id }}"
        amount: "{{ event.payload.data.object.amount }}"
  - id: announce
    action: emit_event            # triggers other workflows, idempotently
    with:
      event_type: order.paid
      payload: { payment_intent: "{{ event.payload.data.object.id }}" }
```

Built-in actions are `http_request`, `transform`, `log`, `emit_event`, `delay` and
`fail` (fault injection). Steps support `when`, `timeout`, `retry` and
`continue_on_error`. You can add your own action in Python:

```python
from eta.actions import action
from eta.errors import RetryableError

@action("crm_upsert")
async def crm_upsert(params, ctx):
    # ctx.idempotency_key is stable across retries of this step
    ...
```

Load it with `ETA_PLUGIN_MODULES=my_package.actions`. See the full reference in
[docs/WORKFLOWS.md](docs/WORKFLOWS.md).

## Components

| Command | Role |
| --- | --- |
| `eta serve [--worker] [--consumers] [--migrate]` | HTTP: webhooks, admin API, metrics, health |
| `eta worker [--concurrency N]` | Executes runs; scale horizontally |
| `eta consume` | Redis Streams consumers for queue-backed sources |
| `eta db upgrade` | Alembic migrations |
| `eta validate` | Lint `config/` (sources, workflows, templates, actions) |
| `eta dlq list\|replay\|discard`, `eta runs list\|show` | Operations |

### Admin API (bearer `ETA_ADMIN_TOKEN`)

`GET /api/stats` · `GET /api/workflows` · `GET /api/actions` · `POST /api/events`
· `GET /api/events[/{id}]` · `GET /api/runs[/{id}]` (with steps + history)
· `POST /api/runs/{id}/cancel` · `GET /api/dlq[/{id}]` · `POST /api/dlq/{id}/replay`
`{"mode": "resume"|"restart", "use_latest_workflow": bool}` · `POST /api/dlq/replay`
(bulk) · `POST /api/dlq/{id}/discard`

## Performance

Run `scripts/benchmark.py` to measure throughput. The numbers below are for 3-step
workflows (template → HTTP → template), Postgres 16 on the **same** 4-vCPU VM, and
1,500 runs:

| Worker processes | Runs/s | Exactly-once check |
| --- | --- | --- |
| 1 | 57 | 1500/1500 ✓ |
| 2 | 92 | 1500/1500 ✓ |
| 4 | 139 | 1500/1500 ✓ |

The ceiling per process is client-side CPU in SQLAlchemy's async layer (about 0.75 ms
per transaction, at roughly 8 transactions per 3-step run). Postgres is not the
bottleneck, so throughput scales by adding worker processes or replicas. See
[docs/OPERATIONS.md](docs/OPERATIONS.md#capacity-planning).

## Documentation

* [docs/PLAN.md](docs/PLAN.md): goals, milestones, delivery contract, results
* [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): components, data model, state machine, failure modes
* [docs/WORKFLOWS.md](docs/WORKFLOWS.md): sources and workflow DSL reference
* [docs/OPERATIONS.md](docs/OPERATIONS.md): deployment, configuration, metrics/alerts, DLQ runbook
* [docs/adr/](docs/adr): architecture decision records

## Development

```bash
make lint        # ruff + strict mypy
make test        # SQLite + local redis-server if present
make test-all    # also Postgres/Redis: ETA_TEST_DATABASE_URL / ETA_TEST_REDIS_URL
```

## License

MIT
