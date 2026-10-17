# Operations runbook

## Deployment topology

| Process | Command | Scale | Notes |
| --- | --- | --- | --- |
| Migrations | `eta db upgrade` | 1 (job, before rollout) | Alembic. It's idempotent |
| API | `eta serve` | ≥2 behind a load balancer | Stateless. Probes: `/healthz` (liveness), `/readyz` (DB reachable) |
| Workers | `eta worker` | N | Set `terminationGracePeriodSeconds` > `ETA_SHUTDOWN_GRACE_SECONDS` |
| Consumers | `eta consume` | ≥1 | Consumer groups spread messages across replicas. Each replica must have a unique `ETA_WORKER_ID` (the default `hostname-pid` already is) |

For a small deployment, a single `eta serve --worker --consumers` process works.

## Configuration (environment)

| Variable | Default | Purpose |
| --- | --- | --- |
| `ETA_DATABASE_URL` | `sqlite+aiosqlite:///./eta.db` | Use `postgresql+asyncpg://…` in production |
| `ETA_DB_POOL_SIZE` / `ETA_DB_POOL_PRE_PING` | `10` / `true` | Pre-ping survives DB failovers but costs about 0.3 ms per transaction |
| `ETA_CONFIG_DIR` | `./config` | Sources and workflows |
| `ETA_ADMIN_TOKEN` | unset (admin API open) | **Set this in production** |
| `ETA_MAX_BODY_BYTES` | `1048576` | Webhook body limit (413 above it) |
| `ETA_WORKER_CONCURRENCY` | `8` | In-flight runs per worker process |
| `ETA_POLL_INTERVAL` | `1.0` | Idle poll period, in seconds |
| `ETA_LEASE_SECONDS` | `60` | How long before a silent worker is presumed dead |
| `ETA_HEARTBEAT_INTERVAL` | `15` | Must be less than the lease. Keep it at a quarter of the lease or less |
| `ETA_SHUTDOWN_GRACE_SECONDS` | `30` | Drain window on SIGTERM |
| `ETA_REAP_INTERVAL` | `10` | How often each worker looks for expired leases |
| `ETA_REDIS_URL` | `redis://localhost:6379/0` | Queue ingress |
| `ETA_PLUGIN_MODULES` | – | Comma-separated modules containing `@action`s |
| `ETA_SECRET_<NAME>` | – | Exposed to templates as `secrets.<NAME>` |
| `ETA_LOG_LEVEL` / `ETA_LOG_JSON` | `INFO` / `true` | |

**Clocks**: lease expiry compares timestamps written by different hosts, so run NTP.
A skew well below `lease_seconds - heartbeat_interval` is safe.

## Metrics and suggested alerts

| Metric | Type | Alert |
| --- | --- | --- |
| `eta_queue_lag_seconds` | gauge | `> 60` for 5m: workers can't keep up, so scale them |
| `eta_queue_ready_runs` | gauge | Trend it, or use it for autoscaling |
| `eta_dead_letters_open` | gauge | `increase > 0`: page or notify the owning team |
| `eta_dead_lettered_total{kind,reason}` | counter | Watch for spikes per reason |
| `eta_runs_finished_total{workflow,outcome}` | counter | Retry ratio = `retry / (succeeded + retry)` |
| `eta_retries_scheduled_total{workflow,step}` | counter | Find downstreams that keep failing |
| `eta_step_duration_seconds{workflow,step,action,outcome}` | histogram | p99 approaching the step timeout |
| `eta_leases_reaped_total` | counter | `> 0` means workers are crashing or stalling (OOM, CPU starvation) |
| `eta_events_received_total{source,ingress,outcome}` | counter | Spikes in `rejected` point to bad signatures (rotated secret? attack?) |
| `eta_queue_messages_total{stream,outcome}` | counter | `poison` or `error` |
| `eta_worker_inflight_runs` | gauge | Saturation, relative to concurrency |

The gauges are refreshed from the database on each `/metrics` scrape.

## Logs

Logs are JSON, one object per line. Each line carries `run_id`, `event_id`,
`workflow` and `attempt` when it applies, so a single run can be followed with
`jq 'select(.run_id=="…")'`. Use `GET /api/runs/{id}` for the persisted timeline:
created, claimed, step_started, step_failed, retry_scheduled, lease_expired and so on.

## Dead-letter queue runbook

1. **Triage**: `GET /api/dlq?workflow=X`, or `eta dlq list`. Group the entries by
   `reason`:
   * `retries_exhausted`: the downstream was unavailable for longer than the retry
     budget.
   * `permanent_error`: the input was bad, a 4xx came back, or a template failed.
   * `poison_message` (`kind=ingress`): a queue message couldn't be parsed.
2. **Inspect**: `GET /api/dlq/{id}` returns the event payload, every step's output
   and error, and the run history.
3. **Fix the cause**:
   * Downstream outage: wait for it to recover.
   * Bug in the workflow: deploy the fixed YAML, which gets a new version.
   * Bad data: there's nothing to fix. Discard the entry.
4. **Act**:
   * `POST /api/dlq/{id}/replay {"mode":"resume"}` re-runs only the failed and later
     steps. Succeeded side effects are **not** repeated.
   * Add `"use_latest_workflow": true` to pick up the fixed definition.
   * `{"mode":"restart"}` re-runs every step under a new idempotency-key generation.
     Use it only if downstream side effects really need to happen again.
   * `POST /api/dlq/replay {"workflow":"X","limit":500}` replays in bulk after an
     outage.
   * `POST /api/dlq/{id}/discard` is final.
5. Replayed entries keep their record (`status=replayed`, `replay_count`). A run that
   fails again creates a new DLQ entry.

## Capacity planning

* Each run costs about `2 + 2 × steps` database transactions.
* One worker process sustains roughly 50–60 three-step runs per second on a modest
  vCPU. It's bound by client CPU, so add processes or replicas. The benchmark scaled
  near-linearly to 4 processes with Postgres on the same host.
* Keep `worker_concurrency × processes` under `max_connections`. Each worker also
  uses `ETA_DB_POOL_SIZE`.
* Long HTTP steps tie up concurrency slots rather than CPU. Raise
  `ETA_WORKER_CONCURRENCY` for I/O-heavy workflows.

## Data retention

`events`, `runs`, `step_results` and `run_history` grow without bound. Schedule a
retention job like this one (on Postgres, partitioning by month is better at high
volume):

```sql
-- 1. Terminal runs past the window. Cascades to step_results, run_history and
--    dead_letters. Runs with an open dead letter are kept.
DELETE FROM runs r
WHERE r.status IN ('succeeded', 'cancelled', 'dead_lettered')
  AND r.finished_at < now() - interval '30 days'
  AND NOT EXISTS (SELECT 1 FROM dead_letters d WHERE d.run_id = r.id AND d.status = 'open');

-- 2. Events past the window that no longer have any runs.
DELETE FROM events e
WHERE e.received_at < now() - interval '30 days'
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.event_id = e.id);
```

Keep the event dedupe window, meaning the retention period, longer than the longest
period over which your providers might redeliver.

## Security checklist

- [ ] `ETA_ADMIN_TOKEN` is set, and the admin API isn't publicly routable
- [ ] Every internet-facing source has `verify` configured (`eta validate` lists them)
- [ ] TLS terminates at the load balancer
- [ ] `ETA_SECRET_*` values come from a secret manager
- [ ] Egress from workers is restricted to the downstream hosts they need, which
      guards against SSRF through templated URLs
- [ ] The container runs as non-root (the provided image uses uid 10001)
