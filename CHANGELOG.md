# Changelog

## 1.0.0 — 2026-10-17

First production-ready release.

* Webhook ingress with GitHub, Stripe and generic HMAC signature verification,
  body limits and header scrubbing
* Redis Streams ingress (consumer groups, ack-after-commit, stale reclaim,
  poison dead-lettering)
* Declarative YAML workflows: triggers with glob and filter matching, sandboxed
  templates, `when`, timeouts, per-step retry policies, `continue_on_error`
* Built-in actions `http_request`, `transform`, `log`, `emit_event`, `delay`, `fail`,
  plus Python plug-ins
* Durable run queue with leases, heartbeats, fencing tokens and a crash-recovery
  reaper
* Layered idempotency: ingress dedupe, unique fan-out, step checkpoints and
  downstream idempotency keys
* Dead-letter queue with resume and restart replay, bulk replay, discard, and
  workflow re-pinning
* Admin API, Prometheus metrics, JSON logs, health and readiness probes, CLI
* Alembic migrations, a Docker image, a docker-compose stack, CI
