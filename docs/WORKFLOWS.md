# Configuration reference

Configuration lives in `ETA_CONFIG_DIR` (default `./config`):

```
config/
  sources.yaml        # ingress sources
  workflows/*.yaml    # one workflow per file, or a YAML list of workflows
```

Run `eta validate` in CI. It checks schemas, unknown actions, template syntax, and
whether the referenced signing secrets are set.

## Sources (`sources.yaml`)

Each source is reachable at `POST /webhooks/<name>`, unless it sets `redis_stream`.
In that case it is consumed by `eta consume` and isn't exposed over HTTP.

```yaml
sources:
  github:
    verify: { type: github, secret_env: GITHUB_WEBHOOK_SECRET }
    event_type: { header: X-GitHub-Event }
    idempotency_key: { header: X-GitHub-Delivery }

  stripe:
    verify: { type: stripe, secret_env: STRIPE_WEBHOOK_SECRET, tolerance_seconds: 300 }
    event_type: { path: type }
    idempotency_key: { path: id }

  partner:
    verify: { type: hmac_sha256, secret_env: PARTNER_SECRET, header: X-Signature, prefix: "sha256=" }
    event_type: { header: X-Event-Type, path: type, default: generic }
    idempotency_key: { header: Idempotency-Key, path: id }

  orders:
    redis_stream:
      stream: orders
      group: eta
      event_type_field: event_type   # message field holding the event type
      payload_field: payload         # JSON string; if absent, all fields form the payload
      idempotency_field: order_id    # fallback: "<stream>:<message-id>"
      max_deliveries: 5              # then dead-lettered as poison
      claim_idle: 60s                # reclaim messages a dead consumer left pending
      batch_size: 50
```

| Field | Meaning |
| --- | --- |
| `enabled` | `false` returns 404 for the source and skips its consumer |
| `verify.type` | `none`, `github` (`X-Hub-Signature-256`), `stripe` (`Stripe-Signature` with timestamp tolerance), `hmac_sha256` (hex HMAC of the raw body in `header`, optional `prefix`) |
| `verify.secret_env` | Name of the env var that holds the secret. Startup fails if it's unset |
| `event_type` | `header` → `path` (dotted JSON path) → `default`, in that order. The request fails with 400 if all three are empty |
| `idempotency_key` | Same lookup order. Falls back to `sha256:<hash of raw body>` |

Bodies are parsed as JSON, as `application/x-www-form-urlencoded` (a single
`payload=` field holding JSON is unwrapped, as GitHub sends it), or as text for
`text/*`. These headers are never stored: `Authorization`, `Cookie` and all
signature headers.

## Workflows

```yaml
name: github_push_notify          # unique, [A-Za-z_][A-Za-z0-9_-]*
description: Notify chat on pushes to main
enabled: true
trigger:                          # one trigger or a list (OR)
  source: github                  # glob: "git*", "*"
  event: push                     # glob or list of globs: ["pull_request.*", "push"]
  filter:                         # all must match (AND)
    - { path: payload.ref, op: eq, value: refs/heads/main }
retry:                            # workflow default policy
  max_attempts: 5                 # includes the first try
  initial_delay: 1s               # 500ms | 2s | 5m | 1h | number of seconds
  max_delay: 5m
  multiplier: 2
  jitter: full                    # full | equal | none
step_timeout: 30s                 # default per-step timeout
steps:
  - id: summarize
    action: transform
    with:
      output: { text: "{{ event.payload.pusher.name }} pushed" }
  - id: notify
    action: http_request
    when: "steps.summarize.output.text != ''"
    timeout: 10s
    retry: { max_attempts: 8 }    # per-step override
    continue_on_error: false
    with:
      url: "{{ secrets.NOTIFY_URL }}"
      json: { text: "{{ steps.summarize.output.text }}" }
```

### Filter operators

`eq`, `ne`, `in`, `not_in` (the value is a list), `exists`, `not_exists`,
`contains` (substring or list membership), `startswith`, `endswith`, `matches`
(regex search), `gt`, `gte`, `lt`, `lte`. Paths are relative to
`{source, type, payload, headers}`. Header names are case-insensitive, and list
indexes work (`payload.items.0.sku`). A missing path is false for every operator
except `ne`, `not_in` and `not_exists`.

### Templates

Templates are Jinja2 running in an **immutable sandbox** with `StrictUndefined`.
These variables are available:

| Variable | Content |
| --- | --- |
| `event.id/source/type/payload/headers/received_at/idempotency_key` | The triggering event |
| `steps.<id>.output`, `steps.<id>.status` | Outputs of completed steps (`succeeded`, `skipped`, `failed_continued`) |
| `run.id/attempt/generation/workflow/workflow_version` | Run metadata |
| `secrets.<NAME>` | `ETA_SECRET_<NAME>` environment variables, and nothing else |

* A string that is **exactly one expression** keeps its native type:
  `count: "{{ event.payload.commits | length }}"` yields an integer, and
  `json: "{{ event.payload }}"` passes the whole object through.
* A mixed string renders as text, for example `"Hello {{ name }}"`.
* Dot access prefers mapping keys, so `payload.items` is the `items` field rather
  than `dict.items`.
* Referencing an undefined variable is a **permanent** error. The run goes straight
  to the DLQ instead of sending an empty value downstream.
* Extra filters: `tojson` and `fromjson`.

`when:` takes either a bare expression (`event.payload.n > 10`) or `{{ … }}`. It's
falsey for `false`, `0`, `""`, `none` and `no`.

## Actions

| Action | Params | Output |
| --- | --- | --- |
| `http_request` | `url`, `method` (POST), `headers`, `params`, `json` or `body`, `timeout` (10s), `expected_status` (default 2xx), `idempotency_header` (`Idempotency-Key`, set to `null` to disable; never sent on GET) | `{status, body}`; the body is JSON-decoded if possible and capped at 64 KiB |
| `transform` | `output` | `output`, rendered |
| `log` | `message`, `level`, `fields` | `{message}` |
| `emit_event` | `event_type`, `payload`, `source` (`internal`) | `{event_id, duplicate, run_ids}` |
| `delay` | `seconds` (≤ 300) | `{slept}` |
| `fail` | `mode` (`retryable`/`permanent`), `message`, `succeed_on_attempt` | for testing retry and DLQ paths |

`http_request` classifies responses as follows: 408, 425, 429 and 5xx, plus network
errors and timeouts, are **retryable** (and `Retry-After` is honoured). Any other
non-expected status is **permanent**.

### Custom actions

```python
# my_company/eta_actions.py
from pydantic import BaseModel
from eta.actions import action
from eta.errors import PermanentError, RetryableError

class Params(BaseModel):
    customer_id: str

@action("crm_upsert", params=Params)        # params are validated → PermanentError if bad
async def crm_upsert(p: Params, ctx):
    resp = await ctx.http.post(
        "https://crm.example.com/upsert",
        json={"id": p.customer_id},
        headers={"Idempotency-Key": ctx.idempotency_key},  # stable across retries
    )
    if resp.status_code >= 500:
        raise RetryableError("crm unavailable")
    if resp.status_code >= 400:
        raise PermanentError(resp.text)
    return resp.json()                       # must be JSON-serialisable
```

Enable it with `ETA_PLUGIN_MODULES=my_company.eta_actions`. Any exception that isn't a
`StepError` is treated as retryable.

## Versioning

A workflow's version is the hash of its normalised definition. Every run is pinned to
the version that was current when the event arrived, so deploying a new definition
only affects new events. When you replay a dead letter, `use_latest_workflow: true`
re-pins the run to the definition loaded now, which is how a fix gets applied to
failed runs.
