from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx
import pytest
from tests.conftest import FAST_RETRY

from eta.api.app import create_app
from eta.ingress.signatures import sign_hmac_sha256, sign_stripe
from eta.models import RunStatus
from eta.runtime import Runtime

SOURCES = {
    "github": {
        "verify": {"type": "github", "secret_env": "SECRET"},
        "event_type": {"header": "X-GitHub-Event"},
        "idempotency_key": {"header": "X-GitHub-Delivery"},
    },
    "stripe": {
        "verify": {"type": "stripe", "secret_env": "SECRET"},
        "event_type": {"path": "type"},
        "idempotency_key": {"path": "id"},
    },
    "open": {"event_type": {"path": "kind", "default": "default"}},
    "queue_only": {"redis_stream": {"stream": "s"}},
}

WORKFLOWS = [
    {
        "name": "on_push",
        "trigger": {
            "source": "github",
            "event": "push",
            "filter": [{"path": "payload.ref", "value": "refs/heads/main"}],
        },
        "retry": FAST_RETRY,
        "steps": [
            {
                "id": "call",
                "action": "http_request",
                "with": {"url": "https://x.test/", "json": "{{ event.payload }}"},
            }
        ],
    },
    {
        "name": "on_payment",
        "trigger": {"source": "stripe", "event": "payment_intent.*"},
        "retry": {**FAST_RETRY, "max_attempts": 1},
        "steps": [
            {"id": "fail", "action": "fail", "with": {"mode": "permanent", "message": "no stock"}}
        ],
    },
    {
        "name": "on_open",
        "trigger": {"source": "open"},
        "steps": [{"id": "log", "action": "log", "with": {"message": "hi"}}],
    },
]

PUSH = json.dumps({"ref": "refs/heads/main", "commits": [1]}).encode()


@pytest.fixture
async def rt(make_runtime) -> Runtime:
    return make_runtime(WORKFLOWS, SOURCES, max_body_bytes=2048)


@pytest.fixture
async def client(rt: Runtime) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(rt, manage_lifecycle=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def github_headers(body: bytes, delivery: str = "d-1", event: str = "push") -> dict[str, str]:
    return {
        "X-GitHub-Event": event,
        "X-GitHub-Delivery": delivery,
        "X-Hub-Signature-256": sign_hmac_sha256("s3cr3t", body, prefix="sha256="),
        "Content-Type": "application/json",
    }


# -------------------------------------------------------------- webhooks


async def test_signed_webhook_is_accepted_and_executed(client, rt, recorder):
    r = await client.post("/webhooks/github", content=PUSH, headers=github_headers(PUSH))
    assert r.status_code == 202
    body = r.json()
    assert not body["duplicate"] and len(body["run_ids"]) == 1

    await rt.worker.drain_ready()
    assert (await rt.store.get_run(body["run_ids"][0])).status == RunStatus.SUCCEEDED
    assert json.loads(recorder.requests[0].content)["ref"] == "refs/heads/main"

    event = await rt.store.get_event(body["event_id"])
    assert event.idempotency_key == "d-1"
    assert "x-hub-signature-256" not in event.headers  # secrets not persisted
    assert event.headers["x-github-event"] == "push"


async def test_redelivered_webhook_is_deduplicated(client, rt):
    first = await client.post("/webhooks/github", content=PUSH, headers=github_headers(PUSH))
    again = await client.post("/webhooks/github", content=PUSH, headers=github_headers(PUSH))
    assert again.status_code == 200
    assert again.json() == {**first.json(), "duplicate": True}
    assert len(await rt.store.list_runs()) == 1


async def test_non_matching_event_is_stored_without_runs(client, rt):
    body = json.dumps({"ref": "refs/heads/dev"}).encode()
    r = await client.post("/webhooks/github", content=body, headers=github_headers(body, "d-2"))
    assert r.status_code == 202 and r.json()["run_ids"] == []
    assert len(await rt.store.list_events()) == 1


@pytest.mark.parametrize(
    ("path", "headers", "body", "status"),
    [
        ("/webhooks/nope", {}, b"{}", 404),
        ("/webhooks/queue_only", {}, b"{}", 404),
        (
            "/webhooks/github",
            {"X-GitHub-Event": "push", "X-Hub-Signature-256": "sha256=00"},
            PUSH,
            401,
        ),
        ("/webhooks/github", {"X-GitHub-Event": "push"}, PUSH, 401),
        ("/webhooks/open", {"Content-Type": "application/json"}, b"{not json", 400),
        ("/webhooks/open", {}, b"x" * 4096, 413),
    ],
)
async def test_webhook_rejections(client, path, headers, body, status):
    r = await client.post(path, content=body, headers=headers)
    assert r.status_code == status


async def test_missing_event_type_is_400(client):
    body = b"{}"
    headers = github_headers(body)
    del headers["X-GitHub-Event"]
    r = await client.post("/webhooks/github", content=body, headers=headers)
    assert r.status_code == 400


async def test_stripe_webhook_dedupes_on_payload_id(client, rt):
    body = json.dumps({"id": "evt_1", "type": "payment_intent.succeeded"}).encode()
    headers = {"Stripe-Signature": sign_stripe("s3cr3t", body), "Content-Type": "application/json"}
    assert (await client.post("/webhooks/stripe", content=body, headers=headers)).status_code == 202
    assert (await client.post("/webhooks/stripe", content=body, headers=headers)).status_code == 200


async def test_unsigned_source_uses_body_hash_for_idempotency(client, rt):
    r1 = await client.post("/webhooks/open", json={"kind": "a"})
    r2 = await client.post("/webhooks/open", json={"kind": "a"})
    r3 = await client.post("/webhooks/open", json={"kind": "b"})
    assert r1.json()["event_id"] == r2.json()["event_id"] != r3.json()["event_id"]
    event = await rt.store.get_event(r1.json()["event_id"])
    assert event.idempotency_key.startswith("sha256:") and event.event_type == "a"


async def test_form_encoded_github_payload(client):
    from urllib.parse import urlencode

    body = urlencode({"payload": PUSH.decode()}).encode()
    headers = github_headers(body)
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    r = await client.post("/webhooks/github", content=body, headers=headers)
    assert r.status_code == 202 and len(r.json()["run_ids"]) == 1


async def test_storage_failure_returns_503(client, rt, monkeypatch):
    async def boom(*_a, **_k):
        raise RuntimeError("db down")

    monkeypatch.setattr(rt.store, "ingest_event", boom)
    r = await client.post("/webhooks/open", json={"kind": "a"})
    assert r.status_code == 503


# ------------------------------------------------------------- admin API


async def test_admin_flow_dlq_inspect_and_replay(client, rt):
    body = json.dumps({"id": "evt_9", "type": "payment_intent.succeeded"}).encode()
    headers = {"Stripe-Signature": sign_stripe("s3cr3t", body), "Content-Type": "application/json"}
    [run_id] = (await client.post("/webhooks/stripe", content=body, headers=headers)).json()[
        "run_ids"
    ]
    await rt.worker.drain_ready()

    dlq = (await client.get("/api/dlq")).json()
    assert len(dlq) == 1 and dlq[0]["run_id"] == run_id and dlq[0]["reason"] == "permanent_error"
    detail = (await client.get(f"/api/dlq/{dlq[0]['id']}")).json()
    assert detail["event"]["payload"]["id"] == "evt_9"
    assert detail["steps"][0]["error"].endswith("no stock")

    run = (await client.get(f"/api/runs/{run_id}")).json()
    assert run["status"] == "dead_lettered"
    assert [h["kind"] for h in run["history"]][-1] == "dead_lettered"

    r = await client.post(f"/api/dlq/{dlq[0]['id']}/replay", json={"mode": "restart"})
    assert r.status_code == 200 and r.json()["run_id"] == run_id
    assert (await client.post(f"/api/dlq/{dlq[0]['id']}/replay")).status_code == 409
    await rt.worker.drain_ready()
    assert len((await client.get("/api/dlq")).json()) == 1  # failed again -> new DLQ entry
    assert len((await client.get("/api/dlq", params={"status": "replayed"})).json()) == 1


async def test_bulk_replay_and_discard(client, rt):
    for i in range(3):
        body = json.dumps({"id": f"evt_{i}", "type": "payment_intent.succeeded"}).encode()
        headers = {
            "Stripe-Signature": sign_stripe("s3cr3t", body),
            "Content-Type": "application/json",
        }
        await client.post("/webhooks/stripe", content=body, headers=headers)
    await rt.worker.drain_ready()
    items = (await client.get("/api/dlq")).json()
    assert len(items) == 3

    assert (await client.post(f"/api/dlq/{items[0]['id']}/discard")).status_code == 200
    assert (await client.post(f"/api/dlq/{items[0]['id']}/discard")).status_code == 409
    r = (
        await client.post(
            "/api/dlq/replay", json={"workflow": "on_payment", "use_latest_workflow": True}
        )
    ).json()
    assert len(r["replayed"]) == 2


async def test_publish_event_and_listing(client, rt):
    r = await client.post(
        "/api/events",
        json={
            "source": "open",
            "event_type": "manual",
            "payload": {"a": 1},
            "idempotency_key": "m1",
        },
    )
    assert r.status_code == 202
    assert (
        await client.post(
            "/api/events", json={"source": "open", "event_type": "manual", "idempotency_key": "m1"}
        )
    ).status_code == 200
    events = (await client.get("/api/events", params={"source": "open"})).json()
    assert len(events) == 1
    ev = (await client.get(f"/api/events/{events[0]['id']}")).json()
    assert len(ev["runs"]) == 1
    assert (await client.get("/api/runs", params={"workflow": "on_open"})).json()[0][
        "status"
    ] == "queued"
    run_id = ev["runs"][0]["id"]
    assert (await client.post(f"/api/runs/{run_id}/cancel")).status_code == 200
    assert (await client.post(f"/api/runs/{run_id}/cancel")).status_code == 409


async def test_catalog_endpoints(client):
    wfs = (await client.get("/api/workflows")).json()
    assert {w["name"] for w in wfs} == {"on_push", "on_payment", "on_open"}
    actions = {a["name"] for a in (await client.get("/api/actions")).json()}
    assert {"http_request", "transform", "emit_event"} <= actions
    stats = (await client.get("/api/stats")).json()
    assert "runs_by_status" in stats and "queue_lag_seconds" in stats


async def test_not_found(client):
    for path in ["/api/runs/x", "/api/events/x", "/api/dlq/x"]:
        assert (await client.get(path)).status_code == 404
    assert (await client.post("/api/dlq/x/replay")).status_code == 404


async def test_admin_token_required_when_configured(make_runtime):
    rt = make_runtime(WORKFLOWS, SOURCES, admin_token="topsecret")
    app = create_app(rt, manage_lifecycle=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        assert (await c.get("/api/runs")).status_code == 401
        assert (
            await c.get("/api/runs", headers={"Authorization": "Bearer wrong"})
        ).status_code == 401
        assert (
            await c.get("/api/runs", headers={"Authorization": "Bearer topsecret"})
        ).status_code == 200
        # Webhooks and probes are not behind the admin token.
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.post("/webhooks/open", json={})).status_code == 202


async def test_ops_endpoints(client, rt):
    assert (await client.get("/healthz")).json()["status"] == "ok"
    assert (await client.get("/readyz")).json() == {"status": "ready"}
    await client.post("/webhooks/open", json={"kind": "x"})
    metrics = (await client.get("/metrics")).text
    assert "eta_events_received_total" in metrics
    assert 'eta_runs{status="queued"}' in metrics


async def test_readyz_reports_unavailable(client, rt, monkeypatch):
    async def down():
        raise ConnectionError("db")

    monkeypatch.setattr(rt.store, "ping", down)
    r = await client.get("/readyz")
    assert r.status_code == 503
