#!/usr/bin/env python3
"""Send correctly signed sample webhooks to a running agent.

python scripts/send_webhook.py github-push
python scripts/send_webhook.py stripe-payment --id evt_123
python scripts/send_webhook.py generic demo.flaky
python scripts/send_webhook.py redis-order --order-id o-1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from eta.ingress.signatures import sign_hmac_sha256, sign_stripe

BASE = os.environ.get("ETA_URL", "http://localhost:8080")


def post(path: str, body: bytes, headers: dict[str, str]) -> None:
    r = httpx.post(
        f"{BASE}{path}", content=body, headers={"Content-Type": "application/json", **headers}
    )
    print(r.status_code, r.text)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("kind", choices=["github-push", "stripe-payment", "generic", "redis-order"])
    p.add_argument("event_type", nargs="?", default="demo.flaky")
    p.add_argument("--id", default=None, help="delivery / event id (reuse it to test dedupe)")
    p.add_argument("--order-id", default=None)
    a = p.parse_args()
    ident = a.id or uuid.uuid4().hex

    if a.kind == "github-push":
        body = json.dumps(
            {
                "ref": "refs/heads/main",
                "repository": {"full_name": "acme/shop"},
                "pusher": {"name": "octocat"},
                "commits": [{"id": "abc"}, {"id": "def"}],
                "head_commit": {"message": "Fix checkout rounding"},
            }
        ).encode()
        secret = os.environ.get("GITHUB_WEBHOOK_SECRET", "dev-github-secret")
        post(
            "/webhooks/github",
            body,
            {
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": ident,
                "X-Hub-Signature-256": sign_hmac_sha256(secret, body, prefix="sha256="),
            },
        )
    elif a.kind == "stripe-payment":
        body = json.dumps(
            {
                "id": a.id or f"evt_{ident[:12]}",
                "type": "payment_intent.succeeded",
                "data": {
                    "object": {
                        "id": "pi_123",
                        "amount": 4200,
                        "currency": "usd",
                        "customer": "cus_9",
                    }
                },
            }
        ).encode()
        secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_dev")
        post("/webhooks/stripe", body, {"Stripe-Signature": sign_stripe(secret, body)})
    elif a.kind == "generic":
        body = json.dumps({"id": ident, "note": "demo"}).encode()
        secret = os.environ.get("GENERIC_WEBHOOK_SECRET", "dev-generic-secret")
        post(
            "/webhooks/generic",
            body,
            {
                "X-Event-Type": a.event_type,
                "X-Signature": sign_hmac_sha256(secret, body, prefix="sha256="),
            },
        )
    else:
        import redis

        r = redis.Redis.from_url(os.environ.get("ETA_REDIS_URL", "redis://localhost:6379/0"))
        msg_id = r.xadd(
            "orders",
            {
                "event_type": "order.created",
                "order_id": a.order_id or ident,
                "payload": json.dumps(
                    {"order_id": a.order_id or ident, "items": [{"price": 10}, {"price": 5}]}
                ),
            },
        )
        print("XADD", msg_id)


if __name__ == "__main__":
    main()
