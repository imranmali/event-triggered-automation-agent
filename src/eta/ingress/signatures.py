"""Webhook signature verification (constant-time comparisons throughout)."""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Callable, Mapping

from eta.errors import SignatureError
from eta.workflows.spec import VerifySpec


def _hmac_hex(secret: str, data: bytes) -> str:
    return hmac.new(secret.encode(), data, hashlib.sha256).hexdigest()


def _header(headers: Mapping[str, str], name: str) -> str:
    lowered = {k.lower(): v for k, v in headers.items()}
    value = lowered.get(name.lower())
    if not value:
        raise SignatureError(f"missing signature header {name}")
    return value


def verify_hmac_sha256(
    body: bytes, headers: Mapping[str, str], secret: str, *, header: str, prefix: str = ""
) -> None:
    provided = _header(headers, header).strip()
    if prefix:
        if not provided.startswith(prefix):
            raise SignatureError("signature has unexpected format")
        provided = provided[len(prefix) :]
    if not hmac.compare_digest(provided, _hmac_hex(secret, body)):
        raise SignatureError("signature mismatch")


def verify_github(body: bytes, headers: Mapping[str, str], secret: str) -> None:
    verify_hmac_sha256(body, headers, secret, header="X-Hub-Signature-256", prefix="sha256=")


def verify_stripe(
    body: bytes,
    headers: Mapping[str, str],
    secret: str,
    *,
    tolerance_seconds: int = 300,
    now: Callable[[], float] = time.time,
) -> None:
    """Stripe: ``Stripe-Signature: t=<ts>,v1=<sig>[,v1=<sig>]`` over ``"<ts>.<body>"``."""
    parts: dict[str, list[str]] = {}
    for item in _header(headers, "Stripe-Signature").split(","):
        key, _, value = item.strip().partition("=")
        parts.setdefault(key, []).append(value)
    try:
        timestamp = int(parts["t"][0])
    except (KeyError, ValueError):
        raise SignatureError("signature timestamp missing") from None
    if abs(now() - timestamp) > tolerance_seconds:
        raise SignatureError("signature timestamp outside tolerance (possible replay)")
    expected = _hmac_hex(secret, f"{timestamp}.".encode() + body)
    if not any(hmac.compare_digest(sig, expected) for sig in parts.get("v1", [])):
        raise SignatureError("signature mismatch")


def verify(spec: VerifySpec, body: bytes, headers: Mapping[str, str], secret: str | None) -> None:
    if spec.type == "none":
        return
    if not secret:
        raise SignatureError("signing secret not configured")
    if spec.type == "github":
        verify_github(body, headers, secret)
    elif spec.type == "stripe":
        verify_stripe(body, headers, secret, tolerance_seconds=spec.tolerance_seconds)
    else:
        verify_hmac_sha256(body, headers, secret, header=spec.header, prefix=spec.prefix)


def sign_hmac_sha256(secret: str, body: bytes, prefix: str = "") -> str:
    """Helper for clients/tests producing signatures."""
    return prefix + _hmac_hex(secret, body)


def sign_stripe(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    return f"t={ts},v1={_hmac_hex(secret, f'{ts}.'.encode() + body)}"
