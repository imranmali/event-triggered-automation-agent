from __future__ import annotations

import pytest

from eta.errors import SignatureError
from eta.ingress.signatures import (
    sign_hmac_sha256,
    sign_stripe,
    verify,
    verify_github,
    verify_stripe,
)
from eta.workflows.spec import VerifySpec

BODY = b'{"hello":"world"}'


def test_github_signature_roundtrip():
    sig = sign_hmac_sha256("secret", BODY, prefix="sha256=")
    verify_github(BODY, {"x-hub-signature-256": sig}, "secret")


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-Hub-Signature-256": "sha256=deadbeef"},
        {"X-Hub-Signature-256": sign_hmac_sha256("other", BODY, prefix="sha256=")},
        {"X-Hub-Signature-256": sign_hmac_sha256("secret", BODY)},  # missing prefix
    ],
)
def test_github_signature_rejections(headers):
    with pytest.raises(SignatureError):
        verify_github(BODY, headers, "secret")


def test_github_signature_detects_body_tampering():
    sig = sign_hmac_sha256("secret", BODY, prefix="sha256=")
    with pytest.raises(SignatureError):
        verify_github(BODY + b" ", {"X-Hub-Signature-256": sig}, "secret")


def test_stripe_signature_roundtrip_and_replay_window():
    header = sign_stripe("whsec", BODY, timestamp=1_000)
    verify_stripe(BODY, {"Stripe-Signature": header}, "whsec", now=lambda: 1_100)
    with pytest.raises(SignatureError, match="tolerance"):
        verify_stripe(BODY, {"Stripe-Signature": header}, "whsec", now=lambda: 2_000)
    with pytest.raises(SignatureError, match="mismatch"):
        verify_stripe(BODY, {"Stripe-Signature": header}, "other", now=lambda: 1_000)


def test_stripe_accepts_any_of_multiple_v1_signatures():
    good = sign_stripe("whsec", BODY, timestamp=5).split("v1=")[1]
    header = f"t=5,v1=bad,v1={good}"
    verify_stripe(BODY, {"Stripe-Signature": header}, "whsec", now=lambda: 5)


def test_generic_hmac_and_none():
    spec = VerifySpec(type="hmac_sha256", secret_env="S", header="X-Sig", prefix="v1=")
    verify(spec, BODY, {"X-Sig": sign_hmac_sha256("k", BODY, prefix="v1=")}, "k")
    with pytest.raises(SignatureError):
        verify(spec, BODY, {"X-Sig": "v1=nope"}, "k")
    with pytest.raises(SignatureError, match="not configured"):
        verify(spec, BODY, {}, None)
    verify(VerifySpec(), BODY, {}, None)  # type none
