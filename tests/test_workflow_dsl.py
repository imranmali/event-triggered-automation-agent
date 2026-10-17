from __future__ import annotations

import random

import pytest

from eta.actions import default_registry
from eta.errors import ConfigError, PermanentError
from eta.workflows.matcher import trigger_matches
from eta.workflows.registry import WorkflowRegistry, parse_sources, parse_workflow
from eta.workflows.spec import RetryPolicy, TriggerSpec, parse_duration
from eta.workflows.templating import evaluate_condition, render


def wf(**overrides):
    base = {
        "name": "wf",
        "trigger": {"source": "github", "event": "push"},
        "steps": [{"id": "a", "action": "log", "with": {"message": "hi"}}],
    }
    base.update(overrides)
    return base


# ------------------------------------------------------------------ durations


@pytest.mark.parametrize(
    ("raw", "seconds"),
    [("500ms", 0.5), ("2s", 2), ("5m", 300), ("1h", 3600), (3, 3.0), ("1.5", 1.5)],
)
def test_parse_duration(raw, seconds):
    assert parse_duration(raw) == seconds


@pytest.mark.parametrize("raw", ["abc", "5d", "-1s", True])
def test_parse_duration_rejects(raw):
    with pytest.raises(ValueError):
        parse_duration(raw)


# ---------------------------------------------------------------- retry policy


def test_backoff_is_exponential_and_capped():
    p = RetryPolicy(initial_delay=1, multiplier=2, max_delay=10, jitter="none")
    assert [p.delay(n) for n in range(1, 7)] == [1, 2, 4, 8, 10, 10]


def test_full_jitter_bounds():
    p = RetryPolicy(initial_delay=4, multiplier=2, max_delay=100, jitter="full")
    rng = random.Random(42)
    samples = [p.delay(3, rng=rng) for _ in range(500)]
    assert all(0 <= s <= 16 for s in samples)
    assert max(samples) > 12 and min(samples) < 4  # actually spread out


def test_equal_jitter_bounds():
    p = RetryPolicy(initial_delay=4, multiplier=2, jitter="equal")
    rng = random.Random(1)
    assert all(8 <= p.delay(3, rng=rng) <= 16 for _ in range(200))


def test_exhausted_counts_first_attempt():
    p = RetryPolicy(max_attempts=3)
    assert not p.exhausted(2)
    assert p.exhausted(3)


# --------------------------------------------------------------------- specs


def test_workflow_version_is_content_hash():
    a = parse_workflow(wf(), default_registry)
    b = parse_workflow(wf(), default_registry)
    c = parse_workflow(wf(description="changed"), default_registry)
    assert a.version == b.version != c.version


def test_step_retry_override():
    spec = parse_workflow(
        wf(
            retry={"max_attempts": 4},
            steps=[
                {"id": "a", "action": "log", "with": {"message": "x"}},
                {
                    "id": "b",
                    "action": "log",
                    "with": {"message": "x"},
                    "retry": {"max_attempts": 9},
                },
            ],
        ),
        default_registry,
    )
    assert spec.retry_for("a").max_attempts == 4
    assert spec.retry_for("b").max_attempts == 9
    assert spec.retry_for(None).max_attempts == 4


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"steps": [{"id": "a", "action": "nope"}]}, "unknown action"),
        (
            {"steps": [{"id": "a", "action": "log", "with": {"message": "x"}}] * 2},
            "duplicate step id",
        ),
        (
            {"steps": [{"id": "a", "action": "log", "with": {"message": "{{ broken"}}]},
            "invalid template",
        ),
        ({"steps": []}, "at least 1"),
        ({"trigger": []}, "at least 1"),
        ({"unknown_key": 1}, "Extra inputs"),
        (
            {"steps": [{"id": "a", "action": "log", "when": "{{ (", "with": {"message": "x"}}]},
            "invalid template",
        ),
    ],
)
def test_invalid_workflows(override, message):
    with pytest.raises(ConfigError, match=message):
        parse_workflow(wf(**override), default_registry)


def test_duplicate_workflow_names_rejected():
    spec = parse_workflow(wf(), default_registry)
    with pytest.raises(ConfigError, match="duplicate workflow"):
        WorkflowRegistry([spec, spec])


def test_sources_require_secret_env_present():
    data = {"sources": {"gh": {"verify": {"type": "github", "secret_env": "GH_SECRET"}}}}
    with pytest.raises(ConfigError, match="GH_SECRET"):
        parse_sources(data, environ={})
    assert "gh" in parse_sources(data, environ={"GH_SECRET": "x"})


# ------------------------------------------------------------------- matcher


def event(**kw):
    return {"source": "github", "type": "push", "payload": {}, "headers": {}, **kw}


def test_trigger_glob_matching():
    t = TriggerSpec(source="git*", event=["pull_request.*", "push"])
    assert trigger_matches(t, event())
    assert trigger_matches(t, event(type="pull_request.opened"))
    assert not trigger_matches(t, event(type="issues"))
    assert not trigger_matches(t, event(source="stripe"))


@pytest.mark.parametrize(
    ("cond", "payload", "expected"),
    [
        ({"path": "payload.ref", "op": "eq", "value": "main"}, {"ref": "main"}, True),
        ({"path": "payload.ref", "op": "ne", "value": "main"}, {"ref": "dev"}, True),
        ({"path": "payload.ref", "op": "ne", "value": "main"}, {}, True),
        ({"path": "payload.n", "op": "gt", "value": 5}, {"n": 6}, True),
        ({"path": "payload.n", "op": "gt", "value": 5}, {"n": "x"}, False),
        ({"path": "payload.n", "op": "lte", "value": 5}, {"n": 5}, True),
        ({"path": "payload.tag", "op": "in", "value": ["a", "b"]}, {"tag": "b"}, True),
        ({"path": "payload.tag", "op": "not_in", "value": ["a"]}, {"tag": "b"}, True),
        ({"path": "payload.labels", "op": "contains", "value": "bug"}, {"labels": ["bug"]}, True),
        ({"path": "payload.x", "op": "exists"}, {"x": None}, True),
        ({"path": "payload.x", "op": "not_exists"}, {}, True),
        (
            {"path": "payload.ref", "op": "startswith", "value": "refs/tags/"},
            {"ref": "refs/tags/v1"},
            True,
        ),
        ({"path": "payload.ref", "op": "matches", "value": r"^v\d+$"}, {"ref": "v12"}, True),
        (
            {"path": "payload.items.1.sku", "op": "eq", "value": "B"},
            {"items": [{"sku": "A"}, {"sku": "B"}]},
            True,
        ),
        ({"path": "payload.missing", "op": "eq", "value": None}, {}, False),
    ],
)
def test_filter_operators(cond, payload, expected):
    t = TriggerSpec(source="*", filter=[cond])
    assert trigger_matches(t, event(payload=payload)) is expected


def test_header_paths_are_case_insensitive():
    t = TriggerSpec(source="*", filter=[{"path": "headers.X-Foo", "value": "1"}])
    assert trigger_matches(t, event(headers={"x-foo": "1"}))


def test_registry_match_skips_disabled():
    reg = WorkflowRegistry(
        [
            parse_workflow(wf(name="on"), default_registry),
            parse_workflow(wf(name="off", enabled=False), default_registry),
        ]
    )
    assert [r.name for r in reg.match("github", "push", {}, {})] == ["on"]


# ---------------------------------------------------------------- templating


CTX = {"event": {"payload": {"n": 3, "items": [1, 2], "name": "x"}}, "steps": {}}


def test_single_expression_renders_native_value():
    assert render("{{ event.payload.n }}", CTX) == 3
    assert render("{{ event.payload.items }}", CTX) == [1, 2]
    assert render({"k": ["{{ event.payload.n + 1 }}"]}, CTX) == {"k": [4]}


def test_mixed_template_renders_string():
    assert render("n={{ event.payload.n }}", CTX) == "n=3"
    assert render("plain", CTX) == "plain"


def test_undefined_variables_are_permanent_errors():
    with pytest.raises(PermanentError, match="template error"):
        render("{{ event.payload.nope.deeper }}", CTX)


def test_sandbox_blocks_unsafe_access():
    with pytest.raises(PermanentError):
        render("{{ event.__class__.__mro__ }}", CTX)
    with pytest.raises(PermanentError):
        render("{{ event.payload.items.append(3) }}", CTX)


@pytest.mark.parametrize(
    ("cond", "expected"),
    [
        ("event.payload.n > 2", True),
        ("{{ event.payload.n > 5 }}", False),
        ("event.payload.items | length == 2", True),
        ("{{ 'false' }}", False),
    ],
)
def test_conditions(cond, expected):
    assert evaluate_condition(cond, CTX) is expected
