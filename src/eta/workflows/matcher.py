"""Trigger matching: which workflows should run for an event."""

from __future__ import annotations

import fnmatch
import re
from typing import Any

from eta.workflows.paths import MISSING, get_path
from eta.workflows.spec import FilterCondition, TriggerSpec


def _compare(op: str, actual: Any, expected: Any) -> bool:
    try:
        if op == "gt":
            return bool(actual > expected)
        if op == "gte":
            return bool(actual >= expected)
        if op == "lt":
            return bool(actual < expected)
        if op == "lte":
            return bool(actual <= expected)
    except TypeError:
        return False
    raise ValueError(op)


def condition_matches(cond: FilterCondition, event: dict[str, Any]) -> bool:
    actual = get_path(event, cond.path)
    op, expected = cond.op, cond.value
    if op == "exists":
        return actual is not MISSING
    if op == "not_exists":
        return actual is MISSING
    if actual is MISSING:
        return op in {"ne", "not_in"}
    if op == "eq":
        return bool(actual == expected)
    if op == "ne":
        return bool(actual != expected)
    if op == "in":
        return actual in expected
    if op == "not_in":
        return actual not in expected
    if op == "contains":
        try:
            return expected in actual
        except TypeError:
            return False
    if op in {"startswith", "endswith", "matches"}:
        if not isinstance(actual, str):
            return False
        if op == "startswith":
            return actual.startswith(str(expected))
        if op == "endswith":
            return actual.endswith(str(expected))
        return re.search(str(expected), actual) is not None
    return _compare(op, actual, expected)


def trigger_matches(trigger: TriggerSpec, event: dict[str, Any]) -> bool:
    """``event`` is ``{"source", "type", "payload", "headers"}``."""
    if not fnmatch.fnmatchcase(str(event["source"]), trigger.source):
        return False
    if not any(fnmatch.fnmatchcase(str(event["type"]), pattern) for pattern in trigger.event):
        return False
    return all(condition_matches(c, event) for c in trigger.filter)
