"""Dotted-path lookup into nested JSON-like structures."""

from __future__ import annotations

from typing import Any, Final


class _Missing:
    def __repr__(self) -> str:
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Final = _Missing()


def get_path(obj: Any, path: str) -> Any:
    """``get_path({"a": [{"b": 1}]}, "a.0.b") == 1``; returns ``MISSING`` if absent.

    Dict keys are matched exactly first, then case-insensitively (HTTP headers).
    """
    current = obj
    for part in path.split(".") if path else []:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
                continue
            lowered = {str(k).lower(): v for k, v in current.items()}
            if part.lower() in lowered:
                current = lowered[part.lower()]
                continue
            return MISSING
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return MISSING
            continue
        return MISSING
    return current
