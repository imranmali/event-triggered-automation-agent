"""Sandboxed Jinja2 templating for step inputs and ``when`` conditions.

* A string that is exactly one expression (``"{{ event.payload.count }}"``)
  renders to the *native* value (int, list, dict...), not its string form.
* Anything else renders as a string.
* Undefined variables are errors (``StrictUndefined``) — a typo in a template
  should fail loudly and permanently, not send ``""`` to a downstream API.
"""

from __future__ import annotations

import json
import re
from typing import Any

from jinja2 import StrictUndefined, TemplateError, Undefined
from jinja2.sandbox import ImmutableSandboxedEnvironment

from eta.errors import ConfigError, PermanentError

_SINGLE_EXPR = re.compile(r"^\s*\{\{(?P<expr>.*?)\}\}\s*$", re.DOTALL)


class _Environment(ImmutableSandboxedEnvironment):
    """Prefer mapping keys over attributes for dot access.

    Stock Jinja resolves ``payload.items`` to the ``dict.items`` *method*; event
    payloads routinely contain keys named ``items``, ``keys`` or ``values``.
    """

    def getattr(self, obj: Any, attribute: str) -> Any:
        if isinstance(obj, dict) and attribute in obj:
            return obj[attribute]
        return super().getattr(obj, attribute)


def _make_env() -> ImmutableSandboxedEnvironment:
    env = _Environment(undefined=StrictUndefined, autoescape=False)
    env.filters["tojson"] = lambda v: json.dumps(v, default=str)
    env.filters["fromjson"] = json.loads
    return env


_env = _make_env()


def _is_single_expression(text: str) -> str | None:
    m = _SINGLE_EXPR.match(text)
    if m and "{{" not in m.group("expr") and "{%" not in text:
        return m.group("expr")
    return None


def render(value: Any, context: dict[str, Any]) -> Any:
    """Recursively render every string inside ``value``."""
    try:
        return _render(value, context)
    except TemplateError as exc:
        raise PermanentError(f"template error: {exc}") from exc


def _render(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        if "{{" not in value and "{%" not in value:
            return value
        expr = _is_single_expression(value)
        if expr is not None:
            result = _env.compile_expression(expr, undefined_to_none=False)(**context)
            if isinstance(result, Undefined):
                str(result)  # StrictUndefined raises UndefinedError here
            return result
        return _env.from_string(value).render(**context)
    if isinstance(value, dict):
        return {_render(k, context): _render(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(v, context) for v in value]
    return value


def evaluate_condition(condition: str, context: dict[str, Any]) -> bool:
    """Evaluate a ``when:`` clause. Accepts ``"{{ expr }}"`` or a bare ``expr``."""
    text = condition if "{{" in condition else "{{ " + condition + " }}"
    result = render(text, context)
    if isinstance(result, str):
        return result.strip().lower() not in {"", "false", "0", "none", "no"}
    return bool(result)


def validate_template(value: Any) -> None:
    """Syntax-check all templates in a structure at config load time."""
    if isinstance(value, str):
        if "{{" in value or "{%" in value:
            try:
                expr = _is_single_expression(value)
                if expr is not None:
                    _env.compile_expression(expr)
                else:
                    _env.parse(value)
            except TemplateError as exc:
                raise ConfigError(f"invalid template {value!r}: {exc}") from exc
    elif isinstance(value, dict):
        for k, v in value.items():
            validate_template(k)
            validate_template(v)
    elif isinstance(value, list):
        for v in value:
            validate_template(v)
