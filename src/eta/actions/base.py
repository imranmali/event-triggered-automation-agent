"""Action plug-in interface and registry.

An action is an async callable ``(params, ctx) -> output``. ``params`` are the
step's rendered ``with:`` block; ``output`` must be JSON-serialisable and is
checkpointed so later steps (and retries) can use it without re-executing.

Actions signal failures with :class:`~eta.errors.RetryableError` or
:class:`~eta.errors.PermanentError`; any other exception is treated as
retryable. Actions with side effects should forward ``ctx.idempotency_key`` to
the downstream system — it is stable across retries of the same step.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from eta.errors import ConfigError, PermanentError
from eta.models import Event, IngestResult

EmitFn = Callable[[str, str, Any, str], Awaitable[IngestResult]]
ActionFn = Callable[[Any, "ActionContext"], Awaitable[Any]]
F = TypeVar("F", bound=ActionFn)


@dataclass(slots=True)
class ActionContext:
    run_id: str
    workflow_name: str
    step_id: str
    attempt: int
    idempotency_key: str
    event: Event
    http: httpx.AsyncClient
    emit: EmitFn
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("eta.action"))


@dataclass(slots=True, frozen=True)
class ActionDef:
    name: str
    fn: ActionFn
    params_model: type[BaseModel] | None = None
    description: str = ""

    async def invoke(self, params: Any, ctx: ActionContext) -> Any:
        if self.params_model is not None:
            try:
                params = self.params_model.model_validate(params)
            except ValidationError as exc:
                raise PermanentError(f"invalid parameters for action {self.name!r}: {exc}") from exc
        return await self.fn(params, ctx)


class ActionRegistry:
    def __init__(self) -> None:
        self._actions: dict[str, ActionDef] = {}

    def register(
        self,
        name: str,
        fn: ActionFn,
        *,
        params: type[BaseModel] | None = None,
        description: str = "",
    ) -> None:
        if name in self._actions:
            raise ConfigError(f"action {name!r} already registered")
        self._actions[name] = ActionDef(name, fn, params, description or (fn.__doc__ or "").strip())

    def action(
        self, name: str, *, params: type[BaseModel] | None = None
    ) -> Callable[[F], F]:
        def decorator(fn: F) -> F:
            self.register(name, fn, params=params)
            return fn

        return decorator

    def get(self, name: str) -> ActionDef:
        try:
            return self._actions[name]
        except KeyError:
            raise PermanentError(f"unknown action {name!r}") from None

    def __contains__(self, name: object) -> bool:
        return name in self._actions

    def names(self) -> list[str]:
        return sorted(self._actions)


#: Process-wide registry; plug-in modules register into it with ``@action``.
default_registry = ActionRegistry()
action = default_registry.action


def load_plugins(modules: list[str]) -> None:
    """Import plug-in modules so their ``@action`` decorators run."""
    for module in modules:
        try:
            importlib.import_module(module)
        except ImportError as exc:
            raise ConfigError(f"cannot import plugin module {module!r}: {exc}") from exc
