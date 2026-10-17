"""Loading and indexing the declarative configuration directory.

Layout::

    config/
      sources.yaml          # ingress sources (webhook auth, id/type extraction)
      workflows/*.yaml      # one workflow per file (or a list per file)
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from eta.actions.base import ActionRegistry
from eta.errors import ConfigError
from eta.models import WorkflowRef
from eta.workflows.matcher import trigger_matches
from eta.workflows.spec import SourceSpec, WorkflowSpec
from eta.workflows.templating import validate_template


@dataclass(slots=True)
class LoadedConfig:
    sources: dict[str, SourceSpec]
    workflows: WorkflowRegistry


class WorkflowRegistry:
    def __init__(self, workflows: Iterable[WorkflowSpec] = ()) -> None:
        self._by_name: dict[str, WorkflowSpec] = {}
        for wf in workflows:
            if wf.name in self._by_name:
                raise ConfigError(f"duplicate workflow name {wf.name!r}")
            self._by_name[wf.name] = wf

    def __len__(self) -> int:
        return len(self._by_name)

    def __iter__(self) -> Any:
        return iter(self._by_name.values())

    def get(self, name: str) -> WorkflowSpec | None:
        return self._by_name.get(name)

    def match(
        self, source: str, event_type: str, payload: Any, headers: Mapping[str, str]
    ) -> list[WorkflowRef]:
        event = {"source": source, "type": event_type, "payload": payload, "headers": dict(headers)}
        return [
            to_ref(wf)
            for wf in self._by_name.values()
            if wf.enabled and any(trigger_matches(tr, event) for tr in wf.triggers)
        ]


def to_ref(wf: WorkflowSpec) -> WorkflowRef:
    return WorkflowRef(name=wf.name, version=wf.version, spec=wf.dump())


def parse_workflow(data: Any, actions: ActionRegistry, origin: str = "<inline>") -> WorkflowSpec:
    try:
        wf = WorkflowSpec.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"{origin}: invalid workflow: {exc}") from exc
    for step in wf.steps:
        if step.action not in actions:
            raise ConfigError(
                f"{origin}: workflow {wf.name!r} step {step.id!r} uses unknown action "
                f"{step.action!r} (known: {', '.join(actions.names())})"
            )
        try:
            validate_template(step.with_)
            if step.when:
                validate_template(step.when if "{{" in step.when else "{{ " + step.when + " }}")
        except ConfigError as exc:
            raise ConfigError(f"{origin}: workflow {wf.name!r} step {step.id!r}: {exc}") from exc
    return wf


def parse_sources(
    data: Any, origin: str = "sources.yaml", *, environ: Mapping[str, str] | None = None
) -> dict[str, SourceSpec]:
    env = os.environ if environ is None else environ
    raw = (data or {}).get("sources", {}) if isinstance(data, dict) else None
    if not isinstance(raw, dict):
        raise ConfigError(f"{origin}: expected a mapping under 'sources'")
    out: dict[str, SourceSpec] = {}
    for name, body in raw.items():
        try:
            spec = SourceSpec.model_validate({"name": name, **(body or {})})
        except ValidationError as exc:
            raise ConfigError(f"{origin}: invalid source {name!r}: {exc}") from exc
        if spec.verify.secret_env and not env.get(spec.verify.secret_env):
            raise ConfigError(
                f"{origin}: source {name!r} requires environment variable "
                f"{spec.verify.secret_env} (signing secret) to be set"
            )
        out[name] = spec
    return out


def _load_yaml(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: invalid YAML: {exc}") from exc


def load_config(
    config_dir: Path, actions: ActionRegistry, *, environ: Mapping[str, str] | None = None
) -> LoadedConfig:
    if not config_dir.is_dir():
        raise ConfigError(f"config directory {config_dir} does not exist")
    sources_file = config_dir / "sources.yaml"
    sources = (
        parse_sources(_load_yaml(sources_file), str(sources_file), environ=environ)
        if sources_file.exists()
        else {}
    )
    workflows: list[WorkflowSpec] = []
    wf_dir = config_dir / "workflows"
    if wf_dir.is_dir():
        for path in sorted([*wf_dir.glob("*.yaml"), *wf_dir.glob("*.yml")]):
            data = _load_yaml(path)
            for item in data if isinstance(data, list) else [data]:
                workflows.append(parse_workflow(item, actions, str(path)))
    return LoadedConfig(sources=sources, workflows=WorkflowRegistry(workflows))
