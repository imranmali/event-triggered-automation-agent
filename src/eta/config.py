"""Runtime settings, loaded from environment variables prefixed with ``ETA_``."""

from __future__ import annotations

import os
import socket
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

SECRET_ENV_PREFIX = "ETA_SECRET_"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ETA_", env_file=".env", extra="ignore")

    # Storage
    database_url: str = "sqlite+aiosqlite:///./eta.db"
    db_pool_size: int = 10

    # Declarative configuration
    config_dir: Path = Path("./config")

    # HTTP API
    host: str = "0.0.0.0"
    port: int = 8080
    admin_token: str | None = None
    max_body_bytes: int = 1_048_576

    # Worker
    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    worker_concurrency: int = 8
    poll_interval: float = 1.0
    lease_seconds: float = 60.0
    heartbeat_interval: float = 15.0
    shutdown_grace_seconds: float = 30.0
    reap_interval: float = 10.0

    # Redis Streams ingress
    redis_url: str = "redis://localhost:6379/0"

    # Logging
    log_level: str = "INFO"
    log_json: bool = True

    # Python action plugins: comma separated module names imported at startup
    plugin_modules: str = ""

    @property
    def plugin_module_list(self) -> list[str]:
        return [m.strip() for m in self.plugin_modules.split(",") if m.strip()]


def load_secrets(environ: dict[str, str] | None = None) -> dict[str, str]:
    """Secrets visible to workflow templates as ``secrets.NAME``.

    Only variables explicitly prefixed with ``ETA_SECRET_`` are exposed, so a
    workflow author can never read arbitrary process environment.
    """
    env = os.environ if environ is None else environ
    return {k[len(SECRET_ENV_PREFIX) :]: v for k, v in env.items() if k.startswith(SECRET_ENV_PREFIX)}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
