"""Configuración del servidor BabelPipe (ARCHITECTURE.md §3, §6).

Los valores se leen del entorno con el prefijo ``BABELPIPE_``. Se usa
``pydantic.BaseModel`` + :meth:`ServerSettings.from_env` porque el proyecto no
declara ``pydantic-settings``; así se mantiene el conjunto de dependencias fijado
en ``pyproject.toml``.

El token se guarda como ``SecretStr`` y nunca se incluye en ``repr`` ni en logs.
Solo es obligatorio cuando ``host`` no es una dirección de localhost.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from shared.constants import MAX_FRAME_BYTES

ENV_PREFIX = "BABELPIPE_"
LOCAL_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})


class ServerSettings(BaseModel):
    """Parámetros de frontera del servidor WebSocket y del endpoint de salud."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    host: str = "127.0.0.1"
    port: int = Field(default=8765, ge=0, le=65535)
    token: SecretStr | None = None
    ws_path: str = Field(default="/ws/translate", min_length=1)
    max_frame_bytes: int = Field(default=MAX_FRAME_BYTES, gt=0)
    max_sessions: int = Field(default=32, gt=0)
    ping_interval: float | None = Field(default=20.0, gt=0)
    rate_limit_per_second: int = Field(default=200, gt=0)
    health_host: str = "127.0.0.1"
    health_port: int = Field(default=0, ge=0, le=65535)

    @field_validator("ws_path")
    @classmethod
    def _normalize_ws_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("ws_path debe empezar con '/'")
        return value.rstrip("/") or "/"

    @model_validator(mode="after")
    def _require_token_on_remote_host(self) -> ServerSettings:
        if self.host not in LOCAL_HOSTS and self.token is None:
            raise ValueError("token obligatorio cuando host no es localhost")
        return self

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> ServerSettings:
        """Construye la configuración desde ``BABELPIPE_*``.

        Las variables ausentes o vacías se ignoran para que los valores por
        defecto permanezcan vigentes. ``environ`` permite inyectar un mapping
        en tests sin depender del entorno del proceso.
        """
        source: Mapping[str, str] = os.environ if environ is None else environ
        values: dict[str, str] = {}
        for name in cls.model_fields:
            raw = source.get(f"{ENV_PREFIX}{name.upper()}")
            if raw is not None and raw != "":
                values[name] = raw
        return cls.model_validate(values)
