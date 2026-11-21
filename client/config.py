"""Configuración persistente del cliente BabelPipe (REQ-07, AC-12).

Contrato (ARCHITECTURE.md §3 ``AppConfig``, §6):
* Esquema Pydantic v2 estricto (``extra="forbid"`` + ``strict=True``) con el token
  como :class:`~pydantic.SecretStr`; nunca aparece en ``repr`` ni se serializa.
* Carga por capas, de menor a mayor prioridad:
  **valores por defecto < config.toml < variables BABELPIPE_* < overrides explícitos**.
* Solo dependencias ya fijadas: ``pydantic`` y la stdlib (``tomllib``, ``os``).
  No se usa ``pydantic-settings`` ni se instala nada en tiempo de ejecución.

Los errores de validación se traducen a :class:`ConfigError` con mensajes
accionables construidos únicamente a partir de la ruta del campo y del tipo de
error: jamás del valor de entrada. Así un token inválido no puede filtrarse en
un mensaje, log o stack trace.
"""

from __future__ import annotations

import ipaddress
import json
import os
import tomllib
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)

ENV_PREFIX = "BABELPIPE_"
"""Prefijo de las variables de entorno (evita colisiones del sistema)."""

LOCAL_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})
"""Hosts de localhost donde el token es opcional (ARCHITECTURE.md §6)."""

# Valores canónicos: única fuente de verdad, reutilizados por los modelos y por
# la capa de defaults. El rango de duck_level viene de SPEC.md REQ-05.
DEFAULT_HOST: str = "127.0.0.1"
DEFAULT_PORT: int = 8765
DEFAULT_RX_SINK: str = "Translator_Rx_Sink"
DEFAULT_TX_SOURCE: str = "Translator_Tx_Mic"
DUCK_LEVEL_MIN: float = 0.10
DUCK_LEVEL_MAX: float = 0.15
DEFAULT_DUCK_LEVEL: float = 0.12
DEFAULT_ECHO_CANCEL: bool = True
DEFAULT_PTT_ENABLED: bool = False
DEFAULT_SOURCE_LANG: str = "en"
DEFAULT_TARGET_LANG: str = "es"
DEFAULT_STT: str = "faster-whisper"
DEFAULT_TRANSLATOR: str = "opus-mt"
DEFAULT_TTS: str = "kokoro"
EMPTY_MODEL_PATH: str = ""

_NODE_NAME_RE = r"[A-Za-z0-9_.-]{1,128}"
_ERROR_HINTS: dict[str, str] = {
    "extra_forbidden": "campo no permitido (revisa ARCHITECTURE.md §3)",
    "missing": "campo obligatorio ausente",
    "greater_than_equal": "valor por debajo del mínimo permitido",
    "less_than_equal": "valor por encima del máximo permitido",
    "greater_than": "valor debe ser mayor que el mínimo",
    "less_than": "valor debe ser menor que el máximo",
    "int_type": "se esperaba un número entero",
    "int_parsing": "se esperaba un número entero",
    "float_type": "se esperaba un número decimal",
    "float_parsing": "se esperaba un número decimal",
    "bool_type": "se esperaba true o false",
    "string_type": "se esperaba texto",
    "string_too_short": "texto demasiado corto",
    "string_too_long": "texto demasiado largo",
    "string_pattern_mismatch": "formato de texto inválido",
    "model_type": "se esperaba una sección con sus campos",
    "json_invalid": "contenido no válido",
}
_KIND_HINTS: dict[str, str] = {
    "str": "texto",
    "int": "un número entero",
    "float": "un número decimal",
    "bool": "true o false",
}
_ENV_SPECS: dict[str, tuple[tuple[str, ...], str]] = {
    "SERVER_HOST": (("server", "host"), "str"),
    "SERVER_PORT": (("server", "port"), "int"),
    "SERVER_TOKEN": (("server", "token"), "str"),
    "AUDIO_RX_SINK": (("audio", "rx_sink"), "str"),
    "AUDIO_TX_SOURCE": (("audio", "tx_source"), "str"),
    "AUDIO_DUCK_LEVEL": (("audio", "duck_level"), "float"),
    "AUDIO_ECHO_CANCEL": (("audio", "echo_cancel"), "bool"),
    "AUDIO_PTT_ENABLED": (("audio", "ptt_enabled"), "bool"),
    "PIPELINE_SOURCE_LANG": (("pipeline", "source_lang"), "str"),
    "PIPELINE_TARGET_LANG": (("pipeline", "target_lang"), "str"),
    "PIPELINE_STT": (("pipeline", "stt"), "str"),
    "PIPELINE_TRANSLATOR": (("pipeline", "translator"), "str"),
    "PIPELINE_TTS": (("pipeline", "tts"), "str"),
    "MODEL_STT_PATH": (("model", "stt_path"), "str"),
    "MODEL_TRANSLATOR_PATH": (("model", "translator_path"), "str"),
    "MODEL_TTS_PATH": (("model", "tts_path"), "str"),
}


class ConfigError(ValueError):
    """Error de configuración con mensaje accionable y sin secretos."""


class _StrictModel(BaseModel):
    """Base común: rechaza campos desconocidos, es inmutable y es estricta."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ServerConfig(_StrictModel):
    """Frontera del servidor de inferencia (ARCHITECTURE.md §3 / §6)."""

    host: str = Field(default=DEFAULT_HOST, min_length=1, max_length=253)
    port: int = Field(default=DEFAULT_PORT, ge=1024, le=65535)
    token: SecretStr | None = None

    @field_validator("host")
    @classmethod
    def _validate_host(cls, value: str) -> str:
        if value in LOCAL_HOSTS:
            return value
        try:
            ipaddress.ip_address(value)
        except ValueError:
            raise ValueError("host debe ser una dirección IP válida o 'localhost'") from None
        return value


class AudioConfig(_StrictModel):
    """Nodos PipeWire, ducking y opciones de captura."""

    rx_sink: str = Field(default=DEFAULT_RX_SINK, pattern=_NODE_NAME_RE)
    tx_source: str = Field(default=DEFAULT_TX_SOURCE, pattern=_NODE_NAME_RE)
    duck_level: float = Field(default=DEFAULT_DUCK_LEVEL, ge=DUCK_LEVEL_MIN, le=DUCK_LEVEL_MAX)
    echo_cancel: bool = DEFAULT_ECHO_CANCEL
    ptt_enabled: bool = DEFAULT_PTT_ENABLED


class PipelineConfig(_StrictModel):
    """Idiomas y componentes intercambiables del pipeline."""

    source_lang: str = Field(default=DEFAULT_SOURCE_LANG, min_length=2, max_length=10)
    target_lang: str = Field(default=DEFAULT_TARGET_LANG, min_length=2, max_length=10)
    stt: str = Field(default=DEFAULT_STT, min_length=1, max_length=64)
    translator: str = Field(default=DEFAULT_TRANSLATOR, min_length=1, max_length=64)
    tts: str = Field(default=DEFAULT_TTS, min_length=1, max_length=64)


class ModelPathsConfig(_StrictModel):
    """Rutas a modelos locales; vacío significa usar el caché del motor."""

    stt_path: str = Field(default=EMPTY_MODEL_PATH, max_length=4096)
    translator_path: str = Field(default=EMPTY_MODEL_PATH, max_length=4096)
    tts_path: str = Field(default=EMPTY_MODEL_PATH, max_length=4096)


class ClientSettings(_StrictModel):
    """Configuración completa y validada del proceso cliente."""

    server: ServerConfig = Field(default_factory=ServerConfig)
    audio: AudioConfig = Field(default_factory=AudioConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    model: ModelPathsConfig = Field(default_factory=ModelPathsConfig)

    @model_validator(mode="after")
    def _token_required_on_remote_host(self) -> ClientSettings:
        token = self.server.token
        has_token = token is not None and bool(token.get_secret_value())
        if self.server.host not in LOCAL_HOSTS and not has_token:
            raise ValueError("server.token es obligatorio cuando server.host no es localhost")
        return self

    def to_toml(self) -> str:
        """Serializa la configuración a TOML **sin el token**.

        El token se omite por completo (ni siquiera enmascarado): debe vivir en
        ``.env`` / variables de entorno y nunca en un fichero versionable.
        """
        data = self.model_dump(mode="json")
        lines: list[str] = []
        for section, values in data.items():
            if not isinstance(values, dict):
                continue
            lines.append(f"[{section}]")
            for key, value in values.items():
                if section == "server" and key == "token":
                    continue
                lines.append(f"{key} = {_toml_scalar(value)}")
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"


def to_toml(settings: ClientSettings) -> str:
    """Atajo de módulo para :meth:`ClientSettings.to_toml`."""
    return settings.to_toml()


_DEFAULTS: dict[str, Any] = {
    "server": {"host": DEFAULT_HOST, "port": DEFAULT_PORT},
    "audio": {
        "rx_sink": DEFAULT_RX_SINK,
        "tx_source": DEFAULT_TX_SOURCE,
        "duck_level": DEFAULT_DUCK_LEVEL,
        "echo_cancel": DEFAULT_ECHO_CANCEL,
        "ptt_enabled": DEFAULT_PTT_ENABLED,
    },
    "pipeline": {
        "source_lang": DEFAULT_SOURCE_LANG,
        "target_lang": DEFAULT_TARGET_LANG,
        "stt": DEFAULT_STT,
        "translator": DEFAULT_TRANSLATOR,
        "tts": DEFAULT_TTS,
    },
    "model": {
        "stt_path": EMPTY_MODEL_PATH,
        "translator_path": EMPTY_MODEL_PATH,
        "tts_path": EMPTY_MODEL_PATH,
    },
}


def _deep_merge(base: Mapping[str, Any], layer: Mapping[str, Any]) -> dict[str, Any]:
    """Fusión recursiva ``base <- layer`` devolviendo un dict nuevo."""
    result: dict[str, Any] = deepcopy(dict(base))
    for key, value in layer.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _set_nested(target: dict[str, Any], path: tuple[str, ...], value: object) -> None:
    """Escribe ``value`` en ``target[path[0]]...[path[-1]]`` creando tablas."""
    cursor: dict[str, Any] = target
    for part in path[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            child = {}
            cursor[part] = child
        cursor = child
    cursor[path[-1]] = value


def _coerce_env_value(kind: str, raw: str) -> object:
    """Convierte el texto de una variable de entorno al tipo del campo."""
    text = raw.strip()
    if kind == "str":
        return text
    if kind == "int":
        return int(text)
    if kind == "float":
        return float(text)
    if kind == "bool":
        lowered = text.lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"no se pudo convertir {kind}")


def _env_layer(environ: Mapping[str, str]) -> dict[str, Any]:
    """Construye la capa de entorno a partir de ``BABELPIPE_*`` conocidas."""
    layer: dict[str, Any] = {}
    for suffix, (path, kind) in _ENV_SPECS.items():
        raw = environ.get(f"{ENV_PREFIX}{suffix}")
        if raw is None or raw.strip() == "":
            continue
        try:
            value = _coerce_env_value(kind, raw)
        except ValueError:
            raise ConfigError(
                f"Variable {ENV_PREFIX}{suffix} inválida: se esperaba {_KIND_HINTS[kind]}."
            ) from None
        _set_nested(layer, path, value)
    return layer


def _read_toml(path: Path) -> dict[str, Any]:
    """Lee y parsea un ``config.toml`` con mensajes sin stack trace."""
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError:
        raise ConfigError(
            f"Sintaxis TOML inválida en {path}; revisa el fichero de configuración."
        ) from None
    except OSError as exc:
        raise ConfigError(f"No se pudo leer {path}: {exc.strerror or 'error de E/S'}.") from None
    return dict(data)


def _describe_validation(exc: ValidationError) -> str:
    """Resume errores de Pydantic SIN incluir ``input`` ni ``ctx`` (secretos)."""
    details: list[str] = []
    for error in exc.errors():
        error_type = str(error.get("type", ""))
        if error_type == "value_error":
            # Mensajes escritos por nuestros validadores: nunca interpolan valores.
            details.append(str(error.get("msg", "validación de modelo inválida")))
            continue
        location = ".".join(str(part) for part in error.get("loc", ())) or "configuración"
        hint = _ERROR_HINTS.get(error_type, "valor inválido")
        details.append(f"{location}: {hint}")
    return "; ".join(details)


def _toml_scalar(value: object) -> str:
    """Representa un valor Python como literal TOML válido."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if value is None:
        return '""'
    return json.dumps(str(value), ensure_ascii=False)


def load_settings(
    config_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> ClientSettings:
    """Carga la configuración aplicando las cuatro capas en orden de prioridad.

    Args:
        config_path: ruta a un ``config.toml`` opcional. ``None`` omite la capa
            TOML (útil para tests y para despliegues solo con entorno).
        env: mapeo de variables de entorno. Por defecto, ``os.environ``.
        overrides: valores explícitos de más alta prioridad (mismo formato
            anidado que ``config.toml``), p. ej. los flags de la CLI.

    Raises:
        ConfigError: fichero ilegible, TOML mal formado, variable inválida o
            configuración que no pasa el esquema estricto. El mensaje nunca
            contiene valores secretos ni stack traces.
    """
    environ: Mapping[str, str] = os.environ if env is None else env
    merged = _deep_merge({}, _DEFAULTS)
    if config_path is not None:
        merged = _deep_merge(merged, _read_toml(config_path))
    merged = _deep_merge(merged, _env_layer(environ))
    if overrides is not None:
        merged = _deep_merge(merged, overrides)
    try:
        return ClientSettings.model_validate(merged)
    except ValidationError as exc:
        raise ConfigError(
            f"Configuración de BabelPipe inválida — {_describe_validation(exc)}. "
            "Revisa config.toml y las variables BABELPIPE_*."
        ) from None


def dump_example() -> str:
    """Devuelve el texto de un ``config.toml.example`` sin secretos.

    Es una función pura: **no escribe ni sobreescribe ningún fichero**. El token
    se documenta como variable de entorno, nunca como valor del fichero.
    """
    header = (
        "# BabelPipe — plantilla de configuración (ARCHITECTURE.md §3)\n"
        "# Copiar a config.toml. Sin secretos reales.\n"
        "# El token se configura fuera del repo con BABELPIPE_SERVER_TOKEN.\n\n"
    )
    return header + ClientSettings().to_toml()


__all__ = [
    "AudioConfig",
    "ClientSettings",
    "ConfigError",
    "ENV_PREFIX",
    "ModelPathsConfig",
    "PipelineConfig",
    "ServerConfig",
    "dump_example",
    "load_settings",
    "to_toml",
]
