"""Eventos JSON de control del protocolo BabelPipe (ARCHITECTURE.md §4.3).

Todos los mensajes son estrictos: campo ``type`` discriminado por ``Literal``
y ``extra='forbid'``. Ningún evento llega a la lógica de negocio sin haber
pasado por :func:`parse_control_message`.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import (
    UUID4,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
)

LanguageCode = Annotated[
    str,
    Field(min_length=2, max_length=10, pattern=r"^[a-zA-Z]{2}([-_][a-zA-Z0-9]{2,8})?$"),
]
ComponentName = Annotated[str, Field(min_length=1, max_length=64)]


def _validate_session_id(value: Any) -> Any:
    """Acepta UUIDs o strings canónicos, rechazando cualquier otro UUID4."""
    if isinstance(value, str):
        try:
            parsed = UUID(value)
        except ValueError as exc:
            raise ValueError("session_id debe ser un UUID4 válido") from exc
        if parsed.version != 4:
            raise ValueError("session_id debe ser un UUID4")
        return parsed
    return value


SessionId = Annotated[UUID4, BeforeValidator(_validate_session_id)]
StreamName = Literal["rx", "tx"]


class _StrictEvent(BaseModel):
    # ConfigDict se hereda a todos los eventos: campo desconocido = error.
    model_config = ConfigDict(extra="forbid")


class AudioConfigMessage(_StrictEvent):
    type: Literal["audio_config"] = "audio_config"
    sample_rate: Literal[16000] = 16000
    channels: Literal[1] = 1
    format: Literal["pcm_s16le"] = "pcm_s16le"
    chunk_ms: Literal[20] = 20


class ConfigMessage(_StrictEvent):
    type: Literal["config"] = "config"
    session_id: SessionId
    mode: StreamName
    source_lang: LanguageCode
    target_lang: LanguageCode
    vad: ComponentName
    stt: ComponentName
    translator: ComponentName
    tts: ComponentName
    audio_config: AudioConfigMessage


class PttMessage(_StrictEvent):
    type: Literal["ptt"] = "ptt"
    active: bool


class ResetMessage(_StrictEvent):
    type: Literal["reset"] = "reset"
    session_id: SessionId


class PingMessage(_StrictEvent):
    type: Literal["ping"] = "ping"
    ts: int = Field(ge=0, le=9_223_372_036_854_775_807)


class ReadyMessage(_StrictEvent):
    type: Literal["ready"] = "ready"
    models: list[str] = Field(max_length=64)
    session_id: SessionId


class SubtitleMessage(_StrictEvent):
    type: Literal["subtitle"] = "subtitle"
    session_id: SessionId
    stream: StreamName
    text: str = Field(max_length=2000)
    is_final: bool
    latency_ms: int | None = Field(default=None, ge=0, le=5000)


class TranslatedMessage(_StrictEvent):
    type: Literal["translated"] = "translated"
    text: str = Field(max_length=2000)
    is_final: bool
    lang: LanguageCode


class DuckMessage(_StrictEvent):
    type: Literal["duck"] = "duck"
    action: Literal["start", "stop"]
    stream: StreamName


class TtsChunkMessage(_StrictEvent):
    type: Literal["tts_chunk"] = "tts_chunk"
    seq: int = Field(ge=0, le=0xFFFFFFFF)
    duration_ms: int = Field(ge=0, le=5000)


class ReconnectMessage(_StrictEvent):
    type: Literal["reconnect"] = "reconnect"
    retry_after_ms: int = Field(ge=0, le=60_000)
    session_id: SessionId
    last_seq: int = Field(ge=0, le=0xFFFFFFFF)


class ErrorMessage(_StrictEvent):
    type: Literal["error"] = "error"
    code: str = Field(min_length=1, max_length=64)
    message: str = Field(max_length=2000)


class PongMessage(_StrictEvent):
    type: Literal["pong"] = "pong"
    ts: int = Field(ge=0, le=9_223_372_036_854_775_807)


ControlMessage = Annotated[
    Union[  # noqa: UP007 - mypy necesita Union explícito para inferir TypeAdapter
        ConfigMessage,
        AudioConfigMessage,
        PttMessage,
        ResetMessage,
        PingMessage,
        ReadyMessage,
        SubtitleMessage,
        TranslatedMessage,
        DuckMessage,
        TtsChunkMessage,
        ReconnectMessage,
        ErrorMessage,
        PongMessage,
    ],
    Field(discriminator="type"),
]

_CONTROL_ADAPTER: TypeAdapter[ControlMessage] = TypeAdapter(ControlMessage)


def _format_validation_error(exc: ValidationError) -> str:
    details: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        details.append(f"{location}: {error['msg']}")
    return "; ".join(details)


def parse_control_message(raw: str | bytes) -> ControlMessage:
    """Parsea un frame JSON de control o lanza ``ValueError`` legible.

    Los bytes se decodifican como UTF-8; se rechazan JSON inválido, ``type``
    desconocido y campos extra. El mensaje de error no expone stack traces.
    """
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("el mensaje de control no es UTF-8 válido") from exc
    else:
        text = raw

    try:
        payload: Any = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"JSON de control inválido: {exc.msg} (línea {exc.lineno}, columna {exc.colno})"
        ) from exc

    try:
        return _CONTROL_ADAPTER.validate_python(payload)
    except ValidationError as exc:
        raise ValueError(f"mensaje de control inválido: {_format_validation_error(exc)}") from exc
