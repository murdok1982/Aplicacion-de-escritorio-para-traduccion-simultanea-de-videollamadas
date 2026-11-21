"""Tests del contrato canónico (SPEC.md §REQ-03 / AC-04, ARCHITECTURE.md §4)."""

from __future__ import annotations

import json
import struct
from typing import Any
from uuid import UUID, uuid4

import numpy as np
import pytest

from shared.constants import (
    BYTES_PER_SAMPLE,
    CHUNK_BYTES,
    CHUNK_MS,
    CHUNK_SAMPLES,
    HEADER_SIZE,
    HEADER_STRUCT,
    MAGIC,
    MAX_FRAME_BYTES,
    SAMPLE_RATE,
    STREAM_RX,
    STREAM_TX,
    VERSION,
)
from shared.models import (
    AudioConfigMessage,
    ConfigMessage,
    DuckMessage,
    ErrorMessage,
    PingMessage,
    PongMessage,
    PttMessage,
    ReadyMessage,
    ReconnectMessage,
    ResetMessage,
    SubtitleMessage,
    TranslatedMessage,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import (
    AudioFrame,
    Flags,
    ProtocolError,
    decode_frame,
    encode_frame,
    iter_pcm_chunks,
)


def _pcm(num_samples: int = CHUNK_SAMPLES) -> bytes:
    """PCM int16 LE determinista con valores positivos y negativos."""
    return b"".join(struct.pack("<h", (i * 7) % 3000 - 1500) for i in range(num_samples))


def _raw_frame(
    *,
    magic: bytes = MAGIC,
    version: int = VERSION,
    flags: int = 0,
    stream_id: int = STREAM_RX,
    sequence: int = 42,
    sample_rate: int = SAMPLE_RATE,
    num_samples: int = CHUNK_SAMPLES,
    pcm: bytes | None = None,
) -> bytes:
    payload = _pcm(num_samples) if pcm is None else pcm
    return (
        struct.pack(
            HEADER_STRUCT,
            magic,
            version,
            flags,
            stream_id,
            sequence,
            sample_rate,
            num_samples,
        )
        + payload
    )


# ---------------------------------------------------------------------------
# Constantes y cabecera
# ---------------------------------------------------------------------------


def test_canonical_constants() -> None:
    assert struct.calcsize(HEADER_STRUCT) == HEADER_SIZE == 16
    assert CHUNK_SAMPLES == 320
    assert CHUNK_BYTES == CHUNK_SAMPLES * BYTES_PER_SAMPLE == 640
    assert CHUNK_SAMPLES * 1000 / SAMPLE_RATE == CHUNK_MS == 20
    assert MAGIC == b"BP"
    assert VERSION == 1
    assert MAX_FRAME_BYTES >= HEADER_SIZE + CHUNK_BYTES
    assert (STREAM_RX, STREAM_TX) == (1, 2)


# ---------------------------------------------------------------------------
# AC-04: roundtrip binario byte-idéntico
# ---------------------------------------------------------------------------


def test_ac04_binary_roundtrip() -> None:
    pcm = _pcm()
    raw = encode_frame(stream_id=STREAM_RX, sequence=42, pcm=pcm)

    assert len(raw) == HEADER_SIZE + CHUNK_BYTES == 656
    assert raw[:HEADER_SIZE] == struct.pack(
        HEADER_STRUCT, MAGIC, VERSION, 0, STREAM_RX, 42, SAMPLE_RATE, CHUNK_SAMPLES
    )
    assert raw[HEADER_SIZE:] == pcm

    frame = decode_frame(raw)
    assert frame == AudioFrame(
        stream_id=STREAM_RX,
        sequence=42,
        flags=0,
        sample_rate=SAMPLE_RATE,
        num_samples=CHUNK_SAMPLES,
        pcm=pcm,
    )
    assert frame.duration_ms == 20.0
    assert frame.is_final is False

    assert (
        encode_frame(
            stream_id=frame.stream_id,
            sequence=frame.sequence,
            flags=frame.flags,
            pcm=frame.pcm,
            sample_rate=frame.sample_rate,
            num_samples=frame.num_samples,
        )
        == raw
    )


@pytest.mark.parametrize(
    "flags",
    [
        Flags.FINAL,
        Flags.DUCK_START,
        Flags.DUCK_STOP,
        Flags.PTT_ACTIVE,
        Flags.FINAL | Flags.DUCK_START | Flags.DUCK_STOP | Flags.PTT_ACTIVE,
    ],
)
@pytest.mark.parametrize("stream_id", [STREAM_RX, STREAM_TX])
@pytest.mark.parametrize("sequence", [0, 70_000, 0xFFFFFFFF])
def test_roundtrip_flags_streams_and_high_sequence(
    flags: Flags, stream_id: int, sequence: int
) -> None:
    pcm = _pcm()
    raw = encode_frame(
        stream_id=stream_id, sequence=sequence, flags=int(flags), pcm=pcm
    )
    frame = decode_frame(raw)

    assert frame.stream_id == stream_id
    assert frame.sequence == sequence
    assert frame.flags == int(flags)
    assert frame.pcm == pcm
    assert frame.is_final is bool(flags & Flags.FINAL)
    assert raw[:HEADER_SIZE] == struct.pack(
        HEADER_STRUCT,
        MAGIC,
        VERSION,
        int(flags),
        stream_id,
        sequence,
        SAMPLE_RATE,
        CHUNK_SAMPLES,
    )
    assert decode_frame(encode_frame(**_frame_kwargs(frame))) == frame


def _frame_kwargs(frame: AudioFrame) -> dict[str, Any]:
    return {
        "stream_id": frame.stream_id,
        "sequence": frame.sequence,
        "flags": frame.flags,
        "pcm": frame.pcm,
        "sample_rate": frame.sample_rate,
        "num_samples": frame.num_samples,
    }


def test_num_samples_derived_from_payload() -> None:
    pcm = _pcm(160)
    raw = encode_frame(stream_id=STREAM_TX, sequence=1, pcm=pcm)
    assert struct.unpack_from(HEADER_STRUCT, raw)[6] == 160
    assert decode_frame(raw).duration_ms == 10.0


# ---------------------------------------------------------------------------
# Rechazo de frames malformados
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected_code"),
    [
        (_raw_frame(magic=b"XX"), "SCHEMA"),
        (_raw_frame(version=2), "SCHEMA"),
        (_raw_frame(sample_rate=48_000), "FORMAT"),
        (_raw_frame()[:-2], "SCHEMA"),
        (_raw_frame(num_samples=0, pcm=b""), "SCHEMA"),
        (_raw_frame() + b"\x00\x00", "SCHEMA"),
        (_raw_frame(stream_id=3), "SCHEMA"),
        (b"BP\x01", "SCHEMA"),
        (_raw_frame(num_samples=5000), "SCHEMA"),
    ],
    ids=[
        "magic",
        "version",
        "sample_rate_48000",
        "payload_truncado",
        "num_samples_cero",
        "payload_sobrante",
        "stream_id_invalido",
        "cabecera_corta",
        "frame_demasiado_grande",
    ],
)
def test_decode_rejects_malformed_frame(raw: bytes, expected_code: str) -> None:
    with pytest.raises(ProtocolError) as exc_info:
        decode_frame(raw)
    assert exc_info.value.code == expected_code
    assert exc_info.value.message


@pytest.mark.parametrize(
    ("kwargs", "expected_code"),
    [
        ({"stream_id": 9}, "SCHEMA"),
        ({"sequence": 0x1_0000_0000}, "SCHEMA"),
        ({"flags": 256}, "SCHEMA"),
        ({"sample_rate": 48_000}, "FORMAT"),
        ({"num_samples": 319}, "SCHEMA"),
        ({"pcm": b"\x00" * 639}, "SCHEMA"),
        ({"pcm": b""}, "SCHEMA"),
    ],
)
def test_encode_validates_inputs(kwargs: dict[str, Any], expected_code: str) -> None:
    params: dict[str, Any] = {
        "stream_id": STREAM_RX,
        "sequence": 1,
        "pcm": _pcm(),
    }
    params.update(kwargs)
    with pytest.raises(ProtocolError) as exc_info:
        encode_frame(**params)
    assert exc_info.value.code == expected_code


def test_protocol_error_is_exception_with_code() -> None:
    error = ProtocolError("SCHEMA", "magic inválido")
    assert isinstance(error, Exception)
    assert error.code == "SCHEMA"
    assert str(error) == "magic inválido"


# ---------------------------------------------------------------------------
# iter_pcm_chunks
# ---------------------------------------------------------------------------


def test_iter_pcm_chunks_from_bytes_drops_incomplete_tail() -> None:
    raw = _pcm() + _pcm() + _pcm(4)
    chunks = list(iter_pcm_chunks(raw))
    assert len(chunks) == 2
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    assert b"".join(chunks) == raw[: 2 * CHUNK_BYTES]


def test_iter_pcm_chunks_from_numpy_array() -> None:
    samples = (np.arange(CHUNK_SAMPLES * 2, dtype=np.int16) - 100).astype(np.int16)
    chunks = list(iter_pcm_chunks(samples))
    assert len(chunks) == 2
    assert chunks[0] == samples[:CHUNK_SAMPLES].tobytes()
    assert chunks[1] == samples[CHUNK_SAMPLES:].tobytes()


@pytest.mark.parametrize(
    ("samples", "chunk_samples"),
    [
        (b"", 0),
        (np.zeros(4, dtype=np.float32), CHUNK_SAMPLES),
        (np.zeros((2, 4), dtype=np.int16), CHUNK_SAMPLES),
    ],
)
def test_iter_pcm_chunks_rejects_bad_input(
    samples: bytes | np.ndarray, chunk_samples: int
) -> None:
    with pytest.raises(ValueError):
        list(iter_pcm_chunks(samples, chunk_samples))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# AC-04: eventos JSON de control
# ---------------------------------------------------------------------------

_SESSION: UUID = uuid4()

_ALL_MESSAGES: list[Any] = [
    ConfigMessage(
        session_id=_SESSION,
        mode="rx",
        source_lang="en",
        target_lang="es",
        vad="silero-vad",
        stt="faster-whisper",
        translator="opus-mt",
        tts="kokoro",
        audio_config=AudioConfigMessage(),
    ),
    AudioConfigMessage(),
    PttMessage(active=True),
    ResetMessage(session_id=_SESSION),
    PingMessage(ts=1_700_000_000_000),
    ReadyMessage(models=["silero-vad", "faster-whisper"], session_id=_SESSION),
    SubtitleMessage(
        session_id=_SESSION,
        stream="rx",
        text="Hola, ¿cómo estás?",
        is_final=True,
        latency_ms=320,
    ),
    TranslatedMessage(text="See you tomorrow", is_final=True, lang="en"),
    DuckMessage(action="start", stream="rx"),
    TtsChunkMessage(seq=7, duration_ms=120),
    ReconnectMessage(retry_after_ms=250, session_id=_SESSION, last_seq=70_000),
    ErrorMessage(code="SCHEMA", message="magic inválido"),
    PongMessage(ts=1_700_000_000_001),
]


@pytest.mark.parametrize("message", _ALL_MESSAGES, ids=lambda m: m.type)
def test_control_message_parses_every_type(message: Any) -> None:
    raw = message.model_dump_json()
    parsed = parse_control_message(raw)
    assert type(parsed) is type(message)
    assert parsed == message
    assert parse_control_message(raw.encode("utf-8")) == message


def test_config_nested_audio_config_is_discriminated() -> None:
    payload = {
        "type": "config",
        "session_id": str(_SESSION),
        "mode": "tx",
        "source_lang": "es",
        "target_lang": "en-GB",
        "vad": "silero-vad",
        "stt": "faster-whisper",
        "translator": "opus-mt",
        "tts": "kokoro",
        "audio_config": {
            "type": "audio_config",
            "sample_rate": 16000,
            "channels": 1,
            "format": "pcm_s16le",
            "chunk_ms": 20,
        },
    }
    parsed = parse_control_message(json.dumps(payload))
    assert isinstance(parsed, ConfigMessage)
    assert parsed.mode == "tx"
    assert parsed.audio_config == AudioConfigMessage()
    assert parsed.session_id == _SESSION


def test_subtitle_message_final_flag() -> None:
    payload = {
        "type": "subtitle",
        "session_id": str(uuid4()),
        "stream": "tx",
        "text": "Nos vemos mañana",
        "is_final": True,
        "latency_ms": 420,
    }
    parsed = parse_control_message(json.dumps(payload))
    assert isinstance(parsed, SubtitleMessage)
    assert parsed.is_final is True
    assert parsed.stream == "tx"
    assert parsed.latency_ms == 420


@pytest.mark.parametrize(
    "raw",
    [
        '{"type":"pong","ts":1,"unexpected":true}',
        '{"type":"duck","action":"start","stream":"rx","level":0.12}',
        '{"type":"config","audio_config":{"type":"audio_config","typo":1}}',
    ],
)
def test_control_message_rejects_extra_fields(raw: str) -> None:
    with pytest.raises(ValueError, match="Extra"):
        parse_control_message(raw)


@pytest.mark.parametrize(
    "raw",
    [
        '{"type":"unknown","foo":1}',
        '{"type":"subtitle"}',
        '{"type":"duck","action":"pause","stream":"rx"}',
    ],
)
def test_control_message_rejects_unknown_or_invalid_payload(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_control_message(raw)


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "",
        "[]",
        "null",
        b"\xff\xfe",
    ],
)
def test_parse_control_message_rejects_invalid_json(raw: str | bytes) -> None:
    with pytest.raises(ValueError):
        parse_control_message(raw)


@pytest.mark.parametrize(
    "bad_session",
    [
        "not-a-uuid",
        "00000000-0000-1000-8000-000000000000",
    ],
)
def test_control_message_rejects_invalid_session_id(bad_session: str) -> None:
    with pytest.raises(ValueError):
        parse_control_message(
            json.dumps({"type": "reset", "session_id": bad_session})
        )
