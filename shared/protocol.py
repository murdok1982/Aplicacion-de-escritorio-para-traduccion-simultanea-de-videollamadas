"""Contrato binario canónico del protocolo BabelPipe.

Cabecera de 16 bytes ``!2sBBHIIH`` seguida de PCM int16 LE mono. Todo frame
que entre al sistema (WebSocket, tests, fixtures) debe pasar por
:func:`decode_frame`; todo frame que salga debe construirse con
:func:`encode_frame`. Ver ARCHITECTURE.md §4.2 y SPEC.md §REQ-03/AC-04.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum

import numpy as np
from numpy.typing import NDArray

from .constants import (
    BYTES_PER_SAMPLE,
    CHUNK_SAMPLES,
    HEADER_SIZE,
    HEADER_STRUCT,
    MAGIC,
    MAX_FRAME_BYTES,
    SAMPLE_RATE,
    STREAM_IDS,
    VERSION,
)

_HEADER = struct.Struct(HEADER_STRUCT)
_UINT8_MAX = 0xFF
_UINT16_MAX = 0xFFFF
_UINT32_MAX = 0xFFFFFFFF

_CODE_SCHEMA = "SCHEMA"
_CODE_FORMAT = "FORMAT"


class ProtocolError(Exception):
    """Error de protocolo con código estable para el evento JSON ``error``.

    ``code`` es ``SCHEMA`` para estructura/magic/versión/payload inválidos y
    ``FORMAT`` para un sample rate distinto de 16 kHz (SPEC.md §5).
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Flags(IntEnum):
    """Bits de la cabecera (offset 3, uint8)."""

    FINAL = 1
    DUCK_START = 2
    DUCK_STOP = 4
    PTT_ACTIVE = 8


@dataclass(frozen=True, slots=True)
class AudioFrame:
    """Frame PCM inmutable ya validado, listo para encode/decode."""

    stream_id: int
    sequence: int
    flags: int
    sample_rate: int
    num_samples: int
    pcm: bytes

    def __post_init__(self) -> None:
        # Las mismas invariantes se comprueban en encode/decode para poder
        # devolver ProtocolError con código; aquí protegen la construcción
        # directa desde cualquier otro módulo.
        if self.stream_id not in STREAM_IDS:
            raise ValueError(f"stream_id inválido: {self.stream_id}")
        if not 0 <= self.sequence <= _UINT32_MAX:
            raise ValueError(f"sequence fuera de uint32: {self.sequence}")
        if not 0 <= self.flags <= _UINT8_MAX:
            raise ValueError(f"flags fuera de uint8: {self.flags}")
        if self.sample_rate != SAMPLE_RATE:
            raise ValueError(f"sample_rate debe ser {SAMPLE_RATE}: {self.sample_rate}")
        if not 0 < self.num_samples <= _UINT16_MAX:
            raise ValueError(f"num_samples fuera de rango: {self.num_samples}")
        if len(self.pcm) != self.num_samples * BYTES_PER_SAMPLE:
            raise ValueError(
                f"pcm de {len(self.pcm)} bytes no coincide con "
                f"{self.num_samples} muestras"
            )

    @property
    def duration_ms(self) -> float:
        """Duración del frame en milisegundos (320 muestras = 20 ms)."""
        return self.num_samples * 1000.0 / self.sample_rate

    @property
    def is_final(self) -> bool:
        """Indica si el frame cierra la cláusula (bit FINAL)."""
        return bool(self.flags & Flags.FINAL)


def encode_frame(
    *,
    stream_id: int,
    sequence: int,
    flags: int = 0,
    pcm: bytes,
    sample_rate: int = SAMPLE_RATE,
    num_samples: int | None = None,
) -> bytes:
    """Serializa un frame PCM con la cabecera canónica de 16 bytes.

    Si ``num_samples`` es ``None`` se deriva de ``len(pcm) // 2``.
    """
    if not isinstance(pcm, bytes | bytearray | memoryview):
        raise ProtocolError(_CODE_SCHEMA, "pcm debe ser bytes")
    pcm_bytes = bytes(pcm)

    if sample_rate != SAMPLE_RATE:
        raise ProtocolError(
            _CODE_FORMAT, f"sample_rate debe ser {SAMPLE_RATE}, recibido {sample_rate}"
        )
    if stream_id not in STREAM_IDS:
        raise ProtocolError(_CODE_SCHEMA, f"stream_id inválido: {stream_id}")
    if not 0 <= sequence <= _UINT32_MAX:
        raise ProtocolError(_CODE_SCHEMA, f"sequence fuera de uint32: {sequence}")
    if not 0 <= flags <= _UINT8_MAX:
        raise ProtocolError(_CODE_SCHEMA, f"flags fuera de uint8: {flags}")

    derived = len(pcm_bytes) // BYTES_PER_SAMPLE
    if len(pcm_bytes) % BYTES_PER_SAMPLE != 0:
        raise ProtocolError(_CODE_SCHEMA, "pcm debe tener un número par de bytes")
    if num_samples is None:
        num_samples = derived
    elif num_samples != derived:
        raise ProtocolError(
            _CODE_SCHEMA,
            f"num_samples={num_samples} no coincide con {derived} muestras en pcm",
        )
    if not 0 < num_samples <= _UINT16_MAX:
        raise ProtocolError(_CODE_SCHEMA, f"num_samples fuera de rango: {num_samples}")
    if HEADER_SIZE + len(pcm_bytes) > MAX_FRAME_BYTES:
        raise ProtocolError(
            _CODE_SCHEMA, f"frame excede MAX_FRAME_BYTES ({MAX_FRAME_BYTES})"
        )

    header = _HEADER.pack(
        MAGIC, VERSION, flags, stream_id, sequence, sample_rate, num_samples
    )
    return header + pcm_bytes


def decode_frame(data: bytes) -> AudioFrame:
    """Valida y deserializa un frame completo (cabecera + payload)."""
    if not isinstance(data, bytes | bytearray | memoryview):
        raise ProtocolError(_CODE_SCHEMA, "frame debe ser bytes")
    frame = bytes(data)

    if len(frame) < HEADER_SIZE:
        raise ProtocolError(
            _CODE_SCHEMA,
            f"frame de {len(frame)} bytes < HEADER_SIZE ({HEADER_SIZE})",
        )
    if len(frame) > MAX_FRAME_BYTES:
        raise ProtocolError(
            _CODE_SCHEMA, f"frame excede MAX_FRAME_BYTES ({MAX_FRAME_BYTES})"
        )

    magic, version, flags, stream_id, sequence, sample_rate, num_samples = (
        _HEADER.unpack_from(frame, 0)
    )
    if magic != MAGIC:
        raise ProtocolError(_CODE_SCHEMA, f"magic inválido: {magic!r}")
    if version != VERSION:
        raise ProtocolError(_CODE_SCHEMA, f"versión inválida: {version}")
    if stream_id not in STREAM_IDS:
        raise ProtocolError(_CODE_SCHEMA, f"stream_id inválido: {stream_id}")
    if sample_rate != SAMPLE_RATE:
        raise ProtocolError(
            _CODE_FORMAT, f"sample_rate debe ser {SAMPLE_RATE}, recibido {sample_rate}"
        )
    if num_samples <= 0:
        raise ProtocolError(_CODE_SCHEMA, "num_samples debe ser > 0")

    pcm = frame[HEADER_SIZE:]
    expected = num_samples * BYTES_PER_SAMPLE
    if len(pcm) != expected:
        raise ProtocolError(
            _CODE_SCHEMA,
            f"payload de {len(pcm)} bytes, se esperaban {expected}",
        )

    return AudioFrame(
        stream_id=stream_id,
        sequence=sequence,
        flags=flags,
        sample_rate=sample_rate,
        num_samples=num_samples,
        pcm=pcm,
    )


def iter_pcm_chunks(
    samples: bytes | NDArray[np.int16],
    chunk_samples: int = CHUNK_SAMPLES,
) -> Iterator[bytes]:
    """Divide PCM int16 LE en chunks completos de ``chunk_samples``.

    Acepta ``bytes`` o un array numpy 1-D de dtype ``int16``. El resto
    incompleto al final se descarta para no emitir frames de duración no
    canónica. La validación se ejecuta al iniciar la iteración.
    """
    if chunk_samples <= 0:
        raise ValueError("chunk_samples debe ser > 0")
    if isinstance(samples, np.ndarray):
        if samples.dtype != np.dtype(np.int16):
            raise ValueError("el array numpy debe tener dtype int16")
        if samples.ndim != 1:
            raise ValueError("el array numpy debe ser unidimensional")
        raw = samples.tobytes()
    else:
        raw = bytes(samples)

    stride = chunk_samples * BYTES_PER_SAMPLE
    usable = len(raw) - (len(raw) % stride)
    for offset in range(0, usable, stride):
        yield raw[offset : offset + stride]
