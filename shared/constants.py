"""Constantes canónicas del contrato BabelPipe.

Esta es la ÚNICA fuente de verdad para el formato de audio y la cabecera
binaria del protocolo (SPEC.md §REQ-03, ARCHITECTURE.md §4.2 y ADR-005).
Cliente y servidor deben importar de aquí, nunca redeclarar estos valores.
"""

from enum import IntEnum
from struct import calcsize

# Formato de audio canónico en el pipeline: 16 kHz, mono, int16 LE.
SAMPLE_RATE: int = 16_000
PIPEWIRE_SAMPLE_RATE: int = 48_000
CHUNK_MS: int = 20
CHUNK_SAMPLES: int = 320
BYTES_PER_SAMPLE: int = 2
CHUNK_BYTES: int = 640
PCM_FORMAT: str = "pcm_s16le"

# Cabecera de 16 bytes: !2sBBHIIH (ver ARCHITECTURE.md §4.2).
MAGIC: bytes = b"BP"
VERSION: int = 1
HEADER_STRUCT: str = "!2sBBHIIH"
HEADER_SIZE: int = 16
MAX_FRAME_BYTES: int = 8192

# Identificadores de stream: 1 = Rx (doblaje), 2 = Tx (micrófono).
STREAM_RX: int = 1
STREAM_TX: int = 2


class StreamId(IntEnum):
    """Streams multiplexados sobre el único WebSocket de traducción."""

    RX = STREAM_RX
    TX = STREAM_TX


STREAM_IDS: frozenset[int] = frozenset({STREAM_RX, STREAM_TX})

if calcsize(HEADER_STRUCT) != HEADER_SIZE:  # pragma: no cover - guard de contrato
    raise RuntimeError("HEADER_STRUCT no coincide con HEADER_SIZE")
