"""Contratos intercambiables del pipeline de inferencia de BabelPipe.

Este modulo define la frontera de tipos entre etapas (VAD -> STT -> Translator
-> TTS). No contiene implementaciones: cada adaptador concreto se selecciona
por configuracion (SPEC.md REQ-04). El formato de audio al que se refieren
estos contratos es el canonico de ``shared.constants``: pcm_s16le, 16 kHz,
mono y chunks de 20 ms (320 muestras / 640 bytes).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass

__all__ = [
    "AudioChunk",
    "PipelineError",
    "Segment",
    "STT",
    "Translator",
    "TTS",
    "VAD",
]


@dataclass(frozen=True)
class AudioChunk:
    """Chunk PCM canonico ya validado en la frontera del servidor.

    ``pcm`` son 640 bytes (320 muestras int16 LE, 20 ms a 16 kHz), ``sequence``
    es el numero monotono del frame y ``timestamp_ms`` la marca de tiempo de
    captura. Los tres campos son inmutables para poder encolar el chunk sin
    copias defensivas.
    """

    pcm: bytes
    sequence: int
    timestamp_ms: int


@dataclass(frozen=True)
class Segment:
    """Unidad de trabajo que atraviesa el pipeline.

    El VAD crea el segmento con ``audio`` y ``is_final``; el STT rellena
    ``text``, el traductor lo sustituye por el texto destino y el TTS consume
    ese texto. ``id`` es correlativo dentro de la sesion y ``latency_ms``
    registra el tiempo de deteccion de la etapa que lo emitio.
    """

    id: int
    text: str = ""
    is_final: bool = False
    audio: bytes = b""
    latency_ms: int = 0


class PipelineError(Exception):
    """Error de pipeline con codigo estable para el evento JSON ``error``.

    Codigos usados por los adaptadores: ``SCHEMA`` (parametro/entrada
    invalida), ``FORMAT`` (audio fuera del contrato canonico) y
    ``MODEL_MISSING`` (falta el extra opcional ``inference``).
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class VAD(ABC):
    """Detector de actividad de voz incremental, intercambiable por config."""

    @abstractmethod
    async def process(self, chunk: AudioChunk) -> list[Segment]:
        """Consume un chunk de 20 ms y devuelve cero o mas segmentos.

        Los segmentos con ``is_final=False`` son parciales; exactamente un
        segmento ``is_final=True`` cierra cada clausula.
        """

        ...

    @abstractmethod
    async def flush(self) -> list[Segment]:
        """Cierra la clausula pendiente al terminar el stream de entrada."""

        ...

    @abstractmethod
    def reset(self) -> None:
        """Descarta todo estado de segmentacion y reinicia la numeracion."""

        ...


class STT(ABC):
    """Transcripcion incremental de audio a texto."""

    @abstractmethod
    async def transcribe(self, segment: Segment) -> Segment:
        """Devuelve una copia de ``segment`` con ``text`` transcrito."""

        ...


class Translator(ABC):
    """Traduccion de texto, intercambiable entre NMT local y LLM compatible."""

    @abstractmethod
    async def translate(self, segment: Segment, target_lang: str) -> Segment:
        """Devuelve una copia de ``segment`` con ``text`` en ``target_lang``."""

        ...


class TTS(ABC):
    """Sintesis de voz en streaming."""

    @abstractmethod
    async def synthesize_stream(self, segment: Segment) -> AsyncIterator[bytes]:
        """Itera chunks PCM int16 LE del texto de ``segment``."""

        ...
