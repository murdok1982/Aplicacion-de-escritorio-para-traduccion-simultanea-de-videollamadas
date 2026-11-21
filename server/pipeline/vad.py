"""Adaptadores VAD incrementales sobre chunks PCM de 20 ms.

La segmentacion NO se hace por ventanas ciegas: un unico estado de maquina
cierra la clausula cuando la actividad de voz cesa durante ``min_silence_ms``
(SPEC.md REQ-04 / AC-06,ARCHITECTURE.md §7: VAD <= 300 ms). SileroVAD usa el
modelo MIT ``silero-vad`` del extra opcional ``inference``; EnergyVAD es la
implementacion determinista de referencia, usada por los tests y como fallback
explicito. EnergyVAD es de testeo/fallback: NO reemplaza a SileroVAD en
produccion, donde las pausas y el ruido requieren el modelo neuronal.
"""

from __future__ import annotations

from abc import abstractmethod
from collections import deque
from importlib import import_module
from typing import Any

import numpy as np

from shared.constants import BYTES_PER_SAMPLE, CHUNK_BYTES, CHUNK_MS, SAMPLE_RATE

from .base import VAD, AudioChunk, PipelineError, Segment

_CODE_SCHEMA = "SCHEMA"
_CODE_FORMAT = "FORMAT"
_CODE_MODEL_MISSING = "MODEL_MISSING"
_MSG_MODEL_MISSING = "instala el extra inference"


def _rms(pcm: bytes) -> float:
    """RMS en unidades LSB int16 de un bloque PCM_s16le."""
    if not pcm:
        return 0.0
    samples = np.frombuffer(pcm, dtype="<i2")
    return float(np.sqrt(np.mean(np.square(samples, dtype=np.float64))))


class _IncrementalVAD(VAD):
    """Maquina de estados compartida por todos los VAD del pipeline.

    El estado de la clausula (buffer, actividad, silencio acumulado) vive aqui;
    cada subclase solo decide si un chunk contiene voz. Asi ambas
    implementaciones respetan ``min_silence_ms`` y emiten como maximo un
    ``is_final=True`` por clausula.
    """

    PREROLL_CHUNKS: int = 0
    PARTIAL_INTERVAL_MS: int = 100

    def __init__(
        self,
        *,
        min_silence_ms: int = 250,
        min_speech_ms: int = 120,
        sample_rate: int = SAMPLE_RATE,
    ) -> None:
        if sample_rate != SAMPLE_RATE:
            raise PipelineError(
                _CODE_FORMAT, f"sample_rate debe ser {SAMPLE_RATE}, recibido {sample_rate}"
            )
        if min_silence_ms <= 0 or min_speech_ms <= 0:
            raise PipelineError(_CODE_SCHEMA, "min_silence_ms y min_speech_ms deben ser > 0")

        self.min_silence_ms = min_silence_ms
        self.min_speech_ms = min_speech_ms
        self.sample_rate = sample_rate
        # Los umbrales no tienen por que ser multiplos de 20 ms (250 ms son
        # 12.5 chunks): se redondea al alza para no cerrar antes de tiempo.
        self._silence_limit = (min_silence_ms + CHUNK_MS - 1) // CHUNK_MS
        self._speech_required = max(1, (min_speech_ms + CHUNK_MS - 1) // CHUNK_MS)
        self._next_id = 0
        self._clear_clause()
        self._preroll: deque[bytes] = deque(maxlen=self.PREROLL_CHUNKS)

    def _clear_clause(self) -> None:
        """Cierra la clausula actual conservando la numeracion de segmentos."""
        self._buffer: list[bytes] = []
        self._open = False
        self._confirmed = False
        self._clause_ms = 0
        self._speech_chunks = 0
        self._silence_run = 0
        self._next_partial_ms = 0

    def _reset_state(self) -> None:
        self._next_id = 0
        self._clear_clause()
        self._preroll.clear()

    @abstractmethod
    def _is_speech(self, chunk: AudioChunk) -> bool:
        """True cuando el chunk contiene voz segun el criterio del adaptador."""

        ...

    def _reset_decoder(self) -> None:
        """Reinicia estado del modelo; los VAD sin modelo no hacen nada."""
        return None

    def reset(self) -> None:
        """Limpia por completo la segmentacion (nueva sesion / evento ``reset``)."""
        self._reset_state()
        self._reset_decoder()

    @staticmethod
    def _validate_chunk(chunk: AudioChunk) -> None:
        """Valida la entrada en la frontera del adaptador."""
        if not isinstance(chunk, AudioChunk):
            raise PipelineError(_CODE_SCHEMA, "chunk debe ser un AudioChunk")
        if len(chunk.pcm) != CHUNK_BYTES:
            raise PipelineError(
                _CODE_FORMAT,
                f"pcm de {len(chunk.pcm)} bytes; se esperaban {CHUNK_BYTES} "
                f"({CHUNK_MS} ms a {SAMPLE_RATE} Hz)",
            )
        if chunk.sequence < 0:
            raise PipelineError(_CODE_SCHEMA, "sequence debe ser >= 0")
        if chunk.timestamp_ms < 0:
            raise PipelineError(_CODE_SCHEMA, "timestamp_ms debe ser >= 0")

    def _segment(self, *, is_final: bool, audio: bytes, latency_ms: int) -> Segment:
        segment = Segment(
            id=self._next_id,
            audio=audio,
            is_final=is_final,
            latency_ms=latency_ms,
        )
        self._next_id += 1
        return segment

    def _partial(self) -> list[Segment]:
        """Emite un parcial cada ``PARTIAL_INTERVAL_MS`` de clausula confirmada."""
        if not self._confirmed or self._clause_ms < self._next_partial_ms:
            return []
        self._next_partial_ms = self._clause_ms + self.PARTIAL_INTERVAL_MS
        return [self._segment(is_final=False, audio=b"".join(self._buffer), latency_ms=0)]

    def _finalize(self, latency_ms: int) -> list[Segment]:
        """Cierra la clausula; recorta el silencio final de la deteccion."""
        speech_audio = b"".join(self._buffer[: len(self._buffer) - self._silence_run])
        confirmed = self._confirmed
        self._clear_clause()
        if not confirmed or not speech_audio:
            return []
        return [self._segment(is_final=True, audio=speech_audio, latency_ms=latency_ms)]

    async def process(self, chunk: AudioChunk) -> list[Segment]:
        """Consume un chunk canonico y devuelve parciales y/o un final."""
        self._validate_chunk(chunk)
        speech = self._is_speech(chunk)

        if not self._open:
            if not speech:
                if self.PREROLL_CHUNKS:
                    self._preroll.append(chunk.pcm)
                return []
            self._buffer.extend(self._preroll)
            self._preroll.clear()
            self._buffer.append(chunk.pcm)
            self._open = True
            self._clause_ms = len(self._buffer) // CHUNK_BYTES * CHUNK_MS
            self._speech_chunks = 1
            self._silence_run = 0
            self._next_partial_ms = 0
        else:
            self._buffer.append(chunk.pcm)
            self._clause_ms += CHUNK_MS
            if speech:
                self._speech_chunks += 1
                self._silence_run = 0
            else:
                self._silence_run += 1

        if not self._confirmed and self._speech_chunks >= self._speech_required:
            self._confirmed = True
            self._next_partial_ms = self._clause_ms

        if self._silence_run >= self._silence_limit:
            return self._finalize(self._silence_run * CHUNK_MS)

        return self._partial()

    async def flush(self) -> list[Segment]:
        """Cierra la clausula pendiente al agotarse la entrada (sin silencio)."""
        if not self._open or not self._buffer:
            return []
        return self._finalize(0)


class EnergyVAD(_IncrementalVAD):
    """VAD determinista de referencia basado en RMS con hysteresis.

    Es de testeo/fallback explicito: no reemplaza a SileroVAD en produccion.
    El umbral de entrada (200 LSB ~ -44 dBFS) evita arrancar con ruido de fondo
    y el de salida (120 LSB) mantiene la clausula abierta durante las pausas
    breves entre silabas. La cola de silencio de 250 ms la gestiona la base.
    """

    SPEECH_ON_RMS = 200.0
    SPEECH_OFF_RMS = 120.0

    def _is_speech(self, chunk: AudioChunk) -> bool:
        threshold = self.SPEECH_OFF_RMS if self._open else self.SPEECH_ON_RMS
        return _rms(chunk.pcm) >= threshold


class SileroVAD(_IncrementalVAD):
    """VAD de produccion: modelo Silero (MIT) sobre ventanas de 512 muestras.

    El extra ``inference`` no se instala por defecto: el modelo y ``torch`` se
    importan de forma LAZY dentro de ``__init__``. Si faltan se lanza
    ``PipelineError(code="MODEL_MISSING")`` con una accion concreta, sin
    descargas de red: ``silero-vad>=5.1`` trae el JIT empaquetado en el wheel.

    Los chunks canoncos de 320 muestras se acumulan en ventanas de 512 (32 ms)
    porque el modelo solo acepta ese tamano a 16 kHz. La probabilidad resultante
    alimenta la misma maquina de estados que EnergyVAD, con hysteresis entre
    ``threshold`` y ``threshold - 0.15``. Se conservan dos chunks de pre-roll
    para no perder el ataque de la voz mientras la primera ventana se llena.
    """

    WINDOW_SAMPLES = 512
    PREROLL_CHUNKS = 2

    def __init__(
        self,
        *,
        threshold: float = 0.5,
        min_silence_ms: int = 250,
        min_speech_ms: int = 120,
        sample_rate: int = SAMPLE_RATE,
    ) -> None:
        super().__init__(
            min_silence_ms=min_silence_ms,
            min_speech_ms=min_speech_ms,
            sample_rate=sample_rate,
        )
        if not 0.0 < threshold < 1.0:
            raise PipelineError(_CODE_SCHEMA, "threshold debe estar en (0, 1)")

        # importlib evita que mypy exija stubs de torch cuando el extra no
        # esta instalado; el nombre se resuelve solo al construir el adaptador.
        try:
            silero: Any = import_module("silero_vad")
            torch: Any = import_module("torch")
            load_silero_vad = silero.load_silero_vad
        except ImportError as exc:
            raise PipelineError(_CODE_MODEL_MISSING, _MSG_MODEL_MISSING) from exc

        self._torch = torch
        self._model = load_silero_vad()
        self.threshold = threshold
        self._neg_threshold = max(threshold - 0.15, 0.01)
        self._window = bytearray()
        self._prob = 0.0

    def _reset_decoder(self) -> None:
        self._window.clear()
        self._prob = 0.0
        self._model.reset_states()

    def _is_speech(self, chunk: AudioChunk) -> bool:
        self._window.extend(chunk.pcm)
        window_bytes = self.WINDOW_SAMPLES * BYTES_PER_SAMPLE
        while len(self._window) >= window_bytes:
            frame = bytes(self._window[:window_bytes])
            del self._window[:window_bytes]
            samples = np.frombuffer(frame, dtype="<i2").astype(np.float32) / 32768.0
            tensor = self._torch.from_numpy(samples)
            with self._torch.no_grad():
                self._prob = float(self._model(tensor, self.sample_rate).item())

        threshold = self._neg_threshold if self._open else self.threshold
        return self._prob >= threshold
