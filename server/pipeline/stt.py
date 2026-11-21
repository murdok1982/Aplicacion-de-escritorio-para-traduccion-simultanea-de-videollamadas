"""Adaptadores STT incrementales sobre el formato canonico (REQ-04 / AC-07).

Los chunks de 20 ms que produce el VAD se acumulan en ``Segment.audio``; el STT
recibe ese Segment (contrato de ``base.STT``) o directamente PCM crudo y devuelve
el texto transcrito. Toda la decodificacion se ejecuta en un hilo de trabajo via
``asyncio.to_thread`` para no bloquear el event loop del servidor.

``FasterWhisperSTT`` y ``WhisperCppSTT`` importan su motor de forma LAZY dentro
de ``__init__``: el extra opcional ``inference`` no se instala por defecto y los
tests nunca descargan modelos. Si falta el paquete se lanza
``PipelineError(code="MODEL_MISSING")`` con una accion concreta.
``DeterministicSTT`` es el fallback de prueba: sin modelos, sin red y estable
para un mismo WAV (util para AC-07 y los tests de latencia del presupuesto).
"""

from __future__ import annotations

import asyncio
import hashlib
from abc import abstractmethod
from collections.abc import AsyncIterator
from dataclasses import replace
from importlib import import_module
from typing import Any, Literal, overload

import numpy as np

from shared.constants import BYTES_PER_SAMPLE, CHUNK_SAMPLES, PCM_FORMAT, SAMPLE_RATE

from .base import STT, PipelineError, Segment

__all__ = [
    "DeterministicSTT",
    "FasterWhisperSTT",
    "ModelSize",
    "WhisperCppSTT",
    "validate_pcm",
]

_CODE_SCHEMA = "SCHEMA"
_CODE_FORMAT = "FORMAT"
_CODE_MODEL_MISSING = "MODEL_MISSING"

_MODEL_SIZES: frozenset[str] = frozenset({"tiny", "base"})
_DEVICES: frozenset[str] = frozenset({"cpu", "cuda"})
_COMPUTE_TYPES: frozenset[str] = frozenset({"int8", "int8_float16", "float16", "float32"})
_MAX_BEAM_SIZE = 5
_INT16_SCALE = 32768.0
_SILENCE_RMS = 200.0

_MSG_MODEL_MISSING_FASTER = (
    "falta faster-whisper: instala el extra inference con `uv sync --extra inference` "
    "(o selecciona DeterministicSTT para tests)"
)
_MSG_MODEL_MISSING_CPP = (
    "falta pywhispercpp: instala `uv pip install pywhispercpp` en el entorno aislado "
    "(o selecciona DeterministicSTT para tests)"
)

ModelSize = Literal["tiny", "base"]


def validate_pcm(
    audio: bytes,
    *,
    sample_rate: int = SAMPLE_RATE,
    sample_format: str = PCM_FORMAT,
) -> None:
    """Valida la frontera de audio: pcm_s16le, 16 kHz, mono, multiplo de 20 ms.

    ``sample_format`` representa el dtype del payload; el contrato solo admite
    int16 LE, por lo que cualquier otro valor se rechaza con ``FORMAT`` antes de
    alimentar al modelo.
    """
    if not isinstance(audio, bytes):
        raise PipelineError(_CODE_SCHEMA, "audio debe ser bytes PCM inmutables")
    if sample_rate != SAMPLE_RATE:
        raise PipelineError(
            _CODE_FORMAT,
            f"sample_rate debe ser {SAMPLE_RATE}, recibido {sample_rate}",
        )
    if sample_format != PCM_FORMAT:
        raise PipelineError(
            _CODE_FORMAT,
            f"formato debe ser {PCM_FORMAT}, recibido {sample_format!r}",
        )
    if not audio:
        raise PipelineError(_CODE_FORMAT, "audio vacio: se requiere al menos 1 chunk de 20 ms")
    if len(audio) % BYTES_PER_SAMPLE:
        raise PipelineError(
            _CODE_FORMAT,
            "longitud de audio impar: int16 LE exige 2 bytes por muestra",
        )
    samples = len(audio) // BYTES_PER_SAMPLE
    if samples % CHUNK_SAMPLES:
        raise PipelineError(
            _CODE_FORMAT,
            f"longitud de {samples} muestras no es multiple de {CHUNK_SAMPLES} "
            f"(20 ms a {SAMPLE_RATE} Hz)",
        )


class _BaseSTT(STT):
    """Configuracion estricta, API asincrona y flujo incremental compartidos."""

    def __init__(
        self,
        *,
        model_size: ModelSize = "base",
        device: str = "cpu",
        compute_type: str = "int8",
        beam_size: int = 1,
        language: str | None = None,
    ) -> None:
        self._validate_config(model_size, device, compute_type, beam_size, language)
        self.model_size = model_size
        self.device = device
        self.compute_type = compute_type
        self.beam_size = beam_size
        self.language = language.lower() if language is not None else None

    @staticmethod
    def _validate_config(
        model_size: str,
        device: str,
        compute_type: str,
        beam_size: int,
        language: str | None,
    ) -> None:
        if model_size not in _MODEL_SIZES:
            raise PipelineError(
                _CODE_SCHEMA,
                f"model_size debe ser 'tiny' o 'base', recibido {model_size!r}",
            )
        if device not in _DEVICES:
            raise PipelineError(_CODE_SCHEMA, f"device no soportado: {device!r}")
        if compute_type not in _COMPUTE_TYPES:
            raise PipelineError(
                _CODE_SCHEMA,
                f"compute_type no soportado: {compute_type!r}",
            )
        if (
            not isinstance(beam_size, int)
            or isinstance(beam_size, bool)
            or not 1 <= beam_size <= _MAX_BEAM_SIZE
        ):
            raise PipelineError(
                _CODE_SCHEMA,
                f"beam_size debe estar en [1, {_MAX_BEAM_SIZE}], recibido {beam_size!r}",
            )
        _BaseSTT._validate_language(language)

    @staticmethod
    def _validate_language(language: str | None) -> None:
        if language is None:
            return
        if (
            not isinstance(language, str)
            or len(language) != 2
            or not language.isascii()
            or not language.isalpha()
        ):
            raise PipelineError(
                _CODE_SCHEMA,
                f"language debe ser ISO 639-1 de 2 letras o None, recibido {language!r}",
            )

    @overload
    async def transcribe(
        self,
        audio: Segment,
        language: str | None = None,
        *,
        sample_rate: int = SAMPLE_RATE,
        sample_format: str = PCM_FORMAT,
    ) -> Segment: ...

    @overload
    async def transcribe(
        self,
        audio: bytes,
        language: str | None = None,
        *,
        sample_rate: int = SAMPLE_RATE,
        sample_format: str = PCM_FORMAT,
    ) -> str: ...

    async def transcribe(
        self,
        audio: Any,
        language: str | None = None,
        *,
        sample_rate: int = SAMPLE_RATE,
        sample_format: str = PCM_FORMAT,
    ) -> Any:
        """Transcribe PCM crudo (``str``) o un Segment (``Segment``) con texto.

        El ``language`` explicito gana sobre el configurado en el constructor;
        ``None`` mantiene el idioma del adaptador, que a su vez puede ser
        ``None`` para autodeteccion.
        """
        if isinstance(audio, Segment):
            pcm = audio.audio
        elif isinstance(audio, bytes):
            pcm = audio
        else:
            raise PipelineError(_CODE_SCHEMA, "audio debe ser bytes o Segment")
        validate_pcm(pcm, sample_rate=sample_rate, sample_format=sample_format)

        if language is None:
            selected = self.language
        else:
            self._validate_language(language)
            selected = language.lower()
        text = await asyncio.to_thread(self._decode, pcm, selected)
        if isinstance(audio, Segment):
            return replace(audio, text=text)
        return text

    async def transcribe_stream(
        self,
        segments: AsyncIterator[Segment],
        language: str | None = None,
    ) -> AsyncIterator[Segment]:
        """Flujo incremental VAD -> STT: transforma cada Segment en orden.

        El VAD ya acumula los chunks de 20 ms en parciales y finales; decodificar
        cada Segment una sola vez evita re-transcribir el historial de la
        clausula y mantiene el presupuesto de 400 ms por etapa.
        """
        async for segment in segments:
            yield await self.transcribe(segment, language)

    @abstractmethod
    def _decode(self, pcm: bytes, language: str | None) -> str:
        """Decodificacion sincrona del PCM; se invoca siempre en un hilo aparte."""
        ...


class DeterministicSTT(_BaseSTT):
    """Fallback de prueba sin modelo: hash + RMS + duracion -> texto estable.

    El hash SHA-256 selecciona la frase del vocabulario, el RMS etiqueta el
    nivel de la senal y la duracion distingue clausulas de distintos fixtures.
    No hay estado entre llamadas, descargas ni dependencias fuera del extra
    base del proyecto.
    """

    _PHRASES: tuple[str, ...] = (
        "Buenos días, ¿cómo estás hoy?",
        "Nos vemos mañana en la estación.",
        "El tren sale a las ocho de la mañana.",
        "Por favor, repite eso un poco más despacio.",
        "Gracias, ya entendí el mensaje.",
        "La reunión empieza en cinco minutos.",
        "Hola, ¿todo bien por tu lado?",
        "Necesito traducir esta frase ahora mismo.",
    )

    def _decode(self, pcm: bytes, language: str | None) -> str:
        digest = hashlib.sha256(pcm).digest()
        phrase = self._PHRASES[int.from_bytes(digest[:4], "big") % len(self._PHRASES)]
        samples = np.frombuffer(pcm, dtype="<i2")
        rms = float(np.sqrt(np.mean(np.square(samples, dtype=np.float64))))
        level = "silencio" if rms < _SILENCE_RMS else "voz"
        duration_ms = round(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
        return f"{phrase} [{language or 'auto'} · {duration_ms} ms · {level}]"


class FasterWhisperSTT(_BaseSTT):
    """STT de produccion: faster-whisper (MIT) int8 sobre CTranslate2.

    El modelo se construye en ``__init__`` con ``beam_size=1`` por defecto para
    cumplir el presupuesto de 400 ms (SPEC.md §7). La entrada se normaliza a
    float32 [-1, 1] como exige la API de ``WhisperModel.transcribe``.
    """

    def __init__(
        self,
        *,
        model_size: ModelSize = "base",
        device: str = "cpu",
        compute_type: str = "int8",
        beam_size: int = 1,
        language: str | None = None,
    ) -> None:
        super().__init__(
            model_size=model_size,
            device=device,
            compute_type=compute_type,
            beam_size=beam_size,
            language=language,
        )
        try:
            module: Any = import_module("faster_whisper")
            factory: Any = module.WhisperModel
        except (ImportError, AttributeError) as exc:
            raise PipelineError(_CODE_MODEL_MISSING, _MSG_MODEL_MISSING_FASTER) from exc
        self._model: Any = factory(
            self.model_size,
            device=self.device,
            compute_type=self.compute_type,
        )

    def _decode(self, pcm: bytes, language: str | None) -> str:
        samples = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / _INT16_SCALE
        segments, _info = self._model.transcribe(
            samples,
            language=language,
            beam_size=self.beam_size,
        )
        return " ".join(str(getattr(seg, "text", "")).strip() for seg in segments).strip()


class WhisperCppSTT(_BaseSTT):
    """Alternativa MIT sobre whisper.cpp a traves de ``pywhispercpp``.

    whisper.cpp es CPU-only: ``device`` y ``compute_type`` se validan para que
    la configuracion sea uniforme, pero el motor gestiona su propia
    cuantizacion. El import de ``pywhispercpp.model`` tambien es lazy y falla
    con ``MODEL_MISSING`` si el binding no esta instalado.
    """

    def __init__(
        self,
        *,
        model_size: ModelSize = "base",
        device: str = "cpu",
        compute_type: str = "int8",
        beam_size: int = 1,
        language: str | None = None,
    ) -> None:
        super().__init__(
            model_size=model_size,
            device=device,
            compute_type=compute_type,
            beam_size=beam_size,
            language=language,
        )
        try:
            module: Any = import_module("pywhispercpp.model")
            factory: Any = module.Model
        except (ImportError, AttributeError) as exc:
            raise PipelineError(_CODE_MODEL_MISSING, _MSG_MODEL_MISSING_CPP) from exc
        self._model: Any = factory(self.model_size)

    def _decode(self, pcm: bytes, language: str | None) -> str:
        samples = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / _INT16_SCALE
        segments = self._model.transcribe(samples)
        return " ".join(str(getattr(seg, "text", "")).strip() for seg in segments).strip()
