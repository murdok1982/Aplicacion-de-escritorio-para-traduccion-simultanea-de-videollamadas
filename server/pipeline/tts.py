"""Adaptadores TTS en streaming (REQ-04 / AC-07 / SPEC.md §6 / ADR-004).

Tres implementaciones de :class:`server.pipeline.base.TTS`:

* :class:`KokoroTTS` — Kokoro-82M (Apache-2.0). El motor ``kokoro`` y el G2P
  ``misaki`` (MIT) se importan de forma LAZY en ``__init__``. La fonetizacion
  se hace SIEMPRE con Misaki y ``fallback=None``: el unico fallback espeak de
  Misaki es opt-in y nunca se construye aqui (espeak-ng/phonemizer son
  GPL-3.0, prohibidos por ADR-004). Las fonemas resultantes se entregan a
  Kokoro con ``generate_from_tokens`` para que su KPipeline no ejecute su
  propio G2P espeak para los lang_code latinos.
* :class:`Qwen3TTS` — alternativa Apache-2.0 con la misma interfaz e imports
  lazy. Si el paquete del motor no esta instalado se lanza ``MODEL_MISSING``.
* :class:`DeterministicTTS` — fallback de prueba sin modelos, sin red y sin
  descargas: deriva audio del fixture ``voice_es_16k.wav`` (o sintetiza un
  tono con numpy si el archivo no existe) y emite chunks canonicos estables.

Todos los adaptadores entregan PCM ``pcm_s16le`` a 16 kHz mono en chunks de
exactamente 640 bytes (320 muestras / 20 ms, ADR-005) y ejecutan el trabajo
CPU-bound (G2P + inferencia + resampleo) en ``asyncio.to_thread`` para no
bloquear el event loop del servidor. El guard de licencias de SPEC.md §6
rechaza cualquier ``voice`` o ``model`` que contenga xtts/f5/piper/espeak.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import wave
from abc import abstractmethod
from collections.abc import AsyncIterator, Callable
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any

import numpy as np
import soxr  # type: ignore[import-untyped]
from numpy.typing import NDArray

from shared.constants import BYTES_PER_SAMPLE, CHUNK_SAMPLES, SAMPLE_RATE
from shared.protocol import iter_pcm_chunks

from .base import TTS, PipelineError, Segment

__all__ = [
    "DeterministicTTS",
    "KokoroTTS",
    "Qwen3TTS",
    "configure_g2p",
    "validate_voice",
]

_CODE_SCHEMA = "SCHEMA"
_CODE_FORMAT = "FORMAT"
_CODE_MODEL_MISSING = "MODEL_MISSING"
_CODE_LICENSE = "LICENSE_VIOLATION"
_CODE_MODEL_ERROR = "MODEL_ERROR"

# Kokoro y Qwen3-TTS generan audio a 24 kHz; el contrato BabelPipe es 16 kHz.
_MODEL_SAMPLE_RATE = 24_000
_DEFAULT_SPEED = 1.0
_MAX_SPEED = 4.0
_LANG_RE = re.compile(r"[a-z]{2,3}\Z")
_INT16_DIVISOR = 32768.0
_INT16_SCALE = 32767.0

# SPEC.md §6 / ADR-004: cada token prohibido lleva su motivo legal exacto.
_FORBIDDEN_TOKENS: dict[str, str] = {
    "xtts": "XTTS v2 usa CPML (no comercial) y esta prohibido por restriccion explicita",
    "f5": "F5-TTS usa CC-BY-NC y esta prohibido por restriccion explicita",
    "piper": "Piper usa GPL-3.0 (copyleft viral) y esta prohibido en el core",
    "espeak": "espeak-ng/phonemizer usa GPL-3.0: usar Misaki (MIT) [ADR-004]",
}
_LICENSE_MESSAGE = (
    "licencia no permitida en el core de BabelPipe (SPEC.md §6 / ADR-004): "
    "usa Kokoro-82M o Qwen3-TTS (Apache-2.0) con G2P Misaki (MIT)"
)

_MSG_MISSING_KOKORO = (
    "faltan kokoro/misaki: instala el extra inference con "
    "`uv sync --extra inference` (o selecciona DeterministicTTS para tests)"
)
_MSG_MISSING_QWEN = (
    "falta el motor Qwen3-TTS: instala el paquete del modelo en el entorno aislado "
    "con `uv pip install qwen-tts` (o selecciona DeterministicTTS para tests)"
)
_MSG_MISSING_MISAKI = (
    "falta misaki (MIT): instala el extra inference con `uv sync --extra inference`; "
    "nunca uses espeak-ng/phonemizer (GPL-3.0, ADR-004)"
)

# Submodulo/clase de Misaki por idioma. El resto usa el G2P ingles, que es el
# que Kokoro emplea tambien para espanol dentro de su alfabeto fonetico.
_MISAKI_G2P: dict[str, tuple[str, str]] = {
    "ja": ("misaki.ja", "JAG2P"),
    "zh": ("misaki.zh", "ZHG2P"),
    "ko": ("misaki.ko", "KOG2P"),
}

# Voice por defecto de cada motor. Ninguno contiene tokens prohibidos.
_KOKORO_VOICE = "af_heart"
_QWEN_VOICE = "cherry"


def validate_voice(name: str) -> None:
    """Guard de licencias para ``voice`` y ``model`` (SPEC.md §6 / ADR-004)."""
    if not isinstance(name, str) or not name.strip():
        raise PipelineError(_CODE_SCHEMA, "voice/model debe ser un nombre no vacio")
    lowered = name.lower()
    for token, reason in _FORBIDDEN_TOKENS.items():
        if token in lowered:
            raise PipelineError(
                _CODE_LICENSE,
                f"nombre {name!r} contiene {token!r}: {reason}; {_LICENSE_MESSAGE}",
            )


def _validate_lang(lang: str) -> str:
    """Valida un codigo ISO 639-1/639-2 y lo normaliza."""
    if not isinstance(lang, str):
        raise PipelineError(_CODE_SCHEMA, "lang debe ser una cadena ISO 639-1")
    normalized = lang.strip().lower()
    if _LANG_RE.fullmatch(normalized) is None:
        raise PipelineError(
            _CODE_SCHEMA,
            f"lang debe ser ISO 639-1/639-2 de 2 o 3 letras, recibido {lang!r}",
        )
    return normalized


def _validate_speed(speed: float) -> float:
    """Rango estricto de velocidad de habla."""
    if isinstance(speed, bool) or not isinstance(speed, int | float):
        raise PipelineError(_CODE_SCHEMA, "speed debe ser un numero")
    value = float(speed)
    if not 0.0 < value <= _MAX_SPEED:
        raise PipelineError(
            _CODE_SCHEMA, f"speed debe estar en (0, {_MAX_SPEED:.1f}], recibido {speed!r}"
        )
    return value


def configure_g2p(lang: str, *, module: Any | None = None) -> Callable[[str], str]:
    """Construye el G2P de Misaki (MIT) sin backend espeak (ADR-004).

    El modulo real se importa de forma LAZY; ``module`` permite inyectar un
    modulo falso en tests. Para ingles (y espanol) se usa
    ``misaki.en.G2P(fallback=None)``: el unico mecanismo espeak de Misaki es
    ``espeak.EspeakFallback`` como ``fallback``, y aqui nunca se instancia. El
    callable devuelto normaliza la tupla ``(phonemes, tokens)`` de Misaki a una
    cadena de fonemas.
    """
    normalized = _validate_lang(lang)
    submodule, class_name = _MISAKI_G2P.get(normalized, ("misaki.en", "G2P"))
    try:
        misaki_sub = module if module is not None else import_module(submodule)
        factory: Any = getattr(misaki_sub, class_name)
    except (ImportError, AttributeError) as exc:
        raise PipelineError(_CODE_MODEL_MISSING, _MSG_MISSING_MISAKI) from exc
    try:
        # `fallback=None` es la configuracion explicita SIN espeak.
        g2p: Any = factory(fallback=None) if class_name == "G2P" else factory()
    except TypeError as exc:
        raise PipelineError(
            _CODE_SCHEMA, "la API de G2P de misaki no es compatible con esta version"
        ) from exc

    def phonemize(text: str) -> str:
        result = g2p(text)
        if isinstance(result, tuple):
            result = result[0]
        if isinstance(result, list | tuple):
            return " ".join(str(item) for item in result)
        return str(result)

    return phonemize


def _fallback_phonemes(text: str) -> str:
    """Fonetizacion determinista de prueba: no es un modelo y no usa espeak."""
    return " ".join(f"/{ord(char):04x}/" for char in text.lower() if char.isalnum())


def _audio_to_numpy(audio: Any) -> NDArray[Any]:
    """Normaliza el audio del motor (ndarray o tensor tipo torch) a ndarray."""
    if isinstance(audio, np.ndarray):
        return audio
    numpy_fn = getattr(audio, "numpy", None)
    if callable(numpy_fn):
        detached = getattr(audio, "detach", None)
        if callable(detached):
            audio = detached()
        cpu = getattr(audio, "cpu", None)
        if callable(cpu):
            audio = cpu()
        return np.asarray(numpy_fn())
    return np.asarray(audio)


def _extract_audio(result: Any) -> NDArray[Any]:
    """Extrae el array de audio de la forma de respuesta del motor TTS."""
    if isinstance(result, np.ndarray):
        return result
    if isinstance(result, dict):
        for key in ("audio", "waveform", "wav"):
            if key in result and result[key] is not None:
                return _audio_to_numpy(result[key])
        raise PipelineError(_CODE_MODEL_ERROR, "respuesta TTS sin audio")
    if isinstance(result, list | tuple):
        for item in result:
            if isinstance(item, np.ndarray):
                return item
            audio = getattr(item, "audio", None)
            if audio is not None:
                return _audio_to_numpy(audio)
        raise PipelineError(_CODE_MODEL_ERROR, "respuesta TTS sin audio")
    for attr in ("audio", "waveform", "wav"):
        value = getattr(result, attr, None)
        if value is not None:
            return _audio_to_numpy(value)
    raise PipelineError(_CODE_MODEL_ERROR, "respuesta TTS sin audio")


def _samples_to_pcm(samples: NDArray[Any] | Any, source_rate: int) -> bytes:
    """Convierte muestras del motor a ``pcm_s16le`` 16 kHz con padding canonico."""
    array = np.asarray(samples)
    if array.ndim == 0:
        raise PipelineError(_CODE_FORMAT, "el TTS devolvio un escalar en lugar de audio")
    if array.ndim > 1:
        array = np.ravel(array)  # salida mono: se aplana cualquier forma [frame, canal]
    if array.size == 0:
        return b""
    if np.issubdtype(array.dtype, np.integer):
        source = array.astype(np.float32) / _INT16_DIVISOR
    else:
        source = array.astype(np.float32)
    if not bool(np.all(np.isfinite(source))):
        raise PipelineError(_CODE_FORMAT, "el TTS devolvio muestras no finitas")

    if source_rate == SAMPLE_RATE:
        floats = source
    else:
        resampled = soxr.resample(source, source_rate, SAMPLE_RATE, quality="HQ")
        floats = np.asarray(resampled, dtype=np.float32)

    clipped = np.clip(floats, -1.0, 1.0)
    remainder = clipped.size % CHUNK_SAMPLES
    if remainder:
        clipped = np.pad(clipped, (0, CHUNK_SAMPLES - remainder), constant_values=0.0)
    int16: NDArray[np.int16] = np.rint(clipped * _INT16_SCALE).astype("<i2")
    return bytes(int16.tobytes())


@lru_cache(maxsize=1)
def _fixture_samples() -> NDArray[np.float32]:
    """Devuelve la voz del fixture 16 kHz, o un tono sintetico si no existe."""
    fixture = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "voice_es_16k.wav"
    if fixture.is_file():
        with wave.open(str(fixture), "rb") as handle:
            valid = (
                handle.getnchannels() == 1
                and handle.getsampwidth() == BYTES_PER_SAMPLE
                and handle.getframerate() == SAMPLE_RATE
            )
            if valid:
                pcm = handle.readframes(handle.getnframes())
                if pcm:
                    ints = np.frombuffer(pcm, dtype="<i2")
                    return (ints.astype(np.float32) / _INT16_DIVISOR).copy()
    # Sin fixture: tono determinista de 0.5 s generado solo con numpy.
    timeline = np.arange(SAMPLE_RATE // 2, dtype=np.float32) / SAMPLE_RATE
    return (0.2 * np.sin(2.0 * np.pi * 180.0 * timeline)).astype(np.float32)


class _BaseTTS(TTS):
    """API asincrona, validacion de frontera y formato canonico compartidos."""

    model_sample_rate: int = _MODEL_SAMPLE_RATE

    def __init__(
        self,
        *,
        voice: str,
        speed: float,
        model: str,
        g2p: Callable[[str], str] | None = None,
    ) -> None:
        validate_voice(voice)
        validate_voice(model)
        self.voice = voice
        self.speed = _validate_speed(speed)
        self.model = model
        self._g2p = g2p

    # Acepta tanto el contrato de base.TTS (Segment) como la API preferida por
    # REQ-04 (texto + idioma). El ignore de override es necesario porque el
    # parametro canonico de la base se llama ``segment`` y aqui es ``text``.
    async def synthesize_stream(  # type: ignore[override]
        self, text: str | Segment, lang: str = "es"
    ) -> AsyncIterator[bytes]:
        """Emite chunks de 640 bytes; el primero sale sin retrasos artificiales."""
        if isinstance(text, Segment):
            raw = text.text
        elif isinstance(text, str):
            raw = text
        else:
            raise PipelineError(_CODE_SCHEMA, "text debe ser str o Segment")
        normalized = _validate_lang(lang)
        if not raw.strip():
            return

        phonemes = self._g2p(raw) if self._g2p is not None else _fallback_phonemes(raw)
        samples = await asyncio.to_thread(
            self._synthesize_samples, raw, str(phonemes), normalized
        )
        pcm = _samples_to_pcm(samples, self.model_sample_rate)
        for chunk in iter_pcm_chunks(pcm):
            yield chunk

    async def synthesize(self, text: str, lang: str = "es") -> bytes:
        """Concatena el stream completo en un unico buffer PCM canonico."""
        return b"".join([chunk async for chunk in self.synthesize_stream(text, lang)])

    @abstractmethod
    def _synthesize_samples(self, text: str, phonemes: str, lang: str) -> NDArray[Any]:
        """Inferencia sincrona; se ejecuta siempre en un hilo de trabajo."""
        ...


class DeterministicTTS(_BaseTTS):
    """TTS de prueba sin modelos ni red: audio estable derivado del fixture.

    Cada texto (mas idioma, voz y velocidad) selecciona de forma determinista
    una ventana del fixture y una ganancia, de modo que el mismo texto siempre
    produce los mismos bytes. No importa motores pesados y nunca accede a red.
    """

    model_sample_rate = SAMPLE_RATE
    MIN_CHUNKS = 2
    MAX_CHUNKS = 6

    def __init__(
        self,
        *,
        voice: str = "es-deterministic",
        speed: float = _DEFAULT_SPEED,
        model: str = "deterministic",
        g2p: Callable[[str], str] | None = None,
    ) -> None:
        super().__init__(voice=voice, speed=speed, model=model, g2p=g2p)

    def _synthesize_samples(self, text: str, phonemes: str, lang: str) -> NDArray[Any]:
        seed = _fixture_samples()
        key = "\x00".join((text, phonemes, lang, self.voice, str(self.speed)))
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
        chunk_count = self.MIN_CHUNKS + int(digest[4]) % (
            self.MAX_CHUNKS - self.MIN_CHUNKS + 1
        )
        needed = chunk_count * CHUNK_SAMPLES
        if seed.size >= needed:
            offset = int.from_bytes(digest[:4], "big") % (seed.size - needed + 1)
            window = seed[offset : offset + needed].copy()
        else:
            repeats = (needed + seed.size - 1) // seed.size
            window = np.tile(seed, repeats)[:needed].copy()
        gain = 0.55 + (int(digest[5]) % 10) / 100.0
        return (window * gain).astype(np.float32)


class KokoroTTS(_BaseTTS):
    """TTS de produccion: Kokoro-82M (Apache-2.0) con G2P Misaki (MIT).

    ``kokoro`` y ``misaki`` se importan de forma LAZY en ``__init__``; sin el
    extra ``inference`` se lanza ``MODEL_MISSING`` con una accion concreta y no
    se descarga nada. La inferencia corre en un hilo aparte y se consume el
    generador nativo de Kokoro, de modo que el audio se produce por bloques y
    el primer chunk canonico se emite en cuanto esta disponible.
    """

    model_sample_rate = _MODEL_SAMPLE_RATE

    def __init__(
        self,
        *,
        voice: str = _KOKORO_VOICE,
        speed: float = _DEFAULT_SPEED,
        device: str = "cpu",
        model: str = "kokoro",
        g2p: Callable[[str], str] | None = None,
        pipeline: Any | None = None,
    ) -> None:
        super().__init__(voice=voice, speed=speed, model=model, g2p=g2p)
        # Misaki (MIT) es obligatorio en los adaptadores de produccion.
        if self._g2p is None:
            self._g2p = configure_g2p("en")
        if pipeline is not None:
            self._pipeline = pipeline
            return
        if device not in {"cpu", "cuda"}:
            raise PipelineError(_CODE_SCHEMA, f"device no soportado: {device!r}")
        try:
            module: Any = import_module("kokoro")
            factory: Any = module.KPipeline
        except (ImportError, AttributeError) as exc:
            raise PipelineError(_CODE_MODEL_MISSING, _MSG_MISSING_KOKORO) from exc
        # El G2P del KPipeline no se utiliza: recibimos fonemas de Misaki y el
        # lang_code solo calibra la carga de voces/segmentacion del modelo.
        self._pipeline = factory(lang_code="a", device=device, model=True)

    def _synthesize_samples(self, text: str, phonemes: str, lang: str) -> NDArray[Any]:
        if not phonemes.strip():
            return np.zeros(0, dtype=np.float32)
        blocks: list[NDArray[Any]] = []
        try:
            results = self._pipeline.generate_from_tokens(
                phonemes, voice=self.voice, speed=self.speed
            )
            for result in results:
                audio = getattr(result, "audio", None)
                if audio is None:
                    continue
                blocks.append(_audio_to_numpy(audio))
        except PipelineError:
            raise
        except Exception as exc:
            raise PipelineError(
                _CODE_MODEL_ERROR, "fallo de inference Kokoro (sin detalles del modelo)"
            ) from exc
        if not blocks:
            raise PipelineError(_CODE_MODEL_ERROR, "Kokoro no devolvio audio")
        return np.concatenate(blocks)


class Qwen3TTS(_BaseTTS):
    """Alternativa Apache-2.0 con la misma interfaz e imports lazy.

    Se prueba ``qwen_tts`` y ``qwen3_tts`` como nombres de paquete del motor;
    si ninguno esta disponible se lanza ``MODEL_MISSING``. El G2P vuelve a ser
    Misaki sin espeak y la salida se normaliza a 16 kHz int16 mono.
    """

    model_sample_rate = _MODEL_SAMPLE_RATE
    MODULE_NAMES: tuple[str, ...] = ("qwen_tts", "qwen3_tts")

    def __init__(
        self,
        *,
        voice: str = _QWEN_VOICE,
        speed: float = _DEFAULT_SPEED,
        model: str = "qwen3-tts",
        model_path: str | None = None,
        g2p: Callable[[str], str] | None = None,
        engine: Any | None = None,
    ) -> None:
        super().__init__(voice=voice, speed=speed, model=model, g2p=g2p)
        if self._g2p is None:
            self._g2p = configure_g2p("en")
        if engine is not None:
            self._engine = engine
            return

        module: Any | None = None
        for name in self.MODULE_NAMES:
            try:
                module = import_module(name)
                break
            except ImportError:
                continue
        if module is None:
            raise PipelineError(_CODE_MODEL_MISSING, _MSG_MISSING_QWEN)
        factory: Any = getattr(module, "Qwen3TTS", None) or getattr(module, "QwenTTS", None)
        if factory is None:
            raise PipelineError(_CODE_MODEL_MISSING, _MSG_MISSING_QWEN)
        options: dict[str, Any] = {"model_path": model_path} if model_path else {}
        self._engine = factory(**options)

    def _synthesize_samples(self, text: str, phonemes: str, lang: str) -> NDArray[Any]:
        try:
            generate = getattr(self._engine, "generate", None)
            if not callable(generate):
                generate = getattr(self._engine, "synthesize", None)
            if not callable(generate):
                raise PipelineError(_CODE_MODEL_ERROR, "el motor Qwen3-TTS no es generativo")
            result = generate(text=text, voice=self.voice, speed=self.speed, language=lang)
        except PipelineError:
            raise
        except Exception as exc:
            raise PipelineError(
                _CODE_MODEL_ERROR, "fallo de inference Qwen3-TTS (sin detalles del modelo)"
            ) from exc
        return _extract_audio(result)
