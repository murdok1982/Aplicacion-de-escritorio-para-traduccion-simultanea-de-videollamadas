"""Resampleo explícito y determinista 48 kHz ↔ 16 kHz del pipeline BabelPipe.

Contrato (SPEC.md §REQ-02 / AC-03, ARCHITECTURE.md §7 ADR-005 y §1):

* La captura de PipeWire llega como float32 (o int16) a 48 kHz y se convierte
  a ``pcm_s16le`` mono a 16 kHz.
* El TTS produce 16 kHz y se devuelve a 48 kHz para inyectarlo en el grafo.
* El formato canónico vive en ``shared.constants``; este módulo solo importa,
  nunca redeclara, ``SAMPLE_RATE``, ``PIPEWIRE_SAMPLE_RATE`` y
  ``CHUNK_SAMPLES``.
* El empaquetado en frames de 640 bytes reutiliza ``shared.protocol.iter_pcm_chunks``.

``StreamingResampler`` mantiene una cola residual acotada: agrupa las muestras
entrantes en bloques de ``CHUNK_SAMPLES * src_rate / dst_rate`` (960 muestras a
48 kHz, es decir 3:1) y emite un único chunk de 320 muestras int16 por bloque.
El estado nunca crece con la duración del stream (O(1)).
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import soxr  # type: ignore[import-untyped]
from numpy.typing import NDArray

from shared.constants import CHUNK_SAMPLES, PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE
from shared.protocol import iter_pcm_chunks

DEFAULT_QUALITY: str = "HQ"
"""Calidad soxr por defecto (medido: SNR de roundtrip ~65 dB > 40 dB exigidos)."""

SUPPORTED_RATES: frozenset[int] = frozenset({SAMPLE_RATE, PIPEWIRE_SAMPLE_RATE})
"""Únicas tasas aceptadas en la frontera (SPEC.md §5, error ``FORMAT``)."""

_INT16_DIVISOR = 32768.0
_INT16_SCALE = 32768.0
_INT16_MIN = -32768
_INT16_MAX = 32767

# Entradas admitidas: PCM int16 LE en bytes o arrays float32/float64/int16.
# Un array 2-D ``[frame, channel]`` se considera interleaved y se reduce a mono
# (1 o 2 canales; el sink virtual de PipeWire es estéreo).
type SampleInput = (
    bytes
    | bytearray
    | memoryview
    | NDArray[np.float32]
    | NDArray[np.float64]
    | NDArray[np.int16]
)


class ResampleError(ValueError):
    """Entrada de audio inválida: tasa, dtype, forma o contenido no finito."""


def _validate_rate(rate: int, name: str = "sample_rate") -> None:
    """Rechaza cualquier tasa distinta de 16 kHz o 48 kHz."""
    if isinstance(rate, bool) or not isinstance(rate, int) or rate not in SUPPORTED_RATES:
        raise ResampleError(
            f"{name} no soportado: {rate!r}; se aceptan {sorted(SUPPORTED_RATES)} Hz"
        )


def _as_float32_mono(samples: SampleInput, *, name: str = "samples") -> NDArray[np.float32]:
    """Normaliza la entrada a float32 mono en [-1, 1], validando la frontera."""
    array: NDArray[Any]
    if isinstance(samples, bytes | bytearray | memoryview):
        raw = bytes(samples)
        if len(raw) % 2 != 0:
            raise ResampleError(f"{name}: PCM int16 LE requiere un número par de bytes")
        ints: NDArray[Any] = np.frombuffer(raw, dtype="<i2")
        array = ints.astype(np.float64) / _INT16_DIVISOR
    elif isinstance(samples, np.ndarray):
        dtype = samples.dtype
        if dtype == np.dtype(np.int16):
            array = samples.astype(np.float64) / _INT16_DIVISOR
        elif dtype == np.dtype(np.float32) or dtype == np.dtype(np.float64):
            array = samples.astype(np.float64)
        else:
            raise ResampleError(
                f"{name}: dtype {dtype} no soportado; se aceptan float32, float64 e int16"
            )
        if samples.ndim == 2:
            channels = samples.shape[1]
            if channels not in (1, 2):
                raise ResampleError(
                    f"{name}: audio interleaved esperado con 1 o 2 canales, recibidos {channels}"
                )
            array = array.mean(axis=1)
        elif samples.ndim != 1:
            raise ResampleError(
                f"{name}: se esperaba señal 1-D o 2-D interleaved, ndim={samples.ndim}"
            )
    else:
        raise ResampleError(f"{name}: tipo no soportado: {type(samples).__name__}")

    if array.size == 0:
        return cast("NDArray[np.float32]", np.empty(0, dtype=np.float32))
    if not bool(np.all(np.isfinite(array))):
        raise ResampleError(f"{name}: contiene muestras NaN o Inf")

    limited = np.clip(array, -1.0, 1.0)
    return cast("NDArray[np.float32]", np.ascontiguousarray(limited, dtype=np.float32))


def _to_int16(samples: NDArray[Any]) -> NDArray[np.int16]:
    """Cuantiza float normalizado a ``pcm_s16le`` mono con recorte seguro."""
    scaled = np.rint(samples.astype(np.float64) * _INT16_SCALE)
    clipped = np.clip(scaled, _INT16_MIN, _INT16_MAX)
    return cast("NDArray[np.int16]", np.ascontiguousarray(clipped, dtype=np.int16))


def _resample_float(
    mono: NDArray[np.float32], src_rate: int, dst_rate: int, quality: str
) -> NDArray[np.float32]:
    """Aplica soxr solo cuando hay cambio de tasa; la identidad es exacta."""
    if src_rate == dst_rate:
        return mono
    result: NDArray[Any] = soxr.resample(mono, src_rate, dst_rate, quality=quality)
    return cast("NDArray[np.float32]", np.ascontiguousarray(result, dtype=np.float32))


def resample_to_16k(
    samples: SampleInput,
    src_rate: int = PIPEWIRE_SAMPLE_RATE,
    *,
    quality: str = DEFAULT_QUALITY,
) -> NDArray[np.int16]:
    """Convierte audio 48 kHz a ``pcm_s16le`` 16 kHz mono.

    Acepta float32/float64 normalizados, int16 en LSB o bytes int16 LE; los
    arrays 2-D de 1-2 canales se downmixing a mono. Con ``src_rate=16000``
    solo se normaliza/cuantiza (sin filtrado).
    """
    _validate_rate(src_rate)
    mono = _as_float32_mono(samples)
    return _to_int16(_resample_float(mono, src_rate, SAMPLE_RATE, quality))


def resample_to_48k(
    samples: SampleInput,
    src_rate: int = SAMPLE_RATE,
    *,
    dtype: type[np.float32] | type[np.int16] = np.float32,
    quality: str = DEFAULT_QUALITY,
) -> NDArray[np.float32] | NDArray[np.int16]:
    """Devuelve el TTS a 48 kHz para PipeWire.

    Por defecto emite float32 normalizado (formato nativo de PipeWire). Con
    ``dtype=np.int16`` emite ``pcm_s16le`` para los mezcladores que lo exijan.
    """
    _validate_rate(src_rate)
    out_dtype = np.dtype(dtype)
    if out_dtype not in (np.dtype(np.float32), np.dtype(np.int16)):
        raise ResampleError(f"dtype de salida no soportado: {out_dtype}")

    mono = _as_float32_mono(samples)
    resampled = _resample_float(mono, src_rate, PIPEWIRE_SAMPLE_RATE, quality)
    if out_dtype == np.dtype(np.int16):
        return _to_int16(resampled)
    return cast("NDArray[np.float32]", np.ascontiguousarray(resampled, dtype=np.float32))


class StreamingResampler:
    """Resampleo continuo 48 kHz → 16 kHz con cola residual de tamaño acotado.

    Cada ``CHUNK_SAMPLES * src_rate / dst_rate`` muestras de entrada (960 a
    48 kHz → 320 a 16 kHz, balance 3:1) producen exactamente un chunk de 640
    bytes generado por :func:`iter_pcm_chunks`. Los restos de entrada se
    conservan entre llamadas a :meth:`process` y se liberan con :meth:`reset`.
    """

    def __init__(
        self,
        src_rate: int = PIPEWIRE_SAMPLE_RATE,
        dst_rate: int = SAMPLE_RATE,
        *,
        quality: str = DEFAULT_QUALITY,
    ) -> None:
        _validate_rate(src_rate, "src_rate")
        _validate_rate(dst_rate, "dst_rate")
        if dst_rate != SAMPLE_RATE:
            raise ResampleError(
                f"dst_rate={dst_rate} no válido: este stream emite {SAMPLE_RATE} Hz; "
                "use resample_to_48k para la ruta TTS"
            )

        input_per_chunk = CHUNK_SAMPLES * src_rate
        if input_per_chunk % dst_rate != 0:
            raise ResampleError(
                f"src_rate={src_rate} no permite chunks enteros de {CHUNK_SAMPLES} muestras"
            )
        self._src_rate = src_rate
        self._dst_rate = dst_rate
        self._quality = quality
        self._input_per_chunk = input_per_chunk // dst_rate
        self._pending: NDArray[np.float32] = np.empty(0, dtype=np.float32)

    @property
    def src_rate(self) -> int:
        """Tasa de entrada configurada."""
        return self._src_rate

    @property
    def dst_rate(self) -> int:
        """Tasa de salida canónica (16 kHz)."""
        return self._dst_rate

    @property
    def pending_samples(self) -> int:
        """Muestras de entrada restantes para completar un bloque (siempre < 960)."""
        return int(self._pending.size)

    def reset(self) -> None:
        """Descarta la cola residual (cambio de sesión o de stream)."""
        self._pending = np.empty(0, dtype=np.float32)

    def process(self, samples: SampleInput) -> list[bytes]:
        """Ingesta un chunk entrante y devuelve 0..N frames PCM de 640 bytes.

        La cola residual hace que cualquier partición del stream (chunks de
        20 ms, de 7 muestras, bytes o arrays) produzca la misma secuencia de
        frames: no se pierden muestras y cada frame es de duración canónica.
        """
        incoming = _as_float32_mono(samples, name="chunk")
        if incoming.size == 0:
            return []

        if self._pending.size == 0:
            self._pending = incoming
        else:
            self._pending = np.concatenate((self._pending, incoming))

        chunks: list[bytes] = []
        needed = self._input_per_chunk
        while self._pending.size >= needed:
            block = self._pending[:needed]
            self._pending = self._pending[needed:].copy()
            resampled = _resample_float(
                block, self._src_rate, self._dst_rate, self._quality
            )
            chunks.extend(iter_pcm_chunks(_to_int16(resampled), chunk_samples=CHUNK_SAMPLES))
        return chunks
