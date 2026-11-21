"""Tests del resampleo 48 kHz ↔ 16 kHz (SPEC.md §REQ-02 / AC-03, ADR-005).

Usa los WAV sintéticos de TASK-003. Si no existen todavía, la fixture de sesión
los regenera con el generador canónico (mismo patrón que ``test_fixtures.py``).
"""

from __future__ import annotations

import importlib.util
import struct
import subprocess
import sys
import types
from pathlib import Path

import numpy as np
import pytest

from client.audio.resampler import (
    ResampleError,
    StreamingResampler,
    resample_to_16k,
    resample_to_48k,
)
from shared.constants import CHUNK_BYTES, CHUNK_SAMPLES, PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE
from shared.protocol import iter_pcm_chunks

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "generate-fixtures.py"
FIXTURES_DIR = ROOT / "tests" / "fixtures"
TONE_HZ = 1_000.0
BLOCK_48K = PIPEWIRE_SAMPLE_RATE * CHUNK_SAMPLES // SAMPLE_RATE  # 960 muestras = 20 ms


def _load_generator() -> types.ModuleType:
    """Importa el script con guiones para reutilizar sus helpers de WAV."""
    spec = importlib.util.spec_from_file_location("babelpipe_generate_fixtures", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


GEN = _load_generator()


@pytest.fixture(scope="session", autouse=True)
def ensure_fixtures() -> None:
    """Regenera los tres WAV necesarios si el workspace aún no los contiene."""
    needed = ("tone_1k_48k.wav", "tone_1k_16k.wav", "silence_16k.wav")
    if all((FIXTURES_DIR / name).is_file() for name in needed):
        return
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert all((FIXTURES_DIR / name).is_file() for name in needed)


def _read(name: str) -> tuple[np.ndarray, tuple[int, int, int, int]]:
    """Lee un fixture y devuelve (pcm int16, (canales, ancho, rate, frames))."""
    path = FIXTURES_DIR / name
    assert path.is_file(), f"Falta el fixture {name}"
    return GEN.read_wav(path)


def _snr(reference: np.ndarray, obtained: np.ndarray) -> float:
    """SNR en dB entre dos señales int16 de la misma longitud."""
    assert reference.size == obtained.size
    ref = reference.astype(np.float64)
    err = obtained.astype(np.float64) - ref
    energy = float(np.sum(err * err))
    if energy == 0.0:
        return float("inf")
    return float(10.0 * np.log10(float(np.sum(ref * ref)) / energy))


def _chunks_to_pcm(chunks: list[bytes]) -> np.ndarray:
    """Concatena frames PCM int16 LE en un único array de muestras."""
    return np.frombuffer(b"".join(chunks), dtype="<i2")


def test_ac03_roundtrip_snr() -> None:
    """AC-03: 48k → 16k → 48k conserva 1 kHz (<1 %) con SNR > 40 dB."""
    src, (_channels, _width, rate, _frames) = _read("tone_1k_48k.wav")
    assert rate == PIPEWIRE_SAMPLE_RATE

    at16 = resample_to_16k(src, src_rate=rate)
    back = resample_to_48k(at16, src_rate=SAMPLE_RATE, dtype=np.int16)

    assert back.dtype == np.dtype(np.int16)
    assert back.size == src.size
    freq_16k = GEN.dominant_frequency(at16, SAMPLE_RATE)
    freq_48k = GEN.dominant_frequency(back, PIPEWIRE_SAMPLE_RATE)
    assert abs(freq_16k - TONE_HZ) / TONE_HZ < 0.01
    assert abs(freq_48k - TONE_HZ) / TONE_HZ < 0.01
    assert _snr(src, back) > 40.0


def test_output_exact_int16_mono_16k_and_640_byte_chunks() -> None:
    """La salida es int16 mono 16 kHz y se empaqueta en chunks de 640 bytes."""
    src, _ = _read("tone_1k_48k.wav")
    out = resample_to_16k(src)

    assert out.dtype == np.dtype(np.int16)
    assert out.ndim == 1
    assert out.size == SAMPLE_RATE  # 1 s de tono
    chunks = list(iter_pcm_chunks(out))
    assert len(chunks) == SAMPLE_RATE // CHUNK_SAMPLES == 50
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    assert isinstance(struct.unpack("<h", chunks[0][:2])[0], int)


def test_streaming_exact_chunks_and_no_sample_loss() -> None:
    """20 ms de entrada @48 kHz producen exactamente 320 muestras @16 kHz."""
    src, _ = _read("tone_1k_48k.wav")
    resampler = StreamingResampler()
    chunks: list[bytes] = []
    for offset in range(0, src.size, BLOCK_48K):
        chunks.extend(resampler.process(src[offset : offset + BLOCK_48K]))

    assert chunks
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    pcm = _chunks_to_pcm(chunks)
    assert pcm.size == SAMPLE_RATE
    assert _snr(resample_to_16k(src), pcm) > 40.0
    assert resampler.pending_samples == 0


def test_streaming_residual_queue_with_irregular_chunks() -> None:
    """Chunks no alineados a 960 muestras no pierden ni duplican muestras."""
    src, _ = _read("tone_1k_48k.wav")
    resampler = StreamingResampler()
    chunks: list[bytes] = []
    step = 7  # 7 no divide 960: fuerza la cola residual en cada bloque
    for offset in range(0, src.size, step):
        chunks.extend(resampler.process(src[offset : offset + step]))

    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    total = sum(len(chunk) // 2 for chunk in chunks)
    assert abs(total - SAMPLE_RATE) <= CHUNK_SAMPLES
    assert total == SAMPLE_RATE
    assert resampler.pending_samples == 0


def test_streaming_accepts_bytes_and_float32_interleaved() -> None:
    """La ruta de streaming acepta bytes int16 LE y float32 2-D interleaved."""
    src, _ = _read("tone_1k_48k.wav")
    from_bytes = StreamingResampler()
    from_float = StreamingResampler()
    chunks_bytes: list[bytes] = []
    chunks_float: list[bytes] = []

    for offset in range(0, src.size, BLOCK_48K):
        block = src[offset : offset + BLOCK_48K]
        chunks_bytes.extend(from_bytes.process(block.tobytes()))
        stereo = np.column_stack((block, block)).astype(np.float32) / 32768.0
        chunks_float.extend(from_float.process(stereo))

    assert chunks_bytes == chunks_float
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks_bytes)


def test_streaming_state_is_resettable() -> None:
    """El estado residual es O(1), consultable y reiniciable."""
    resampler = StreamingResampler()
    assert resampler.src_rate == PIPEWIRE_SAMPLE_RATE
    assert resampler.dst_rate == SAMPLE_RATE
    assert resampler.process(np.zeros(0, dtype=np.float32)) == []

    resampler.process(np.zeros(100, dtype=np.float32))
    assert resampler.pending_samples == 100
    resampler.reset()
    assert resampler.pending_samples == 0


def test_silence_stays_silent_in_both_directions() -> None:
    """El silencio no genera ruido de cuantización ni en el stream ni en TTS."""
    silence, _ = _read("silence_16k.wav")
    up = resample_to_48k(silence)
    down = resample_to_16k(silence, src_rate=SAMPLE_RATE)
    assert up.dtype == np.dtype(np.float32)
    assert up.size == PIPEWIRE_SAMPLE_RATE
    assert not np.any(up)
    assert down.dtype == np.dtype(np.int16)
    assert not np.any(down)

    resampler = StreamingResampler()
    chunks: list[bytes] = []
    zeros = np.zeros(BLOCK_48K, dtype=np.float32)
    for _ in range(50):
        chunks.extend(resampler.process(zeros))
    assert chunks
    assert all(not np.any(np.frombuffer(chunk, dtype="<i2")) for chunk in chunks)


def test_resample_to_48k_output_dtypes() -> None:
    """La ruta TTS emite float32 por defecto e int16 bajo petición."""
    src, _ = _read("tone_1k_16k.wav")
    floating = resample_to_48k(src)
    integer = resample_to_48k(src, dtype=np.int16)
    assert floating.dtype == np.dtype(np.float32)
    assert floating.size == PIPEWIRE_SAMPLE_RATE
    assert float(np.max(np.abs(floating))) <= 1.0
    assert integer.dtype == np.dtype(np.int16)
    assert integer.size == PIPEWIRE_SAMPLE_RATE


@pytest.mark.parametrize("bad_rate", [44_100, 22_050, 8_000, 0, -1])
def test_unsupported_sample_rates_rejected(bad_rate: int) -> None:
    """Cualquier tasa fuera de {16000, 48000} se rechaza con ResampleError."""
    samples = np.zeros(BLOCK_48K, dtype=np.float32)
    with pytest.raises(ResampleError):
        resample_to_16k(samples, src_rate=bad_rate)
    with pytest.raises(ResampleError):
        resample_to_48k(samples, src_rate=bad_rate)
    with pytest.raises(ResampleError):
        StreamingResampler(src_rate=bad_rate)


def test_unsupported_destination_rate_rejected() -> None:
    """El stream solo entrega 16 kHz; 48 kHz pertenece a resample_to_48k."""
    with pytest.raises(ResampleError):
        StreamingResampler(dst_rate=PIPEWIRE_SAMPLE_RATE)


def test_invalid_sample_shapes_and_dtypes_rejected() -> None:
    """La frontera valida dtype, dimensionalidad, canales y finitud."""
    with pytest.raises(ResampleError):
        resample_to_16k(np.zeros(32, dtype=np.float16))
    with pytest.raises(ResampleError):
        resample_to_16k(np.zeros((2, 2, 2), dtype=np.float32))
    with pytest.raises(ResampleError):
        resample_to_16k(np.zeros((8, 3), dtype=np.float32))
    with pytest.raises(ResampleError):
        resample_to_16k(b"\x01")
    with pytest.raises(ResampleError):
        resample_to_16k(np.array([np.nan], dtype=np.float32))
    with pytest.raises(ResampleError):
        resample_to_48k(np.zeros(32, dtype=np.float32), dtype=np.float64)  # type: ignore[arg-type]
    with pytest.raises(ResampleError):
        StreamingResampler().process([0.0, 1.0])
