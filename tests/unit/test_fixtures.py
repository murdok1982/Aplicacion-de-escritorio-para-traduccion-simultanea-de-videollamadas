"""Tests de los fixtures WAV sinteticos generados por ``scripts/generate-fixtures.py``.

La fixture de sesion autouse regenera todos los WAV antes de ejecutar cualquier
test, de modo que la suite nunca depende de binarios commiteados ni de estado
previo del directorio ``tests/fixtures/``.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "generate-fixtures.py"
FIXTURES_DIR = ROOT / "tests" / "fixtures"


def _load_generator() -> types.ModuleType:
    """Importa el script con guiones como modulo para reutilizar sus constantes."""
    spec = importlib.util.spec_from_file_location("babelpipe_generate_fixtures", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


GEN = _load_generator()


@pytest.fixture(scope="session", autouse=True)
def regenerate_fixtures() -> None:
    """Regenera los siete WAV una unica vez por sesion de pytest."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(list(FIXTURES_DIR.glob("*.wav"))) == len(GEN.FIXTURES)


def _read(name: str) -> tuple[np.ndarray, tuple[int, int, int, int]]:
    path = FIXTURES_DIR / name
    assert path.is_file(), f"Falta el fixture {name}"
    return GEN.read_wav(path)


def _longest_low_energy_run(pcm: np.ndarray, sample_rate: int, threshold: float) -> float:
    """Duracion (s) de la racha mas larga de ventanas de 20 ms por debajo de ``threshold``."""
    window = GEN.frame_count(sample_rate, 0.020)
    longest = current = 0
    for start in range(0, pcm.size - window + 1, window):
        if GEN.rms(pcm[start : start + window]) < threshold:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest * 0.020


@pytest.mark.parametrize("spec", GEN.FIXTURES, ids=lambda value: value.filename)
def test_fixture_format(spec: Any) -> None:
    """Tasa de muestreo, canales, dtype y duracion exactos para cada fixture."""
    pcm, metadata = _read(spec.filename)
    channels, sample_width, rate, frames = metadata

    assert rate == spec.sample_rate
    assert channels == 1
    assert sample_width == 2
    assert pcm.dtype == np.dtype("<i2")
    assert frames == GEN.frame_count(spec.sample_rate, spec.duration_s)
    assert pcm.size == frames


@pytest.mark.parametrize(
    "filename",
    ["tone_1k_48k.wav", "tone_1k_16k.wav"],
)
def test_tone_frequency(filename: str) -> None:
    """El tono de 1 kHz se conserva con desviacion < 20 Hz en ambas tasas."""
    pcm, (_channels, _width, rate, _frames) = _read(filename)
    measured = GEN.dominant_frequency(pcm, rate)
    assert abs(measured - GEN.TONE_FREQ_HZ) <= 20.0


def test_two_clauses_gap() -> None:
    """Existe un hueco de ~400 ms con RMS casi nulo entre dos clausulas con voz."""
    pcm, (_channels, _width, rate, _frames) = _read("two_clauses_16k.wav")
    gap_start = GEN.frame_count(rate, GEN.CLAUSE_DURATION_S)
    gap_end = gap_start + GEN.frame_count(rate, GEN.GAP_DURATION_S)

    gap_rms = GEN.rms(pcm[gap_start:gap_end])
    assert gap_rms < GEN.RMS_SILENCE_THRESHOLD
    assert GEN.rms(pcm[:gap_start]) > 1000.0
    assert GEN.rms(pcm[gap_end:]) > 1000.0

    longest_gap = _longest_low_energy_run(pcm, rate, GEN.RMS_SILENCE_THRESHOLD)
    assert abs(longest_gap - GEN.GAP_DURATION_S) <= 0.04


def test_voice_has_vad_activity() -> None:
    """La voz sintetica tiene actividad suficiente y pausas entre silabas."""
    pcm, (_channels, _width, rate, _frames) = _read("voice_es_16k.wav")
    window = GEN.frame_count(rate, 0.020)
    energies = [
        GEN.rms(pcm[start : start + window]) for start in range(0, pcm.size - window + 1, window)
    ]
    active = sum(energy > GEN.RMS_SILENCE_THRESHOLD for energy in energies) / len(energies)
    assert 0.30 < active < 0.95
    assert min(energies) < GEN.RMS_SILENCE_THRESHOLD
    assert max(energies) > 1000.0


def test_noise_is_deterministic() -> None:
    """El ruido blanco se regenera byte a byte con la semilla fija 42."""
    pcm, (_channels, _width, rate, _frames) = _read("noise_16k.wav")
    expected = GEN.synthesize_noise(rate, 1.0, seed=GEN.NOISE_SEED)
    expected_pcm = np.rint(expected * 32767).astype("<i2")
    assert np.array_equal(pcm, expected_pcm)
    other = GEN.synthesize_noise(rate, 1.0, seed=GEN.NOISE_SEED + 1)
    assert not np.array_equal(pcm, np.rint(other * 32767).astype("<i2"))


def test_echo_is_delayed_attenuated_copy() -> None:
    """El fixture de eco contiene la voz mas una copia x0.35 retrasada 120 ms."""
    pcm, (_channels, _width, rate, _frames) = _read("echo_48k.wav")
    voice = GEN.synthesize_voice(rate, GEN.CLAUSE_DURATION_S)
    delay = GEN.frame_count(rate, GEN.ECHO_DELAY_S)
    residual = pcm[: voice.size].astype(np.float64) / 32767.0 - voice

    assert float(np.max(np.abs(residual[:delay]))) < 2e-4
    expected_tail = GEN.ECHO_ATTENUATION * voice[: voice.size - delay]
    assert float(np.max(np.abs(residual[delay:] - expected_tail))) < 2e-4


def test_verify_cli() -> None:
    """El comando --verify valida los siete WAV y sale 0 con un resumen."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--verify"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    for spec in GEN.FIXTURES:
        assert spec.filename in result.stdout
    assert "7/7 fixtures validos" in result.stdout
