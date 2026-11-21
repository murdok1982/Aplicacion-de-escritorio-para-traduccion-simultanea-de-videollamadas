"""Tests de los adaptadores VAD incrementales (SPEC.md REQ-04 / AC-06).

Los WAV se regeneran al inicio de la sesion con el generador canonico, de modo
que el test no depende de binarios commiteados ni de descargas de red. Silero
se ejerce con modulos falsos inyectados en ``sys.modules``: el extra
``inference`` no esta instalado en el entorno por defecto.
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
import types
import wave
from pathlib import Path

import numpy as np
import pytest

from server.pipeline.base import AudioChunk, PipelineError, Segment
from server.pipeline.vad import EnergyVAD, SileroVAD
from shared.constants import CHUNK_BYTES, CHUNK_MS, SAMPLE_RATE

ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = ROOT / "tests" / "fixtures"
GENERATOR = ROOT / "scripts" / "generate-fixtures.py"


@pytest.fixture(scope="session", autouse=True)
def regenerate_fixtures() -> None:
    """Genera los WAV sinteticos una vez por sesion de pytest."""
    result = subprocess.run(
        [sys.executable, str(GENERATOR)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    for name in ("two_clauses_16k.wav", "voice_es_16k.wav"):
        assert (FIXTURES_DIR / name).is_file(), f"Falta el fixture {name}"


def _read_pcm(name: str) -> np.ndarray:
    """Lee un fixture WAV como array int16 LE."""
    with wave.open(str(FIXTURES_DIR / name), "rb") as handle:
        raw = handle.readframes(handle.getnframes())
    return np.frombuffer(raw, dtype="<i2").copy()


def _chunks(name: str) -> list[AudioChunk]:
    """Divide el fixture en chunks canoncos de 20 ms con sequence monotona."""
    pcm = _read_pcm(name)
    assert pcm.size % (CHUNK_BYTES // 2) == 0
    raw = pcm.tobytes()
    return [
        AudioChunk(
            pcm=raw[offset : offset + CHUNK_BYTES],
            sequence=index,
            timestamp_ms=index * CHUNK_MS,
        )
        for index, offset in enumerate(range(0, len(raw), CHUNK_BYTES))
    ]


async def _feed(
    vad: EnergyVAD | SileroVAD, chunks: list[AudioChunk]
) -> list[Segment]:
    """Alimenta todos los chunks y cierra el stream con ``flush()``."""
    emitted: list[Segment] = []
    for chunk in chunks:
        emitted.extend(await vad.process(chunk))
    emitted.extend(await vad.flush())
    return emitted


def _finals(segments: list[Segment]) -> list[Segment]:
    return [segment for segment in segments if segment.is_final]


def _duration_s(segment: Segment) -> float:
    return len(segment.audio) / 2 / SAMPLE_RATE


# ---------------------------------------------------------------------------
# AC-06: segmentacion por pausas naturales, sin ventanas ciegas
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac06_no_blind_windows() -> None:
    """Dos clausulas de 2 s separadas por 400 ms -> exactamente 2 finales."""
    vad = EnergyVAD()
    chunks = _chunks("two_clauses_16k.wav")
    assert len(chunks) == 220  # 4.4 s a 20 ms/chunk

    emitted = await _feed(vad, chunks)
    finals = _finals(emitted)

    assert len(finals) == 2
    assert finals[0].id < finals[1].id
    assert all(abs(_duration_s(segment) - 2.0) <= 0.1 for segment in finals)
    assert all(segment.latency_ms <= 300 for segment in finals)

    # El hueco de 400 ms coincide con el primer cierre (250 ms de silencio),
    # pero no puede producir un tercer segmento: despues del cierre solo queda
    # silencio y la segunda clausula se cierra con flush().
    assert len(_finals(await _feed(EnergyVAD(), chunks[100:120]))) == 0


@pytest.mark.asyncio
async def test_flush_closes_pending_segment() -> None:
    """Un stream sin silencio final cierra su clausula pendiente con flush()."""
    vad = EnergyVAD()
    chunks = _chunks("voice_es_16k.wav")
    assert len(chunks) == 100

    processed: list[Segment] = []
    for chunk in chunks:
        processed.extend(await vad.process(chunk))
    assert _finals(processed) == []

    closed = await vad.flush()
    assert len(_finals(closed)) == 1
    assert abs(_duration_s(_finals(closed)[0]) - 2.0) <= 0.1
    assert await vad.flush() == []


@pytest.mark.asyncio
async def test_reset_clears_state() -> None:
    """reset() descarta audio pendiente, parciales y reinicia la numeracion."""
    vad = EnergyVAD()
    chunks = _chunks("two_clauses_16k.wav")

    for chunk in chunks[:100]:
        await vad.process(chunk)
    vad.reset()

    assert await vad.flush() == []
    emitted = await _feed(vad, chunks)
    assert emitted[0].id == 0
    assert len(_finals(emitted)) == 2


@pytest.mark.asyncio
async def test_partials_precede_final() -> None:
    """Cada cierre va precedido de eventos is_final=False con audio creciente."""
    vad = EnergyVAD()
    emitted = await _feed(vad, _chunks("two_clauses_16k.wav"))
    final_ids = [segment.id for segment in emitted if segment.is_final]

    partials = [
        segment for segment in emitted if not segment.is_final and segment.audio
    ]
    assert partials
    assert all(segment.latency_ms == 0 for segment in partials)
    for final_id in final_ids:
        before = [segment for segment in partials if segment.id < final_id]
        assert before
        assert len(set(segment.id for segment in emitted)) == len(emitted)


@pytest.mark.asyncio
async def test_chunk_boundary_validation() -> None:
    """La frontera rechaza PCM de otro tamano y tipos inesperados."""
    vad = EnergyVAD()
    good = _chunks("voice_es_16k.wav")[0]

    with pytest.raises(PipelineError) as exc_info:
        await vad.process(
            AudioChunk(pcm=good.pcm[:-2], sequence=good.sequence, timestamp_ms=0)
        )
    assert exc_info.value.code == "FORMAT"
    assert str(CHUNK_BYTES) in exc_info.value.message

    with pytest.raises(PipelineError) as exc_info:
        await vad.process(good.pcm)  # type: ignore[arg-type]
    assert exc_info.value.code == "SCHEMA"


@pytest.mark.parametrize("min_silence_ms", [250, 300, 500])
@pytest.mark.asyncio
async def test_min_silence_respected(min_silence_ms: int) -> None:
    """Un silencio mas corto que min_silence_ms nunca cierra la clausula."""
    chunks = _chunks("voice_es_16k.wav")
    vad = EnergyVAD(min_silence_ms=min_silence_ms)

    for chunk in chunks[:100]:
        assert _finals(await vad.process(chunk)) == []
    assert len(_finals(await vad.flush())) == 1


@pytest.mark.asyncio
async def test_gap_shorter_than_min_silence_does_not_split() -> None:
    """Con min_silence_ms=500 el hueco de 400 ms NO divide la clausula."""
    vad = EnergyVAD(min_silence_ms=500)
    emitted = await _feed(vad, _chunks("two_clauses_16k.wav"))
    finals = _finals(emitted)

    assert len(finals) == 1
    assert abs(_duration_s(finals[0]) - 4.4) <= 0.15


# ---------------------------------------------------------------------------
# SileroVAD: extra ausente y cableado completo sin red
# ---------------------------------------------------------------------------


def test_silero_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sin el extra ``inference`` falla con MODEL_MISSING y accion concreta."""
    monkeypatch.setitem(sys.modules, "silero_vad", None)
    monkeypatch.setitem(sys.modules, "torch", None)

    with pytest.raises(PipelineError) as exc_info:
        SileroVAD()

    assert exc_info.value.code == "MODEL_MISSING"
    assert "instala el extra inference" in str(exc_info.value)


class _FakeTensor:
    """Tensor minimo: solo expone los bytes de entrada y el resultado escalar."""

    def __init__(self, data: np.ndarray) -> None:
        self.data = data

    def item(self) -> float:
        return float(self.data)


class _FakeSileroModel:
    """Modelo deterministico: 0.9 si la ventana tiene voz, 0.0 si no."""

    def reset_states(self) -> None:
        return None

    def __call__(self, tensor: _FakeTensor, sample_rate: int) -> _FakeTensor:
        rms = float(np.sqrt(np.mean(np.square(tensor.data, dtype=np.float64))))
        return _FakeTensor(np.array(0.9 if rms > 0.005 else 0.0))


def _fake_module(name: str, **attributes: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


@pytest.mark.asyncio
async def test_silero_incremental_with_fake_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SileroVAD procesa ventanas de 512 muestras y cierra dos clausulas."""
    monkeypatch.setitem(
        sys.modules,
        "silero_vad",
        _fake_module("silero_vad", load_silero_vad=lambda: _FakeSileroModel()),
    )
    monkeypatch.setitem(
        sys.modules,
        "torch",
        _fake_module(
            "torch",
            from_numpy=lambda samples: _FakeTensor(samples),
            no_grad=contextlib.nullcontext,
        ),
    )

    vad = SileroVAD()
    emitted = await _feed(vad, _chunks("two_clauses_16k.wav"))
    finals = _finals(emitted)

    assert len(finals) == 2
    assert all(abs(_duration_s(segment) - 2.0) <= 0.15 for segment in finals)
    assert all(segment.is_final for segment in finals)
    assert any(not segment.is_final for segment in emitted)
