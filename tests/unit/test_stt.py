"""Tests de los adaptadores STT incrementales (REQ-04 / AC-07).

 Los WAV sinteticos se regeneran con el generador canonico: ningun test descarga
 modelos ni toca el extra ``inference``. Los motores de produccion se ejercitan
 con modulos falsos inyectados en ``sys.modules`` para verificar el cableado
 (idioma, beam, cuantizacion y forma del array) sin red.
"""

from __future__ import annotations

import asyncio
import inspect
import subprocess
import sys
import threading
import types
import wave
from collections.abc import AsyncIterator
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pytest

from server.pipeline.base import PipelineError, Segment
from server.pipeline.stt import (
    DeterministicSTT,
    FasterWhisperSTT,
    WhisperCppSTT,
    validate_pcm,
)
from shared.constants import BYTES_PER_SAMPLE, CHUNK_BYTES, SAMPLE_RATE

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


def _pcm(name: str) -> bytes:
    """Devuelve el payload PCM de un fixture verificando su cabecera WAV."""
    with wave.open(str(FIXTURES_DIR / name), "rb") as handle:
        assert handle.getnchannels() == 1
        assert handle.getsampwidth() == BYTES_PER_SAMPLE
        assert handle.getframerate() == SAMPLE_RATE
        return handle.readframes(handle.getnframes())


def _fake_module(name: str, **attributes: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


# ---------------------------------------------------------------------------
# DeterministicSTT: estabilidad, formato y presupuesto de latencia
# ---------------------------------------------------------------------------


def test_deterministic_stt_is_stable() -> None:
    """Mismo WAV -> mismo texto no vacio y de longitud razonable."""
    stt = DeterministicSTT()
    texts: dict[str, str] = {}
    for name in ("voice_es_16k.wav", "two_clauses_16k.wav"):
        pcm = _pcm(name)
        first = asyncio.run(stt.transcribe(pcm))
        second = asyncio.run(stt.transcribe(pcm))
        assert first == second
        assert isinstance(first, str) and first
        assert 10 <= len(first) <= 120
        texts[name] = first

    # La duracion (2.0 s vs 4.4 s) distingue ambos fixtures por construccion.
    assert texts["voice_es_16k.wav"] != texts["two_clauses_16k.wav"]


def test_deterministic_stt_respects_language() -> None:
    """El idioma explicito y el del constructor se reflejan en el texto."""
    pcm = _pcm("voice_es_16k.wav")[:CHUNK_BYTES]
    assert "[es ·" in asyncio.run(DeterministicSTT(language="es").transcribe(pcm))
    assert "[en ·" in asyncio.run(DeterministicSTT().transcribe(pcm, language="EN"))
    assert "[auto ·" in asyncio.run(DeterministicSTT().transcribe(pcm))


@pytest.mark.parametrize(
    ("audio", "kwargs", "code"),
    [
        (b"", {}, "FORMAT"),
        (b"\x00" * (CHUNK_BYTES - 2), {}, "FORMAT"),
        (b"\x00" * (CHUNK_BYTES + 1), {}, "FORMAT"),
        (b"\x00" * CHUNK_BYTES, {"sample_rate": 48_000}, "FORMAT"),
        (b"\x00" * CHUNK_BYTES, {"sample_format": "pcm_f32le"}, "FORMAT"),
        ("no son bytes", {}, "SCHEMA"),
    ],
)
def test_invalid_audio_format_raises(
    audio: object, kwargs: dict[str, Any], code: str
) -> None:
    """La frontera rechaza rate, dtype y longitud fuera del contratom."""
    with pytest.raises(PipelineError) as exc_info:
        validate_pcm(audio, **kwargs)  # type: ignore[arg-type]
    assert exc_info.value.code == code


@pytest.mark.asyncio
async def test_transcribe_validates_on_public_api() -> None:
    """``transcribe`` valida antes de despachar al motor (sin llamadas al modelo)."""
    stt = DeterministicSTT()
    pcm = _pcm("voice_es_16k.wav")[:CHUNK_BYTES]

    with pytest.raises(PipelineError) as exc_info:
        await stt.transcribe(pcm[:-2])
    assert exc_info.value.code == "FORMAT"

    with pytest.raises(PipelineError) as exc_info:
        await stt.transcribe(pcm, sample_rate=48_000)
    assert exc_info.value.code == "FORMAT"


def test_config_validation() -> None:
    """Los parametros del adaptador se validan con codigo SCHEMA."""
    invalid: list[dict[str, Any]] = [
        {"model_size": "large"},
        {"device": "tpu"},
        {"compute_type": "binary"},
        {"beam_size": 0},
        {"beam_size": True},
        {"language": "español"},
    ]
    for options in invalid:
        with pytest.raises(PipelineError) as exc_info:
            DeterministicSTT(**options)
        assert exc_info.value.code == "SCHEMA"


@pytest.mark.asyncio
async def test_ac07_stt_stage_latency_budget() -> None:
    """La etapa STT determinista cumple el presupuesto de 400 ms (SPEC §7)."""
    stt = DeterministicSTT()
    pcm = _pcm("voice_es_16k.wav")
    started = perf_counter()
    text = await stt.transcribe(pcm)
    elapsed_ms = (perf_counter() - started) * 1000
    assert text
    assert elapsed_ms < 400.0, f"STT tardo {elapsed_ms:.1f} ms (presupuesto 400 ms)"


# ---------------------------------------------------------------------------
# Contrato de base.STT y flujo incremental
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_segment_contract_and_stream() -> None:
    """Segment entra y sale por el contrato de base.STT sin mutar el original."""
    stt = DeterministicSTT()
    pcm = _pcm("voice_es_16k.wav")
    segment = Segment(id=7, audio=pcm, is_final=True, latency_ms=42)

    result = await stt.transcribe(segment)
    assert isinstance(result, Segment)
    assert result.id == 7 and result.is_final and result.latency_ms == 42
    assert result.audio == pcm
    assert result.text == await stt.transcribe(pcm)
    assert segment.text == ""

    async def source() -> AsyncIterator[Segment]:
        yield Segment(id=0, audio=pcm[:CHUNK_BYTES], is_final=False)
        yield Segment(id=1, audio=pcm, is_final=True)

    emitted = [item async for item in stt.transcribe_stream(source())]
    assert [item.id for item in emitted] == [0, 1]
    assert [item.is_final for item in emitted] == [False, True]
    assert all(item.text for item in emitted)


# ---------------------------------------------------------------------------
# Motores ausentes y cableado con modelos falsos (sin red ni descargas)
# ---------------------------------------------------------------------------


def test_faster_whisper_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sin el extra ``inference`` falla con MODEL_MISSING y accion concreta."""
    monkeypatch.setitem(sys.modules, "faster_whisper", None)

    with pytest.raises(PipelineError) as exc_info:
        FasterWhisperSTT()

    assert exc_info.value.code == "MODEL_MISSING"
    assert "uv sync --extra inference" in str(exc_info.value)


def test_whisper_cpp_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """El adaptador MIT alternativo da el mismo error accionable."""
    monkeypatch.setitem(sys.modules, "pywhispercpp", None)
    monkeypatch.setitem(sys.modules, "pywhispercpp.model", None)

    with pytest.raises(PipelineError) as exc_info:
        WhisperCppSTT()

    assert exc_info.value.code == "MODEL_MISSING"
    assert "pywhispercpp" in str(exc_info.value)


@pytest.mark.asyncio
async def test_faster_whisper_with_fake_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """El texto del modelo falso se devuelve y el idioma/config se respetan."""
    captured: dict[str, Any] = {}

    class _FakeWhisper:
        def __init__(self, name: str, *, device: str, compute_type: str) -> None:
            captured["model_size"] = name
            captured["device"] = device
            captured["compute_type"] = compute_type

        def transcribe(
            self, samples: np.ndarray, *, language: str | None, beam_size: int
        ) -> tuple[list[object], object]:
            captured["language"] = language
            captured["beam_size"] = beam_size
            captured["dtype"] = str(samples.dtype)
            captured["samples"] = samples.shape[0]
            segments = [types.SimpleNamespace(text="Hola"), types.SimpleNamespace(text=" mundo")]
            return segments, object()

    monkeypatch.setitem(
        sys.modules,
        "faster_whisper",
        _fake_module("faster_whisper", WhisperModel=_FakeWhisper),
    )
    stt = FasterWhisperSTT(model_size="tiny", language="es")
    pcm = _pcm("voice_es_16k.wav")[:CHUNK_BYTES]

    assert await stt.transcribe(pcm, language="en") == "Hola mundo"
    assert captured["language"] == "en"
    assert captured["model_size"] == "tiny"
    assert captured["device"] == "cpu"
    assert captured["compute_type"] == "int8"
    assert captured["beam_size"] == 1
    assert captured["dtype"] == "float32"
    assert captured["samples"] == CHUNK_BYTES // BYTES_PER_SAMPLE

    assert await stt.transcribe(pcm) == "Hola mundo"
    assert captured["language"] == "es"


@pytest.mark.asyncio
async def test_whisper_cpp_with_fake_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """whisper.cpp recibe float32 y devuelve los segmentos del modelo falso."""
    captured: dict[str, Any] = {}

    class _FakeCppModel:
        def __init__(self, name: str) -> None:
            captured["model_size"] = name

        def transcribe(self, samples: np.ndarray) -> list[object]:
            captured["dtype"] = str(samples.dtype)
            return [types.SimpleNamespace(text="Hola"), types.SimpleNamespace(text="mundo")]

    monkeypatch.setitem(
        sys.modules,
        "pywhispercpp.model",
        _fake_module("pywhispercpp.model", Model=_FakeCppModel),
    )
    stt = WhisperCppSTT(model_size="tiny", language="es")
    text = await stt.transcribe(_pcm("voice_es_16k.wav")[:CHUNK_BYTES])

    assert text == "Hola mundo"
    assert captured["model_size"] == "tiny"
    assert captured["dtype"] == "float32"


@pytest.mark.asyncio
async def test_transcribe_is_async_and_non_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El decode corre en un hilo: el event loop sigue avanzando mientras tanto."""
    release = threading.Event()

    class _BlockingWhisper:
        def __init__(self, name: str, *, device: str, compute_type: str) -> None:
            return None

        def transcribe(
            self, samples: np.ndarray, *, language: str | None, beam_size: int
        ) -> tuple[list[object], object]:
            assert release.wait(timeout=2.0), "el modelo falso no fue liberado"
            return [types.SimpleNamespace(text="listo")], object()

    monkeypatch.setitem(
        sys.modules,
        "faster_whisper",
        _fake_module("faster_whisper", WhisperModel=_BlockingWhisper),
    )
    stt = FasterWhisperSTT()
    assert inspect.iscoroutinefunction(stt.transcribe)

    timer = threading.Timer(0.12, release.set)
    timer.start()
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while not release.is_set():
            ticks += 1
            await asyncio.sleep(0.01)

    tick_task = asyncio.create_task(ticker())
    try:
        text = await stt.transcribe(_pcm("voice_es_16k.wav")[:CHUNK_BYTES])
    finally:
        release.set()
        timer.cancel()
    await tick_task

    assert text == "listo"
    assert ticks >= 5, f"el event loop quedo bloqueado (ticks={ticks})"
