"""Tests de los adaptadores TTS en streaming (REQ-04 / AC-07 / SPEC.md §6).

Ningun test descarga modelos ni habla por red: los motores ausentes se simulan
con ``sys.modules`` en None y los motores de produccion se ejercitan con modulo
falso inyectado. Se comprueban el formato canonico (640 bytes int16 LE a
16 kHz), el presupuesto de 250 ms del primer chunk, el determinismo, el guard
de licencias y que el G2P sea Misaki sin backend espeak (ADR-004).
"""

from __future__ import annotations

import asyncio
import sys
import threading
import types
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pytest

import server.pipeline.tts as tts_module
from server.pipeline.base import PipelineError, Segment
from server.pipeline.tts import (
    DeterministicTTS,
    KokoroTTS,
    Qwen3TTS,
    configure_g2p,
    validate_voice,
)
from shared.constants import BYTES_PER_SAMPLE, CHUNK_BYTES, CHUNK_SAMPLES, SAMPLE_RATE


def _fake_module(name: str, **attributes: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


def _inject_misaki(monkeypatch: pytest.MonkeyPatch, captured: dict[str, Any]) -> None:
    """Inyecta un ``misaki.en.G2P`` falso y registra como se configura."""

    class _FakeG2P:
        def __init__(self, **kwargs: object) -> None:
            captured["g2p_kwargs"] = kwargs

        def __call__(self, text: str) -> tuple[str, list[str]]:
            captured["g2p_text"] = text
            return "h o l a m u n d o", list("holamundo")

    fake_en = _fake_module("misaki.en", G2P=_FakeG2P)
    monkeypatch.setitem(sys.modules, "misaki", _fake_module("misaki", en=fake_en))
    monkeypatch.setitem(sys.modules, "misaki.en", fake_en)


async def _collect(tts: DeterministicTTS | KokoroTTS | Qwen3TTS, text: str) -> list[bytes]:
    return [chunk async for chunk in tts.synthesize_stream(text)]


# ---------------------------------------------------------------------------
# Formato canonico, latencia y determinismo (SPEC.md §7 / ADR-005)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chunks_are_exact_640_bytes_int16_16k() -> None:
    """Todo chunk es 640 bytes int16 LE; el rate implicito es 16000 Hz."""
    assert SAMPLE_RATE == 16_000
    assert CHUNK_BYTES == CHUNK_SAMPLES * BYTES_PER_SAMPLE == 640

    chunks = await _collect(DeterministicTTS(), "Hola mundo")
    assert len(chunks) >= 2
    assert all(isinstance(chunk, bytes) and len(chunk) == CHUNK_BYTES for chunk in chunks)

    samples = np.frombuffer(chunks[0], dtype="<i2")
    assert samples.dtype == np.dtype("<i2")
    assert samples.size == CHUNK_SAMPLES
    assert int(np.max(np.abs(samples.astype(np.int32)))) > 0, "audio silencioso"


@pytest.mark.asyncio
async def test_first_chunk_within_latency_budget() -> None:
    """El primer chunk tarda <= 250 ms con el adaptador determinista."""
    started = perf_counter()
    stream = DeterministicTTS().synthesize_stream("Hola, ¿cómo estás?")
    first = await anext(stream)
    elapsed_ms = (perf_counter() - started) * 1000
    await stream.aclose()

    assert len(first) == CHUNK_BYTES
    assert elapsed_ms <= 250.0, f"primer chunk tardo {elapsed_ms:.1f} ms (presupuesto 250 ms)"
    print(f"\nLatencia del primer chunk (DeterministicTTS): {elapsed_ms:.3f} ms")


@pytest.mark.asyncio
async def test_deterministic_same_text_same_bytes() -> None:
    """Mismo texto (e idioma/voz) produce exactamente los mismos bytes."""
    tts = DeterministicTTS()
    first = await _collect(tts, "Nos vemos mañana")
    second = await _collect(tts, "Nos vemos mañana")
    assert first and first == second

    other = await _collect(tts, "Frase distinta")
    assert other != first

    joined = await tts.synthesize("Nos vemos mañana")
    assert joined == b"".join(first)


@pytest.mark.asyncio
async def test_segment_contract_and_empty_text() -> None:
    """El contrato de base.TTS acepta Segment; texto vacio no emite audio."""
    tts = DeterministicTTS()
    segment = Segment(id=7, text="Hola mundo", is_final=True, latency_ms=42)
    chunks = [chunk async for chunk in tts.synthesize_stream(segment)]
    assert chunks and all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    assert [chunk async for chunk in tts.synthesize_stream("   ")] == []


@pytest.mark.asyncio
async def test_numpy_fallback_without_fixture(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Sin el WAV, la semilla se sintetiza con numpy y sigue siendo estable."""
    fake_path = tmp_path / "no-existe.wav"
    monkeypatch.setattr(tts_module, "Path", lambda *_args: fake_path)
    tts_module._fixture_samples.cache_clear()
    try:
        seed = tts_module._fixture_samples()
        assert seed.dtype == np.float32
        assert seed.size == SAMPLE_RATE // 2
        assert np.array_equal(seed, tts_module._fixture_samples())
        chunks = await _collect(DeterministicTTS(), "Hola mundo")
        assert chunks and all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    finally:
        tts_module._fixture_samples.cache_clear()


# ---------------------------------------------------------------------------
# Validacion de frontera y guard de licencias (SPEC.md §6 / ADR-004)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "lang", "speed"),
    [
        (123, "es", 1.0),
        ("Hola", "", 1.0),
        ("Hola", "español", 1.0),
        ("Hola", "es", 0.0),
        ("Hola", "es", True),
        ("Hola", "es", 100.0),
    ],
)
@pytest.mark.asyncio
async def test_invalid_inputs_are_rejected(text: object, lang: str, speed: float) -> None:
    """Entradas fuera de contrato se rechazan con codigo SCHEMA."""
    with pytest.raises(PipelineError) as exc_info:
        await anext(
            DeterministicTTS(speed=speed).synthesize_stream(  # type: ignore[arg-type]
                text, lang  # type: ignore[arg-type]
            )
        )
    assert exc_info.value.code == "SCHEMA"


@pytest.mark.parametrize(
    ("token", "license_hint"),
    [
        ("xtts", "CPML"),
        ("f5", "CC-BY-NC"),
        ("piper", "GPL-3.0"),
        ("espeak", "ADR-004"),
    ],
)
def test_license_guard_rejects_forbidden_names(token: str, license_hint: str) -> None:
    """Ningun adaptador admite voices/modelos con licencia prohibida."""
    factories: tuple[Any, ...] = (
        lambda value: DeterministicTTS(voice=value),
        lambda value: DeterministicTTS(model=value),
        lambda value: KokoroTTS(voice=value),
        lambda value: KokoroTTS(model=value),
        lambda value: Qwen3TTS(voice=value),
        lambda value: Qwen3TTS(model=value),
    )
    for factory in factories:
        with pytest.raises(PipelineError) as exc_info:
            factory(f"{token}-engine")
        assert exc_info.value.code == "LICENSE_VIOLATION"
        assert license_hint in str(exc_info.value)
        assert "SPEC.md §6" in str(exc_info.value)

    with pytest.raises(PipelineError) as exc_info:
        validate_voice(f"Mi-{token.upper()}-Voz")
    assert exc_info.value.code == "LICENSE_VIOLATION"


def test_license_guard_accepts_allowed_names() -> None:
    """Kokoro, Qwen3 y Misaki no disparan el guard."""
    validate_voice("af_heart")
    validate_voice("kokoro")
    validate_voice("qwen3-tts")
    validate_voice("es-deterministic")


# ---------------------------------------------------------------------------
# G2P Misaki (MIT), nunca espeak (ADR-004)
# ---------------------------------------------------------------------------


def test_misaki_configured_without_espeak(monkeypatch: pytest.MonkeyPatch) -> None:
    """Se construye misaki.en.G2P(fallback=None): sin espeak ni phonemizer."""
    captured: dict[str, Any] = {}
    _inject_misaki(monkeypatch, captured)

    phonemize = configure_g2p("es")
    assert phonemize("Hola mundo") == "h o l a m u n d o"
    assert captured["g2p_text"] == "Hola mundo"
    assert captured["g2p_kwargs"] == {"fallback": None}

    for forbidden in ("misaki.espeak", "phonemizer", "espeakng_loader"):
        assert forbidden not in sys.modules

    source = Path(tts_module.__file__).read_text(encoding="utf-8")
    assert "from misaki import espeak" not in source
    assert "import phonemizer" not in source
    assert "fallback=None" in source


def test_missing_misaki_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sin misaki el error es MODEL_MISSING y menciona el extra inference."""
    monkeypatch.setitem(sys.modules, "misaki", None)
    monkeypatch.setitem(sys.modules, "misaki.en", None)
    with pytest.raises(PipelineError) as exc_info:
        configure_g2p("es")
    assert exc_info.value.code == "MODEL_MISSING"
    assert "uv sync --extra inference" in str(exc_info.value)
    assert "ADR-004" in str(exc_info.value)


# ---------------------------------------------------------------------------
# KokoroTTS: extra ausente, streaming, to_thread y G2P inyectado
# ---------------------------------------------------------------------------


def test_kokoro_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sin kokoro (ni con misaki falso) falla con MODEL_MISSING sin red."""
    captured: dict[str, Any] = {}
    _inject_misaki(monkeypatch, captured)
    monkeypatch.setitem(sys.modules, "kokoro", None)

    with pytest.raises(PipelineError) as exc_info:
        KokoroTTS()
    assert exc_info.value.code == "MODEL_MISSING"
    assert "uv sync --extra inference" in str(exc_info.value)


@pytest.mark.asyncio
async def test_kokoro_streaming_with_fake_model_uses_to_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El modelo falso corre en un hilo, usa Misaki y emite chunks canonicos."""
    captured: dict[str, Any] = {}
    _inject_misaki(monkeypatch, captured)
    release = threading.Event()

    class _FakePipeline:
        def __init__(self, **kwargs: object) -> None:
            captured["pipeline_kwargs"] = kwargs

        def generate_from_tokens(
            self, phonemes: str, *, voice: str, speed: float
        ) -> Any:
            captured["phonemes"] = phonemes
            captured["voice"] = voice
            captured["speed"] = speed
            assert release.wait(timeout=2.0), "el pipeline falso no fue liberado"
            # 2400 muestras a 24 kHz -> 1600 a 16 kHz -> 5 chunks de 640 bytes.
            yield types.SimpleNamespace(
                audio=np.linspace(-0.5, 0.5, 2400, dtype=np.float32)
            )

    monkeypatch.setitem(
        sys.modules, "kokoro", _fake_module("kokoro", KPipeline=_FakePipeline)
    )
    tts = KokoroTTS(voice="af_heart", speed=1.25)

    original_to_thread = asyncio.to_thread
    thread_calls: list[str] = []

    async def spy_to_thread(func: Any, /, *args: object, **kwargs: object) -> Any:
        thread_calls.append(func.__name__)
        return await original_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(tts_module.asyncio, "to_thread", spy_to_thread)

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
        chunks = await _collect(tts, "Hola mundo")
    finally:
        release.set()
        timer.cancel()
    await tick_task

    assert len(chunks) == 5
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    assert thread_calls == ["_synthesize_samples"]
    assert ticks >= 5, f"el event quedo bloqueado (ticks={ticks})"
    assert captured["phonemes"] == "h o l a m u n d o"
    assert captured["g2p_kwargs"] == {"fallback": None}
    assert captured["voice"] == "af_heart"
    assert captured["speed"] == 1.25
    assert captured["pipeline_kwargs"] == {"lang_code": "a", "device": "cpu", "model": True}


# ---------------------------------------------------------------------------
# Qwen3TTS: extra ausente y streaming con modelo falso
# ---------------------------------------------------------------------------


def test_qwen_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sin el motor Qwen3-TTS el error es MODEL_MISSING y accionable."""
    captured: dict[str, Any] = {}
    _inject_misaki(monkeypatch, captured)
    monkeypatch.setitem(sys.modules, "qwen_tts", None)
    monkeypatch.setitem(sys.modules, "qwen3_tts", None)

    with pytest.raises(PipelineError) as exc_info:
        Qwen3TTS()
    assert exc_info.value.code == "MODEL_MISSING"
    assert "qwen-tts" in str(exc_info.value)


@pytest.mark.asyncio
async def test_qwen_streaming_with_fake_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """El motor falso recibe texto/voz/velocidad/idioma y emite chunks 640."""
    captured: dict[str, Any] = {}
    _inject_misaki(monkeypatch, captured)

    class _FakeEngine:
        def __init__(self, **kwargs: object) -> None:
            captured["engine_kwargs"] = kwargs

        def generate(
            self, *, text: str, voice: str, speed: float, language: str
        ) -> dict[str, Any]:
            captured.update(
                text=text, voice=voice, speed=speed, language=language
            )
            return {"audio": np.linspace(-0.4, 0.4, 2400, dtype=np.float32)}

    monkeypatch.setitem(
        sys.modules, "qwen_tts", _fake_module("qwen_tts", Qwen3TTS=_FakeEngine)
    )
    tts = Qwen3TTS(model_path="/modelos/qwen3", speed=1.1)

    original_to_thread = asyncio.to_thread
    calls: list[str] = []

    async def spy_to_thread(func: Any, /, *args: object, **kwargs: object) -> Any:
        calls.append(func.__name__)
        return await original_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(tts_module.asyncio, "to_thread", spy_to_thread)

    chunks = await _collect(tts, "Hola")
    assert len(chunks) == 5
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    assert calls == ["_synthesize_samples"]
    assert captured["text"] == "Hola"
    assert captured["voice"] == "cherry"
    assert captured["speed"] == 1.1
    assert captured["language"] == "es"
    assert captured["engine_kwargs"] == {"model_path": "/modelos/qwen3"}
    assert captured["g2p_kwargs"] == {"fallback": None}
