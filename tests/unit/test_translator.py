"""Tests de los adaptadores de traduccion (REQ-04 / AC-07 / SPEC.md §6).

Ningun test descarga modelos ni habla por red: los motores ausentes se simulan
con ``sys.modules`` y los motores de produccion se ejercitan con clientes y
modelos falsos inyectados. Tambien se comprueba el guard de licencias, el
endurecimiento SSRF del endpoint LLM y el presupuesto de 350 ms de la etapa MT.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import sys
import threading
import types
from pathlib import Path
from time import perf_counter
from typing import Any

import pytest
from pydantic import SecretStr

import server.pipeline.translator as translator_module
from server.pipeline.base import PipelineError, Segment
from server.pipeline.translator import (
    DeterministicTranslator,
    LLMTranslator,
    OpusMTTranslator,
    validate_model_name,
)


def _fake_module(name: str, **attributes: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


def _model_dir(tmp_path: Path, name: str = "opus-mt-es-en") -> Path:
    path = tmp_path / name
    path.mkdir()
    return path


# ---------------------------------------------------------------------------
# DeterministicTranslator: frases conocidas, estabilidad y reversibilidad
# ---------------------------------------------------------------------------


def test_deterministic_known_and_unknown_texts() -> None:
    """Frases del fixture y texto arbitrario: estable, reversible y sin red."""
    translator = DeterministicTranslator()

    assert asyncio.run(translator.translate("Hola mundo", "es", "en")) == "Hello world"
    assert (
        asyncio.run(translator.translate("Hola, ¿cómo estás?", "es", "en"))
        == "Hello, how are you?"
    )
    assert asyncio.run(translator.translate("Hello world", "en", "es")) == "Hola mundo"

    unknown = "Frase sin diccionario №42 🦜"
    first = asyncio.run(translator.translate(unknown, "es", "en"))
    second = asyncio.run(translator.translate(unknown, "es", "en"))
    assert first == second
    assert first != unknown
    assert translator.decode(first, "es", "en") == unknown
    assert asyncio.run(translator.translate(unknown, "es", "fr")) != first
    assert translator.decode("Hello world", "es", "en") == "Hola mundo"
    assert translator.decode("texto sin marca", "es", "en") == "texto sin marca"

    assert asyncio.run(translator.translate("", "es", "en")) == ""
    assert asyncio.run(translator.translate(unknown, "es", "es")) == unknown


@pytest.mark.asyncio
async def test_segment_contract_preserves_metadata() -> None:
    """El contrato de base.Translator entra y sale sin mutar el Segment."""
    translator = DeterministicTranslator()
    segment = Segment(
        id=7, text="Hola mundo", is_final=True, audio=b"\x00\x01", latency_ms=42
    )

    result = await translator.translate(segment, "en")
    assert isinstance(result, Segment)
    assert result.text == "Hello world"
    assert result.id == 7 and result.is_final and result.latency_ms == 42
    assert result.audio == b"\x00\x01"
    assert segment.text == "Hola mundo"

    via_helper = await translator.translate_segment(segment, "en")
    assert via_helper.text == "Hello world" and via_helper.id == 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("text", "source", "target"),
    [
        (None, "es", "en"),
        ("Hola", "", "en"),
        ("Hola", "es", None),
        ("Hola", "es", "english"),
        (123, "es", "en"),
    ],
)
async def test_translate_validates_external_inputs(
    text: object, source: str, target: str
) -> None:
    """La frontera del adaptador rechaza entradas fuera de contrato con SCHEMA."""
    with pytest.raises(PipelineError) as exc_info:
        await DeterministicTranslator().translate(  # type: ignore[arg-type]
            text, source, target
        )
    assert exc_info.value.code == "SCHEMA"


# ---------------------------------------------------------------------------
# Guard de licencias (SPEC.md §6 / ADR-003)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("token", "license_hint"),
    [
        ("nllb", "CC-BY-NC-4.0"),
        ("xtts", "CPML"),
        ("f5", "CC-BY-NC"),
        ("piper", "GPL-3.0"),
    ],
)
def test_license_guard_rejects_forbidden_models(
    token: str, license_hint: str, tmp_path: Path
) -> None:
    """NLLB/XTTS/F5/Piper se rechazan en ruta, nombre LLM y adaptador de test."""
    model_dir = _model_dir(tmp_path, f"opus-{token}-model")
    factories: tuple[Any, ...] = (
        lambda: OpusMTTranslator(model_dir),
        lambda: LLMTranslator(model=f"{token}-model", client=object()),
        lambda: DeterministicTranslator(model_name=f"{token}-test"),
    )
    for factory in factories:
        with pytest.raises(PipelineError) as exc_info:
            factory()
        assert exc_info.value.code == "LICENSE_VIOLATION"
        assert license_hint in str(exc_info.value)
        assert "SPEC.md §6" in str(exc_info.value)

    with pytest.raises(PipelineError) as exc_info:
        validate_model_name(token.upper())
    assert exc_info.value.code == "LICENSE_VIOLATION"


def test_license_guard_accepts_allowed_models(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Opus-MT y LLM locales autorizados no disparan el guard."""
    monkeypatch.setitem(
        sys.modules,
        "ctranslate2",
        _fake_module("ctranslate2", Translator=lambda *args, **kwargs: object()),
    )
    validate_model_name("opus-mt-es-en")
    validate_model_name("llama3.2")
    assert OpusMTTranslator(_model_dir(tmp_path)) is not None
    assert LLMTranslator(client=object()) is not None


# ---------------------------------------------------------------------------
# OpusMTTranslator: extra ausente, validacion y cableado CT2 sin bloquear
# ---------------------------------------------------------------------------


def test_opusmt_missing_extra(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Sin ctranslate2 se lanza MODEL_MISSING con accion concreta."""
    monkeypatch.setitem(sys.modules, "ctranslate2", None)
    with pytest.raises(PipelineError) as exc_info:
        OpusMTTranslator(_model_dir(tmp_path))
    assert exc_info.value.code == "MODEL_MISSING"
    assert "uv sync --extra inference" in str(exc_info.value)


def test_opusmt_validates_model_path_and_config(tmp_path: Path) -> None:
    """La ruta debe existir y ser directorio; la configuracion es estricta."""
    model_dir = _model_dir(tmp_path)
    with pytest.raises(PipelineError) as exc_info:
        OpusMTTranslator(tmp_path / "no-existe")
    assert exc_info.value.code == "SCHEMA"
    assert "directorio existente" in str(exc_info.value)

    file_path = tmp_path / "modelo.bin"
    file_path.write_bytes(b"ct2")
    with pytest.raises(PipelineError) as exc_info:
        OpusMTTranslator(file_path)
    assert exc_info.value.code == "SCHEMA"

    with pytest.raises(PipelineError) as exc_info:
        OpusMTTranslator(model_dir, device="tpu")
    assert exc_info.value.code == "SCHEMA"
    with pytest.raises(PipelineError) as exc_info:
        OpusMTTranslator(model_dir, beam_size=0)
    assert exc_info.value.code == "SCHEMA"


@pytest.mark.asyncio
async def test_opusmt_translates_via_to_thread_without_blocking(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """El modelo falso traduce en un hilo: el event loop sigue avanzando."""
    captured: dict[str, Any] = {}
    release = threading.Event()

    class _FakeCT2Translator:
        def __init__(self, path: str, *, device: str, compute_type: str) -> None:
            captured["path"] = path
            captured["device"] = device
            captured["compute_type"] = compute_type

        def translate_batch(self, source: list[str], **options: object) -> list[Any]:
            captured["source"] = source
            captured["options"] = options
            assert release.wait(timeout=2.0), "el modelo falso no fue liberado"
            return [[types.SimpleNamespace(tokens=["Hello", "▁world"])]]

    monkeypatch.setitem(
        sys.modules,
        "ctranslate2",
        _fake_module("ctranslate2", Translator=_FakeCT2Translator),
    )
    model_dir = _model_dir(tmp_path)
    translator = OpusMTTranslator(model_dir, beam_size=2)

    original_to_thread = asyncio.to_thread
    thread_calls: list[str] = []

    async def spy_to_thread(func: Any, /, *args: object, **kwargs: object) -> Any:
        thread_calls.append(func.__name__)
        return await original_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(translator_module.asyncio, "to_thread", spy_to_thread)

    assert inspect.iscoroutinefunction(translator.translate)
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
        text = await translator.translate("Hola mundo", "es", "en")
    finally:
        release.set()
        timer.cancel()
    await tick_task

    assert text == "Hello world"
    assert thread_calls == ["_translate_text"]
    assert ticks >= 5, f"el event loop quedo bloqueado (ticks={ticks})"
    assert captured["source"] == ["Hola mundo"]
    assert captured["options"]["beam_size"] == 2
    assert captured["device"] == "cpu"
    assert captured["compute_type"] == "int8"
    assert captured["path"] == str(model_dir)


# ---------------------------------------------------------------------------
# LLMTranslator: SSRF, secreto y respuesta sin envoltorios
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("base_url", "allowed_hosts"),
    [
        ("http://169.254.169.254/latest/meta-data", ()),
        ("file:///etc/passwd", ()),
        ("http://evil.example.com/v1", ()),
        ("http://0.0.0.0:11434/v1", ()),
        ("http://192.168.1.10:11434/v1", ()),
        ("http://192.168.1.10:11434/v1", ("evil.example.com",)),
        ("http://8.8.8.8/v1", ("8.8.8.8",)),
        ("http://user:pass@localhost:11434/v1", ()),
        ("ftp://localhost/v1", ()),
        ("http://metadata.google.internal/v1", ()),
    ],
)
def test_llm_rejects_unsafe_base_url(base_url: str, allowed_hosts: tuple[str, ...]) -> None:
    """Metadata, file://, hosts arbitrarios y credenciales quedan fuera."""
    with pytest.raises(PipelineError) as exc_info:
        LLMTranslator(base_url=base_url, allowed_hosts=allowed_hosts, client=object())
    assert exc_info.value.code == "SCHEMA"


@pytest.mark.parametrize(
    ("base_url", "allowed_hosts"),
    [
        ("http://127.0.0.1:11434/v1", ()),
        ("http://localhost:11434/v1", ()),
        ("http://[::1]:11434/v1", ()),
        ("http://10.0.0.5:8000/v1", ("10.0.0.5",)),
        ("http://gpu-lan.local:8000/v1", ("gpu-lan.local",)),
    ],
)
def test_llm_accepts_localhost_and_allowlisted_lan(
    base_url: str, allowed_hosts: tuple[str, ...]
) -> None:
    """Loopback y LAN explicitamente permitida son los unicos destinos legales."""
    translator = LLMTranslator(
        base_url=base_url, allowed_hosts=allowed_hosts, client=object()
    )
    assert translator.base_url == base_url.rstrip("/")


def test_llm_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sin openai se lanza MODEL_MISSING en lugar de fallar en la primera llamada."""
    monkeypatch.setitem(sys.modules, "openai", None)
    with pytest.raises(PipelineError) as exc_info:
        LLMTranslator()
    assert exc_info.value.code == "MODEL_MISSING"
    assert "uv sync --extra inference" in str(exc_info.value)


def test_llm_openai_client_receives_secret_unmasked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El import lazy construye el cliente con la clave desenmascarada."""
    captured: dict[str, Any] = {}

    class _FakeOpenAI:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setitem(sys.modules, "openai", _fake_module("openai", OpenAI=_FakeOpenAI))
    LLMTranslator(api_key=SecretStr("clave-local-123"), timeout=4.5)
    assert captured["base_url"] == "http://127.0.0.1:11434/v1"
    assert captured["api_key"] == "clave-local-123"
    assert captured["timeout"] == 4.5


@pytest.mark.asyncio
async def test_llm_translation_strips_wrappers_and_hides_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """La salida es solo el texto; la clave nunca aparece en logs ni en errores."""
    captured: dict[str, Any] = {}

    class _Completions:
        def create(self, **kwargs: object) -> object:
            captured.update(kwargs)
            return types.SimpleNamespace(
                choices=[
                    types.SimpleNamespace(
                        message=types.SimpleNamespace(
                            content='```text\n"Hello world"\n```'
                        )
                    )
                ]
            )

    fake_client = types.SimpleNamespace(
        chat=types.SimpleNamespace(completions=_Completions())
    )
    token = "token-super-secreto-ABC123XYZ"
    with caplog.at_level(logging.DEBUG):
        translator = LLMTranslator(client=fake_client, api_key=token, model="llama3.2")
        text = await translator.translate("Hola mundo", "es", "en")

    assert text == "Hello world"
    assert captured["model"] == "llama3.2"
    assert captured["temperature"] == 0.0
    assert "UNICAMENTE" in captured["messages"][0]["content"]
    user_prompt = captured["messages"][1]["content"]
    assert "Hola mundo" in user_prompt and "Idioma destino: en" in user_prompt
    assert token not in caplog.text
    assert token not in str(translator.api_key)
    assert token not in repr(translator.api_key)


def test_llm_rejects_bad_timeout() -> None:
    """Timeout configurable pero acotado para no colgar la etapa MT."""
    with pytest.raises(PipelineError) as exc_info:
        LLMTranslator(timeout=0, client=object())
    assert exc_info.value.code == "SCHEMA"
    with pytest.raises(PipelineError) as exc_info:
        LLMTranslator(timeout=True, client=object())
    assert exc_info.value.code == "SCHEMA"


# ---------------------------------------------------------------------------
# Presupuesto de latencia de la etapa MT (AC-07 / SPEC.md §7)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_translator_stage_latency_budget() -> None:
    """El adaptador determinista responde muy por debajo de los 350 ms."""
    translator = DeterministicTranslator()
    started = perf_counter()
    text = await translator.translate("Hola mundo", "es", "en")
    single_ms = (perf_counter() - started) * 1000
    assert text == "Hello world"
    assert single_ms < 350.0, f"traduccion tardo {single_ms:.1f} ms (presupuesto 350 ms)"

    started = perf_counter()
    for index in range(20):
        await translator.translate(f"clausula numero {index}", "es", "en")
    batch_ms = (perf_counter() - started) * 1000
    assert batch_ms < 350.0, f"20 traducciones tardaron {batch_ms:.1f} ms"
