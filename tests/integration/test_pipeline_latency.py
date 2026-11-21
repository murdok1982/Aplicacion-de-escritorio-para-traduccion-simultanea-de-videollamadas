"""Tests de integración del orquestador y del presupuesto de latencia (TASK-015 / AC-07).

Ejercitan el pipeline completo sin modelos, sin descargas y sin hardware:
``EnergyVAD`` + ``DeterministicSTT`` + ``DeterministicTranslator`` +
``DeterministicTTS`` sobre los fixtures WAV canónicos de 16 kHz.

Cubren el desglose de SPEC.md §7, el orden de eventos por cláusula, dos
cláusulas con dos ciclos de ducking/métricas, el backpressure no bloqueante con
``OVERRUN`` (ADR-006) y la cancelación/reinicio sin tareas colgadas.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import wave
from collections.abc import AsyncIterator
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import pytest

from server.pipeline.base import STT, AudioChunk, PipelineError, Segment
from server.pipeline.orchestrator import (
    NonBlockingQueue,
    PipelineEvent,
    PipelineOrchestrator,
)
from server.pipeline.stt import DeterministicSTT
from server.pipeline.translator import DeterministicTranslator
from server.pipeline.tts import DeterministicTTS
from server.pipeline.vad import EnergyVAD
from shared.constants import CHUNK_BYTES, CHUNK_MS, SAMPLE_RATE

ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = ROOT / "tests" / "fixtures"
GENERATOR = ROOT / "scripts" / "generate-fixtures.py"

# Presupuesto extremo a extremo y por etapa (SPEC.md §7, AC-07).
BUDGET_MS: dict[str, float] = {
    "capture_ms": 20.0,
    "vad_ms": 300.0,
    "stt_ms": 400.0,
    "translate_ms": 350.0,
    "tts_ms": 250.0,
    "network_ms": 30.0,
    "render_ms": 50.0,
}
TOTAL_BUDGET_MS = 1500.0
RTT_MS = 30.0


@pytest.fixture(scope="session", autouse=True)
def regenerate_fixtures() -> None:
    """Genera los WAV sintéticos una vez por sesión (sin binarios commiteados)."""
    result = subprocess.run(
        [sys.executable, str(GENERATOR)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    for name in ("voice_es_16k.wav", "two_clauses_16k.wav"):
        assert (FIXTURES_DIR / name).is_file(), f"Falta el fixture {name}"


def _pcm(name: str) -> bytes:
    """Lee un WAV canónico 16 kHz mono int16 LE."""
    with wave.open(str(FIXTURES_DIR / name), "rb") as wav:
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == SAMPLE_RATE
        return wav.readframes(wav.getnframes())


def _chunks(name: str) -> list[AudioChunk]:
    """Divide el WAV en chunks canónicos de 640 bytes con sequence monotona."""
    raw = _pcm(name)
    assert raw and len(raw) % CHUNK_BYTES == 0
    return [
        AudioChunk(
            pcm=raw[offset : offset + CHUNK_BYTES],
            sequence=index,
            timestamp_ms=index * CHUNK_MS,
        )
        for index, offset in enumerate(range(0, len(raw), CHUNK_BYTES))
    ]


async def _stream(chunks: list[AudioChunk]) -> AsyncIterator[AudioChunk]:
    for chunk in chunks:
        yield chunk


def _orchestrator(
    *,
    stt: STT | None = None,
    queue_maxsize: int = 50,
) -> PipelineOrchestrator:
    """Construye el pipeline determinista con el RTT LAN simulado."""
    return PipelineOrchestrator(
        vad=EnergyVAD(),
        stt=stt if stt is not None else DeterministicSTT(),
        translator=DeterministicTranslator(source_lang="en", target_lang="es"),
        tts=DeterministicTTS(),
        queue_maxsize=queue_maxsize,
        network_rtt_ms=RTT_MS,
    )


async def _collect(
    orchestrator: PipelineOrchestrator,
    chunks: list[AudioChunk],
    *,
    session_id: object = None,
    source_lang: str = "en",
    target_lang: str = "es",
) -> tuple[list[PipelineEvent], float | None]:
    """Drena la sesión y devuelve eventos y latencia del primer chunk TTS."""
    sid = session_id if session_id is not None else uuid4()
    events: list[PipelineEvent] = []
    first_tts_ms: float | None = None
    started = perf_counter()
    async for event in orchestrator.process_session(
        sid, source_lang, target_lang, _stream(chunks)
    ):
        if event.kind == "tts_audio" and first_tts_ms is None:
            first_tts_ms = (perf_counter() - started) * 1000.0
        events.append(event)
    return events, first_tts_ms


def _metrics(events: list[PipelineEvent]) -> list[dict[str, Any]]:
    return [event.payload for event in events if event.kind == "metrics"]


def _duck(events: list[PipelineEvent], action: str) -> list[PipelineEvent]:
    return [
        event
        for event in events
        if event.kind == "duck" and event.payload["action"] == action
    ]


def _order_signature(events: list[PipelineEvent]) -> list[str]:
    """Firma de eventos relevantes al orden (excluye ``translated``)."""
    signature: list[str] = []
    for event in events:
        if event.kind == "subtitle":
            signature.append("subtitle_final" if event.payload["is_final"] else "subtitle")
        elif event.kind == "duck":
            signature.append(f"duck_{event.payload['action']}")
        elif event.kind == "tts_audio":
            signature.append("tts_audio")
        elif event.kind == "metrics":
            signature.append("metrics")
    return signature


# ---------------------------------------------------------------------------
# AC-07: presupuesto de latencia por etapa y primer chunk TTS < 1500 ms
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac07_latency_budget() -> None:
    """Cláusula de voz: primer TTS < 1500 ms y cada etapa dentro de su presupuesto."""
    orchestrator = _orchestrator()
    events, first_tts_ms = await _collect(orchestrator, _chunks("voice_es_16k.wav"))

    assert first_tts_ms is not None
    assert first_tts_ms < TOTAL_BUDGET_MS, f"primer TTS tardó {first_tts_ms:.1f} ms"

    metrics = _metrics(events)
    assert len(metrics) == 1
    measured = metrics[0]
    for stage, limit in BUDGET_MS.items():
        assert measured[stage] <= limit, f"{stage}={measured[stage]:.3f} ms > {limit} ms"
    assert measured["network_ms"] == pytest.approx(RTT_MS)
    assert measured["total_ms"] < TOTAL_BUDGET_MS
    assert measured["total_ms"] == pytest.approx(sum(measured[k] for k in BUDGET_MS))
    assert orchestrator.last_metrics is not None
    canonical = orchestrator.last_metrics.to_dict()
    assert all(canonical[stage] == measured[stage] for stage in BUDGET_MS)


@pytest.mark.asyncio
async def test_event_order_single_clause() -> None:
    """Una cláusula: subtitle → duck start → tts_audio → duck stop → metrics."""
    orchestrator = _orchestrator()
    events, _ = await _collect(orchestrator, _chunks("voice_es_16k.wav"))

    signature = _order_signature(events)
    assert signature[0] == "subtitle"
    assert signature[-1] == "metrics"
    assert "subtitle_final" in signature

    final_index = signature.index("subtitle_final")
    assert signature[final_index + 1] == "duck_start"
    stop_index = signature.index("duck_stop")
    assert signature[stop_index + 1] == "metrics"
    assert stop_index > final_index + 1
    assert all(item == "tts_audio" for item in signature[final_index + 2 : stop_index])

    chunks_audio = [event.payload for event in events if event.kind == "tts_audio"]
    assert chunks_audio
    assert all(len(pcm) == CHUNK_BYTES for pcm in chunks_audio)

    # El evento ``translated`` se emite tras el subtítulo final y antes del duck.
    kinds = [event.kind for event in events]
    idx_final = next(
        index
        for index, event in enumerate(events)
        if event.kind == "subtitle" and event.payload["is_final"]
    )
    assert idx_final < kinds.index("translated") < kinds.index("duck")


@pytest.mark.asyncio
async def test_two_clauses_produce_two_metric_sets_and_duck_cycles() -> None:
    """Dos cláusulas separadas por silencio → 2 métricas y 2 ciclos de duck."""
    orchestrator = _orchestrator()
    events, first_tts_ms = await _collect(orchestrator, _chunks("two_clauses_16k.wav"))

    assert first_tts_ms is not None and first_tts_ms < TOTAL_BUDGET_MS
    metrics = _metrics(events)
    assert len(metrics) == 2
    assert [event.payload["clause_id"] for event in events if event.kind == "metrics"] == [0, 1]
    assert len(_duck(events, "start")) == 2
    assert len(_duck(events, "stop")) == 2
    assert len([event for event in events if event.kind == "translated"]) == 2
    assert len(orchestrator.metrics_history) == 2

    # Los eventos ``tts_audio`` llevan PCM crudo (sin ``clause_id``). Se
    # agrupan por la cláusula con ``duck start`` activo: el bucle TTS es
    # secuencial y emite ``duck start -> tts_audio... -> duck stop`` sin
    # interleaving de otras cláusulas. No vale "último evento con clause_id":
    # los stages corren en paralelo y un ``subtitle`` parcial de la cláusula
    # siguiente puede intercalarse antes del ``tts_audio`` de la anterior
    # (carrera de planificación, no error del pipeline).
    grouped: dict[int, list[PipelineEvent]] = {0: [], 1: []}
    active_tts_clause: int | None = None
    for event in events:
        if (
            event.kind == "duck"
            and isinstance(event.payload, dict)
            and event.payload["action"] == "start"
        ):
            active_tts_clause = int(event.payload["clause_id"])
        if isinstance(event.payload, dict) and "clause_id" in event.payload:
            grouped[int(event.payload["clause_id"])].append(event)
        elif event.kind == "tts_audio" and active_tts_clause is not None:
            grouped[active_tts_clause].append(event)

    for clause_id in (0, 1):
        clause_events = grouped[clause_id]
        final_indexes = [
            index
            for index, event in enumerate(clause_events)
            if event.kind == "subtitle" and event.payload["is_final"]
        ]
        assert len(final_indexes) == 1
        final_index = final_indexes[0]
        kinds = [event.kind for event in clause_events[final_index:]]
        # subtitle(final), [translated], duck(start), tts_audio..., duck(stop), metrics
        assert kinds[0] == "subtitle"
        assert kinds[1] == "translated"
        assert kinds[2] == "duck"
        assert clause_events[final_index + 2].payload["action"] == "start"
        assert "tts_audio" in kinds
        assert kinds[-2] == "duck"
        assert clause_events[-2].payload["action"] == "stop"
        assert kinds[-1] == "metrics"


# ---------------------------------------------------------------------------
# Colas no bloqueantes y OVERRUN (ARCHITECTURE.md §4.4 / ADR-006)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nonblocking_queue_drops_oldest_without_waiting() -> None:
    """La cola acotada descarta el más antiguo y avisa, sin ``await``."""
    seen: list[str] = []
    queue: NonBlockingQueue[int] = NonBlockingQueue(2, name="audio", on_overrun=seen.append)

    assert queue.enqueue(1) is True
    assert queue.enqueue(2) is True
    started = perf_counter()
    assert queue.enqueue(3) is False
    assert (perf_counter() - started) * 1000.0 < 10.0
    assert seen == ["audio"]
    assert queue.dropped == 1
    assert queue.queue.qsize() == 2
    assert await queue.dequeue() == 2
    assert await queue.dequeue() == 3
    assert queue.drain() == 0


class _SlowSTT(STT):
    """STT determinista con retardo artificial para forzar backpressure."""

    def __init__(self, delay_s: float) -> None:
        self.delay_s = delay_s
        self.inner = DeterministicSTT()

    async def transcribe(self, segment: Segment) -> Segment:
        await asyncio.sleep(self.delay_s)
        return await self.inner.transcribe(segment)


@pytest.mark.asyncio
async def test_backpressure_emits_overrun_and_never_blocks() -> None:
    """Con un consumidor lento, la captura sigue y se emite ``error OVERRUN``."""
    orchestrator = _orchestrator(stt=_SlowSTT(0.01), queue_maxsize=2)
    chunks = _chunks("voice_es_16k.wav")

    events, _ = await asyncio.wait_for(_collect(orchestrator, chunks), timeout=10.0)

    overruns = [
        event
        for event in events
        if event.kind == "error" and event.payload["code"] == "OVERRUN"
    ]
    assert overruns
    assert all("cola" in event.payload["message"] for event in overruns)
    assert any(queue.dropped > 0 for queue in orchestrator._queues)
    assert events[-1].kind == "metrics"


# ---------------------------------------------------------------------------
# Cancelación / reinicio y validación en la frontera
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_and_reset_leave_no_pending_tasks() -> None:
    """``cancel()`` y ``reset()`` drenan las colas y no dejan tareas vivas."""
    orchestrator = _orchestrator()
    chunks = _chunks("voice_es_16k.wav")

    async def slow_stream() -> AsyncIterator[AudioChunk]:
        for chunk in chunks:
            yield chunk
            await asyncio.sleep(0.002)

    generator = orchestrator.process_session(uuid4(), "en", "es", slow_stream())
    first = await asyncio.wait_for(generator.__anext__(), timeout=5.0)
    assert first.kind == "subtitle"
    assert orchestrator.active_tasks

    await orchestrator.cancel()
    tail = await asyncio.wait_for(_drain(generator), timeout=5.0)
    assert isinstance(tail, list)
    assert not orchestrator.active_tasks
    assert not _live_pipeline_tasks()

    await orchestrator.reset()
    assert orchestrator.metrics_history == []
    assert orchestrator.last_metrics is None
    assert not orchestrator.active_tasks

    # Tras el reinicio, el VAD segmenta desde cero y la sesión funciona.
    events, _ = await _collect(orchestrator, chunks)
    assert len(_metrics(events)) == 1
    assert not orchestrator.active_tasks
    assert not _live_pipeline_tasks()


async def _drain(generator: AsyncIterator[PipelineEvent]) -> list[PipelineEvent]:
    return [event async for event in generator]


def _live_pipeline_tasks() -> list[asyncio.Task[object]]:
    return [
        task
        for task in asyncio.all_tasks()
        if task.get_name().startswith("bp-") and not task.done()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("session_id", "source_lang", "target_lang"),
    [
        ("x" * 100, "en", "es"),
        ("", "en", "es"),
        (uuid4(), "123", "es"),
        (uuid4(), "en", "bad-language-code!!"),
    ],
)
async def test_invalid_session_parameters_rejected(
    session_id: object, source_lang: str, target_lang: str
) -> None:
    """La frontera rechaza parámetros inválidos con ``error SCHEMA``."""
    orchestrator = _orchestrator()
    with pytest.raises(PipelineError) as exc_info:
        async for _ in orchestrator.process_session(
            session_id, source_lang, target_lang, _stream([])
        ):
            pass
    assert exc_info.value.code == "SCHEMA"
    assert not orchestrator.active_tasks
