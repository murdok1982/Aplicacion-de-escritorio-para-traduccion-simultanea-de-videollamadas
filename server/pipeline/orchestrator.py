"""Orquestador por sesión con colas no bloqueantes y métricas de latencia (TASK-015 / AC-07).

Este módulo cablea las cuatro etapas intercambiables del pipeline (VAD → STT →
Traducción → TTS) en una sesión concurrentes sobre un único ``event loop``.

Decisiones de diseño (SPEC.md §7 / ARCHITECTURE.md §4.4 / ADR-006):

* **Una tarea asyncio por sesión.** ``process_session`` es un generador
  asíncrono: crea un productor (captura) y un *worker* por etapa, todos
  comunicados por :class:`NonBlockingQueue` (un ``asyncio.Queue(maxsize=N)``
  envuelto). El productor NUNCA espera a la cola: si está llena descarta el
  elemento más antiguo y publica un evento ``error`` con código ``OVERRUN``.
  La conexión permanece abierta: no se introduce un tipo ``warning`` nuevo.
* **CPU-bound fuera del event loop.** Cada adaptador ya despacha su inferencia
  con ``asyncio.to_thread``; el orquestador se limita a ``await`` sobre esas
  corrutinas y a medir ``time.perf_counter()`` alrededor de cada etapa.
* **Métricas por cláusula.** Cada cierre del VAD (`is_final=True`) produce un
  :class:`StageMetrics` con el desglose canonico (captura, VAD, STT, MT, TTS,
  red, render y total) y emite el evento ``metrics`` tras ``duck stop``.
* **Orden garantizado.** Los eventos salen por una única cola FIFO: para cada
  cláusula ``subtitle`` → ``translated`` → ``duck start`` → ``tts_audio`` →
  ``duck stop`` → ``metrics``. Los parciales solo emiten ``subtitle``.
* **Cancelación limpia.** ``cancel()`` y ``reset()`` cancelan y ``await`` todas
  las tareas propias, drenan las colas y reinician la segmentación del VAD.

No hay modelos reales ni descargas: los adaptadores inyectados son los
deterministas de ``stt.py``, ``translator.py``, ``tts.py`` y ``vad.py`` (o los
equivalentes de ``mock.py``).
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Annotated, Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from shared.models import LanguageCode

from .base import STT, TTS, VAD, AudioChunk, PipelineError, Segment, Translator

__all__ = [
    "NonBlockingQueue",
    "PipelineEvent",
    "PipelineEventKind",
    "PipelineOrchestrator",
    "StageMetrics",
]

T = TypeVar("T")

_CODE_SCHEMA = "SCHEMA"
_CODE_MODEL_ERROR = "MODEL_ERROR"
_CODE_OVERRUN = "OVERRUN"

PipelineEventKind = Literal["subtitle", "translated", "tts_audio", "duck", "metrics", "error"]


@dataclass(frozen=True, slots=True)
class PipelineEvent:
    """Evento entregado al consumidor del pipeline.

    ``kind`` discrimina el ``payload``: ``subtitle``/``translated``/``duck``/
    ``error`` llevan un ``dict``, ``tts_audio`` lleva los 640 bytes PCM y
    ``metrics`` lleva un ``dict[str, float]`` (``StageMetrics.to_dict()``).
    ``error`` es el canal de las condiciones no fatales de ADR-006 (OVERRUN).
    """

    kind: PipelineEventKind
    payload: Any


@dataclass(slots=True)
class StageMetrics:
    """Desglose de latencia de una cláusula, en milisegundos (SPEC.md §7)."""

    capture_ms: float = 0.0
    vad_ms: float = 0.0
    stt_ms: float = 0.0
    translate_ms: float = 0.0
    tts_ms: float = 0.0
    network_ms: float = 0.0
    render_ms: float = 0.0
    total_ms: float = 0.0

    def to_dict(self) -> dict[str, float]:
        """Serialización plana, apta para el evento JSON ``metrics``."""
        return {
            "capture_ms": self.capture_ms,
            "vad_ms": self.vad_ms,
            "stt_ms": self.stt_ms,
            "translate_ms": self.translate_ms,
            "tts_ms": self.tts_ms,
            "network_ms": self.network_ms,
            "render_ms": self.render_ms,
            "total_ms": self.total_ms,
        }


class NonBlockingQueue(Generic[T]):
    """``asyncio.Queue(maxsize=N)`` con productor estrictamente no bloqueante.

    :meth:`enqueue` usa ``put_nowait``; si la cola está llena retira el elemento
    más antiguo con ``get_nowait`` antes de insertar el nuevo y notifica el
    descarte mediante ``on_overrun``. Nunca llama a ``await`` y por tanto no
    puede bloquear al hilo de captura (ARCHITECTURE.md §4.4).
    """

    def __init__(
        self,
        maxsize: int,
        *,
        name: str = "queue",
        on_overrun: Callable[[str], None] | None = None,
    ) -> None:
        if isinstance(maxsize, bool) or not isinstance(maxsize, int) or maxsize <= 0:
            raise PipelineError(_CODE_SCHEMA, "maxsize debe ser un entero > 0")
        self.maxsize = maxsize
        self.name = name
        self.dropped = 0
        self._on_overrun = on_overrun
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)

    @property
    def queue(self) -> asyncio.Queue[T]:
        """Cola subyacente (útil para inspeccionar ``qsize`` en tests)."""
        return self._queue

    def enqueue(self, item: T) -> bool:
        """Inserta ``item`` sin esperar; ``False`` significa que hubo OVERRUN."""
        try:
            self._queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            self.dropped += 1
            self._queue.put_nowait(item)
            if self._on_overrun is not None:
                self._on_overrun(self.name)
            return False

    async def dequeue(self) -> T:
        """Espera cooperativamente el siguiente elemento (solo el consumidor)."""
        return await self._queue.get()

    def drain(self) -> int:
        """Descarta todo lo pendiente sin bloquear; devuelve cuántos items."""
        removed = 0
        while True:
            try:
                self._queue.get_nowait()
                removed += 1
            except asyncio.QueueEmpty:
                return removed


class _EndOfStream:
    """Sentinela internal de fin de stream (no viaja al consumidor)."""

    __slots__ = ()


_EOS: _EndOfStream = _EndOfStream()
_AudioItem = AudioChunk | _EndOfStream


_SessionIdText = Annotated[str, Field(min_length=1, max_length=64)]


class _SessionParams(BaseModel):
    """Esquema estricto de los parámetros de ``process_session``."""

    model_config = ConfigDict(extra="forbid")

    session_id: _SessionIdText | UUID
    source_lang: LanguageCode
    target_lang: LanguageCode


@dataclass(slots=True)
class _Clause:
    """Contexto compartido por todos los segmentos de una cláusula."""

    id: int
    metrics: StageMetrics = field(default_factory=StageMetrics)


@dataclass(frozen=True, slots=True)
class _WorkItem:
    """Segmento ya etiquetado con la cláusula a la que pertenece."""

    segment: Segment
    clause: _Clause


_WorkItemT = _WorkItem | _EndOfStream


class PipelineOrchestrator:
    """Orquesta un pipeline VAD → STT → MT → TTS por sesión, sin bloqueos.

    Uso típico::

        orchestrator = PipelineOrchestrator(
            vad=EnergyVAD(),
            stt=DeterministicSTT(),
            translator=DeterministicTranslator(target_lang="es"),
            tts=DeterministicTTS(),
        )
        async for event in orchestrator.process_session(
            uuid4(), "en", "es", chunk_iterator
        ):
            ...

    Los adaptadores se inyectan (REQ-04) y nunca se construyen ni descargan
    aquí. ``queue_maxsize`` fija el tamaño de TODAS las colas intermedias y
    ``network_rtt_ms`` simula el RTT LAN del presupuesto de red.
    """

    def __init__(
        self,
        vad: VAD,
        stt: STT,
        translator: Translator,
        tts: TTS,
        *,
        queue_maxsize: int = 50,
        network_rtt_ms: float = 30.0,
    ) -> None:
        self._validate_adapter(vad, "vad", ("process", "flush", "reset"))
        self._validate_adapter(stt, "stt", ("transcribe",))
        self._validate_adapter(translator, "translator", ("translate",))
        self._validate_adapter(tts, "tts", ("synthesize_stream",))
        if isinstance(network_rtt_ms, bool) or not isinstance(network_rtt_ms, int | float):
            raise PipelineError(_CODE_SCHEMA, "network_rtt_ms debe ser un número")
        if not 0.0 <= float(network_rtt_ms) <= 1000.0:
            raise PipelineError(_CODE_SCHEMA, "network_rtt_ms debe estar en [0, 1000]")

        self.vad = vad
        self.stt = stt
        self.translator = translator
        self.tts = tts
        self.queue_maxsize = queue_maxsize
        self.network_rtt_ms = float(network_rtt_ms)

        # Estado observable (una sesión a la vez por instancia).
        self.metrics = StageMetrics()
        self.metrics_history: list[StageMetrics] = []
        self.last_metrics: StageMetrics | None = None
        self.session_id: str = ""
        self.source_lang: str = ""
        self.target_lang: str = ""

        self.output_queue: asyncio.Queue[PipelineEvent | None] = asyncio.Queue()
        self.audio_queue: NonBlockingQueue[_AudioItem] | None = None
        self.segment_queue: NonBlockingQueue[_WorkItemT] | None = None
        self.stt_queue: NonBlockingQueue[_WorkItemT] | None = None
        self.tts_queue: NonBlockingQueue[_WorkItemT] | None = None
        self._queues: list[NonBlockingQueue[Any]] = []

        self._tasks: list[asyncio.Task[None]] = []
        self._watcher: asyncio.Task[None] | None = None
        self._cancelled = False
        self._clause_counter = 0
        self._current_clause: _Clause | None = None
        self._capture_accum = 0.0
        self._vad_accum = 0.0

    # ------------------------------------------------------------------
    # Validación y API pública
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_adapter(
        adapter: object, name: str, methods: tuple[str, ...]
    ) -> None:
        """Comprueba que el adaptante inyectado cumple su interfaz."""
        for method in methods:
            if not callable(getattr(adapter, method, None)):
                raise PipelineError(_CODE_SCHEMA, f"{name} debe implementar {method}()")

    async def process_session(
        self,
        session_id: str | UUID,
        source_lang: str,
        target_lang: str,
        chunks: AsyncIterator[AudioChunk],
    ) -> AsyncIterator[PipelineEvent]:
        """Procesa un stream de chunks y emite eventos hasta cerrar la sesión.

        Los parámetros se validan con un esquema Pydantic estricto antes de
        arrancar ninguna tarea. El productor de ``chunks`` nunca espera a una
        cola: el backpressure se resuelve descartando el elemento más antiguo.
        """
        try:
            params = _SessionParams(
                session_id=session_id,
                source_lang=source_lang,
                target_lang=target_lang,
            )
        except ValidationError as exc:
            detail = "; ".join(
                f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
                for error in exc.errors()
            )
            raise PipelineError(_CODE_SCHEMA, f"parámetros de sesión inválidos: {detail}") from exc
        if not hasattr(chunks, "__aiter__"):
            raise PipelineError(_CODE_SCHEMA, "chunks debe ser un AsyncIterator de AudioChunk")

        self.session_id = str(params.session_id)
        self.source_lang = str(params.source_lang).lower()
        self.target_lang = str(params.target_lang).lower()
        self._prepare_session()

        self.audio_queue = NonBlockingQueue(
            self.queue_maxsize, name="audio", on_overrun=self._on_overrun
        )
        self.segment_queue = NonBlockingQueue(
            self.queue_maxsize, name="vad→stt", on_overrun=self._on_overrun
        )
        self.stt_queue = NonBlockingQueue(
            self.queue_maxsize, name="stt→mt", on_overrun=self._on_overrun
        )
        self.tts_queue = NonBlockingQueue(
            self.queue_maxsize, name="mt→tts", on_overrun=self._on_overrun
        )
        self._queues = [self.audio_queue, self.segment_queue, self.stt_queue, self.tts_queue]

        self._tasks = [
            asyncio.create_task(self._produce(chunks), name="bp-capture"),
            asyncio.create_task(self._vad_loop(), name="bp-vad"),
            asyncio.create_task(self._stt_loop(), name="bp-stt"),
            asyncio.create_task(self._translate_loop(), name="bp-translate"),
            asyncio.create_task(self._tts_loop(), name="bp-tts"),
        ]
        self._watcher = asyncio.create_task(self._watch_tasks(), name="bp-watch")

        try:
            while True:
                event = await self.output_queue.get()
                if event is None:
                    break
                yield event
        finally:
            await self._cleanup()

    async def cancel(self) -> None:
        """Aborta la sesión: cancela todas las tareas y despierta al consumidor."""
        self._cancelled = True
        await self._cleanup()
        self.output_queue.put_nowait(None)

    async def reset(self) -> None:
        """Cancela la sesión y reinicia VAD, colas y métricas para reanudar."""
        self._cancelled = True
        await self._cleanup()
        self._drain_output()
        self.vad.reset()
        self.metrics = StageMetrics()
        self.metrics_history.clear()
        self.last_metrics = None
        self._clause_counter = 0
        self._current_clause = None
        self._capture_accum = 0.0
        self._vad_accum = 0.0
        self._cancelled = False

    @property
    def active_tasks(self) -> list[asyncio.Task[None]]:
        """Tareas propias todavía vivas (para verificar cancelación limpia)."""
        tasks = list(self._tasks)
        if self._watcher is not None:
            tasks.append(self._watcher)
        return [task for task in tasks if not task.done()]

    # ------------------------------------------------------------------
    # Estado interno
    # ------------------------------------------------------------------

    def _prepare_session(self) -> None:
        """Reinicia el estado de la sesión antes de arrancar las tareas."""
        self.vad.reset()
        self.metrics = StageMetrics()
        self.metrics_history.clear()
        self.last_metrics = None
        self._cancelled = False
        self._clause_counter = 0
        self._current_clause = None
        self._capture_accum = 0.0
        self._vad_accum = 0.0
        self._drain_output()

    def _drain_output(self) -> None:
        while True:
            try:
                self.output_queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    def _emit_event(self, kind: PipelineEventKind, payload: Any) -> None:
        """Publica un evento en la cola FIFO de salida (nunca bloquea)."""
        if not self._cancelled:
            self.output_queue.put_nowait(PipelineEvent(kind=kind, payload=payload))

    def _on_overrun(self, queue_name: str) -> None:
        """Emite el evento no fatal ``error OVERRUN`` de ADR-006."""
        self._emit_event(
            "error",
            {
                "code": _CODE_OVERRUN,
                "message": (
                    f"cola {queue_name} llena ({self.queue_maxsize}); "
                    "se descartó el elemento más antiguo"
                ),
                "queue": queue_name,
            },
        )

    def _emit_stage_error(self, stage: str) -> None:
        """Emite un error acotado, sin stack traces ni datos sensibles."""
        self._emit_event(
            "error",
            {"code": _CODE_MODEL_ERROR, "message": f"fallo en la etapa {stage}"},
        )

    def _route_segment(self, segment: Segment) -> None:
        """Etiqueta un segmento del VAD con su cláusula y lo encola hacia STT."""
        assert self.segment_queue is not None  # noqa: S101 - siempre creada en process_session
        if self._current_clause is None:
            self._current_clause = _Clause(id=self._clause_counter)
            self._clause_counter += 1
            self.metrics = self._current_clause.metrics
        clause = self._current_clause
        if segment.is_final:
            clause.metrics.capture_ms = self._capture_accum
            clause.metrics.vad_ms = self._vad_accum
            self._capture_accum = 0.0
            self._vad_accum = 0.0
            self._current_clause = None
        self.segment_queue.enqueue(_WorkItem(segment=segment, clause=clause))

    # ------------------------------------------------------------------
    # Workers (uno por etapa)
    # ------------------------------------------------------------------

    async def _produce(self, chunks: AsyncIterator[AudioChunk]) -> None:
        """Captura: lee el iterator y alimenta la cola sin esperar nunca."""
        assert self.audio_queue is not None  # noqa: S101
        try:
            async for chunk in chunks:
                if self._cancelled:
                    break
                started = time.perf_counter()
                # La captura/resampleo real es upstream; aquí se mide el
                # tiempo de admisión del chunk en el pipeline (presupuesto 20 ms).
                self._capture_accum += (time.perf_counter() - started) * 1000.0
                self.audio_queue.enqueue(chunk)
                # Yield cooperativo: un productor que llega en rafaga no debe
                # hambrunar al worker del VAD (cada chunk real llega cada 20 ms).
                await asyncio.sleep(0)
        except Exception:
            self._emit_stage_error("captura")
        finally:
            self.audio_queue.enqueue(_EOS)

    async def _vad_loop(self) -> None:
        """Consume audio, segmenta por pausas y cierra la cláusula pendiente."""
        assert self.audio_queue is not None and self.segment_queue is not None  # noqa: S101
        try:
            while True:
                item = await self.audio_queue.dequeue()
                if isinstance(item, _EndOfStream):
                    for segment in await self.vad.flush():
                        self._route_segment(segment)
                    break
                started = time.perf_counter()
                segments = await self.vad.process(item)
                self._vad_accum += (time.perf_counter() - started) * 1000.0
                for segment in segments:
                    self._route_segment(segment)
        except Exception:
            self._emit_stage_error("VAD")
        finally:
            self.segment_queue.enqueue(_EOS)

    async def _stt_loop(self) -> None:
        """Transcribe cada segmento, emite ``subtitle`` y pasa al traductor."""
        assert self.segment_queue is not None and self.stt_queue is not None  # noqa: S101
        try:
            while True:
                item = await self.segment_queue.dequeue()
                if isinstance(item, _EndOfStream):
                    break
                started = time.perf_counter()
                transcribed = await self.stt.transcribe(item.segment)
                item.clause.metrics.stt_ms += (time.perf_counter() - started) * 1000.0
                self._emit_event(
                    "subtitle",
                    {
                        "session_id": self.session_id,
                        "stream": "rx",
                        "text": transcribed.text,
                        "is_final": transcribed.is_final,
                        "latency_ms": int(transcribed.latency_ms),
                        "clause_id": item.clause.id,
                    },
                )
                self.stt_queue.enqueue(_WorkItem(segment=transcribed, clause=item.clause))
        except Exception:
            self._emit_stage_error("STT")
        finally:
            self.stt_queue.enqueue(_EOS)

    async def _translate_loop(self) -> None:
        """Traduce solo los segmentos finales y emite ``translated``."""
        assert self.stt_queue is not None and self.tts_queue is not None  # noqa: S101
        try:
            while True:
                item = await self.stt_queue.dequeue()
                if isinstance(item, _EndOfStream):
                    break
                if not item.segment.is_final:
                    continue
                started = time.perf_counter()
                translated = await self.translator.translate(item.segment, self.target_lang)
                item.clause.metrics.translate_ms += (time.perf_counter() - started) * 1000.0
                self._emit_event(
                    "translated",
                    {
                        "text": translated.text,
                        "is_final": True,
                        "lang": self.target_lang,
                        "clause_id": item.clause.id,
                    },
                )
                self.tts_queue.enqueue(_WorkItem(segment=translated, clause=item.clause))
        except Exception:
            self._emit_stage_error("traducción")
        finally:
            self.tts_queue.enqueue(_EOS)

    async def _tts_loop(self) -> None:
        """Duck → TTS streaming → duck → métricas, en orden para cada cláusula."""
        assert self.tts_queue is not None  # noqa: S101
        try:
            while True:
                item = await self.tts_queue.dequeue()
                if isinstance(item, _EndOfStream):
                    break
                clause = item.clause
                started = time.perf_counter()
                self._emit_event(
                    "duck", {"action": "start", "stream": "rx", "clause_id": clause.id}
                )
                clause.metrics.render_ms += (time.perf_counter() - started) * 1000.0

                started = time.perf_counter()
                # Los adaptadores pueden devolver directamente un
                # ``AsyncIterator`` (async generator) o una corrutina que lo
                # produce (mock); se normalizan ambos contratos aquí.
                produced = self.tts.synthesize_stream(item.segment)
                stream: AsyncIterator[bytes]
                if inspect.isawaitable(produced):
                    stream = await produced
                else:
                    stream = produced
                first_chunk = False
                async for pcm in stream:
                    if not first_chunk:
                        clause.metrics.tts_ms += (time.perf_counter() - started) * 1000.0
                        first_chunk = True
                    self._emit_event("tts_audio", pcm)
                if not first_chunk:
                    clause.metrics.tts_ms += (time.perf_counter() - started) * 1000.0

                # RTT LAN simulado: se duerme de verdad (yield cooperativo) y se
                # registra el valor configurado, no el jitter del planificador.
                clause.metrics.network_ms = self.network_rtt_ms
                if self.network_rtt_ms > 0.0:
                    await asyncio.sleep(self.network_rtt_ms / 1000.0)

                started = time.perf_counter()
                self._emit_event(
                    "duck", {"action": "stop", "stream": "rx", "clause_id": clause.id}
                )
                clause.metrics.render_ms += (time.perf_counter() - started) * 1000.0

                clause.metrics.total_ms = sum(
                    (
                        clause.metrics.capture_ms,
                        clause.metrics.vad_ms,
                        clause.metrics.stt_ms,
                        clause.metrics.translate_ms,
                        clause.metrics.tts_ms,
                        clause.metrics.network_ms,
                        clause.metrics.render_ms,
                    )
                )
                self.metrics_history.append(clause.metrics)
                self.last_metrics = clause.metrics
                self.metrics = clause.metrics
                metrics_payload: dict[str, Any] = clause.metrics.to_dict()
                metrics_payload["clause_id"] = clause.id
                self._emit_event("metrics", metrics_payload)
        except Exception:
            self._emit_stage_error("TTS")

    async def _watch_tasks(self) -> None:
        """Espera a los workers y señala el fin del stream al consumidor."""
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                self._emit_stage_error("orquestador")
        self.output_queue.put_nowait(None)

    async def _cleanup(self) -> None:
        """Cancela y awaits todas las tareas propias; idempotente."""
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        watcher = self._watcher
        if watcher is not None:
            if not watcher.done():
                watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
        self._tasks = []
        self._watcher = None
        for queue in self._queues:
            queue.drain()
