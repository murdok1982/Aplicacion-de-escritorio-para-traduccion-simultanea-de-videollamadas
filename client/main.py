"""Orquestador del modo vídeo (YouTube/VLC) de BabelPipe (TASK-017 / REQ-05).

Contrato (SPEC.md AC-08 / AC-09, ARCHITECTURE.md §2.1, §4.3 y §7 ADR-002):

* El audio multimedia se enruta a ``Translator_Rx_Sink``. El monitor de ese sink
  se captura a 48 kHz (``pcm_s16le``) y se entrega a :class:`RxStream`, que lo
  resamplea a 16 kHz y lo envía por el WebSocket al servidor de inferencia.
* Los eventos ``subtitle`` y ``translated`` se normalizan en :class:`SubtitleEvent`
  y se publican al callback ``on_subtitle`` (que TASK-018 conectará al overlay
  GTK; este módulo no importa GTK). Los eventos ``duck`` gobiernan el estado del
  :class:`~client.audio.ducking.DuckingMixer`.
* El TTS recibido se resamplea a 48 kHz y se mezcla con el original del monitor
  mediante el mezclador con ducking; la salida se escribe en el sink físico con
  ``transport.write_physical_sink_48k()``. El ducking se mantiene activo hasta
  que se han renderizado todos los chunks TTS de la cláusula, aunque el evento
  ``duck stop`` llegue antes de que el último chunk entre en la cola.
* :meth:`VideoModeOrchestrator.run` es asíncrona y se detiene de forma limpia con
  un :class:`asyncio.Event` (:meth:`VideoModeOrchestrator.stop`). :meth:`cleanup`
  descarga el grafo con ``PipeWireGraph.destroy_all()``.

La frontera de transporte es :class:`~client.audio.transport.AudioTransport`:
su lectura de monitor es el límite del grafo en modo vídeo. En producción
:class:`PipeWireRxMonitor` usa ``pw-record --target <rx_sink>.monitor`` para
capturar exactamente ese nodo; en tests se inyecta un transporte determinista.
No hay mocks en esta ruta de producción.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import math
import os
import re
import signal
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from client.audio.ducking import (
    DEFAULT_DUCK_LEVEL,
    DEFAULT_RAMP_MS,
    DUCK_LEVEL_MAX,
    DUCK_LEVEL_MIN,
    DuckingMixer,
)
from client.audio.pipewire_graph import RX_SINK_NAME, PipeWireGraph
from client.audio.resampler import resample_to_48k
from client.audio.transport import (
    AudioProcess,
    AudioTransport,
    PipeWireAudioTransport,
    ProcessRunner,
    SubprocessProcessRunner,
    TransportError,
)
from client.net.stream import RxStream
from client.net.ws_client import WSClient, WSClientSettings
from shared.constants import CHUNK_MS, PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE, STREAM_RX
from shared.models import (
    DuckMessage,
    ErrorMessage,
    SubtitleMessage,
    TranslatedMessage,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import AudioFrame

logger = logging.getLogger("babelpipe.client.video")

CAPTURE_SAMPLES: int = PIPEWIRE_SAMPLE_RATE * CHUNK_MS // 1000
"""Un chunk canónico de 20 ms a 48 kHz (960 muestras)."""

_VOICE_RMS_THRESHOLD: float = 100.0
"""RMS en LSB a partir del cual se considera que el instante contiene voz."""

_DUCK_RELEASE_GRACE_S: float = 0.5
"""Tiempo máximo que se mantiene el ducking si no llega ningún chunk TTS."""

_NODE_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
_END = object()
_STOP_TIMEOUT_S = 5.0
_MONITOR_MAX_SAMPLES = PIPEWIRE_SAMPLE_RATE


class VideoAudioSettings(BaseModel):
    """Parámetros de audio del modo vídeo, validados en la frontera."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    duck_level: float = Field(default=DEFAULT_DUCK_LEVEL, ge=DUCK_LEVEL_MIN, le=DUCK_LEVEL_MAX)
    ramp_ms: float = Field(default=DEFAULT_RAMP_MS, gt=0.0, le=1000.0)
    rx_sink: str = Field(default=RX_SINK_NAME, min_length=1, max_length=128)

    @field_validator("rx_sink")
    @classmethod
    def _validate_sink(cls, value: str) -> str:
        if _NODE_NAME_RE.fullmatch(value) is None:
            raise ValueError("rx_sink contiene caracteres no permitidos")
        return value


class VideoPipelineSettings(BaseModel):
    """Idiomas y componentes negociados con el servidor de inferencia."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_lang: str = Field(default="en", min_length=2, max_length=10)
    target_lang: str = Field(default="es", min_length=2, max_length=10)
    vad: str = Field(default="mock-vad", min_length=1, max_length=64)
    stt: str = Field(default="mock-stt", min_length=1, max_length=64)
    translator: str = Field(default="mock-translator", min_length=1, max_length=64)
    tts: str = Field(default="mock-tts", min_length=1, max_length=64)


class VideoModeSettings(BaseModel):
    """Configuración estricta del orquestador de modo vídeo."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    audio: VideoAudioSettings = Field(default_factory=VideoAudioSettings)
    pipeline: VideoPipelineSettings = Field(default_factory=VideoPipelineSettings)
    capture_samples: int = Field(default=CAPTURE_SAMPLES, ge=1, le=_MONITOR_MAX_SAMPLES)
    queue_maxsize: int = Field(default=50, ge=1, le=10_000)
    create_rx_sink: bool = True


@dataclass(frozen=True, slots=True)
class SubtitleEvent:
    """Subtítulo normalizado para el overlay (texto ya escapado por GTK)."""

    text: str
    is_final: bool
    lang: str
    kind: Literal["subtitle", "translated"]
    latency_ms: int | None = None


@dataclass(slots=True)
class VideoModeMetrics:
    """Instantánea de contadores observables del modo vídeo."""

    chunks: int = 0
    subtitles: int = 0
    translated: int = 0
    duck_starts: int = 0
    duck_stops: int = 0
    tts_chunks: int = 0
    tts_samples: int = 0
    rendered_chunks: int = 0
    tts_rendered_chunks: int = 0
    errors: int = 0
    tts_latency_ms: float = 0.0

    def __getitem__(self, key: str) -> float:
        """Acceso tipo dict para informes y tests."""
        return float(getattr(self, key))


SubtitleCallback = Callable[[SubtitleEvent], None | Awaitable[None]]
MixedCallback = Callable[[bytes, int, bool], None | Awaitable[None]]
MonitorReader = Callable[[int], NDArray[np.int16]]
Clock = Callable[[], float]


class PipeWireRxMonitor:
    """Captura real del monitor de ``Translator_Rx_Sink`` con ``pw-record``.

    Es la implementación de producción del lector de monitor: no ejecuta shells,
    no usa ``sudo`` y delega la creación del proceso en el ``ProcessRunner``
    inyectable (el mismo que usan los tests de transporte para prohibir
    comandos reales).
    """

    def __init__(self, sink_name: str = RX_SINK_NAME, runner: ProcessRunner | None = None) -> None:
        if not isinstance(sink_name, str) or _NODE_NAME_RE.fullmatch(sink_name) is None:
            raise ValueError("sink_name contiene caracteres no permitidos")
        self.sink_name = sink_name
        self._runner: ProcessRunner = runner if runner is not None else SubprocessProcessRunner()
        self._process: AudioProcess | None = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Abre ``pw-record`` sobre ``<rx_sink>.monitor`` (idempotente)."""
        if self._running:
            return
        try:
            self._process = self._runner.spawn(
                (
                    "pw-record",
                    "--format=s16",
                    f"--rate={PIPEWIRE_SAMPLE_RATE}",
                    "--channels=1",
                    "--target",
                    f"{self.sink_name}.monitor",
                )
            )
        except TransportError:
            self._process = None
            raise
        self._running = True

    def __call__(self, num_samples: int) -> NDArray[np.int16]:
        """Lee ``num_samples`` int16 LE del monitor (bloquea hasta completar)."""
        if isinstance(num_samples, bool) or not isinstance(num_samples, int):
            raise ValueError("num_samples debe ser int")
        if not 0 < num_samples <= _MONITOR_MAX_SAMPLES:
            raise ValueError(f"num_samples fuera de rango: {num_samples}")
        if not self._running or self._process is None or self._process.stdout is None:
            raise TransportError("el monitor del Rx no está iniciado; llame a start()")
        remaining = num_samples * 2
        parts: list[bytes] = []
        while remaining > 0:
            part = self._process.stdout.read(remaining)
            if not part:
                raise TransportError("el flujo del monitor de Translator_Rx_Sink terminó")
            parts.append(part)
            remaining -= len(part)
        return np.frombuffer(b"".join(parts), dtype="<i2").copy()

    def stop(self) -> None:
        """Cierra el proceso de captura; idempotente y seguro tras señal."""
        if not self._running:
            return
        self._running = False
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            with suppress(OSError):
                process.stdin.close()
        with suppress(OSError):
            process.terminate()
        if isinstance(self._runner, SubprocessProcessRunner):
            self._runner.wait(process)
        else:
            with suppress(OSError):
                process.wait(timeout=_STOP_TIMEOUT_S)
        self._process = None


def _rms_int16(samples: NDArray[np.int16]) -> float:
    """RMS en LSB int16 de un bloque PCM, para marcar el instante de voz."""
    if samples.size == 0:
        return 0.0
    values = samples.astype(np.float64)
    return float(math.sqrt(float(np.mean(np.square(values)))))


async def _invoke(callback: Callable[..., Any], *args: Any) -> None:
    """Invoca un callback síncrono o asíncrono sin bloquear el event loop."""
    result = callback(*args)
    if result is not None and inspect.isawaitable(result):
        await result


async def _consume(result: Awaitable[None]) -> None:
    """Envuelve el awaitable devuelto por un callback en una tarea controlada."""
    await result


class VideoModeOrchestrator:
    """Integra monitor Rx → WS → ducking → sink físico → callback de subtítulos."""

    def __init__(
        self,
        settings: VideoModeSettings,
        transport: AudioTransport,
        ws_client: WSClient,
        *,
        on_subtitle: SubtitleCallback | None = None,
        on_mixed: MixedCallback | None = None,
        graph: PipeWireGraph | None = None,
        monitor: MonitorReader | None = None,
        clock: Clock = time.monotonic,
    ) -> None:
        if not isinstance(settings, VideoModeSettings):
            raise ValueError("settings debe ser VideoModeSettings")
        if not isinstance(transport, AudioTransport):
            raise ValueError("transport debe ser una implementación de AudioTransport")
        if not isinstance(ws_client, WSClient):
            raise ValueError("ws_client debe ser un WSClient")
        if on_subtitle is not None and not callable(on_subtitle):
            raise ValueError("on_subtitle debe ser invocable o None")
        if on_mixed is not None and not callable(on_mixed):
            raise ValueError("on_mixed debe ser invocable o None")
        if not callable(clock):
            raise ValueError("clock debe ser invocable")

        self.settings = settings
        self._transport = transport
        self._ws_client = ws_client
        self._on_subtitle_callback = on_subtitle
        self._on_mixed_callback = on_mixed
        self._graph = graph if graph is not None else PipeWireGraph()
        self._monitor = (
            monitor
            if monitor is not None
            else cast("MonitorReader", transport.read_physical_sink_monitor_48k)
        )
        monitor_start = getattr(self._monitor, "start", None)
        monitor_stop = getattr(self._monitor, "stop", None)
        self._monitor_start = (
            cast("Callable[[], None]", monitor_start) if callable(monitor_start) else None
        )
        self._monitor_stop = (
            cast("Callable[[], None]", monitor_stop) if callable(monitor_stop) else None
        )
        self._clock = clock
        self._mixer = DuckingMixer(
            duck_level=settings.audio.duck_level, ramp_ms=settings.audio.ramp_ms
        )
        self._original_queue: asyncio.Queue[bytes | object] = asyncio.Queue(
            maxsize=settings.queue_maxsize
        )
        self._tts_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=settings.queue_maxsize)
        self._stop = asyncio.Event()
        self._running = False
        self._monitor_started = False
        self._transport_started = False
        self._ducking = False
        self._duck_release_pending = False
        self._duck_stop_at: float | None = None
        self._tts_announced_for_clause = 0
        self._tts_rendered_for_clause = 0
        self._last_voice_ts: float | None = None
        self._metrics = VideoModeMetrics()
        self._tts_tasks: set[asyncio.Task[None]] = set()
        self._callback_tasks: set[asyncio.Task[None]] = set()

    @property
    def mixer(self) -> DuckingMixer:
        """Mezclador usado por el render (permite inspeccionar la ganancia)."""
        return self._mixer

    @property
    def metrics(self) -> VideoModeMetrics:
        """Copia de los contadores, legible mientras el stream está activo."""
        return replace(self._metrics)

    @property
    def is_running(self) -> bool:
        return self._running

    def stop(self) -> None:
        """Solicita la parada limpia mediante el ``asyncio.Event`` interno."""
        self._stop.set()
        if not self._original_queue.full():
            self._original_queue.put_nowait(_END)

    def cleanup(self) -> None:
        """Descarga los módulos creados por esta sesión (idempotente)."""
        self._graph.destroy_all()

    async def run(self) -> None:
        """Ejecuta captura, stream Rx y render hasta :meth:`stop` o error."""
        if self._running:
            raise RuntimeError("VideoModeOrchestrator.run ya está en ejecución")
        self._running = True
        self._stop.clear()
        self._ducking = False
        self._duck_release_pending = False
        self._duck_stop_at = None
        self._tts_announced_for_clause = 0
        self._tts_rendered_for_clause = 0
        self._last_voice_ts = None
        self._mixer.reset()

        rx = RxStream(
            source_rate=PIPEWIRE_SAMPLE_RATE,
            queue_maxsize=self.settings.queue_maxsize,
            on_subtitle=self._on_subtitle,
            on_translated=self._on_translated,
            on_duck=self._on_duck,
            on_error=self._on_error,
        )
        previous_audio = self._ws_client.on_audio_frame
        previous_text = self._ws_client._handle_text

        def _on_audio(frame: AudioFrame) -> None:
            if frame.stream_id == STREAM_RX:
                task = asyncio.create_task(self._on_tts_frame(frame), name="babelpipe-video-tts")
                self._tts_tasks.add(task)
                task.add_done_callback(self._tts_tasks.discard)

        def _handle_text(raw: str) -> None:
            previous_text(raw)
            try:
                message = parse_control_message(raw)
            except ValueError:
                return
            if isinstance(message, TtsChunkMessage):
                # El anuncio y el PCM comparten seq; se libera para que el
                # dedup global del WSClient no descarte el audio del TTS.
                self._ws_client._seen_set.discard(message.seq)
                self._tts_announced_for_clause += 1

        self._ws_client.on_audio_frame = _on_audio
        self._ws_client._handle_text = _handle_text  # type: ignore[method-assign]

        tasks: list[asyncio.Task[Any]] = []
        try:
            if self.settings.create_rx_sink:
                await asyncio.to_thread(self._graph.create_rx_sink)
            self._transport.start()
            self._transport_started = True
            if self._monitor_start is not None:
                await asyncio.to_thread(self._monitor_start)
                self._monitor_started = True
            await self._ws_client.connect()

            tasks = [
                asyncio.create_task(rx.run(self._ws_client), name="babelpipe-video-rx"),
                asyncio.create_task(self._capture_loop(rx), name="babelpipe-video-capture"),
                asyncio.create_task(self._render_loop(), name="babelpipe-video-render"),
                asyncio.create_task(self._stop.wait(), name="babelpipe-video-stopper"),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not tasks[3] and not task.cancelled():
                    exc = task.exception()
                    if exc is not None:
                        raise exc
        finally:
            self._stop.set()
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            for task in tuple(self._tts_tasks):
                task.cancel()
            if self._tts_tasks:
                await asyncio.gather(*tuple(self._tts_tasks), return_exceptions=True)
            self._tts_tasks.clear()
            for task in tuple(self._callback_tasks):
                task.cancel()
            if self._callback_tasks:
                await asyncio.gather(*tuple(self._callback_tasks), return_exceptions=True)
            self._callback_tasks.clear()
            self._ws_client.on_audio_frame = previous_audio
            self._ws_client._handle_text = previous_text  # type: ignore[method-assign]
            if self._transport_started:
                self._transport.stop()
                self._transport_started = False
            if self._monitor_started and self._monitor_stop is not None:
                await asyncio.to_thread(self._monitor_stop)
                self._monitor_started = False
            self._running = False
            logger.info(
                "video_mode_stopped",
                extra={
                    "chunks": self._metrics.chunks,
                    "rendered": self._metrics.rendered_chunks,
                    "tts_chunks": self._metrics.tts_chunks,
                    "errors": self._metrics.errors,
                },
            )

    # ------------------------------------------------------------------ bucles

    async def _capture_loop(self, rx: RxStream) -> None:
        """Lee el monitor a 48 kHz y alimenta RxStream y el mezclador."""
        while not self._stop.is_set():
            samples = await asyncio.to_thread(self._monitor, self.settings.capture_samples)
            if not self._ducking and _rms_int16(samples) > _VOICE_RMS_THRESHOLD:
                # Se congela en cuanto arranca el doblaje para que la latencia
                # incluya la pausa del VAD y no el tono de cola posterior.
                self._last_voice_ts = self._clock()
            await rx.push_async(samples)
            self._metrics.chunks += 1
            original = samples.astype("<i2", copy=False).tobytes()
            try:
                self._original_queue.put_nowait(original)
            except asyncio.QueueFull:
                with suppress(asyncio.QueueEmpty):
                    self._original_queue.get_nowait()
                self._original_queue.put_nowait(original)

    async def _render_loop(self) -> None:
        """Mezcla original + TTS con ducking y escribe en el sink físico."""
        index = 0
        while not self._stop.is_set():
            item = await self._original_queue.get()
            if item is _END:
                return
            original = cast("bytes", item)
            tts = b""
            had_tts = False
            try:
                tts = self._tts_queue.get_nowait()
                had_tts = True
            except asyncio.QueueEmpty:
                pass

            mixed = await asyncio.to_thread(
                self._mixer.process,
                original,
                tts,
                ducking_active=self._ducking,
            )
            if not isinstance(mixed, bytes):
                mixed = np.asarray(mixed, dtype="<i2").tobytes()
            written = await asyncio.to_thread(self._transport.write_physical_sink_48k, mixed)
            self._metrics.rendered_chunks += 1
            if had_tts:
                # `duck stop` puede adelantarse a los frames TTS: el doblaje
                # sigue con el original atenuado hasta que todos los chunks
                # anunciados hayan sido renderizados.
                self._ducking = True
                self._metrics.tts_rendered_chunks += 1
                self._tts_rendered_for_clause += 1
            if self._duck_release_pending and self._tts_queue.empty():
                grace_expired = (
                    self._duck_stop_at is not None
                    and self._clock() - self._duck_stop_at >= _DUCK_RELEASE_GRACE_S
                )
                if (
                    self._tts_rendered_for_clause >= self._tts_announced_for_clause
                    or grace_expired
                ):
                    self._ducking = False
            if self._on_mixed_callback is not None:
                task = asyncio.create_task(
                    _invoke(self._on_mixed_callback, mixed, index, had_tts),
                    name="babelpipe-video-mixed-cb",
                )
                self._callback_tasks.add(task)
                task.add_done_callback(self._callback_tasks.discard)
            expected = len(mixed) // 2
            if written != expected:
                logger.warning(
                    "sink_short_write", extra={"written": written, "expected": expected}
                )
            index += 1

    async def _on_tts_frame(self, frame: AudioFrame) -> None:
        """Resamplea un chunk TTS a 48 kHz y lo encola para el render."""
        pcm48 = cast(
            "NDArray[np.int16]",
            await asyncio.to_thread(
                resample_to_48k, frame.pcm, SAMPLE_RATE, dtype=np.int16
            ),
        )
        try:
            self._tts_queue.put_nowait(pcm48.tobytes())
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._tts_queue.get_nowait()
            self._tts_queue.put_nowait(pcm48.tobytes())
        self._metrics.tts_chunks += 1
        self._metrics.tts_samples += int(pcm48.size)
        if self._metrics.tts_latency_ms == 0.0 and self._last_voice_ts is not None:
            self._metrics.tts_latency_ms = max(
                0.0, (self._clock() - self._last_voice_ts) * 1000.0
            )

    # --------------------------------------------------------------- eventos WS

    def _on_subtitle(self, message: SubtitleMessage) -> None:
        self._metrics.subtitles += 1
        self._emit_subtitle(
            SubtitleEvent(
                text=message.text,
                is_final=message.is_final,
                lang=self.settings.pipeline.source_lang,
                kind="subtitle",
                latency_ms=message.latency_ms,
            )
        )

    def _on_translated(self, message: TranslatedMessage) -> None:
        self._metrics.translated += 1
        self._emit_subtitle(
            SubtitleEvent(
                text=message.text,
                is_final=message.is_final,
                lang=message.lang,
                kind="translated",
            )
        )

    def _emit_subtitle(self, event: SubtitleEvent) -> None:
        if self._on_subtitle_callback is None:
            return
        result = self._on_subtitle_callback(event)
        if result is not None and inspect.isawaitable(result):
            task = asyncio.create_task(_consume(result), name="babelpipe-video-subtitle-cb")
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)

    def _on_duck(self, message: DuckMessage) -> None:
        if message.action == "start":
            self._duck_release_pending = False
            self._duck_stop_at = None
            self._tts_announced_for_clause = 0
            self._tts_rendered_for_clause = 0
            self._ducking = True
            self._metrics.duck_starts += 1
            return
        self._duck_release_pending = True
        self._duck_stop_at = self._clock()
        self._metrics.duck_stops += 1
        if (
            self._tts_queue.empty()
            and self._tts_rendered_for_clause >= self._tts_announced_for_clause
        ):
            self._ducking = False

    def _on_error(self, message: ErrorMessage) -> None:
        self._metrics.errors += 1
        logger.warning("video_mode_error", extra={"code": message.code, "message": message.message})


def run_video_mode(
    settings: VideoModeSettings,
    transport: AudioTransport,
    ws_client: WSClient,
    *,
    on_subtitle: SubtitleCallback | None = None,
    on_mixed: MixedCallback | None = None,
    graph: PipeWireGraph | None = None,
    monitor: MonitorReader | None = None,
    clock: Clock = time.monotonic,
) -> VideoModeOrchestrator:
    """Fábrica del orquestador; ``run()`` y ``cleanup()`` se llaman aparte."""
    return VideoModeOrchestrator(
        settings,
        transport,
        ws_client,
        on_subtitle=on_subtitle,
        on_mixed=on_mixed,
        graph=graph,
        monitor=monitor,
        clock=clock,
    )


def _env_bool(name: str, default: bool) -> bool:
    """Interpreta un booleano de entorno de forma estricta."""
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"{name} debe ser un booleano, recibido {raw!r}")


def _settings_from_env() -> tuple[VideoModeSettings, WSClientSettings, str]:
    """Construye la configuración de producción validada desde variables de entorno."""
    duck_raw = os.getenv("BABELPIPE_DUCK_LEVEL")
    audio = VideoAudioSettings(
        duck_level=float(duck_raw) if duck_raw is not None else DEFAULT_DUCK_LEVEL,
        rx_sink=os.getenv("BABELPIPE_RX_SINK", RX_SINK_NAME),
    )
    pipeline = VideoPipelineSettings(
        source_lang=os.getenv("BABELPIPE_SOURCE_LANG", "en"),
        target_lang=os.getenv("BABELPIPE_TARGET_LANG", "es"),
        vad=os.getenv("BABELPIPE_VAD", "mock-vad"),
        stt=os.getenv("BABELPIPE_STT", "mock-stt"),
        translator=os.getenv("BABELPIPE_TRANSLATOR", "mock-translator"),
        tts=os.getenv("BABELPIPE_TTS", "mock-tts"),
    )
    physical_sink = os.getenv("BABELPIPE_PHYSICAL_SINK", "@DEFAULT_SINK@")
    settings = VideoModeSettings(
        audio=audio,
        pipeline=pipeline,
        create_rx_sink=_env_bool("BABELPIPE_CREATE_RX_SINK", False),
    )
    token = os.getenv("BABELPIPE_TOKEN")
    ws_settings = WSClientSettings(
        host=os.getenv("BABELPIPE_HOST", "127.0.0.1"),
        port=int(os.getenv("BABELPIPE_PORT", "8765")),
        token=SecretStr(token) if token else None,
        secure=_env_bool("BABELPIPE_SECURE", False),
        mode="rx",
        source_lang=pipeline.source_lang,
        target_lang=pipeline.target_lang,
        vad=pipeline.vad,
        stt=pipeline.stt,
        translator=pipeline.translator,
        tts=pipeline.tts,
        ping_interval_s=None,
    )
    return settings, ws_settings, physical_sink


async def _serve(orchestrator: VideoModeOrchestrator) -> None:
    """Arranca el daemon y detiene el grafo en señales SIGINT/SIGTERM."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(sig, orchestrator.stop)
    task = asyncio.create_task(orchestrator.run(), name="babelpipe-video-daemon")
    try:
        await task
    finally:
        orchestrator.stop()
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        orchestrator.cleanup()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI ``--setup`` / ``--run`` / ``--teardown`` del daemon cliente."""
    parser = argparse.ArgumentParser(prog="babelpipe-client", description="BabelPipe Rx daemon")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup", action="store_true", help="crea los nodos virtuales")
    group.add_argument("--run", action="store_true", help="ejecuta el modo vídeo")
    group.add_argument("--teardown", action="store_true", help="descarga los nodos creados")
    args = parser.parse_args(argv)

    graph = PipeWireGraph(register_handlers=False)
    if args.setup:
        graph.create_rx_sink()
        graph.create_tx_source()
        logger.info("nodes_ready", extra={"modules": list(graph.loaded_module_ids)})
        return 0
    if args.teardown:
        graph.destroy_all()
        return 0

    settings, ws_settings, physical_sink = _settings_from_env()
    transport = PipeWireAudioTransport(physical_sink=physical_sink)
    monitor = PipeWireRxMonitor(settings.audio.rx_sink)
    client = WSClient(ws_settings)
    orchestrator = run_video_mode(settings, transport, client, graph=graph, monitor=monitor)
    asyncio.run(_serve(orchestrator))
    return 0


if __name__ == "__main__":  # pragma: no cover - punto de entrada del script
    raise SystemExit(main())


__all__ = [
    "CAPTURE_SAMPLES",
    "PipeWireRxMonitor",
    "SubtitleEvent",
    "VideoAudioSettings",
    "VideoModeMetrics",
    "VideoModeOrchestrator",
    "VideoModeSettings",
    "VideoPipelineSettings",
    "main",
    "run_video_mode",
]
