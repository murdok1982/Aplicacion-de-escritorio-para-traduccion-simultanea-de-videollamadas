"""Modo llamada bidireccional completo de BabelPipe (TASK-020 / REQ-06 / AC-10).

Contrato (SPEC.md AC-10 y §7, ARCHITECTURE.md §2.1 y §4):

* :func:`run_call_mode` arranca **simultáneamente** un :class:`~client.net.stream.RxStream`
  (audio del interlocutor → ``Translator_Rx_Sink.monitor`` → doblaje con
  :class:`~client.audio.ducking.DuckingMixer` en el sink físico) y un
  :class:`~client.net.stream.TxStream` (mic → AEC → PTT → WS → ``Translator_Tx_Mic``).
* Un único :class:`~client.audio.echo_cancel.EchoCancelManager` propiedad de esta
  sesión configura el módulo WebRTC cuya referencia es el **monitor del sink
  físico**. Ese mismo monitor se reparte entre el AEC y la captura Rx mediante
  :class:`_MonitorFanout`, de modo que no hay dos lectores compitiendo por la
  misma señal y la alineación del AEC se mantiene exacta.
* El ``duck_level`` y el ``ptt_enabled`` llegan desde la configuración validada
  en la frontera con Pydantic v2 (:class:`CallModeSettings`).
* Las tareas se ejecutan con :func:`asyncio.wait` sobre un conjunto de hijos y
  se detienen con un :class:`asyncio.Event`: se avisa a los streams, se les da
  una ventana de gracia y se cancela yespera a **todos** los hijos antes de
  devolver las métricas. No queda ninguna tarea, socket, transporte ni módulo
  AEC colgados.
* Los subtítulos de ambos sentidos se exponen al overlay mediante un único
  ``on_subtitle`` (el evento ``SubtitleMessage.stream`` distingue ``rx`` de
  ``tx``) y el desglose de latencia se devuelve en :class:`CallMetrics`.

Este módulo no modifica los contratos de ``shared/`` ni reescribe
``RxStream``/``TxStream``: solo cablea sus APIs públicas y aplica, de forma
local y reversible, el mismo ajuste de dedup de ``tts_chunk`` que TASK-019
documentó en el handoff para que el anuncio JSON no descarte su frame PCM.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, ConfigDict, Field, field_validator

from client.audio.ducking import DEFAULT_DUCK_LEVEL, DuckingMixer
from client.audio.echo_cancel import (
    DEFAULT_ATTENUATION_DB,
    DEFAULT_DELAY_MS,
    CommandRunner,
    EchoCancelManager,
)
from client.audio.ptt import PushToTalk
from client.audio.resampler import resample_to_48k
from client.audio.transport import AudioTransport
from client.net.stream import RxStream, TxStream
from client.net.ws_client import WSClient
from shared.constants import CHUNK_SAMPLES, SAMPLE_RATE, STREAM_RX
from shared.models import (
    DuckMessage,
    ErrorMessage,
    SubtitleMessage,
    TranslatedMessage,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import AudioFrame

logger = logging.getLogger("babelpipe.client.call_mode")

CAPTURE_SAMPLES: int = CHUNK_SAMPLES * 3
"""Bloque de captura a 48 kHz (960 muestras = 20 ms en el pipeline 16 kHz)."""

_FINAL_SILENCE_CHUNKS: int = 25
"""500 ms de silencio en el monitor cierran la cláusula Rx si VAD no lo hizo."""

_SPEECH_RMS: float = 200.0
"""Umbral de voz del VAD mock; solo marca el inicio de la métrica de latencia."""

_GRACE_S: float = 0.2
"""Ventana para que los streams terminen limpiamente antes de cancelarlos."""

_NODE_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
_LANG_RE = re.compile(r"[a-zA-Z]{2}([-_][a-zA-Z0-9]{2,8})?\Z")


class _StrictSettings(BaseModel):
    """Base de configuración estricta-but-tolerante con objetos de aplicación.

    ``extra='ignore'`` permite recibir el objeto de configuración completo de
    ``client.main`` (que contiene más secciones), mientras que los tipos y
    rangos de **todos** los campos que esta llamada consume se validan aquí.
    ``from_attributes`` acepta tanto dicts como objetos/dataclasses.
    """

    model_config = ConfigDict(extra="ignore", frozen=True, from_attributes=True)


class CallAudioSettings(_StrictSettings):
    """Frontera validada de los parámetros de audio de la llamada."""

    ptt_enabled: bool = False
    echo_cancel: bool = True
    duck_level: float = Field(default=DEFAULT_DUCK_LEVEL, ge=0.10, le=0.15)
    mic_source: str = Field(default="@DEFAULT_SOURCE@", min_length=1, max_length=128)
    physical_sink: str = Field(default="@DEFAULT_SINK@", min_length=1, max_length=128)
    aec_delay_ms: float = Field(default=DEFAULT_DELAY_MS, ge=0.0, le=5000.0)
    aec_attenuation_db: float = Field(default=DEFAULT_ATTENUATION_DB, ge=0.0, le=120.0)

    @field_validator("mic_source", "physical_sink")
    @classmethod
    def _validate_node(cls, value: str) -> str:
        if value.startswith("@DEFAULT_") and value.endswith("@"):
            return value
        if _NODE_RE.fullmatch(value) is None:
            raise ValueError(f"nombre de nodo PipeWire inválido: {value!r}")
        return value


class CallPipelineSettings(_StrictSettings):
    """Idiomas independientes para el doblaje (Rx) y el micrófono (Tx)."""

    rx_source_lang: str = Field(default="en", min_length=2, max_length=10)
    rx_target_lang: str = Field(default="es", min_length=2, max_length=10)
    tx_source_lang: str = Field(default="es", min_length=2, max_length=10)
    tx_target_lang: str = Field(default="en", min_length=2, max_length=10)

    @field_validator(
        "rx_source_lang", "rx_target_lang", "tx_source_lang", "tx_target_lang"
    )
    @classmethod
    def _validate_lang(cls, value: str) -> str:
        if _LANG_RE.fullmatch(value) is None:
            raise ValueError(f"código de idioma inválido: {value!r}")
        return value


class CallModeSettings(_StrictSettings):
    """Esquema completo de la llamada; se construye en la frontera de entrada."""

    audio: CallAudioSettings = Field(default_factory=CallAudioSettings)
    pipeline: CallPipelineSettings = Field(default_factory=CallPipelineSettings)


@dataclass(frozen=True, slots=True)
class CallMetrics:
    """Instantánea combinada de ambas direcciones al terminar la llamada."""

    rx_frames: int
    tx_frames: int
    rx_latency_ms: float
    tx_latency_ms: float
    latency_ms: float
    echo_attenuation_db: float
    """Ratio acumulado de energía del TxStream (diagnóstico global; AC-10 se mide
    sobre una ventana eco-only en ``tests/e2e/test_bidirectional.py``)."""

    rx_subtitles: int
    tx_subtitles: int
    rx_translations: tuple[str, ...]
    tx_translations: tuple[str, ...]
    duck_starts: int
    duck_stops: int
    sink_samples: int
    tx_samples: int
    aec_args: str
    aec_enabled: bool
    transport_running: bool
    ptt_state: str

    def __getitem__(self, key: str) -> Any:
        """Acceso tipo dict para informes y aserciones."""
        return getattr(self, key)


async def _invoke[T](callback: Callable[[T], None | Awaitable[None]] | None, value: T) -> None:
    """Invoca un callback síncrono o asíncrono sin bloquear el event loop."""
    if callback is None:
        return
    result = callback(value)
    if result is not None and inspect.isawaitable(result):
        await result


def _validate_clients(
    settings: CallModeSettings, ws_rx: WSClient, ws_tx: WSClient
) -> None:
    """Comprueba en la frontera que los sockets negocian los idiomas configurados."""
    if ws_rx.settings.mode != "rx" or ws_tx.settings.mode != "tx":
        raise ValueError("ws_client_rx debe ser mode='rx' y ws_client_tx mode='tx'")
    expected: tuple[tuple[str, str], ...] = (
        (ws_rx.settings.source_lang, settings.pipeline.rx_source_lang),
        (ws_rx.settings.target_lang, settings.pipeline.rx_target_lang),
        (ws_tx.settings.source_lang, settings.pipeline.tx_source_lang),
        (ws_tx.settings.target_lang, settings.pipeline.tx_target_lang),
    )
    for actual, wanted in expected:
        if actual.lower() != wanted.lower():
            raise ValueError(
                f"el idioma del socket ({actual}) no coincide con la configuración ({wanted})"
            )


class _MonitorFanout:
    """Único lector del monitor del sink físico, con distribución a dos consumidores.

    ``TxStream`` llama a :meth:`read_reference` desde su hilo de captura; ahí se
    hace la lectura real (en lockstep con el micrófono, como en TASK-019) y se
    publica una copia en la cola Rx desde el propio event loop con
    ``call_soon_threadsafe``. Así el AEC conserva la alineación de 120 ms y el
    pipeline Rx recibe exactamente la misma señal del interlocutor.
    """

    def __init__(self, transport: AudioTransport, loop: asyncio.AbstractEventLoop) -> None:
        self._transport = transport
        self._loop = loop
        self._rx: asyncio.Queue[NDArray[np.int16]] = asyncio.Queue(maxsize=CAPTURE_SAMPLES)

    def read_reference(self, num_samples: int) -> NDArray[np.int16]:
        """Lee la referencia AEC y publica una copia para la captura Rx."""
        chunk = self._transport.read_physical_sink_monitor_48k(num_samples)
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._offer_rx, chunk.copy())
        return chunk

    def _offer_rx(self, chunk: NDArray[np.int16]) -> None:
        try:
            self._rx.put_nowait(chunk)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._rx.get_nowait()
            self._rx.put_nowait(chunk)

    async def read_rx(self) -> NDArray[np.int16]:
        """Espera el siguiente bloque del interlocutor (sin competir con el AEC)."""
        return await self._rx.get()


class _CallController:
    """Dueño de los recursos de una llamada; todo se libera en ``run``."""

    def __init__(
        self,
        settings: CallModeSettings,
        transport: AudioTransport,
        ws_client_rx: WSClient,
        ws_client_tx: WSClient,
        *,
        on_subtitle: Callable[[SubtitleMessage], None | Awaitable[None]] | None,
        aec_runner: CommandRunner | None,
        ptt: PushToTalk | None,
        stop_event: asyncio.Event,
    ) -> None:
        self.settings = settings
        self.transport = transport
        self.ws_rx = ws_client_rx
        self.ws_tx = ws_client_tx
        self._on_subtitle_cb = on_subtitle
        self._aec_runner = aec_runner
        self._ptt = ptt
        self._stop = stop_event
        self._mixer = DuckingMixer(settings.audio.duck_level)
        self._fanout = _MonitorFanout(transport, asyncio.get_running_loop())
        self._duck_active = False
        self._last_original: NDArray[np.int16] | None = None
        self._rx_voice_ts: float | None = None
        self._rx_latency_ms = 0.0
        self._rx_subtitles = 0
        self._tx_subtitles = 0
        self._rx_translations: list[str] = []
        self._tx_translations: list[str] = []
        self._duck_starts = 0
        self._duck_stops = 0
        self._sink_samples = 0
        self._aux_tasks: set[asyncio.Task[None]] = set()
        self._aec_args = ""
        self._ptt_state = "PTT_IDLE"

    # ------------------------------------------------------------------ API

    async def run(self) -> CallMetrics:
        """Ejecuta ambos streams hasta el stop y devuelve las métricas combinadas."""
        aec: EchoCancelManager | None = None
        if self.settings.audio.echo_cancel:
            aec = EchoCancelManager(
                self._aec_runner,
                source_name=self.settings.audio.mic_source,
                sink_name=self.settings.audio.physical_sink,
            )
            self._aec_args = aec.aec_args

        ptt = self._ptt if self._ptt is not None else PushToTalk()
        ptt.set_enabled(self.settings.audio.ptt_enabled)

        rx = RxStream(
            on_subtitle=self._on_subtitle,
            on_translated=self._on_rx_translated,
            on_duck=self._on_duck,
            on_error=self._on_error,
        )
        tx = TxStream(
            self.transport,
            aec=aec,
            reference=self._fanout.read_reference,
            ptt=ptt,
            capture_samples=CAPTURE_SAMPLES,
            aec_delay_ms=self.settings.audio.aec_delay_ms,
            aec_attenuation_db=self.settings.audio.aec_attenuation_db,
            on_subtitle=self._on_subtitle,
            on_translated=self._on_tx_translated,
            on_error=self._on_error,
        )

        restore_dedup = self._install_tts_dedup(self.ws_rx)
        previous_audio = self.ws_rx.on_audio_frame
        self.ws_rx.on_audio_frame = self._on_rx_tts

        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(rx.run(self.ws_rx), name="babelpipe-call-rx"),
            asyncio.create_task(tx.run(self.ws_tx), name="babelpipe-call-tx"),
            asyncio.create_task(self._rx_capture_loop(rx), name="babelpipe-call-rx-capture"),
            asyncio.create_task(self._stop.wait(), name="babelpipe-call-stop"),
        ]
        stopper = tasks[-1]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is stopper or task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    raise exc
        finally:
            self._stop.set()
            rx.stop()
            tx.stop()
            if not all(task.done() for task in tasks):
                await asyncio.wait(tasks, timeout=_GRACE_S)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for task in tuple(self._aux_tasks):
                task.cancel()
            if self._aux_tasks:
                await asyncio.gather(*tuple(self._aux_tasks), return_exceptions=True)
            self._aux_tasks.clear()
            restore_dedup()
            self.ws_rx.on_audio_frame = previous_audio
            await self.ws_rx.close()
            await self.ws_tx.close()
            self._ptt_state = ptt.state.value

        tx_metrics = tx.metrics
        rx_metrics = rx.metrics
        tx_latency = tx_metrics.tts_latency_ms
        return CallMetrics(
            rx_frames=rx_metrics.chunks,
            tx_frames=tx_metrics.chunks,
            rx_latency_ms=self._rx_latency_ms,
            tx_latency_ms=tx_latency,
            latency_ms=max(self._rx_latency_ms, tx_latency),
            echo_attenuation_db=tx_metrics.echo_attenuation_db,
            rx_subtitles=self._rx_subtitles,
            tx_subtitles=self._tx_subtitles,
            rx_translations=tuple(self._rx_translations),
            tx_translations=tuple(self._tx_translations),
            duck_starts=self._duck_starts,
            duck_stops=self._duck_stops,
            sink_samples=self._sink_samples,
            tx_samples=tx_metrics.tts_samples,
            aec_args=self._aec_args,
            aec_enabled=aec.status().enabled if aec is not None else False,
            transport_running=self.transport.is_running,
            ptt_state=self._ptt_state,
        )

    # ------------------------------------------------------------ interno

    async def _rx_capture_loop(self, rx: RxStream) -> None:
        """Alimenta el stream Rx con el monitor del interlocutor y cierra la cláusula."""
        silent = 0
        finalized = False
        while True:
            chunk = await self._fanout.read_rx()
            self._last_original = chunk.copy()
            rms = math.sqrt(float(np.mean(np.square(chunk.astype(np.float64)))))
            await rx.push_async(chunk)
            if rms >= _SPEECH_RMS:
                silent = 0
                if self._rx_voice_ts is None:
                    self._rx_voice_ts = time.monotonic()
                continue
            silent += 1
            if not finalized and silent >= _FINAL_SILENCE_CHUNKS:
                finalized = True
                await rx.push_async(
                    np.zeros(CAPTURE_SAMPLES, dtype=np.int16), final=True
                )

    async def _on_rx_tts(self, frame: AudioFrame) -> None:
        """Dobla el TTS del interlocutor en el sink físico aplicando ducking."""
        if frame.stream_id != STREAM_RX:
            return
        pcm48 = cast(
            "NDArray[np.int16]",
            await asyncio.to_thread(
                resample_to_48k, frame.pcm, SAMPLE_RATE, dtype=np.int16
            ),
        )
        original = self._last_original
        if original is None or original.size != pcm48.size:
            original = np.zeros(pcm48.size, dtype=np.int16)
        mixed = self._mixer.process(original, pcm48, ducking_active=self._duck_active)
        written = await asyncio.to_thread(self.transport.write_physical_sink_48k, mixed)
        self._sink_samples += written
        if self._rx_voice_ts is not None:
            self._rx_latency_ms = max(
                0.0, (time.monotonic() - self._rx_voice_ts) * 1000.0
            )
            self._rx_voice_ts = None
        logger.info(
            "rx_tts_rendered",
            extra={"samples": written, "duck_gain": round(self._mixer.gain, 3)},
        )

    def _on_duck(self, message: DuckMessage) -> None:
        """Sincroniza el mezclador con los eventos ``duck`` del stream Rx."""
        if message.stream != "rx":
            return
        if message.action == "start":
            self._duck_starts += 1
            self._duck_active = True
            return
        self._duck_stops += 1
        self._duck_active = False
        task = asyncio.create_task(self._release_duck(), name="babelpipe-call-duck-release")
        self._aux_tasks.add(task)
        task.add_done_callback(self._aux_tasks.discard)

    async def _release_duck(self) -> None:
        """Devuelve la rampa a ganancia 1.0 escribiendo silencio tras el doblaje."""
        if self._stop.is_set():
            return
        count = max(self._mixer.ramp_samples, CAPTURE_SAMPLES)
        silence = np.zeros(count, dtype=np.int16)
        mixed = self._mixer.process(silence, silence, ducking_active=False)
        written = await asyncio.to_thread(self.transport.write_physical_sink_48k, mixed)
        self._sink_samples += written

    async def _on_subtitle(self, message: SubtitleMessage) -> None:
        if message.stream == "rx":
            self._rx_subtitles += 1
        else:
            self._tx_subtitles += 1
        await _invoke(self._on_subtitle_cb, message)

    async def _on_rx_translated(self, message: TranslatedMessage) -> None:
        if message.is_final:
            self._rx_translations.append(message.text)

    async def _on_tx_translated(self, message: TranslatedMessage) -> None:
        if message.is_final:
            self._tx_translations.append(message.text)

    async def _on_error(self, message: ErrorMessage) -> None:
        logger.warning("call_error", extra={"code": message.code})

    @staticmethod
    def _install_tts_dedup(client: WSClient) -> Callable[[], None]:
        """Libera el ``seq`` del anuncio ``tts_chunk`` para no perder su PCM.

        Es el mismo ajuste que TASK-019 aplicó en ``TxStream``; aquí se aplica
        al socket Rx (que no lo tiene) y se restaura al terminar. No toca el
        contrato de ``shared`` ni los streams.
        """
        original = client._handle_text

        def wrapped(raw: str) -> None:
            original(raw)
            try:
                message = parse_control_message(raw)
            except ValueError:
                return
            if isinstance(message, TtsChunkMessage):
                client._seen_set.discard(message.seq)

        client._handle_text = wrapped  # type: ignore[method-assign]

        def restore() -> None:
            client._handle_text = original  # type: ignore[method-assign]

        return restore


async def run_call_mode(
    settings: object,
    transport: AudioTransport,
    ws_client_rx: WSClient,
    ws_client_tx: WSClient,
    *,
    on_subtitle: Callable[[SubtitleMessage], None | Awaitable[None]] | None = None,
    aec_runner: CommandRunner | None = None,
    ptt: PushToTalk | None = None,
    stop_event: asyncio.Event | None = None,
) -> CallMetrics:
    """Arranca y detiene una llamada bidireccional completa (AC-10).

    ``settings`` puede ser un :class:`CallModeSettings`, un dict o el objeto de
    configuración de la aplicación; siempre se valida aquí antes de usarse.
    ``aec_runner`` y ``ptt`` son costuras de integración para tests sin hardware;
    en producción se usan los objetos reales. ``stop_event`` permite al llamador
    pedir la parada limpia mientras la coroutine sigue ejecutándose.

    Devuelve :class:`CallMetrics` con latencia combinada, atenuación de eco,
    subtítulos, ducking y estado de limpieza del grafo.
    """
    if not isinstance(transport, AudioTransport):
        raise ValueError("transport debe ser una implementación de AudioTransport")
    if not isinstance(ws_client_rx, WSClient) or not isinstance(ws_client_tx, WSClient):
        raise ValueError("ws_client_rx y ws_client_tx deben ser WSClient")
    if ws_client_rx is ws_client_tx:
        raise ValueError("Rx y Tx deben usar sockets distintos (idiomas y modos distintos)")
    if stop_event is not None and not isinstance(stop_event, asyncio.Event):
        raise ValueError("stop_event debe ser un asyncio.Event")
    if on_subtitle is not None and not callable(on_subtitle):
        raise ValueError("on_subtitle debe ser invocable")

    validated: CallModeSettings
    if isinstance(settings, CallModeSettings):
        validated = settings
    elif isinstance(settings, Mapping):
        validated = CallModeSettings.model_validate(dict(settings))
    else:
        validated = CallModeSettings.model_validate(settings)
    _validate_clients(validated, ws_client_rx, ws_client_tx)

    controller = _CallController(
        validated,
        transport,
        ws_client_rx,
        ws_client_tx,
        on_subtitle=on_subtitle,
        aec_runner=aec_runner,
        ptt=ptt,
        stop_event=stop_event if stop_event is not None else asyncio.Event(),
    )
    return await controller.run()


__all__ = [
    "CAPTURE_SAMPLES",
    "CallAudioSettings",
    "CallMetrics",
    "CallModeSettings",
    "CallPipelineSettings",
    "run_call_mode",
]
