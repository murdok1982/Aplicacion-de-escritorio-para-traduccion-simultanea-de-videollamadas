"""Productor/consumidor Rx del cliente BabelPipe (TASK-016 / REQ-03).

La señal del monitor de ``Translator_Rx_Sink`` llega a 48 kHz y se convierte
con :class:`~client.audio.resampler.StreamingResampler` en frames canónicos de
16 kHz / 320 muestras / 640 bytes. Cada frame se serializa con
:func:`shared.protocol.encode_frame` (``stream_id=STREAM_RX``, ``sequence``
monótono, flags ``FINAL`` / ``PTT_ACTIVE`` cuando corresponde) y se entrega al
:class:`~client.net.ws_client.WSClient`.

La cola de captura es un :class:`asyncio.Queue` de 50 elementos con
*backpressure no bloqueante*: si se llena se descarta el chunk más antiguo y se
publica un ``error OVERRUN`` local en el propio flujo de eventos (ADR-006).
Nunca se bloquea el hilo de captura; :meth:`RxStream.push_async` usa
``asyncio.to_thread`` para el resampleo CPU-bound.

:meth:`RxStream.run` envía los chunks y consume los eventos del servidor en
paralelo: publica ``subtitle`` / ``translated`` a sus callbacks, dispara
``duck`` start/stop y mantiene métricas (``chunks``, ``overruns``,
``reconnects``, ...). Se detiene de forma limpia con :meth:`RxStream.stop`, que
no es más que un :class:`asyncio.Event`.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import Any, cast

import numpy as np
from numpy.typing import NDArray

from client.audio.echo_cancel import (
    DEFAULT_ATTENUATION_DB,
    DEFAULT_DELAY_MS,
    EchoCancelManager,
    suppress_echo,
)
from client.audio.ptt import PushToTalk
from client.audio.resampler import SampleInput, StreamingResampler, resample_to_48k
from client.audio.transport import AudioTransport
from shared.constants import (
    CHUNK_BYTES,
    CHUNK_SAMPLES,
    PIPEWIRE_SAMPLE_RATE,
    SAMPLE_RATE,
    STREAM_RX,
    STREAM_TX,
)
from shared.models import (
    DuckMessage,
    ErrorMessage,
    SubtitleMessage,
    TranslatedMessage,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import AudioFrame, Flags, decode_frame, encode_frame

from .ws_client import ClientAuthError, ClientClosedError, WSClient

logger = logging.getLogger("babelpipe.client.net.stream")

DEFAULT_QUEUE_MAXSIZE = 50
_MAX_SEQUENCE = 0xFFFFFFFF
_LOCAL_OVERRUN = "OVERRUN"


@dataclass(slots=True)
class StreamMetrics:
    """Instantánea de contadores del stream Rx."""

    chunks_in: int = 0
    """Chunks de entrada (48 kHz) ingestados."""
    chunks: int = 0
    """Frames canónicos de 640 bytes producidos y enviados."""
    overruns: int = 0
    """Chunks descartados por backpressure local."""
    reconnects: int = 0
    """Reconexiones del transporte durante este stream."""
    subtitles: int = 0
    translated: int = 0
    duck_starts: int = 0
    duck_stops: int = 0
    errors: int = 0
    duplicates: int = 0

    @property
    def frames(self) -> int:
        """Alias legible de :attr:`chunks`."""
        return self.chunks

    def __getitem__(self, key: str) -> int:
        """Acceso tipo dict para informes y tests."""
        return int(getattr(self, key))


_END = object()


class RxStream:
    """Convierte PCM 48 kHz en frames Rx 16 kHz y gestiona los eventos WS."""

    def __init__(
        self,
        *,
        source_rate: int = PIPEWIRE_SAMPLE_RATE,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        initial_sequence: int = 0,
        ptt_active: bool = False,
        resampler: StreamingResampler | None = None,
        on_subtitle: Callable[[SubtitleMessage], None | Awaitable[None]] | None = None,
        on_translated: Callable[[TranslatedMessage], None | Awaitable[None]] | None = None,
        on_duck: Callable[[DuckMessage], None | Awaitable[None]] | None = None,
        on_error: Callable[[ErrorMessage], None | Awaitable[None]] | None = None,
    ) -> None:
        if source_rate not in (SAMPLE_RATE, PIPEWIRE_SAMPLE_RATE):
            raise ValueError(
                f"source_rate no soportado: {source_rate}; se aceptan {SAMPLE_RATE} o "
                f"{PIPEWIRE_SAMPLE_RATE} Hz"
            )
        if not 1 <= queue_maxsize <= 10_000:
            raise ValueError("queue_maxsize debe estar en [1, 10000]")
        if not isinstance(initial_sequence, int) or not 0 <= initial_sequence <= _MAX_SEQUENCE:
            raise ValueError("initial_sequence debe ser un uint32")
        if not isinstance(ptt_active, bool):
            raise ValueError("ptt_active debe ser bool")

        self._source_rate = source_rate
        self._queue_maxsize = queue_maxsize
        self._queue: asyncio.Queue[bytes | object] = asyncio.Queue(maxsize=queue_maxsize)
        self._resampler = resampler or StreamingResampler(src_rate=source_rate)
        if self._resampler.src_rate != source_rate:
            raise ValueError("el resampler recibido no coincide con source_rate")
        self._resample_lock = asyncio.Lock()
        self._sequence = initial_sequence
        self._ptt_active = ptt_active
        self._stop = asyncio.Event()
        self._running = False
        self._ws_client: WSClient | None = None
        self._local_errors: deque[ErrorMessage] = deque(maxlen=64)
        self._metrics = StreamMetrics()
        self.on_subtitle = on_subtitle
        self.on_translated = on_translated
        self.on_duck = on_duck
        self.on_error = on_error

    # ------------------------------------------------------------------ API

    @property
    def metrics(self) -> StreamMetrics:
        """Instantánea de contadores; se puede leer mientras ``run`` está vivo."""
        snapshot = replace(self._metrics)
        if self._ws_client is not None:
            snapshot.reconnects = self._ws_client.metrics.reconnects
        return snapshot

    @property
    def sequence(self) -> int:
        """Próxima ``sequence`` que se asignará a un frame."""
        return self._sequence

    @property
    def pending_frames(self) -> int:
        """Frames esperando en la cola de envío."""
        return self._queue.qsize()

    @property
    def local_errors(self) -> tuple[ErrorMessage, ...]:
        """Errores no fatales generados localmente (p. ej. ``OVERRUN``)."""
        return tuple(self._local_errors)

    def push(self, samples: SampleInput, *, final: bool = False) -> int:
        """Inyecta PCM @48 kHz y devuelve el número de frames 16 kHz emitidos.

        El resampleo ocurre en el hilo llamante; use :meth:`push_async` desde
        un event loop para descargar el trabajo CPU-bound con ``to_thread``.
        """
        chunks = self._resampler.process(samples)
        self._metrics.chunks_in += 1
        return self._enqueue_chunks(chunks, final=final)

    async def push_async(self, samples: SampleInput, *, final: bool = False) -> int:
        """Variante no bloqueante de :meth:`push` usando ``asyncio.to_thread``."""
        async with self._resample_lock:
            chunks = await asyncio.to_thread(self._resampler.process, samples)
        self._metrics.chunks_in += 1
        return self._enqueue_chunks(chunks, final=final)

    def set_ptt(self, active: bool) -> None:
        """Activa/desactiva el bit ``PTT_ACTIVE`` de los frames posteriores."""
        self._ptt_active = bool(active)

    def finish(self) -> None:
        """Marca con ``FINAL`` el último frame aún pendiente en la cola."""
        items: list[bytes | object] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if items and isinstance(items[-1], bytes):
            frame = decode_frame(items[-1])
            items[-1] = encode_frame(
                stream_id=frame.stream_id,
                sequence=frame.sequence,
                flags=frame.flags | int(Flags.FINAL),
                pcm=frame.pcm,
                sample_rate=frame.sample_rate,
                num_samples=frame.num_samples,
            )
        for item in items:
            self._queue.put_nowait(item)

    def stop(self) -> None:
        """Solicita la parada limpia de :meth:`run` mediante ``asyncio.Event``."""
        self._stop.set()
        if not self._queue.full():
            self._queue.put_nowait(_END)

    async def run(self, ws_client: WSClient) -> None:
        """Envía frames y consume eventos hasta ``stop()`` o cierre del socket.

        Propaga :class:`~client.net.ws_client.ClientAuthError` si el servidor
        rechaza el token, sin exponer trazas ni secretos al llamador.
        """
        if self._running:
            raise RuntimeError("RxStream.run ya está en ejecución")
        self._running = True
        self._stop.clear()
        self._ws_client = ws_client
        previous_reconnect = ws_client.on_reconnect

        def _on_reconnect() -> None:
            self._metrics.reconnects += 1
            if previous_reconnect is not None:
                result = previous_reconnect()
                if result is not None and hasattr(result, "__await__"):
                    asyncio.create_task(
                        _invoke_awaitable(result), name="rx-previous-reconnect"
                    )

        ws_client.on_reconnect = _on_reconnect
        await ws_client.connect()
        sender = asyncio.create_task(self._send_loop(ws_client), name="babelpipe-rx-sender")
        consumer = asyncio.create_task(
            self._events_loop(ws_client), name="babelpipe-rx-events"
        )
        stopper = asyncio.create_task(self._stop.wait(), name="babelpipe-rx-stopper")
        try:
            done, pending = await asyncio.wait(
                {sender, consumer, stopper}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task is consumer and not task.cancelled():
                    exc = task.exception()
                    if exc is not None:
                        raise exc
        finally:
            self._stop.set()
            for task in (sender, consumer, stopper):
                task.cancel()
            await asyncio.gather(sender, consumer, stopper, return_exceptions=True)
            ws_client.on_reconnect = previous_reconnect
            self._running = False
            self._ws_client = None
            logger.info(
                "rx_stream_stopped",
                extra={
                    "frames": self._metrics.chunks,
                    "overruns": self._metrics.overruns,
                    "reconnects": self._metrics.reconnects,
                },
            )

    # ------------------------------------------------------------ interno

    def _enqueue_chunks(self, chunks: list[bytes], *, final: bool) -> int:
        emitted = 0
        for index, pcm in enumerate(chunks):
            flags = int(Flags.PTT_ACTIVE) if self._ptt_active else 0
            if final and index == len(chunks) - 1:
                flags |= int(Flags.FINAL)
            raw = encode_frame(
                stream_id=STREAM_RX,
                sequence=self._sequence,
                flags=flags,
                pcm=pcm,
                sample_rate=SAMPLE_RATE,
                num_samples=CHUNK_BYTES // 2,
            )
            self._sequence = (self._sequence + 1) & _MAX_SEQUENCE
            self._enqueue(raw)
            self._metrics.chunks += 1
            emitted += 1
        return emitted

    def _enqueue(self, raw: bytes) -> None:
        try:
            self._queue.put_nowait(raw)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            self._metrics.overruns += 1
            self._enqueue_error(
                _LOCAL_OVERRUN,
                f"cola Rx llena ({self._queue_maxsize}); se descartó el chunk más antiguo",
            )
            self._queue.put_nowait(raw)

    def _enqueue_error(self, code: str, message: str) -> None:
        error = ErrorMessage(code=code, message=message)
        self._local_errors.append(error)
        if self._ws_client is not None:
            self._ws_client.report_local_error(code, message)

    async def _send_loop(self, ws_client: WSClient) -> None:
        while not self._stop.is_set():
            item = await self._queue.get()
            if item is _END:
                return
            if not isinstance(item, bytes):
                continue
            try:
                ws_client.send_frame(item)
            except (ClientClosedError, ClientAuthError, ValueError) as exc:
                logger.warning(
                    "rx_send_aborted", extra={"reason": type(exc).__name__}
                )
                return

    async def _events_loop(self, ws_client: WSClient) -> None:
        async for message in ws_client.messages():
            if isinstance(message, SubtitleMessage):
                self._metrics.subtitles += 1
                await _invoke(self.on_subtitle, message)
            elif isinstance(message, TranslatedMessage):
                self._metrics.translated += 1
                await _invoke(self.on_translated, message)
            elif isinstance(message, DuckMessage):
                if message.action == "start":
                    self._metrics.duck_starts += 1
                else:
                    self._metrics.duck_stops += 1
                await _invoke(self.on_duck, message)
            elif isinstance(message, ErrorMessage):
                self._metrics.errors += 1
                await _invoke(self.on_error, message)


async def _invoke[T](callback: Callable[[T], None | Awaitable[None]] | None, value: T) -> None:
    """Invoca un callback síncrono o asíncrono sin bloquear el event loop."""
    if callback is None:
        return
    result = callback(value)
    if result is not None and inspect.isawaitable(result):
        await result


async def _invoke_awaitable(value: Any) -> None:
    """Envuelve el resultado awaitable de un callback previo de reconexión."""
    if inspect.isawaitable(value):
        await value


# ---------------------------------------------------------------------------
# Canal Tx: micrófono -> AEC -> PTT -> WS -> TTS -> Translator_Tx_Mic
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TxStreamMetrics(StreamMetrics):
    """Contadores del canal Tx (extienden los de :class:`StreamMetrics`)."""

    silence_frames: int = 0
    """Frames de silencio emitidos mientras el PTT está cerrado."""
    tts_chunks: int = 0
    """Chunks TTS recibidos del servidor y escritos en el micrófono virtual."""
    tts_samples: int = 0
    """Muestras a 48 kHz escritas en ``Translator_Tx_Mic``."""
    aec_frames: int = 0
    """Bloques de micrófono que pasaron por el modelo AEC determinista."""
    echo_energy_before: float = 0.0
    """Energía acumulada de la señal de micrófono antes del AEC."""
    echo_energy_after: float = 0.0
    """Energía acumulada de la señal de micrófono después del AEC."""
    tts_latency_ms: float = 0.0
    """Latencia del último par voz capturada -> TTS escrito (ms)."""

    @property
    def echo_attenuation_db(self) -> float:
        """Atenuación global medidas entre micrófono crudo y señal limpia."""
        if self.echo_energy_after <= 0.0 or self.echo_energy_before <= 0.0:
            return 0.0
        return 20.0 * math.log10(
            math.sqrt(self.echo_energy_before) / math.sqrt(self.echo_energy_after)
        )


def _sample_count(samples: SampleInput) -> int:
    """Número de muestras de entrada para poder generar silencio equivalente."""
    if isinstance(samples, bytes | bytearray | memoryview):
        return len(samples) // 2
    array = np.asarray(samples)
    if array.ndim == 2:
        return int(array.shape[0])
    return int(array.size)


def _rolling_window(
    previous: NDArray[np.int16], incoming: NDArray[np.int16], *, keep: int, total: int
) -> NDArray[np.int16]:
    """Ventana deslizante de ``total`` muestras con ceros al inicio."""
    head = previous[-keep:] if keep > 0 else previous[:0]
    joined = np.concatenate((head, incoming))
    if joined.size < total:
        joined = np.pad(joined, (total - joined.size, 0))
    return cast("NDArray[np.int16]", joined[-total:].astype(np.int16, copy=False))


class TxStream:
    """Canal de llamada: micrófono 48 kHz -> AEC -> PTT -> WS -> Tx 48 kHz.

    El micrófono se captura a 48 kHz y se compara con el monitor del sink
    físico (referencia del AEC). :func:`~client.audio.echo_cancel.suppress_echo`
    modela la cancelación de forma determinista con una ventana deslizante lo
    suficientemente larga para el retardo del eco; en producción el módulo
    WebRTC de PipeWire hace el trabajo real y :class:`EchoCancelManager`
    configura la referencia mediante ``build_aec_args``. Después se aplica
    :class:`~client.audio.ptt.PushToTalk`, se resamplea a 16 kHz y se envía con
    ``stream_id=STREAM_TX``. Los TTS del servidor vuelven a 48 kHz y se escriben
    en ``Translator_Tx_Mic`` a través del transporte inyectado.

    Igual que :class:`RxStream`, usa una :class:`asyncio.Queue` acotada con
    descarte del chunk más antiguo (nunca bloquea) y una única
    :class:`asyncio.Event` de parada. Toda E/S y todo resampleo se descargan con
    :func:`asyncio.to_thread`: el event loop nunca espera sobre audio.
    """

    def __init__(
        self,
        transport: AudioTransport,
        *,
        aec: EchoCancelManager | None = None,
        reference: Callable[[int], NDArray[np.int16]] | None = None,
        aec_delay_ms: float = DEFAULT_DELAY_MS,
        aec_attenuation_db: float = DEFAULT_ATTENUATION_DB,
        ptt: PushToTalk | None = None,
        capture_samples: int = CHUNK_SAMPLES * 3,
        source_rate: int = PIPEWIRE_SAMPLE_RATE,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        initial_sequence: int = 0,
        resampler: StreamingResampler | None = None,
        clock: Callable[[], float] = time.monotonic,
        on_subtitle: Callable[[SubtitleMessage], None | Awaitable[None]] | None = None,
        on_translated: Callable[[TranslatedMessage], None | Awaitable[None]] | None = None,
        on_tts: Callable[[bytes], None | Awaitable[None]] | None = None,
        on_error: Callable[[ErrorMessage], None | Awaitable[None]] | None = None,
    ) -> None:
        if not isinstance(transport, AudioTransport):
            raise ValueError("transport debe ser una implementación de AudioTransport")
        if source_rate != PIPEWIRE_SAMPLE_RATE:
            raise ValueError(
                f"source_rate debe ser {PIPEWIRE_SAMPLE_RATE} Hz en el canal Tx: {source_rate}"
            )
        if isinstance(capture_samples, bool) or not isinstance(capture_samples, int):
            raise ValueError("capture_samples debe ser int")
        if not 0 < capture_samples <= PIPEWIRE_SAMPLE_RATE:
            raise ValueError("capture_samples debe estar en (0, 48000]")
        if not 1 <= queue_maxsize <= 10_000:
            raise ValueError("queue_maxsize debe estar en [1, 10000]")
        if not isinstance(initial_sequence, int) or not 0 <= initial_sequence <= _MAX_SEQUENCE:
            raise ValueError("initial_sequence debe ser un uint32")
        if (
            isinstance(aec_delay_ms, bool)
            or not isinstance(aec_delay_ms, int | float)
            or not np.isfinite(aec_delay_ms)
            or not 0 <= aec_delay_ms <= 5000
        ):
            raise ValueError("aec_delay_ms debe estar en [0, 5000]")
        if (
            isinstance(aec_attenuation_db, bool)
            or not isinstance(aec_attenuation_db, int | float)
            or not np.isfinite(aec_attenuation_db)
            or not 0 <= aec_attenuation_db <= 120
        ):
            raise ValueError("aec_attenuation_db debe estar en [0, 120]")
        if not callable(clock):
            raise ValueError("clock debe ser invocable")

        self._transport = transport
        self._aec = aec
        self._reference = reference or transport.read_physical_sink_monitor_48k
        self._aec_delay_ms = float(aec_delay_ms)
        self._aec_delay_samples = round(self._aec_delay_ms * PIPEWIRE_SAMPLE_RATE / 1000.0)
        self._aec_attenuation_db = float(aec_attenuation_db)
        self._aec_window_samples = self._aec_delay_samples + capture_samples
        self._mic_window: NDArray[np.int16] = np.empty(0, dtype=np.int16)
        self._ref_window: NDArray[np.int16] = np.empty(0, dtype=np.int16)
        self._ptt = ptt if ptt is not None else PushToTalk(enabled=False)
        self._capture_samples = capture_samples
        self._queue_maxsize = queue_maxsize
        self._queue: asyncio.Queue[bytes | object] = asyncio.Queue(maxsize=queue_maxsize)
        self._resampler = resampler or StreamingResampler(src_rate=PIPEWIRE_SAMPLE_RATE)
        if self._resampler.src_rate != source_rate:
            raise ValueError("el resampler recibido no coincide con source_rate")
        self._clock = clock
        self._sequence = initial_sequence
        self._stop = asyncio.Event()
        self._running = False
        self._ws_client: WSClient | None = None
        self._local_errors: deque[ErrorMessage] = deque(maxlen=64)
        self._metrics = TxStreamMetrics()
        self._tts_tasks: set[asyncio.Task[None]] = set()
        self._last_voice_ts: float | None = None
        self.on_subtitle = on_subtitle
        self.on_translated = on_translated
        self.on_tts = on_tts
        self.on_error = on_error

    # ------------------------------------------------------------------ API

    @property
    def metrics(self) -> TxStreamMetrics:
        """Instantánea de contadores; se puede leer mientras ``run`` está vivo."""
        snapshot = replace(self._metrics)
        if self._ws_client is not None:
            snapshot.reconnects = self._ws_client.metrics.reconnects
        return snapshot

    @property
    def sequence(self) -> int:
        """Próxima ``sequence`` que se asignará a un frame Tx."""
        return self._sequence

    @property
    def pending_frames(self) -> int:
        """Frames esperando en la cola de envío."""
        return self._queue.qsize()

    @property
    def local_errors(self) -> tuple[ErrorMessage, ...]:
        """Errores no fatales generados localmente (p. ej. ``OVERRUN``)."""
        return tuple(self._local_errors)

    @property
    def ptt(self) -> PushToTalk:
        """Gate PTT usado por este stream (para que la UI consulte el estado)."""
        return self._ptt

    @property
    def aec_args(self) -> str:
        """Argumentos del módulo AEC; usa el monitor del sink físico."""
        return self._aec.aec_args if self._aec is not None else ""

    async def push_mic(self, samples: SampleInput, *, flags: int = 0) -> int:
        """Inyecta un bloque PCM 48 kHz ya limpio y devuelve frames emitidos.

        El gating PTT se aplica **antes** del resampleo: si
        :meth:`~client.audio.ptt.PushToTalk.should_send` devuelve ``False`` se
        resamplea silencio para conservar la alineación de chunks, sin marcar
        ``PTT_ACTIVE`` ni contar frames de voz.
        """
        header_flags = int(flags)
        if self._ptt.enabled and self._ptt.active:
            header_flags |= int(Flags.PTT_ACTIVE)
        if self._ptt.should_send(header_flags):
            chunks = await asyncio.to_thread(self._resampler.process, samples)
            emitted = self._enqueue_chunks(chunks, flags=header_flags)
            if emitted and self._last_voice_ts is None:
                self._last_voice_ts = self._clock()
            return emitted

        count = _sample_count(samples)
        silence = np.zeros(count, dtype=np.int16)
        chunks = await asyncio.to_thread(self._resampler.process, silence)
        emitted = self._enqueue_chunks(chunks, flags=0)
        self._metrics.silence_frames += emitted
        return emitted

    def finish(self) -> None:
        """Marca con ``FINAL`` el último frame pendiente en la cola."""
        items: list[bytes | object] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if items and isinstance(items[-1], bytes):
            frame = decode_frame(items[-1])
            items[-1] = encode_frame(
                stream_id=frame.stream_id,
                sequence=frame.sequence,
                flags=frame.flags | int(Flags.FINAL),
                pcm=frame.pcm,
                sample_rate=frame.sample_rate,
                num_samples=frame.num_samples,
            )
        for item in items:
            self._queue.put_nowait(item)

    def stop(self) -> None:
        """Solicita la parada limpia de :meth:`run` mediante ``asyncio.Event``."""
        self._stop.set()
        if not self._queue.full():
            self._queue.put_nowait(_END)

    async def run(self, ws_client: WSClient) -> None:
        """Captura, envía frames Tx y escribe los TTS hasta ``stop()``.

        Propaga :class:`~client.net.ws_client.ClientAuthError` si el token es
        rechazado, sin exponerlo en logs ni en el mensaje de la excepción.
        """
        if self._running:
            raise RuntimeError("TxStream.run ya está en ejecución")
        self._running = True
        self._stop.clear()
        self._ws_client = ws_client
        previous_audio = ws_client.on_audio_frame

        def _on_audio(frame: AudioFrame) -> None:
            if frame.stream_id == STREAM_TX:
                task = asyncio.create_task(self._on_tts_frame(frame), name="babelpipe-tx-tts")
                self._tts_tasks.add(task)
                task.add_done_callback(self._tts_tasks.discard)
            if previous_audio is not None:
                result = previous_audio(frame)
                if result is not None and inspect.isawaitable(result):
                    task = asyncio.create_task(
                        _invoke_awaitable(result), name="tx-previous-audio"
                    )
                    self._tts_tasks.add(task)
                    task.add_done_callback(self._tts_tasks.discard)

        ws_client.on_audio_frame = _on_audio

        # El servidor anuncia cada TTS con `tts_chunk {seq}` y justo después
        # envía el PCM en un frame binario con el mismo `seq`. El dedup global
        # del WSClient registra el seq del anuncio y descartaría el audio como
        # duplicado; liberamos ese seq en cuanto se procesa el anuncio. El
        # handler original se restaura al detener el stream, así que un stream
        # Rx que comparta el socket no se ve afectado.
        previous_handle_text = ws_client._handle_text

        def _handle_text(raw: str) -> None:
            previous_handle_text(raw)
            try:
                message = parse_control_message(raw)
            except ValueError:
                return
            if isinstance(message, TtsChunkMessage):
                ws_client._seen_set.discard(message.seq)

        ws_client._handle_text = _handle_text  # type: ignore[method-assign]

        if self._aec is not None:
            await asyncio.to_thread(self._aec.enable)
        self._transport.start()
        await ws_client.connect()
        sender = asyncio.create_task(self._send_loop(ws_client), name="babelpipe-tx-sender")
        capture = asyncio.create_task(self._capture_loop(), name="babelpipe-tx-capture")
        events = asyncio.create_task(self._events_loop(ws_client), name="babelpipe-tx-events")
        stopper = asyncio.create_task(self._stop.wait(), name="babelpipe-tx-stopper")
        try:
            done, _ = await asyncio.wait(
                {sender, capture, events, stopper}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task in (capture, events) and not task.cancelled():
                    exc = task.exception()
                    if exc is not None:
                        raise exc
        finally:
            self._stop.set()
            for task in (sender, capture, events, stopper):
                task.cancel()
            await asyncio.gather(sender, capture, events, stopper, return_exceptions=True)
            for task in tuple(self._tts_tasks):
                task.cancel()
            if self._tts_tasks:
                await asyncio.gather(*tuple(self._tts_tasks), return_exceptions=True)
            self._tts_tasks.clear()
            ws_client.on_audio_frame = previous_audio
            ws_client._handle_text = previous_handle_text  # type: ignore[method-assign]
            try:
                self._transport.stop()
            finally:
                if self._aec is not None:
                    await asyncio.to_thread(self._aec.disable)
            self._running = False
            self._ws_client = None
            logger.info(
                "tx_stream_stopped",
                extra={
                    "frames": self._metrics.chunks,
                    "silence_frames": self._metrics.silence_frames,
                    "tts_chunks": self._metrics.tts_chunks,
                    "overruns": self._metrics.overruns,
                },
            )

    # ------------------------------------------------------------ interno

    def _read_and_clean(self) -> NDArray[np.int16]:
        """Lee micrófono y referencia y aplica el modelo AEC (fuera del loop)."""
        mic = self._transport.read_mic_48k(self._capture_samples)
        if self._reference is None:
            return mic
        reference = self._reference(self._capture_samples)
        if mic.size != reference.size:
            raise ValueError("la referencia AEC y el micrófono tienen distinta longitud")

        mic_window = _rolling_window(
            self._mic_window, mic, keep=self._aec_delay_samples, total=self._aec_window_samples
        )
        ref_window = _rolling_window(
            self._ref_window,
            reference,
            keep=self._aec_delay_samples,
            total=self._aec_window_samples,
        )
        cleaned_window = suppress_echo(
            mic_window,
            ref_window,
            delay_ms=self._aec_delay_ms,
            attenuation_db=self._aec_attenuation_db,
        )
        self._mic_window = mic_window
        self._ref_window = ref_window
        self._metrics.aec_frames += 1
        self._metrics.echo_energy_before += float(
            np.sum(mic.astype(np.float64) ** 2)
        )
        self._metrics.echo_energy_after += float(
            np.sum(cleaned_window[-mic.size :].astype(np.float64) ** 2)
        )
        return cast(
            "NDArray[np.int16]", cleaned_window[-mic.size :].astype(np.int16, copy=True)
        )

    async def _capture_loop(self) -> None:
        while not self._stop.is_set():
            cleaned = await asyncio.to_thread(self._read_and_clean)
            await self.push_mic(cleaned)

    def _enqueue_chunks(self, chunks: list[bytes], *, flags: int) -> int:
        emitted = 0
        for pcm in chunks:
            raw = encode_frame(
                stream_id=STREAM_TX,
                sequence=self._sequence,
                flags=flags,
                pcm=pcm,
                sample_rate=SAMPLE_RATE,
                num_samples=CHUNK_BYTES // 2,
            )
            self._sequence = (self._sequence + 1) & _MAX_SEQUENCE
            self._enqueue(raw)
            self._metrics.chunks += 1
            emitted += 1
        return emitted

    def _enqueue(self, raw: bytes) -> None:
        try:
            self._queue.put_nowait(raw)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            self._metrics.overruns += 1
            self._enqueue_error(
                _LOCAL_OVERRUN,
                f"cola Tx llena ({self._queue_maxsize}); se descartó el chunk más antiguo",
            )
            self._queue.put_nowait(raw)

    def _enqueue_error(self, code: str, message: str) -> None:
        error = ErrorMessage(code=code, message=message)
        self._local_errors.append(error)
        if self._ws_client is not None:
            self._ws_client.report_local_error(code, message)

    async def _send_loop(self, ws_client: WSClient) -> None:
        while not self._stop.is_set():
            item = await self._queue.get()
            if item is _END:
                return
            if not isinstance(item, bytes):
                continue
            try:
                ws_client.send_frame(item)
            except (ClientClosedError, ClientAuthError, ValueError) as exc:
                logger.warning("tx_send_aborted", extra={"reason": type(exc).__name__})
                return

    async def _on_tts_frame(self, frame: AudioFrame) -> None:
        """Resamplea el TTS a 48 kHz y lo inyecta en ``Translator_Tx_Mic``."""
        pcm48 = await asyncio.to_thread(
            resample_to_48k, frame.pcm, SAMPLE_RATE, dtype=np.int16
        )
        samples = cast("NDArray[np.int16]", pcm48)
        written = await asyncio.to_thread(self._transport.write_tx_48k, samples)
        self._metrics.tts_chunks += 1
        self._metrics.tts_samples += written
        if self._last_voice_ts is not None:
            self._metrics.tts_latency_ms = max(
                0.0, (self._clock() - self._last_voice_ts) * 1000.0
            )
            self._last_voice_ts = None
        await _invoke(self.on_tts, samples.tobytes())

    async def _events_loop(self, ws_client: WSClient) -> None:
        async for message in ws_client.messages():
            if isinstance(message, SubtitleMessage):
                self._metrics.subtitles += 1
                await _invoke(self.on_subtitle, message)
            elif isinstance(message, TranslatedMessage):
                self._metrics.translated += 1
                await _invoke(self.on_translated, message)
            elif isinstance(message, ErrorMessage):
                self._metrics.errors += 1
                await _invoke(self.on_error, message)


__all__ = [
    "DEFAULT_QUEUE_MAXSIZE",
    "RxStream",
    "StreamMetrics",
    "TxStream",
    "TxStreamMetrics",
]
