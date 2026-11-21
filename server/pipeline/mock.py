"""Servidor de inferencia mock, determinista y autocontenido (TASK-010).

Este modulo es la "produccion" de la tarea: no es un stub de pruebas, sino un
servidor WebSocket completo que reutiliza el contrato canonico de ``shared``
para ejercitar el pipeline VAD -> STT -> Translator -> TTS sin modelos, sin
descargas y sin hardware. Cada etapa es una implementacion determinista de las
interfaces de :mod:`server.pipeline.base` y toda entrada externa se valida en
la frontera (``decode_frame`` para binario, ``parse_control_message`` para
JSON), devolviendo siempre eventos ``error`` sin stack traces.

Detalles del contrato que aqui se concretan (ARCHITECTURE.md §4.3-4.4):

* La reconexion no mete ``last_seq`` dentro de ``config`` (el modelo Pydantic
  es ``extra='forbid'``): el cliente envia ``config`` con su ``session_id`` y
  despues un evento ``reconnect`` con ``session_id`` + ``last_seq``. El servidor
  conserva el dedup por sesion entre conexiones y responde con ``reconnect``.
* El bit ``FINAL`` de la cabecera cierra la clausula pendiente (flush del VAD),
  ademas del cierre natural por silencio de ``min_silence_ms``. Al cerrar la
  conexion tambien se hace flush, para no perder la ultima clausula.
* El backpressure usa ``put_nowait`` sobre una cola acotada: si se llena se
  descarta el chunk mas antiguo y se emite ``error OVERRUN``. Ninguna recepcion
  de audio espera a la cola ni a la red: todos los eventos salen por una cola
  de salida que un unico task drena en orden FIFO.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import struct
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import websockets
from pydantic import BaseModel
from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServer, WebSocketServerProtocol

from shared.constants import (
    BYTES_PER_SAMPLE,
    CHUNK_BYTES,
    CHUNK_MS,
    CHUNK_SAMPLES,
    SAMPLE_RATE,
    STREAM_RX,
    STREAM_TX,
)
from shared.models import (
    AudioConfigMessage,
    ConfigMessage,
    DuckMessage,
    ErrorMessage,
    PingMessage,
    PongMessage,
    PttMessage,
    ReadyMessage,
    ReconnectMessage,
    ResetMessage,
    StreamName,
    SubtitleMessage,
    TranslatedMessage,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import ProtocolError, decode_frame, encode_frame

from .base import STT, TTS, VAD, AudioChunk, PipelineError, Segment, Translator

logger = logging.getLogger("babelpipe.server.mock")

MOCK_MODELS: tuple[str, ...] = ("mock-vad", "mock-stt", "mock-translator", "mock-tts")
DEFAULT_WS_PATH = "/ws/translate"
_CODE_SCHEMA = "SCHEMA"
_CODE_FORMAT = "FORMAT"
_CODE_MODEL_ERROR = "MODEL_ERROR"
_MAX_LATENCY_MS = 5000


@dataclass(slots=True)
class MockServerStats:
    """Contadores observables del servidor mock (para aserciones de carga)."""

    frames_received: int = 0
    frames_accepted: int = 0
    frames_processed: int = 0
    frames_duplicate: int = 0
    frames_invalid: int = 0
    frames_dropped_overrun: int = 0
    control_invalid: int = 0
    ptt_updates: int = 0
    subtitles_partial: int = 0
    subtitles_final: int = 0
    translated: int = 0
    tts_chunks: int = 0


@dataclass(slots=True)
class _SessionState:
    """Estado de dedup y negociacion que sobrevive a una reconexion."""

    session_id: UUID
    mode: StreamName = "rx"
    source_lang: str = "en"
    target_lang: str = "es"
    last_seq: dict[int, int] = field(default_factory=dict)
    segments_seen: int = 0
    ptt_active: bool = False
    audio_configured: bool = False


@dataclass(frozen=True, slots=True)
class _QueuedChunk:
    """Chunk canonical ya encolado junto a su marca de fin de clausula."""

    chunk: AudioChunk
    is_final: bool


class MockVAD(VAD):
    """VAD determinista por energia con hysteresis y cierre por silencio.

    Respeta la maquina de estados del contrato: no corta por ventanas ciegas,
    emite parciales cada ``partial_interval_ms`` y como maximo un final por
    clausula. Los umbrales son fijos y derivados solo del PCM de entrada.
    """

    SPEECH_ON_RMS = 200.0
    SPEECH_OFF_RMS = 120.0

    def __init__(
        self,
        *,
        min_silence_ms: int = 250,
        partial_interval_ms: int = 100,
        sample_rate: int = SAMPLE_RATE,
    ) -> None:
        if sample_rate != SAMPLE_RATE:
            raise PipelineError(_CODE_FORMAT, f"sample_rate debe ser {SAMPLE_RATE}")
        if min_silence_ms <= 0 or partial_interval_ms <= 0:
            raise PipelineError(_CODE_SCHEMA, "los intervalos deben ser > 0")
        self.sample_rate = sample_rate
        self.min_silence_ms = min_silence_ms
        self.partial_interval_ms = partial_interval_ms
        self._silence_limit = max(1, math.ceil(min_silence_ms / CHUNK_MS))
        self._partial_chunks = max(1, math.ceil(partial_interval_ms / CHUNK_MS))
        self._next_id = 0
        self._clear_clause()

    def _clear_clause(self) -> None:
        """Limpia la clausula actual conservando la numeracion de segmentos."""
        self._audio: list[bytes] = []
        self._open = False
        self._speech_chunks = 0
        self._silence_run = 0
        self._chunks = 0
        self._next_partial = 0

    def reset(self) -> None:
        """Descarta la segmentacion y reinicia la numeracion."""
        self._next_id = 0
        self._clear_clause()

    @staticmethod
    def _rms(pcm: bytes) -> float:
        """RMS en LSB int16 de un bloque ``pcm_s16le`` canonico."""
        count = len(pcm) // BYTES_PER_SAMPLE
        if count == 0:
            return 0.0
        samples = struct.unpack(f"<{count}h", pcm)
        return math.sqrt(sum(sample * sample for sample in samples) / count)

    def _segment(self, *, is_final: bool, audio: bytes, latency_ms: int) -> Segment:
        segment = Segment(
            id=self._next_id,
            audio=audio,
            is_final=is_final,
            latency_ms=max(0, min(latency_ms, _MAX_LATENCY_MS)),
        )
        self._next_id += 1
        return segment

    def _finalize(self) -> Segment:
        """Cierra la clausula recortando el silencio final de deteccion."""
        silence = min(self._silence_run, len(self._audio))
        keep = len(self._audio) - silence
        audio = b"".join(self._audio[:keep])
        latency_ms = self._silence_run * CHUNK_MS
        self._clear_clause()
        return self._segment(is_final=True, audio=audio, latency_ms=latency_ms)

    async def process(self, chunk: AudioChunk) -> list[Segment]:
        """Consume un chunk canonico y devuelve parciales y/o un cierre."""
        if not isinstance(chunk, AudioChunk):
            raise PipelineError(_CODE_SCHEMA, "chunk debe ser un AudioChunk")
        if len(chunk.pcm) != CHUNK_BYTES:
            raise PipelineError(_CODE_FORMAT, f"pcm debe tener {CHUNK_BYTES} bytes")
        threshold = self.SPEECH_OFF_RMS if self._open else self.SPEECH_ON_RMS
        speech = self._rms(chunk.pcm) >= threshold

        if not self._open:
            if not speech:
                return []
            self._open = True
            self._audio = [chunk.pcm]
            self._chunks = 1
            self._speech_chunks = 1
            self._silence_run = 0
        else:
            self._audio.append(chunk.pcm)
            self._chunks += 1
            if speech:
                self._speech_chunks += 1
                self._silence_run = 0
            else:
                self._silence_run += 1

        segments: list[Segment] = []
        if self._chunks >= self._next_partial:
            self._next_partial = self._chunks + self._partial_chunks
            segments.append(
                self._segment(is_final=False, audio=b"".join(self._audio), latency_ms=0)
            )
        if self._silence_run >= self._silence_limit:
            segments.append(self._finalize())
        return segments

    async def flush(self) -> list[Segment]:
        """Cierra la clausula abierta al agotarse la entrada."""
        if not self._open or not self._audio:
            return []
        return [self._finalize()]


class MockSTT(STT):
    """Transcripcion fija derivada del hash del audio de la clausula."""

    PHRASES: tuple[str, ...] = (
        "Hola, ¿cómo estás?",
        "Nos vemos mañana",
        "Babelpipe traduce esta cláusula",
        "El audio llega sin pérdida",
    )

    async def transcribe(self, segment: Segment) -> Segment:
        """Devuelve una copia con texto estable para el mismo PCM."""
        if not isinstance(segment, Segment):
            raise PipelineError(_CODE_SCHEMA, "segment debe ser un Segment")
        if segment.audio:
            digest = hashlib.blake2b(segment.audio, digest_size=8).digest()
            text = self.PHRASES[digest[0] % len(self.PHRASES)]
        else:
            text = ""
        return Segment(
            id=segment.id,
            text=text,
            is_final=segment.is_final,
            audio=segment.audio,
            latency_ms=segment.latency_ms,
        )


class MockTranslator(Translator):
    """Traduccion determinista por idioma destino, sin modelos ni red."""

    TRANSLATIONS: dict[str, str] = {
        "es": "Hola, ¿cómo estás?",
        "en": "Hello, how are you?",
        "fr": "Bonjour, comment ça va ?",
        "de": "Hallo, wie geht es dir?",
    }

    async def translate(self, segment: Segment, target_lang: str) -> Segment:
        """Sustituye el texto por su equivalente en ``target_lang``."""
        if not isinstance(segment, Segment):
            raise PipelineError(_CODE_SCHEMA, "segment debe ser un Segment")
        if not isinstance(target_lang, str) or not 2 <= len(target_lang) <= 10:
            raise PipelineError(_CODE_SCHEMA, "target_lang invalido")
        text = self.TRANSLATIONS.get(
            target_lang.lower(), f"[{target_lang}] {segment.text}"
        )
        return Segment(
            id=segment.id,
            text=text,
            is_final=segment.is_final,
            audio=segment.audio,
            latency_ms=segment.latency_ms,
        )


class MockTTS(TTS):
    """TTS sintetico: PCM determinista derivado del texto de la clausula."""

    def __init__(self, *, chunks_per_clause: int = 2, sample_rate: int = SAMPLE_RATE) -> None:
        if chunks_per_clause <= 0:
            raise PipelineError(_CODE_SCHEMA, "chunks_per_clause debe ser > 0")
        if sample_rate != SAMPLE_RATE:
            raise PipelineError(_CODE_FORMAT, f"sample_rate debe ser {SAMPLE_RATE}")
        self.chunks_per_clause = chunks_per_clause
        self.sample_rate = sample_rate

    async def synthesize_stream(self, segment: Segment) -> AsyncIterator[bytes]:
        """Devuelve el iterador de chunks ``pcm_s16le`` canonicos (640 bytes)."""
        if not isinstance(segment, Segment):
            raise PipelineError(_CODE_SCHEMA, "segment debe ser un Segment")
        return self._pcm_chunks(segment.text)

    async def _pcm_chunks(self, text: str) -> AsyncIterator[bytes]:
        """Genera PCM determinista a partir del hash del texto."""
        digest = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
        frequency = 180.0 + float(digest[1] % 8) * 40.0
        amplitude = 6000.0 + float(digest[2] % 4) * 2000.0
        step = 2.0 * math.pi * frequency / self.sample_rate
        for index in range(self.chunks_per_clause):
            offset = index * CHUNK_SAMPLES * step
            samples = (
                struct.pack("<h", int(amplitude * math.sin(offset + position * step)))
                for position in range(CHUNK_SAMPLES)
            )
            yield b"".join(samples)


def _drain_inbound(queue: asyncio.Queue[_QueuedChunk | None]) -> None:
    """Vacia la cola de audio sin esperar (usado por ``reset`` y cierre)."""
    while True:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            return


def _signal_end(queue: asyncio.Queue[_QueuedChunk | None]) -> None:
    """Encola el centinela de fin de stream sin bloquear nunca."""
    try:
        queue.put_nowait(None)
    except asyncio.QueueFull:
        _drain_inbound(queue)
        queue.put_nowait(None)


class _ConnectionContext:
    """Estado de una conexion: colas, pipeline y sesion negociada."""

    def __init__(
        self,
        server: MockInferenceServer,
        websocket: WebSocketServerProtocol,
    ) -> None:
        self.server = server
        self.websocket = websocket
        self.inbound: asyncio.Queue[_QueuedChunk | None] = asyncio.Queue(
            maxsize=server.queue_maxsize
        )
        self.outbound: asyncio.Queue[BaseModel | bytes | None] = asyncio.Queue()
        self.session: _SessionState | None = None
        self.vad = MockVAD(min_silence_ms=server.min_silence_ms)
        self.stt = MockSTT()
        self.translator = MockTranslator()
        self.tts = MockTTS()
        self.tts_seq = 0
        self.sender_task: asyncio.Task[None] | None = None
        self.consumer_task: asyncio.Task[None] | None = None

    def emit(self, message: BaseModel | bytes | None) -> None:
        """Publica un evento en la cola de salida (nunca bloquea)."""
        self.outbound.put_nowait(message)

    async def send_loop(self) -> None:
        """Unico escritor del socket: conserva el orden FIFO de los eventos."""
        try:
            while True:
                item = await self.outbound.get()
                if item is None:
                    return
                if isinstance(item, bytes):
                    await self.websocket.send(item)
                else:
                    await self.websocket.send(item.model_dump_json())
        except ConnectionClosed:
            return

    async def consume_loop(self) -> None:
        """Consumidor del pipeline: VAD -> STT -> MT -> TTS por cada chunk."""
        try:
            while True:
                item = await self.inbound.get()
                if item is None:
                    return
                self.server._stats.frames_processed += 1
                for segment in await self.vad.process(item.chunk):
                    await self._emit_segment(segment)
                if item.is_final:
                    for segment in await self.vad.flush():
                        await self._emit_segment(segment)
                if self.server.processing_delay_ms > 0:
                    await asyncio.sleep(self.server.processing_delay_ms / 1000.0)
        finally:
            for segment in await self.vad.flush():
                await self._emit_segment(segment)

    async def _emit_segment(self, segment: Segment) -> None:
        """Emite la secuencia completa de eventos de un segmento."""
        session = self.session
        if session is None:
            return
        stats = self.server._stats
        try:
            transcribed = await self.stt.transcribe(segment)
            self.emit(
                SubtitleMessage(
                    session_id=session.session_id,
                    stream=session.mode,
                    text=transcribed.text,
                    is_final=transcribed.is_final,
                    latency_ms=max(0, min(transcribed.latency_ms, _MAX_LATENCY_MS)),
                )
            )
            if transcribed.is_final:
                stats.subtitles_final += 1
            else:
                stats.subtitles_partial += 1
            if not transcribed.is_final:
                return

            translated = await self.translator.translate(transcribed, session.target_lang)
            self.emit(
                TranslatedMessage(
                    text=translated.text, is_final=True, lang=session.target_lang
                )
            )
            stats.translated += 1

            # El ducking arranca antes del primer TTS y se suelta al terminar.
            self.emit(DuckMessage(action="start", stream=session.mode))
            stream = await self.tts.synthesize_stream(translated)
            async for pcm in stream:
                seq = self.tts_seq
                self.tts_seq += 1
                self.emit(TtsChunkMessage(seq=seq, duration_ms=CHUNK_MS))
                stream_id = STREAM_RX if session.mode == "rx" else STREAM_TX
                self.emit(encode_frame(stream_id=stream_id, sequence=seq, pcm=pcm))
                stats.tts_chunks += 1
            self.emit(DuckMessage(action="stop", stream=session.mode))
            session.segments_seen += 1
        except PipelineError as exc:
            self.server._emit_error(self, exc.code, exc.message)
        except Exception:
            logger.exception("fallo interno en el pipeline mock")
            self.server._emit_error(self, _CODE_MODEL_ERROR, "fallo interno del pipeline mock")


class MockInferenceServer:
    """Servidor WS mock autocontenido en un puerto efímero (solo localhost).

    Uso tipico en pytest::

        server = MockInferenceServer(queue_maxsize=4, processing_delay_ms=5)
        await server.start()
        async with websockets.connect(server.url) as ws:
            ...
        await server.stop()

    ``start()`` reinicia estadisticas y sesiones; ``stop()`` cierra el socket y
    espera a que terminen los handlers, sin dejar puertos colgados.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        ws_path: str = DEFAULT_WS_PATH,
        queue_maxsize: int = 50,
        min_silence_ms: int = 250,
        processing_delay_ms: float = 0.0,
        emit_duplicate_warning: bool = False,
        max_frame_bytes: int = 8192,
    ) -> None:
        if not 0 <= port <= 65535:
            raise ValueError("port debe estar en [0, 65535]")
        if queue_maxsize <= 0:
            raise ValueError("queue_maxsize debe ser > 0")
        if processing_delay_ms < 0:
            raise ValueError("processing_delay_ms debe ser >= 0")
        if min_silence_ms <= 0:
            raise ValueError("min_silence_ms debe ser > 0")
        self.host = host
        self.port_requested = port
        self.ws_path = ws_path
        self.queue_maxsize = queue_maxsize
        self.min_silence_ms = min_silence_ms
        self.processing_delay_ms = processing_delay_ms
        self.emit_duplicate_warning = emit_duplicate_warning
        self.max_frame_bytes = max_frame_bytes
        self._server: WebSocketServer | None = None
        self._stats = MockServerStats()
        self._sessions: dict[UUID, _SessionState] = {}
        self._ready = False

    @property
    def port(self) -> int:
        """Puerto real del socket (util con ``port=0``)."""
        if self._server is None:
            return self.port_requested
        sock = next(iter(self._server.sockets), None)
        return int(sock.getsockname()[1]) if sock is not None else self.port_requested

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}{self.ws_path}"

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def stats(self) -> dict[str, int]:
        """Instantanea de contadores para aserciones de carga."""
        return asdict(self._stats)

    async def start(self) -> MockInferenceServer:
        """Arranca el servidor en ``host:port`` y devuelve ``self``."""
        if self._server is not None:
            raise RuntimeError("el servidor mock ya esta arrancado")
        self._server = await websockets.serve(
            self._connection_handler,
            self.host,
            self.port_requested,
            ping_interval=None,
            max_size=self.max_frame_bytes,
        )
        self._stats = MockServerStats()
        self._sessions.clear()
        self._ready = True
        return self

    async def stop(self) -> None:
        """Detiene el servidor y espera a que no queden conexiones vivas."""
        self._ready = False
        server = self._server
        if server is None:
            return
        server.close()
        await server.wait_closed()
        self._server = None

    async def __aenter__(self) -> MockInferenceServer:
        return await self.start()

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.stop()

    @staticmethod
    def _emit_error(ctx: _ConnectionContext, code: str, message: str) -> None:
        """Envia un ``error`` acotado, sin trazas ni datos sensibles."""
        safe = " ".join(str(message).split())[:2000] or "error"
        ctx.emit(ErrorMessage(code=code, message=safe))

    def _session_for(self, ctx: _ConnectionContext) -> _SessionState:
        """Sesion activa; si no hubo ``config``, crea una por defecto."""
        if ctx.session is None:
            session = _SessionState(session_id=uuid4())
            self._sessions[session.session_id] = session
            ctx.session = session
        return ctx.session

    async def _connection_handler(
        self,
        websocket: WebSocketServerProtocol,
        path: str | None = None,
    ) -> None:
        """Valida la ruta y despacha cada frame recibido en la conexion."""
        request_path = path if path is not None else str(getattr(websocket, "path", ""))
        if urlsplit(request_path).path.rstrip("/") != self.ws_path.rstrip("/"):
            await websocket.close(code=1008, reason="INVALID_PATH")
            return

        ctx = _ConnectionContext(self, websocket)
        ctx.sender_task = asyncio.create_task(ctx.send_loop(), name="mock-ws-sender")
        ctx.consumer_task = asyncio.create_task(ctx.consume_loop(), name="mock-pipeline")
        try:
            async for raw in websocket:
                try:
                    if isinstance(raw, bytes):
                        self._handle_binary(ctx, raw)
                    else:
                        await self._handle_text(ctx, raw)
                except Exception:
                    logger.exception("fallo al despachar un mensaje en el mock")
                    self._emit_error(ctx, _CODE_SCHEMA, "mensaje no procesable")
        except ConnectionClosed:
            logger.debug("conexion mock cerrada")
        finally:
            await self._finish_connection(ctx)

    @staticmethod
    async def _finish_connection(ctx: _ConnectionContext) -> None:
        """Cierra productor y consumidor con timeout para no colgar el handler."""
        _signal_end(ctx.inbound)
        consumer = ctx.consumer_task
        if consumer is not None:
            try:
                await asyncio.wait_for(consumer, timeout=5.0)
            except TimeoutError:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
        ctx.emit(None)
        sender = ctx.sender_task
        if sender is not None:
            try:
                await asyncio.wait_for(sender, timeout=5.0)
            except TimeoutError:
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)

    def _handle_binary(self, ctx: _ConnectionContext, raw: bytes) -> None:
        """Valida, deduplica y encola un frame PCM sin bloquear."""
        try:
            frame = decode_frame(raw)
        except ProtocolError as exc:
            self._stats.frames_invalid += 1
            self._emit_error(ctx, exc.code, exc.message)
            return

        self._stats.frames_received += 1
        session = self._session_for(ctx)
        last = session.last_seq.get(frame.stream_id)
        if last is not None and frame.sequence <= last:
            self._stats.frames_duplicate += 1
            if self.emit_duplicate_warning:
                self._emit_error(
                    ctx,
                    "DUPLICATE",
                    f"sequence {frame.sequence} <= last_seq {last}",
                )
            return
        session.last_seq[frame.stream_id] = frame.sequence

        queued = _QueuedChunk(
            chunk=AudioChunk(
                pcm=frame.pcm,
                sequence=frame.sequence,
                timestamp_ms=frame.sequence * CHUNK_MS,
            ),
            is_final=frame.is_final,
        )
        try:
            ctx.inbound.put_nowait(queued)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                ctx.inbound.get_nowait()
            ctx.inbound.put_nowait(queued)
            self._stats.frames_dropped_overrun += 1
            self._emit_error(
                ctx,
                "OVERRUN",
                f"cola llena ({self.queue_maxsize}); se descarto el chunk mas antiguo",
            )
        self._stats.frames_accepted += 1

    async def _handle_text(self, ctx: _ConnectionContext, raw: str) -> None:
        """Parsea y aplica un mensaje de control con esquema estricto."""
        try:
            message = parse_control_message(raw)
        except ValueError as exc:
            self._stats.control_invalid += 1
            self._emit_error(ctx, _CODE_SCHEMA, str(exc))
            return

        if isinstance(message, ConfigMessage):
            session = self._sessions.get(message.session_id)
            if session is None:
                session = _SessionState(session_id=message.session_id)
                self._sessions[message.session_id] = session
            session.mode = message.mode
            session.source_lang = message.source_lang
            session.target_lang = message.target_lang
            # No se borra last_seq: la reconexion necesita el dedup persistente.
            ctx.session = session
            ctx.tts_seq = 0
            ctx.emit(
                ReadyMessage(models=list(MOCK_MODELS), session_id=message.session_id)
            )
        elif isinstance(message, PingMessage):
            ctx.emit(PongMessage(ts=message.ts))
        elif isinstance(message, ResetMessage):
            session = self._sessions.get(message.session_id)
            if session is not None:
                session.last_seq.clear()
                session.segments_seen = 0
            ctx.vad.reset()
            _drain_inbound(ctx.inbound)
            ctx.tts_seq = 0
        elif isinstance(message, ReconnectMessage):
            session = self._sessions.setdefault(
                message.session_id, _SessionState(session_id=message.session_id)
            )
            stream_id = STREAM_RX if session.mode == "rx" else STREAM_TX
            session.last_seq[stream_id] = max(
                session.last_seq.get(stream_id, 0), message.last_seq
            )
            ctx.session = session
            ctx.emit(
                ReconnectMessage(
                    retry_after_ms=0,
                    session_id=message.session_id,
                    last_seq=session.last_seq[stream_id],
                )
            )
        elif isinstance(message, PttMessage):
            session = self._session_for(ctx)
            session.ptt_active = message.active
            self._stats.ptt_updates += 1
        elif isinstance(message, AudioConfigMessage):
            session = self._session_for(ctx)
            session.audio_configured = True
        else:
            self._emit_error(
                ctx,
                _CODE_SCHEMA,
                f"mensaje '{message.type}' no aceptado desde el cliente",
            )
