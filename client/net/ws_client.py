"""Cliente WebSocket asíncrono de BabelPipe (TASK-016 / REQ-03 / AC-05).

Un único socket multiplexa frames PCM binarios y eventos JSON de control. Esta
capa es la mitad cliente del contrato de ``shared``:

* La URL es ``ws(s)://{host}:{port}/ws/translate?token=...``. El token se
  conserva como :class:`~pydantic.SecretStr`: jamás aparece en logs, en
  ``repr`` ni en el texto de las excepciones. Opcionalmente se envía también
  como cabecera ``Authorization: Bearer``.
* Toda configuración externa pasa por :class:`WSClientSettings` (Pydantic v2,
  ``extra='forbid'``) y todo JSON recibido por
  :func:`shared.models.parse_control_message`.
* Reconexión con backoff exponencial 250 ms → 8 s y jitter. Tras cada caída se
  reenvía ``config`` con el ``session_id`` y, acto seguido, un evento
  ``reconnect`` con ``last_seq`` (``ConfigMessage`` es ``extra='forbid'`` y no
  admite ese campo). Se reinyectan los frames no confirmados desde
  ``last_seq - 1`` para perder como máximo un chunk; el servidor descarta los
  duplicados por ``sequence``.
* Los códigos ``error`` no fatales (``OVERRUN``, ``DUPLICATE``,
  ``ECHO_DETECTED``, ``SCHEMA``, ...) se publican al consumidor sin cerrar el
  socket (ADR-006). Solo ``AUTH_FAILED`` es fatal y eleva
  :class:`ClientAuthError`.
* :meth:`WSClient.send_frame` y :meth:`WSClient.close` son no bloqueantes y
  devuelven un *awaitable* opcional: funcionan con ``client.send_frame(f)`` y
  con ``await client.send_frame(f)``. Nunca hay hilos ni esperas en la captura.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Generator
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import Any, cast
from urllib.parse import urlencode
from uuid import UUID, uuid4

from pydantic import (
    UUID4,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from shared.constants import MAX_FRAME_BYTES
from shared.models import (
    AudioConfigMessage,
    ConfigMessage,
    ControlMessage,
    ErrorMessage,
    PingMessage,
    PongMessage,
    ReconnectMessage,
    StreamName,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import AudioFrame, ProtocolError, decode_frame, encode_frame

logger = logging.getLogger("babelpipe.client.net")

_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
_LANG_CODE = re.compile(r"[a-zA-Z]{2}([-_][a-zA-Z0-9]{2,8})?\Z")
_CODE_AUTH = "AUTH_FAILED"
_CODE_OVERRUN = "OVERRUN"
_CODE_SCHEMA = "SCHEMA"
_SEEN_SEQUENCES = 4096


class WSClientError(Exception):
    """Base de errores del cliente, sin trazas ni datos sensibles."""


class ClientAuthError(WSClientError):
    """El servidor rechazó el token. Cierra el socket y detiene los reintentos."""


class ClientConnectionError(WSClientError):
    """Se agotaron los intentos de conexión/recuperación del transporte."""


class ClientClosedError(WSClientError):
    """El cliente ya fue cerrado; no admite más operaciones."""


class WSClientSettings(BaseModel):
    """Frontera de configuración estricta del cliente WebSocket."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    host: str = Field(default="127.0.0.1", min_length=1, max_length=253)
    port: int = Field(default=8765, ge=1, le=65535)
    token: SecretStr | None = None
    secure: bool = False
    ws_path: str = Field(default="/ws/translate", min_length=1, max_length=256)
    session_id: UUID4 | None = None
    mode: StreamName = "rx"
    source_lang: str = Field(default="en", min_length=2, max_length=10)
    target_lang: str = Field(default="es", min_length=2, max_length=10)
    vad: str = Field(default="mock-vad", min_length=1, max_length=64)
    stt: str = Field(default="mock-stt", min_length=1, max_length=64)
    translator: str = Field(default="mock-translator", min_length=1, max_length=64)
    tts: str = Field(default="mock-tts", min_length=1, max_length=64)
    additional_headers: dict[str, str] = Field(default_factory=dict)
    max_reconnect_attempts: int = Field(default=6, ge=0, le=1000)
    initial_backoff_ms: int = Field(default=250, ge=1, le=60_000)
    max_backoff_ms: int = Field(default=8000, ge=1, le=60_000)
    jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)
    queue_maxsize: int = Field(default=50, ge=1, le=10_000)
    replay_size: int = Field(default=256, ge=1, le=10_000)
    ping_interval_s: float | None = Field(default=20.0, gt=0.0)
    open_timeout_s: float = Field(default=5.0, gt=0.0)
    close_timeout_s: float = Field(default=2.0, gt=0.0)
    max_frame_bytes: int = Field(default=MAX_FRAME_BYTES, gt=0)

    @field_validator("host")
    @classmethod
    def _validate_host(cls, value: str) -> str:
        if any(char.isspace() for char in value) or any(char in value for char in "/?#@\\"):
            raise ValueError("host inválido (sin espacios ni caracteres de URL)")
        return value

    @field_validator("ws_path")
    @classmethod
    def _validate_ws_path(cls, value: str) -> str:
        if not value.startswith("/") or any(char in value for char in "?#\\"):
            raise ValueError("ws_path debe empezar con '/' y no contener '?' ni '#'")
        return value.rstrip("/") or "/"

    @field_validator("source_lang", "target_lang")
    @classmethod
    def _validate_lang(cls, value: str) -> str:
        if not _LANG_CODE.fullmatch(value):
            raise ValueError("código de idioma inválido (ISO 639-1/3166)")
        return value

    @field_validator("additional_headers")
    @classmethod
    def _validate_headers(cls, value: dict[str, str]) -> dict[str, str]:
        cleaned: dict[str, str] = {}
        for name, header_value in value.items():
            if not _HEADER_NAME.fullmatch(name):
                raise ValueError(f"nombre de cabecera inválido: {name!r}")
            if any(char in header_value for char in "\r\n\x00"):
                raise ValueError("las cabeceras no pueden contener saltos de línea")
            cleaned[name] = header_value
        return cleaned

    @model_validator(mode="after")
    def _validate_backoff(self) -> WSClientSettings:
        if self.max_backoff_ms < self.initial_backoff_ms:
            raise ValueError("max_backoff_ms debe ser >= initial_backoff_ms")
        return self


@dataclass(slots=True)
class WSClientMetrics:
    """Contadores observables del transporte (instantánea copiable)."""

    connects: int = 0
    reconnects: int = 0
    frames_sent: int = 0
    frames_replayed: int = 0
    frames_received: int = 0
    duplicates: int = 0
    overruns: int = 0
    reconnect_events: int = 0
    pings_sent: int = 0
    pongs_received: int = 0
    errors: int = 0

    def __getitem__(self, key: str) -> int:
        """Acceso tipo dict para informes y tests."""
        return int(getattr(self, key))


class _NoopAwaitable(Awaitable[None]):
    """Awaitable ya completado: ``send_frame`` funcione con o sin ``await``."""

    __slots__ = ()

    def __await__(self) -> Generator[Any, None, None]:
        if False:  # pragma: no cover - convierte la función en generador
            yield
        return None


_DONE = _NoopAwaitable()
_END = object()
_Replay = tuple[int, int, bytes]  # (stream_id, sequence, bytes del frame)


class _CloseAwaitable(Awaitable[None]):
    """Espera a que el runner y sus tareas hijas terminen, sin warn si se ignora."""

    __slots__ = ("_tasks",)

    def __init__(self, tasks: tuple[asyncio.Task[Any], ...]) -> None:
        self._tasks = tasks

    def __await__(self) -> Generator[Any, None, None]:
        pending = [task for task in self._tasks if not task.done()]
        if not pending:
            if False:  # pragma: no cover - mantiene el generador
                yield
            return None
        yield from asyncio.gather(*pending, return_exceptions=True).__await__()
        return None


async def _invoke[T](callback: Callable[[T], None | Awaitable[None]] | None, value: T) -> None:
    """Invoca un callback sincrónico o asíncrono sin bloquear el event loop."""
    if callback is None:
        return
    result = callback(value)
    if result is not None and hasattr(result, "__await__"):
        await result


class WSClient:
    """Transporte WebSocket con reconexión, reanudación y dedup local.

    Ejemplo::

        client = WSClient(host="127.0.0.1", port=8765)
        await client.connect()
        client.send_frame(encode_frame(stream_id=STREAM_RX, sequence=0, pcm=pcm))
        async for event in client.messages():
            if isinstance(event, SubtitleMessage):
                print(event.text)
        await client.close()
    """

    def __init__(
        self,
        settings: WSClientSettings | None = None,
        **overrides: Any,
    ) -> None:
        if settings is not None and overrides:
            raise ValueError("pase settings o argumentos sueltos, no ambos")
        self.settings = (
            settings if settings is not None else WSClientSettings.model_validate(overrides)
        )
        self._session_id: UUID = self.settings.session_id or uuid4()
        self._last_sent: dict[int, int] = {}
        self._sent_frames: deque[_Replay] = deque(maxlen=self.settings.replay_size)
        self._unsent: deque[_Replay] = deque()
        self._connected_once = False
        self._outbound: deque[bytes | str] = deque()
        self._outbound_event = asyncio.Event()
        self._inbound: asyncio.Queue[ControlMessage | object] = asyncio.Queue(
            maxsize=max(self.settings.queue_maxsize, 16)
        )
        self._seen_sequences: deque[int] = deque(maxlen=_SEEN_SEQUENCES)
        self._seen_set: set[int] = set()
        self._stopping = asyncio.Event()
        self._connected = asyncio.Event()
        self._handshake_signal = asyncio.Event()
        self._closed = False
        self._fatal_error: WSClientError | None = None
        self._runner: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._first_connect: asyncio.Future[None] | None = None
        self._ws: ClientConnection | None = None
        self._metrics = WSClientMetrics()
        self._last_pong = 0.0
        self.on_reconnect: Callable[[], None | Awaitable[None]] | None = None
        self.on_audio_frame: Callable[[AudioFrame], None | Awaitable[None]] | None = None

    # ------------------------------------------------------------------ API

    @property
    def session_id(self) -> UUID:
        """UUID4 de la sesión, estable durante toda la vida del cliente."""
        return self._session_id

    @property
    def metrics(self) -> WSClientMetrics:
        """Instantánea de contadores; segura de leer mientras el socket vive."""
        return replace(self._metrics)

    @property
    def is_connected(self) -> bool:
        """``True`` mientras haya un WebSocket abierto y operativo."""
        return self._connected.is_set() and not self._closed

    @property
    def uri(self) -> str:
        """URI completa **solo para depuración local**: contiene el token."""
        return self._build_uri(redact=True)

    @property
    def last_pong_monotonic(self) -> float:
        """Instante ``time.monotonic()`` del último ``pong`` JSON recibido."""
        return self._last_pong

    async def connect(self) -> None:
        """Arranca el bucle de transporte y espera al primer handshake.

        Es idempotente: sucesivas llamadas no abren sockets adicionales. Si el
        servidor rechaza el auth se eleva :class:`ClientAuthError` sin exponer
        el token; si se agotan los reintentos, :class:`ClientConnectionError`.
        """
        if self._closed:
            raise ClientClosedError("el cliente WebSocket ya está cerrado")
        if self._fatal_error is not None:
            raise self._fatal_error
        if self._runner is None:
            loop = asyncio.get_running_loop()
            self._first_connect = loop.create_future()
            self._runner = self._spawn(self._run(), "babelpipe-ws-runner")
        if self._connected.is_set():
            return
        future = self._first_connect
        if future is None:
            return
        if future.done() and not future.cancelled():
            exc = future.exception()
            if exc is not None:
                raise exc
            return
        await asyncio.shield(future)
        # El socket puede abrirse antes de que llegue el primer evento: un
        # AUTH_FAILED inmediato debe elevarse desde connect(), no después.
        if self._fatal_error is not None:
            raise self._fatal_error
        with suppress(TimeoutError):
            await asyncio.wait_for(self._handshake_signal.wait(), timeout=0.1)
        if self._fatal_error is not None:
            raise self._fatal_error

    def send_frame(self, frame: AudioFrame | bytes) -> Awaitable[None]:
        """Encola un frame PCM validado sin bloquear al productor de audio.

        Acepta un :class:`~shared.protocol.AudioFrame` o sus bytes ya
        codificados. Si el buffer está lleno se descarta el frame más antiguo y
        se publica un ``error OVERRUN`` local (ADR-006). Devuelve un awaitable
        no-op para admitir tanto ``send_frame(f)`` como ``await send_frame(f)``.
        """
        if self._closed:
            raise ClientClosedError("el cliente WebSocket ya está cerrado")
        if self._fatal_error is not None:
            raise self._fatal_error
        if isinstance(frame, AudioFrame):
            raw = encode_frame(
                stream_id=frame.stream_id,
                sequence=frame.sequence,
                flags=frame.flags,
                pcm=frame.pcm,
                sample_rate=frame.sample_rate,
                num_samples=frame.num_samples,
            )
        else:
            raw = bytes(frame)
        try:
            decoded = decode_frame(raw)
        except (ProtocolError, TypeError, ValueError) as exc:
            message = exc.message if isinstance(exc, ProtocolError) else str(exc)
            raise ValueError(f"frame PCM inválido: {message}") from None

        item: _Replay = (decoded.stream_id, decoded.sequence, raw)
        if len(self._outbound) >= max(self.settings.queue_maxsize * 4, 64):
            while self._outbound and isinstance(self._outbound[0], bytes):
                self._outbound.popleft()
                break
            self._metrics.overruns += 1
            self._publish_local_error(_CODE_OVERRUN, "buffer de salida lleno; frame descartado")
        self._last_sent[decoded.stream_id] = max(
            decoded.sequence, self._last_sent.get(decoded.stream_id, -1)
        )
        self._unsent.append(item)
        self._outbound.append(raw)
        self._outbound_event.set()
        return _DONE

    async def messages(self) -> AsyncIterator[ControlMessage]:
        """Iterador de eventos JSON validados (incluidos errores locales).

        La iteración termina limpiamente al llamar a :meth:`close` y propaga
        :class:`ClientAuthError` si el servidor rechazó las credenciales.
        """
        while True:
            if self._fatal_error is not None:
                raise self._fatal_error
            item = await self._inbound.get()
            if item is _END:
                if self._fatal_error is not None:
                    raise self._fatal_error
                return
            yield cast("ControlMessage", item)

    def report_local_error(self, code: str, message: str) -> None:
        """Publica un ``error`` generado en el cliente (p. ej. ``OVERRUN``)."""
        self._publish_local_error(code, message)

    def close(self) -> Awaitable[None]:
        """Detiene el transporte sin bloquear y devuelve un awaitable opcional.

        ``client.close()`` y ``await client.close()`` son equivalentes; la
        segunda forma garantiza que no queden tareas pendientes al retornar.
        """
        if not self._closed:
            self._closed = True
            self._stopping.set()
            self._connected.clear()
            self._put_event(_END)
            self._outbound_event.set()
            tasks: list[asyncio.Task[Any]] = []
            if self._runner is not None and not self._runner.done():
                tasks.append(self._runner)
            tasks.extend(task for task in self._tasks if not task.done())
            for task in tasks:
                task.cancel()
        return _CloseAwaitable(tuple(self._tasks | ({self._runner} if self._runner else set())))

    # ------------------------------------------------------------ interno

    def _build_uri(self, *, redact: bool) -> str:
        scheme = "wss" if self.settings.secure else "ws"
        base = (
            f"{scheme}://{self.settings.host}:{self.settings.port}{self.settings.ws_path}"
        )
        if self.settings.token is None:
            return base
        token = "***" if redact else self.settings.token.get_secret_value()
        return f"{base}?{urlencode({'token': token})}"

    def _headers(self) -> dict[str, str]:
        headers = dict(self.settings.additional_headers)
        if self.settings.token is not None:
            has_auth = any(name.lower() == "authorization" for name in headers)
            if not has_auth:
                headers["Authorization"] = (
                    f"Bearer {self.settings.token.get_secret_value()}"
                )
        return headers

    def _spawn(self, coro: Any, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _backoff(self, failed_attempt: int) -> float:
        base = float(
            min(
                self.settings.initial_backoff_ms * (2**failed_attempt),
                self.settings.max_backoff_ms,
            )
        )
        jitter = 1.0 + self.settings.jitter_ratio * random.uniform(-1.0, 1.0)
        return max(0.01, base * jitter / 1000.0)

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=delay)
        except TimeoutError:
            return

    async def _run(self) -> None:
        failed = 0
        try:
            while not self._stopping.is_set():
                try:
                    socket = await ws_connect(
                        self._build_uri(redact=False),
                        additional_headers=self._headers(),
                        open_timeout=self.settings.open_timeout_s,
                        close_timeout=self.settings.close_timeout_s,
                        ping_interval=None,  # keepalive JSON propio del protocolo
                        max_size=self.settings.max_frame_bytes,
                    )
                except asyncio.CancelledError:
                    raise
                except WebSocketException as exc:
                    if self._stopping.is_set():
                        return
                    failed += 1
                    if failed > self.settings.max_reconnect_attempts:
                        self._fail(
                            ClientConnectionError(
                                f"no se pudo conectar tras {failed - 1} reintentos"
                            )
                        )
                        return
                    logger.warning(
                        "ws_connect_failed",
                        extra={"attempt": failed, "reason": type(exc).__name__},
                    )
                    await self._sleep_or_stop(self._backoff(failed - 1))
                    continue

                failed = 0
                self._ws = socket
                is_resume = self._connected_once
                if is_resume:
                    self._metrics.reconnects += 1
                else:
                    self._connected_once = True
                self._connected.set()
                self._publish_first_connect(None)
                logger.info(
                    "ws_connected",
                    extra={
                        "uri": self.uri,
                        "session_id": str(self._session_id),
                        "reconnect": self._metrics.reconnects > 0,
                    },
                )

                reader = self._spawn(self._read_loop(socket), "babelpipe-ws-reader")
                writer = self._spawn(self._write_loop(socket), "babelpipe-ws-writer")
                pinger = self._spawn(self._ping_loop(), "babelpipe-ws-pinger")
                stopper = self._spawn(self._stopping.wait(), "babelpipe-ws-stop")
                self._prepare_handshake(resume=is_resume)
                try:
                    done, _ = await asyncio.wait(
                        {reader, writer, pinger, stopper},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in done:
                        if task is reader and not task.cancelled():
                            task_exc = task.exception()
                            if task_exc is not None:
                                raise task_exc
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "ws_session_failed", extra={"reason": type(exc).__name__}
                    )
                finally:
                    for task in (reader, writer, pinger, stopper):
                        task.cancel()
                    await asyncio.gather(*(reader, writer, pinger, stopper), return_exceptions=True)
                    self._connected.clear()
                    self._ws = None
                    with suppress(ConnectionClosed, WebSocketException, RuntimeError, OSError):
                        await socket.close()

                if self._stopping.is_set() or self._fatal_error is not None:
                    return
                # Un cierre limpio del socket también cuenta como reconexión.
                await self._sleep_or_stop(self._backoff(0))
        except asyncio.CancelledError:
            raise
        finally:
            self._connected.clear()
            self._ws = None
            self._put_event(_END)

    def _prepare_handshake(self, *, resume: bool) -> None:
        """Prepara config + reconnect + replay preservando el orden de los frames."""
        queued = list(self._outbound)
        self._outbound.clear()

        self._enqueue_json(
            ConfigMessage(
                session_id=self._session_id,
                mode=self.settings.mode,
                source_lang=self.settings.source_lang,
                target_lang=self.settings.target_lang,
                vad=self.settings.vad,
                stt=self.settings.stt,
                translator=self.settings.translator,
                tts=self.settings.tts,
                audio_config=AudioConfigMessage(),
            )
        )
        if resume:
            last_seq = max(self._last_sent.values(), default=-1)
            if last_seq >= 0:
                self._enqueue_json(
                    ReconnectMessage(
                        retry_after_ms=0,
                        session_id=self._session_id,
                        last_seq=max(0, last_seq - 1),
                    )
                )

        replay: dict[tuple[int, int], bytes] = {}
        # Un frame ya enviado se reintenta para no perder el que estaba en vuelo.
        if resume:
            for stream_id, sequence, raw in list(self._sent_frames)[-1:]:
                replay[(stream_id, sequence)] = raw
        for item in self._unsent:
            replay[(item[0], item[1])] = item[2]
        self._unsent.clear()
        for raw in replay.values():
            self._outbound.append(raw)
            self._metrics.frames_replayed += 1
        for queued_item in queued:
            if isinstance(queued_item, bytes):
                try:
                    frame = decode_frame(queued_item)
                except ProtocolError:
                    continue
                if (frame.stream_id, frame.sequence) in replay:
                    continue
            self._outbound.append(queued_item)
        self._outbound_event.set()

    async def _write_loop(self, socket: ClientConnection) -> None:
        """Único escritor del socket: conserva el orden FIFO de los eventos.

        Un cierre abrupto del par (p. ej. ``transport.abort()``) hace que
        :meth:`send` eleve ``ConnectionClosedError``. Se captura igual que en
        :meth:`_read_loop` para que el cierre nunca propague una excepción
        hacia la tarea que espera ``close()`` y provoque un flake en la
        reconexión (AC-05).
        """
        while not self._stopping.is_set():
            self._outbound_event.clear()
            while self._outbound and not self._stopping.is_set():
                item = self._outbound.popleft()
                try:
                    await socket.send(item)
                except (ConnectionClosed, WebSocketException) as exc:
                    logger.info(
                        "ws_send_closed",
                        extra={"reason": type(exc).__name__},
                    )
                    return
                if isinstance(item, bytes):
                    try:
                        frame = decode_frame(item)
                    except ProtocolError:
                        continue
                    self._metrics.frames_sent += 1
                    self._sent_frames.append((frame.stream_id, frame.sequence, item))
                    if (
                        self._unsent
                        and self._unsent[0][0] == frame.stream_id
                        and self._unsent[0][1] == frame.sequence
                    ):
                        self._unsent.popleft()
            await self._outbound_event.wait()

    async def _read_loop(self, socket: ClientConnection) -> None:
        try:
            async for raw in socket:
                if isinstance(raw, bytes):
                    self._handle_audio(raw)
                else:
                    self._handle_text(raw)
        except ConnectionClosed as exc:
            close_code = exc.rcvd.code if exc.rcvd is not None else 1006
            logger.info(
                "ws_closed",
                extra={"code": close_code, "session_id": str(self._session_id)},
            )

    async def _ping_loop(self) -> None:
        interval = self.settings.ping_interval_s
        if interval is None:
            await asyncio.Event().wait()
            return
        while not self._stopping.is_set():
            await asyncio.sleep(interval)
            if self._connected.is_set() and not self._stopping.is_set():
                self._enqueue_json(PingMessage(ts=int(time.time() * 1000)))
                self._metrics.pings_sent += 1

    def _handle_audio(self, raw: bytes) -> None:
        try:
            frame = decode_frame(raw)
        except ProtocolError as exc:
            self._publish_local_error(exc.code, exc.message)
            return
        self._metrics.frames_received += 1
        if frame.sequence in self._seen_set:
            self._metrics.duplicates += 1
            return
        self._seen_sequences.append(frame.sequence)
        self._seen_set.add(frame.sequence)
        if len(self._seen_set) > _SEEN_SEQUENCES:
            self._seen_set = set(self._seen_sequences)
        if self.on_audio_frame is not None:
            self._spawn(_invoke(self.on_audio_frame, frame), "babelpipe-ws-audio-cb")

    def _handle_text(self, raw: str) -> None:
        try:
            message = parse_control_message(raw)
        except ValueError as exc:
            self._publish_local_error(_CODE_SCHEMA, str(exc))
            return

        if isinstance(message, ErrorMessage):
            self._metrics.errors += 1
            if message.code == _CODE_AUTH:
                self._fail(ClientAuthError(message.message or "autenticación rechazada"))
                return
            self._put_event(message)
        elif isinstance(message, ReconnectMessage):
            self._metrics.reconnect_events += 1
            self._put_event(message)
        elif isinstance(message, PingMessage):
            self._enqueue_json(PongMessage(ts=message.ts))
            self._put_event(message)
        elif isinstance(message, PongMessage):
            self._metrics.pongs_received += 1
            self._last_pong = time.monotonic()
            self._put_event(message)
        elif isinstance(message, TtsChunkMessage):
            if message.seq in self._seen_set:
                self._metrics.duplicates += 1
                return
            self._seen_sequences.append(message.seq)
            self._seen_set.add(message.seq)
            if len(self._seen_set) > _SEEN_SEQUENCES:
                self._seen_set = set(self._seen_sequences)
            self._put_event(message)
        else:
            self._put_event(message)

    def _enqueue_json(self, message: Any) -> None:
        self._outbound.append(message.model_dump_json())
        self._outbound_event.set()

    def _put_event(self, event: ControlMessage | object) -> None:
        self._handshake_signal.set()
        try:
            self._inbound.put_nowait(event)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._inbound.get_nowait()
            self._metrics.overruns += 1
            self._inbound.put_nowait(event)

    def _publish_local_error(self, code: str, message: str) -> None:
        safe = " ".join(str(message).split())[:2000] or "error local"
        self._put_event(ErrorMessage(code=code, message=safe))

    def _publish_first_connect(self, error: BaseException | None) -> None:
        future = self._first_connect
        if future is None or future.done():
            return
        if error is None:
            future.set_result(None)
        else:
            future.set_exception(error)

    def _fail(self, error: WSClientError) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
            logger.error(
                "ws_auth_failed" if isinstance(error, ClientAuthError) else "ws_failed",
                extra={"reason": str(error)},
            )
        self._stopping.set()
        self._connected.clear()
        self._publish_first_connect(error)
        self._put_event(_END)


__all__ = [
    "ClientAuthError",
    "ClientClosedError",
    "ClientConnectionError",
    "WSClient",
    "WSClientError",
    "WSClientMetrics",
    "WSClientSettings",
]
