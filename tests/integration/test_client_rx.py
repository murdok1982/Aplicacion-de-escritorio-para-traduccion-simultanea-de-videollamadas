"""Tests de integración del cliente Rx y su transporte WS (TASK-016 / AC-05).

Cubren, sin hardware y sin red externa:

* el formato exacto de los frames que produce :class:`RxStream` (16 kHz, 320
  muestras int16 LE, 640 bytes, ``sequence`` monotóno y flags);
* el backpressure no bloqueante con ``OVERRUN`` local (ADR-006);
* la reconexión AC-05 con ``session_id`` + ``last_seq`` ante una caída a mitad
  de stream, sin subtítulos duplicados y con pérdida de ≤ 1 chunk;
* el manejo de ``ping``/``pong`` y de ``error`` no fatal sin cerrar el socket;
* ``AUTH_FAILED`` -> :class:`ClientAuthError` con token ausente en logs;
* el cierre limpio sin tareas asyncio colgadas.

Los servidores mínimos de este fichero usan ``websockets.serve`` en un puerto
efímero de localhost; el resto de pruebas reutiliza ``MockInferenceServer``.
"""

from __future__ import annotations

import asyncio
import logging
import wave
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest
import pytest_asyncio
import websockets
from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServer, WebSocketServerProtocol

from client.net.stream import RxStream
from client.net.ws_client import ClientAuthError, WSClient
from server.pipeline.mock import MockInferenceServer
from shared.constants import (
    CHUNK_BYTES,
    CHUNK_SAMPLES,
    HEADER_SIZE,
    SAMPLE_RATE,
    STREAM_RX,
)
from shared.models import (
    ConfigMessage,
    DuckMessage,
    ErrorMessage,
    PingMessage,
    PongMessage,
    ReadyMessage,
    ReconnectMessage,
    SubtitleMessage,
    TranslatedMessage,
    parse_control_message,
)
from shared.protocol import Flags, decode_frame, encode_frame, iter_pcm_chunks

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
TIMEOUT = 8.0


def _read_wav(name: str) -> bytes:
    path = FIXTURES / name
    assert path.is_file(), f"falta el fixture {path}"
    with wave.open(str(path), "rb") as wav:
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2
        return wav.readframes(wav.getnframes())


def _drain_frames(stream: RxStream) -> list[bytes]:
    frames: list[bytes] = []
    while True:
        try:
            item = stream._queue.get_nowait()  # noqa: SLF001 - test de la cola interna
        except asyncio.QueueEmpty:
            return frames
        if isinstance(item, bytes):
            frames.append(item)


# --------------------------------------------------------------------------
# Servidores mínimos para simular caída, ping y auth
# --------------------------------------------------------------------------


class _DropServer:
    """WS local que aborta la primera conexión y aplica dedup por sesión."""

    def __init__(self, *, total: int, drop_after: int) -> None:
        self.total = total
        self.drop_after = drop_after
        self.connections = 0
        self.handshakes: dict[int, list[object]] = {}
        self.processed: dict[int, list[int]] = {}
        self.duplicates: list[int] = []
        self.subtitle_texts: list[str] = []
        self.reconnect_acks: list[ReconnectMessage] = []
        self.ping_seen: list[int] = []
        self.drop_event = asyncio.Event()
        self._last_seq = -1
        self._server: WebSocketServer | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(next(iter(self._server.sockets)).getsockname()[1])

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/ws/translate"

    async def start(self) -> _DropServer:
        self._server = await websockets.serve(
            self._handler, "127.0.0.1", 0, ping_interval=None
        )
        return self

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def __aenter__(self) -> _DropServer:
        return await self.start()

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def _handler(self, websocket: WebSocketServerProtocol) -> None:
        index = self.connections
        self.connections += 1
        self.handshakes[index] = []
        self.processed[index] = []
        session_id = uuid4()
        try:
            async for raw in websocket:
                if isinstance(raw, bytes):
                    frame = decode_frame(raw)
                    if frame.sequence <= self._last_seq:
                        self.duplicates.append(frame.sequence)
                        continue
                    self._last_seq = frame.sequence
                    self.processed[index].append(frame.sequence)
                    text = f"sub-{frame.sequence:03d}"
                    self.subtitle_texts.append(text)
                    await websocket.send(
                        SubtitleMessage(
                            session_id=session_id,
                            stream="rx",
                            text=text,
                            is_final=True,
                        ).model_dump_json()
                    )
                    if index == 0 and len(self.processed[index]) >= self.drop_after:
                        self.drop_event.set()
                        websocket.transport.abort()  # caída abrupta, sin close frame
                        return
                else:
                    message = parse_control_message(raw)
                    self.handshakes[index].append(message)
                    if isinstance(message, ConfigMessage):
                        session_id = message.session_id
                        await websocket.send(
                            ReadyMessage(
                                models=[], session_id=message.session_id
                            ).model_dump_json()
                        )
                    elif isinstance(message, ReconnectMessage):
                        self._last_seq = max(self._last_seq, message.last_seq)
                        self.reconnect_acks.append(message)
                        await websocket.send(
                            ReconnectMessage(
                                retry_after_ms=0,
                                session_id=message.session_id,
                                last_seq=self._last_seq,
                            ).model_dump_json()
                        )
                    elif isinstance(message, PingMessage):
                        self.ping_seen.append(message.ts)
                        await websocket.send(PongMessage(ts=message.ts).model_dump_json())
                    elif isinstance(message, ErrorMessage):
                        self.handshakes[index].append(message)
        except ConnectionClosed:
            pass


class _AuthServer:
    """WS que exige token y responde ``AUTH_FAILED`` a cualquier cliente."""

    def __init__(self, *, token: str) -> None:
        self.token = token
        self.authorization: str | None = None
        self.query_token: str | None = None
        self._server: WebSocketServer | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(next(iter(self._server.sockets)).getsockname()[1])

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/ws/translate"

    async def start(self) -> _AuthServer:
        self._server = await websockets.serve(
            self._handler, "127.0.0.1", 0, ping_interval=None
        )
        return self

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def __aenter__(self) -> _AuthServer:
        return await self.start()

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def _handler(self, websocket: WebSocketServerProtocol) -> None:
        from urllib.parse import parse_qs, urlsplit

        path = str(getattr(websocket, "path", ""))
        self.authorization = websocket.request_headers.get("Authorization")
        self.query_token = parse_qs(urlsplit(path).query).get("token", [None])[0]
        await websocket.send(
            ErrorMessage(code="AUTH_FAILED", message="token rechazado").model_dump_json()
        )
        await websocket.close(code=1008, reason="AUTH_FAILED")


class _PingServer:
    """WS que envía ping y un error no fatal, y espera el pong del cliente."""

    def __init__(self) -> None:
        self.pong_ts: int | None = None
        self._server: WebSocketServer | None = None
        self._done = asyncio.Event()

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(next(iter(self._server.sockets)).getsockname()[1])

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/ws/translate"

    async def start(self) -> _PingServer:
        self._server = await websockets.serve(
            self._handler, "127.0.0.1", 0, ping_interval=None
        )
        return self

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def wait_pong(self) -> None:
        await asyncio.wait_for(self._done.wait(), TIMEOUT)

    async def __aenter__(self) -> _PingServer:
        return await self.start()

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def _handler(self, websocket: WebSocketServerProtocol) -> None:
        await websocket.send(PingMessage(ts=4242).model_dump_json())
        await websocket.send(
            ErrorMessage(code="OVERRUN", message="error no fatal de prueba").model_dump_json()
        )
        await websocket.send(PongMessage(ts=99).model_dump_json())
        try:
            async for raw in websocket:
                if not isinstance(raw, bytes):
                    message = parse_control_message(raw)
                    if isinstance(message, PongMessage):
                        self.pong_ts = message.ts
                        self._done.set()
        except ConnectionClosed:
            pass


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------


def test_frames_rx_exactos_y_sequence_monotono() -> None:
    """El fixture 48 kHz produce 50 frames de 320 muestras a 16 kHz."""
    pcm48 = _read_wav("tone_1k_48k.wav")
    assert len(pcm48) == 48_000 * 2

    stream = RxStream(queue_maxsize=64)
    # Particiones irregulares: el resampler debe conservar la alineación.
    offset = 0
    while offset < len(pcm48):
        size = min(138, len(pcm48) - offset)
        stream.push(pcm48[offset : offset + size])
        offset += size
    stream.set_ptt(True)
    stream.push(pcm48[: 960 * 2])  # 960 muestras = un frame más con PTT_ACTIVE
    frames = _drain_frames(stream)

    assert len(frames) == 51
    assert all(len(raw) == HEADER_SIZE + CHUNK_BYTES for raw in frames)
    decoded = [decode_frame(raw) for raw in frames]
    assert [frame.sequence for frame in decoded] == list(range(51))
    assert all(frame.stream_id == STREAM_RX for frame in decoded)
    assert all(frame.sample_rate == SAMPLE_RATE for frame in decoded)
    assert all(frame.num_samples == CHUNK_SAMPLES for frame in decoded)
    assert all(
        frame.pcm == raw[HEADER_SIZE:] for frame, raw in zip(decoded, frames, strict=True)
    )
    assert all(not frame.is_final for frame in decoded)
    assert decoded[-1].flags & int(Flags.PTT_ACTIVE)
    assert not decoded[0].flags & int(Flags.PTT_ACTIVE)

    samples = np.frombuffer(decoded[0].pcm, dtype="<i2").astype(np.float64)
    spectrum = np.abs(np.fft.rfft(samples * np.hanning(len(samples))))
    peak_hz = int(np.argmax(spectrum)) * SAMPLE_RATE / len(samples)
    assert abs(peak_hz - 1000.0) < 50.0
    assert stream.metrics.chunks == 51
    assert stream.metrics.overruns == 0


@pytest.mark.asyncio
async def test_backpressure_descarta_antiguo_y_no_bloquea() -> None:
    silence = b"\x00\x00" * 960  # 960 muestras @48 kHz = 1 frame @16 kHz
    stream = RxStream(queue_maxsize=50)

    async def blast() -> None:
        for _ in range(60):
            await stream.push_async(silence)

    await asyncio.wait_for(blast(), timeout=5.0)
    assert stream.metrics.chunks == 60
    assert stream.metrics.overruns == 10
    assert stream.pending_frames == 50
    errors = stream.local_errors
    assert len(errors) == 10
    assert all(isinstance(error, ErrorMessage) for error in errors)
    assert all(error.code == "OVERRUN" for error in errors)
    assert all("Traceback" not in error.message for error in errors)


@pytest_asyncio.fixture
async def mock_server() -> AsyncIterator[MockInferenceServer]:
    server = MockInferenceServer(queue_maxsize=50, min_silence_ms=250)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_stream_rx_con_servidor_mock_publica_eventos_y_ducking(
    mock_server: MockInferenceServer,
) -> None:
    """El stream 16 kHz llega al mock y dispara subtitle/translated/duck."""
    client = WSClient(host="127.0.0.1", port=mock_server.port, ping_interval_s=None)
    subtitles: list[SubtitleMessage] = []
    translated: list[TranslatedMessage] = []
    ducks: list[DuckMessage] = []
    duck_done = asyncio.Event()

    def on_duck(message: DuckMessage) -> None:
        ducks.append(message)
        if message.action == "stop":
            duck_done.set()

    stream = RxStream(
        source_rate=SAMPLE_RATE,
        on_subtitle=subtitles.append,
        on_translated=translated.append,
        on_duck=on_duck,
    )
    run_task = asyncio.create_task(stream.run(client))
    try:
        await client.connect()
        pcm16 = _read_wav("voice_es_16k.wav")
        chunks = list(iter_pcm_chunks(pcm16, CHUNK_SAMPLES))
        assert len(chunks) == 100
        for index, pcm in enumerate(chunks):
            stream.push(pcm)
            if index % 10 == 0:
                await asyncio.sleep(0)  # deja drenar al productor del socket
        stream.finish()
        await asyncio.wait_for(duck_done.wait(), TIMEOUT)
    finally:
        stream.stop()
        await asyncio.wait_for(run_task, TIMEOUT)
        await client.close()

    assert any(event.is_final for event in subtitles)
    assert translated and all(event.is_final for event in translated)
    assert [event.action for event in ducks] == ["start", "stop"]
    assert all(event.stream == "rx" for event in ducks)
    assert stream.metrics.chunks == 100
    assert stream.metrics.duck_starts == 1
    assert stream.metrics.duck_stops == 1


@pytest.mark.asyncio
async def test_ac05_reanuda_con_last_seq_sin_duplicar_subtitulos() -> None:
    """Caída a mitad de stream: resume con last_seq y pierde ≤ 1 chunk."""
    total = 24
    drop_after = 10
    chunks = list(iter_pcm_chunks(_read_wav("tone_1k_16k.wav"), CHUNK_SAMPLES))[:total]
    assert len(chunks) == total

    async with _DropServer(total=total, drop_after=drop_after) as server:
        client = WSClient(
            host="127.0.0.1",
            port=server.port,
            initial_backoff_ms=10,
            max_backoff_ms=40,
            jitter_ratio=0.0,
            max_reconnect_attempts=20,
            ping_interval_s=None,
        )
        received: list[str] = []

        async def consume() -> None:
            async for message in client.messages():
                if isinstance(message, SubtitleMessage):
                    received.append(message.text)

        consumer = asyncio.create_task(consume())
        try:
            await client.connect()
            for sequence, pcm in enumerate(chunks):
                client.send_frame(
                    encode_frame(stream_id=STREAM_RX, sequence=sequence, pcm=pcm)
                )
                await asyncio.sleep(0.015 if sequence < drop_after else 0.005)

            loop = asyncio.get_running_loop()
            deadline = loop.time() + TIMEOUT
            while loop.time() < deadline and len(set(server.subtitle_texts)) < total:
                await asyncio.sleep(0.01)
        finally:
            await client.close()
            await asyncio.wait_for(consumer, TIMEOUT)

    assert server.drop_event.is_set()
    assert server.connections >= 2
    assert client.metrics.reconnects >= 1

    # Handshake de reanudación: config con session_id + reconnect con last_seq.
    second = server.handshakes.get(1, [])
    config = next((msg for msg in second if isinstance(msg, ConfigMessage)), None)
    reconnect = next((msg for msg in second if isinstance(msg, ReconnectMessage)), None)
    assert config is not None and config.session_id == client.session_id
    assert reconnect is not None and reconnect.last_seq >= drop_after - 2
    assert server.reconnect_acks

    processed = [seq for frames in server.processed.values() for seq in frames]
    assert len(processed) == len(set(processed)), "frames duplicados en servidor"
    # El frame en vuelo en el momento de la caída puede perderse: ≤ 1 chunk.
    assert len(set(processed)) >= total - 1, "se perdió más de un chunk"
    assert len(set(range(total)) - set(processed)) <= 1

    assert len(server.subtitle_texts) == len(set(server.subtitle_texts))
    assert len(server.subtitle_texts) >= total - 1
    assert len(received) == len(set(received)), "subtítulos duplicados en el cliente"
    assert len(received) >= total - 1, "se perdió más de un subtítulo"
    assert server.duplicates, "el frame en vuelo debió reenviarse y ser deduplicado"
    print(
        f"[stats] AC-05: reconexiones={client.metrics.reconnects} "
        f"duplicados={server.duplicates} chunks={total} "
        f"subtitulos_cliente={len(received)}"
    )


@pytest.mark.asyncio
async def test_ping_pong_y_error_no_fatal_no_cierran_socket() -> None:
    async with _PingServer() as server:
        client = WSClient(host="127.0.0.1", port=server.port)
        events: list[object] = []

        async def consume() -> None:
            async for message in client.messages():
                events.append(message)

        consumer = asyncio.create_task(consume())
        try:
            await client.connect()
            await server.wait_pong()
            await asyncio.sleep(0.05)
            assert client.is_connected
        finally:
            await client.close()
            await asyncio.wait_for(consumer, TIMEOUT)

    assert server.pong_ts == 4242
    assert client.metrics.pongs_received >= 1
    assert any(isinstance(event, PingMessage) for event in events)
    assert any(isinstance(event, PongMessage) and event.ts == 99 for event in events)
    errors = [event for event in events if isinstance(event, ErrorMessage)]
    assert any(error.code == "OVERRUN" for error in errors)


@pytest.mark.asyncio
async def test_auth_failed_cierra_con_client_auth_error_y_sin_token_en_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "super-secreto-babelpipe-016"
    async with _AuthServer(token=secret) as server:
        client = WSClient(
            host="127.0.0.1",
            port=server.port,
            token=secret,
            max_reconnect_attempts=1,
        )
        caplog.set_level(logging.DEBUG, logger="babelpipe.client.net")
        with pytest.raises(ClientAuthError) as raised:
            await client.connect()
        assert secret not in str(raised.value)
        assert secret not in caplog.text
        assert secret not in client.uri
        assert secret not in repr(client.settings)
        assert server.authorization == f"Bearer {secret}"
        assert server.query_token == secret
        await client.close()
    assert client.metrics.errors == 1


@pytest.mark.asyncio
async def test_close_sin_tareas_colgadas(mock_server: MockInferenceServer) -> None:
    client = WSClient(host="127.0.0.1", port=mock_server.port, ping_interval_s=None)
    stream = RxStream(source_rate=SAMPLE_RATE)
    run_task = asyncio.create_task(stream.run(client))
    await client.connect()
    stream.push(b"\x01\x00" * 320)
    await asyncio.sleep(0.05)

    stream.stop()
    await asyncio.wait_for(run_task, TIMEOUT)
    await client.close()
    for _ in range(5):
        await asyncio.sleep(0)

    alive = [
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith(("babelpipe-ws-", "babelpipe-rx-"))
    ]
    assert alive == [], f"tareas colgadas: {alive}"
    assert client.is_connected is False
