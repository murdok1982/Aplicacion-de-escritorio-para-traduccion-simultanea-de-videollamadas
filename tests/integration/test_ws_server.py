"""Tests de integración del servidor WebSocket real (TASK-009 / AC-04).

Arrancan el servidor en un puerto efímero con un token de prueba y comprueban
autenticación, control JSON, frames binarios, rate limiting y ``/healthz``.
"""

from __future__ import annotations

import asyncio
import json
import struct
import urllib.request
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
import websockets
from pydantic import SecretStr
from websockets.exceptions import ConnectionClosed

from server.config import ServerSettings
from server.health import HealthState
from server.main import ServerHandle, create_server
from shared.constants import CHUNK_SAMPLES, HEADER_STRUCT, MAGIC, SAMPLE_RATE, STREAM_RX, VERSION
from shared.models import ConfigMessage, PingMessage, ResetMessage, parse_control_message

TOKEN = "test-token"


def _pcm() -> bytes:
    return b"".join(struct.pack("<h", (index * 37) % 2000 - 1000) for index in range(CHUNK_SAMPLES))


def _raw_frame(*, magic: bytes = MAGIC, sample_rate: int = SAMPLE_RATE) -> bytes:
    return (
        struct.pack(
            HEADER_STRUCT,
            magic,
            VERSION,
            0,
            STREAM_RX,
            42,
            sample_rate,
            CHUNK_SAMPLES,
        )
        + _pcm()
    )


def _config_payload(session_id: UUID) -> str:
    return ConfigMessage(
        session_id=session_id,
        mode="rx",
        source_lang="en",
        target_lang="es",
        vad="silero-vad",
        stt="faster-whisper",
        translator="opus-mt",
        tts="kokoro",
        audio_config={
            "sample_rate": 16000,
            "channels": 1,
            "format": "pcm_s16le",
            "chunk_ms": 20,
        },
    ).model_dump_json()


def _uri(handle: ServerHandle, token: str | None = None) -> str:
    base = f"ws://127.0.0.1:{handle.port}{handle.settings.ws_path}"
    return f"{base}?token={token}" if token is not None else base


@pytest_asyncio.fixture
async def server() -> AsyncIterator[ServerHandle]:
    settings = ServerSettings(
        host="127.0.0.1",
        port=0,
        token=SecretStr(TOKEN),
        ping_interval=None,
        health_host="127.0.0.1",
        health_port=0,
    )
    handle = await create_server(settings, HealthState())
    try:
        yield handle
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_handshake_sin_token_cierra_1008(server: ServerHandle) -> None:
    with pytest.raises(ConnectionClosed) as excinfo:
        async with websockets.connect(_uri(server)) as websocket:
            await websocket.recv()
    close = excinfo.value.rcvd
    assert close is not None
    assert close.code == 1008
    assert close.reason == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_handshake_con_token_incorrecto_cierra_1008(server: ServerHandle) -> None:
    with pytest.raises(ConnectionClosed) as excinfo:
        async with websockets.connect(_uri(server, "wrong-token")) as websocket:
            await websocket.recv()
    close = excinfo.value.rcvd
    assert close is not None
    assert close.code == 1008
    assert close.reason == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_config_devuelve_ready_y_autorizacion_bearer(server: ServerHandle) -> None:
    session_id = uuid4()

    async with websockets.connect(_uri(server, TOKEN)) as websocket:
        await websocket.send(_config_payload(session_id))
        ready = parse_control_message(await websocket.recv())
        assert ready.type == "ready"
        assert ready.session_id == session_id
        assert ready.models == []
        assert server.health.active_sessions == 1

    async with websockets.connect(
        _uri(server),
        extra_headers={"Authorization": f"Bearer {TOKEN}"},
    ) as websocket:
        await websocket.send(_config_payload(uuid4()))
        ready = parse_control_message(await websocket.recv())
        assert ready.type == "ready"


@pytest.mark.asyncio
async def test_ping_pong_y_reset_mantienen_la_conexion(server: ServerHandle) -> None:
    session_id = uuid4()
    async with websockets.connect(_uri(server, TOKEN)) as websocket:
        await websocket.send(_config_payload(session_id))
        ready = parse_control_message(await websocket.recv())
        assert ready.type == "ready"

        await websocket.send(PingMessage(ts=4242).model_dump_json())
        pong = parse_control_message(await websocket.recv())
        assert pong.type == "pong"
        assert pong.ts == 4242

        await websocket.send(ResetMessage(session_id=session_id).model_dump_json())
        await websocket.send(PingMessage(ts=7).model_dump_json())
        pong = parse_control_message(await websocket.recv())
        assert pong.type == "pong"
        assert pong.ts == 7


@pytest.mark.asyncio
async def test_frame_con_magic_invalido_devuelve_schema_y_no_cierra(server: ServerHandle) -> None:
    async with websockets.connect(_uri(server, TOKEN)) as websocket:
        await websocket.send(_raw_frame(magic=b"XX"))
        error = parse_control_message(await websocket.recv())
        assert error.type == "error"
        assert error.code == "SCHEMA"
        assert "Traceback" not in error.message

        await websocket.send(PingMessage(ts=11).model_dump_json())
        pong = parse_control_message(await websocket.recv())
        assert pong.type == "pong"
        assert pong.ts == 11


@pytest.mark.asyncio
async def test_frame_con_sample_rate_incorrecto_devuelve_format(server: ServerHandle) -> None:
    async with websockets.connect(_uri(server, TOKEN)) as websocket:
        await websocket.send(_raw_frame(sample_rate=48000))
        error = parse_control_message(await websocket.recv())
        assert error.type == "error"
        assert error.code == "FORMAT"
        assert "48000" in error.message

        await websocket.send(PingMessage(ts=12).model_dump_json())
        pong = parse_control_message(await websocket.recv())
        assert pong.type == "pong"


@pytest.mark.asyncio
async def test_frame_valido_es_aceptado_sin_cerrar(server: ServerHandle) -> None:
    async with websockets.connect(_uri(server, TOKEN)) as websocket:
        await websocket.send(_raw_frame())
        await websocket.send(PingMessage(ts=13).model_dump_json())
        pong = parse_control_message(await websocket.recv())
        assert pong.type == "pong"
        assert pong.ts == 13


@pytest.mark.asyncio
async def test_json_con_campo_extra_devuelve_schema_sin_stack_trace(server: ServerHandle) -> None:
    payload: dict[str, Any] = {"type": "ping", "ts": 1, "campo_extra": True}
    async with websockets.connect(_uri(server, TOKEN)) as websocket:
        await websocket.send(json.dumps(payload))
        error = parse_control_message(await websocket.recv())
        assert error.type == "error"
        assert error.code == "SCHEMA"
        assert "Traceback" not in error.message
        assert 'File "' not in error.message

        await websocket.send("{json invalido")
        error = parse_control_message(await websocket.recv())
        assert error.type == "error"
        assert error.code == "SCHEMA"
        assert "Traceback" not in error.message


@pytest.mark.asyncio
async def test_healthz_devuelve_200_con_estado(server: ServerHandle) -> None:
    url = f"http://127.0.0.1:{server.health_port}/healthz"

    def fetch() -> tuple[int, dict[str, Any]]:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))

    status, body = await asyncio.to_thread(fetch)
    assert status == 200
    assert body["status"] == "ok"
    assert body["ready"] is True
    assert body["models"] == []
    assert isinstance(body["uptime_seconds"], int | float)


@pytest.mark.asyncio
async def test_rate_limit_devuelve_error_rate_limit() -> None:
    settings = ServerSettings(
        host="127.0.0.1",
        port=0,
        token=SecretStr(TOKEN),
        ping_interval=None,
        rate_limit_per_second=2,
        health_port=0,
    )
    handle = await create_server(settings, HealthState())
    try:
        async with websockets.connect(_uri(handle, TOKEN)) as websocket:
            codes: list[str] = []
            for index in range(6):
                await websocket.send(PingMessage(ts=index).model_dump_json())
                message = parse_control_message(await websocket.recv())
                codes.append(message.type if message.type == "pong" else message.code)
            assert "RATE_LIMIT" in codes
    finally:
        await handle.close()
