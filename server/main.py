"""Servidor WebSocket real de BabelPipe (ARCHITECTURE.md §4, §6).

Un único endpoint multiplexa frames PCM binarios y eventos JSON de control.
Toda entrada externa se valida en esta frontera: token Bearer/query, esquemas
Pydantic estrictos para JSON y cabecera binaria canónica para PCM. Ningún error
devuelve al cliente un stack trace.
"""

from __future__ import annotations

import asyncio
import functools
import hmac
import logging
from collections import deque
from dataclasses import dataclass, field
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

import websockets
from pydantic import BaseModel, SecretStr
from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServer, WebSocketServerProtocol

from shared.models import (
    ConfigMessage,
    ErrorMessage,
    PingMessage,
    PongMessage,
    ReadyMessage,
    ResetMessage,
    parse_control_message,
)
from shared.protocol import AudioFrame, ProtocolError, decode_frame

from .config import ServerSettings
from .health import HealthApp, HealthState, make_health_app

logger = logging.getLogger("babelpipe.server")

RATE_LIMIT_WINDOW_SECONDS = 1.0


@dataclass
class SessionState:
    """Estado por conexión: configuración, dedup de sequence y límite de tasa."""

    session_id: UUID
    mode: str = "rx"
    source_lang: str = ""
    target_lang: str = ""
    configured: bool = False
    frames_received: int = 0
    last_seq: dict[int, int] = field(default_factory=dict)
    recent_messages: deque[float] = field(default_factory=deque)

    def reset(self) -> None:
        """Limpia la segmentación y los contadores de la sesión."""
        self.configured = False
        self.frames_received = 0
        self.last_seq.clear()
        self.recent_messages.clear()


@dataclass
class ServerContext:
    """Estado compartido por todas las conexiones del servidor."""

    settings: ServerSettings
    health: HealthState
    sessions: dict[UUID, SessionState] = field(default_factory=dict)


@dataclass
class ServerHandle:
    """Servidor WS y aplicación de salud ya arrancados."""

    server: WebSocketServer
    health_app: HealthApp
    settings: ServerSettings
    health: HealthState

    @property
    def port(self) -> int:
        """Puerto real del socket (útil cuando se pidió ``port=0``)."""
        sock = next(iter(self.server.sockets), None)
        if sock is None:
            return self.settings.port
        return int(sock.getsockname()[1])

    @property
    def health_port(self) -> int:
        return self.health_app.port

    async def close(self) -> None:
        """Detiene el health-check y cierra el WebSocket sin dejar puertos."""
        self.health_app.stop()
        self.server.close()
        await self.server.wait_closed()


def _path_of(websocket: WebSocketServerProtocol, path: str | None) -> str:
    if path is not None:
        return path
    value: Any = getattr(websocket, "path", "")
    return value if isinstance(value, str) else ""


def _authorized(
    path: str,
    websocket: WebSocketServerProtocol,
    settings: ServerSettings,
) -> bool:
    """Comprueba el token de query o de ``Authorization: Bearer``.

    Un servidor sin token configurado solo puede darse en localhost (garantía
    de :class:`ServerSettings`) y arranca en modo desarrollo local.
    """
    token: SecretStr | None = settings.token
    if token is None:
        return True

    expected = token.get_secret_value()
    candidates = parse_qs(urlsplit(path).query).get("token", [])
    header = websocket.request_headers.get("Authorization", "")
    if header.lower().startswith("bearer "):
        candidates.append(header[7:].strip())
    return any(hmac.compare_digest(expected, candidate) for candidate in candidates)


async def _send_json(websocket: WebSocketServerProtocol, message: BaseModel) -> None:
    await websocket.send(message.model_dump_json())


async def _send_error(websocket: WebSocketServerProtocol, code: str, message: str) -> None:
    """Envía un ``error`` con mensaje acotado y sin trazas de ejecución."""
    safe = " ".join(str(message).split())[:2000]
    await _send_json(websocket, ErrorMessage(code=code, message=safe or "error"))


def _rate_limited(session: SessionState, settings: ServerSettings, now: float) -> bool:
    """Ventana deslizante de un segundo sobre el número de mensajes."""
    window = session.recent_messages
    while window and now - window[0] >= RATE_LIMIT_WINDOW_SECONDS:
        window.popleft()
    if len(window) >= settings.rate_limit_per_second:
        return True
    window.append(now)
    return False


def _register_session(
    ctx: ServerContext,
    session: SessionState,
    config: ConfigMessage,
) -> bool:
    """Asocia la configuración a la conexión; ``False`` si hay saturación."""
    old_id = session.session_id
    new_id = config.session_id
    was_registered = ctx.sessions.get(old_id) is session

    if new_id != old_id or not was_registered:
        if was_registered:
            ctx.sessions.pop(old_id, None)
        if new_id not in ctx.sessions and len(ctx.sessions) >= ctx.settings.max_sessions:
            return False
        ctx.sessions[new_id] = session

    session.session_id = new_id
    session.reset()
    session.mode = config.mode
    session.source_lang = config.source_lang
    session.target_lang = config.target_lang
    session.configured = True
    ctx.health.set_active_sessions(len(ctx.sessions))
    ctx.health.set_models([])
    return True


def _drop_session(ctx: ServerContext, session: SessionState | None) -> None:
    if session is None:
        return
    if ctx.sessions.get(session.session_id) is session:
        ctx.sessions.pop(session.session_id, None)
        ctx.health.set_active_sessions(len(ctx.sessions))


async def _handle_text(
    raw: str,
    websocket: WebSocketServerProtocol,
    session: SessionState,
    ctx: ServerContext,
) -> None:
    try:
        message = parse_control_message(raw)
    except ValueError as exc:
        await _send_error(websocket, "SCHEMA", str(exc))
        return

    if isinstance(message, ConfigMessage):
        if not _register_session(ctx, session, message):
            await _send_error(websocket, "SESSION_LIMIT", "max_sessions alcanzado")
            await websocket.close(code=1008, reason="SESSION_LIMIT")
            return
        await _send_json(websocket, ReadyMessage(models=[], session_id=message.session_id))
    elif isinstance(message, PingMessage):
        await _send_json(websocket, PongMessage(ts=message.ts))
    elif isinstance(message, ResetMessage):
        target = ctx.sessions.get(message.session_id)
        if target is not None:
            target.reset()
        session.reset()
    else:
        await _send_error(
            websocket,
            "SCHEMA",
            f"mensaje '{message.type}' no aceptado desde el cliente",
        )


async def _handle_binary(
    raw: bytes,
    websocket: WebSocketServerProtocol,
    session: SessionState,
) -> None:
    try:
        frame: AudioFrame = decode_frame(raw)
    except ProtocolError as exc:
        await _send_error(websocket, exc.code, exc.message)
        return

    last = session.last_seq.get(frame.stream_id)
    if last is not None and frame.sequence <= last:
        return

    session.last_seq[frame.stream_id] = frame.sequence
    session.frames_received += 1


async def connection_handler(
    websocket: WebSocketServerProtocol,
    path: str | None = None,
    *,
    ctx: ServerContext,
) -> None:
    """Handler de conexión: autentica y despacha cada frame recibido."""
    request_path = _path_of(websocket, path)
    if urlsplit(request_path).path.rstrip("/") != ctx.settings.ws_path.rstrip("/"):
        await websocket.close(code=1008, reason="INVALID_PATH")
        return
    if not _authorized(request_path, websocket, ctx.settings):
        await websocket.close(code=1008, reason="AUTH_FAILED")
        return

    session = SessionState(session_id=uuid4())
    try:
        async for raw in websocket:
            if _rate_limited(session, ctx.settings, monotonic()):
                await _send_error(
                    websocket,
                    "RATE_LIMIT",
                    "demasiados mensajes por segundo",
                )
                continue
            if isinstance(raw, bytes):
                await _handle_binary(raw, websocket, session)
            else:
                await _handle_text(raw, websocket, session, ctx)
    except ConnectionClosed:
        logger.debug("conexión WS cerrada: %s", session.session_id)
    finally:
        _drop_session(ctx, session)


async def create_server(
    settings: ServerSettings | None = None,
    health: HealthState | None = None,
) -> ServerHandle:
    """Arranca WS y health-check; devuelve el handle con los puertos reales."""
    current_settings = settings if settings is not None else ServerSettings.from_env()
    current_health = health if health is not None else HealthState()
    ctx = ServerContext(settings=current_settings, health=current_health)
    handler = functools.partial(connection_handler, ctx=ctx)

    server = await websockets.serve(
        handler,
        current_settings.host,
        current_settings.port,
        ping_interval=current_settings.ping_interval,
        max_size=current_settings.max_frame_bytes,
    )
    health_app = make_health_app(
        current_health,
        host=current_settings.health_host,
        port=current_settings.health_port,
    )
    try:
        health_app.start()
    except BaseException:
        server.close()
        await server.wait_closed()
        raise
    current_health.set_ready(True)
    return ServerHandle(
        server=server,
        health_app=health_app,
        settings=current_settings,
        health=current_health,
    )


async def run_server(settings: ServerSettings | None = None) -> None:
    """Punto de entrada asíncrono: mantiene el servidor vivo hasta cancelación."""
    handle = await create_server(settings)
    logger.info(
        "BabelPipe WS en ws://%s:%d%s | health en http://%s:%d/healthz",
        handle.settings.host,
        handle.port,
        handle.settings.ws_path,
        handle.settings.health_host,
        handle.health_port,
    )
    try:
        await asyncio.Future()
    finally:
        await handle.close()


def main() -> None:
    """Entrada CLI registrada como ``babelpipe-server``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
