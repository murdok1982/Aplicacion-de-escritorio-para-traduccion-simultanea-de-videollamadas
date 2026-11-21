"""Endpoint HTTP de salud del servidor BabelPipe (stdlib únicamente).

``make_health_app()`` monta un ``ThreadingHTTPServer`` en un hilo daemon y
responde ``GET /healthz`` con un JSON de estado. La aplicación HTTP y el loop
asyncio comparten :class:`HealthState`, protegido por un ``RLock``.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

HEALTH_PATH = "/healthz"


class HealthState:
    """Estado observable: disponibilidad, modelos, sesiones y uptime."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._ready = False
        self._models: list[str] = []
        self._active_sessions = 0
        self._started_at = time.monotonic()

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready

    @property
    def models(self) -> list[str]:
        with self._lock:
            return list(self._models)

    @property
    def active_sessions(self) -> int:
        with self._lock:
            return self._active_sessions

    @property
    def uptime_seconds(self) -> float:
        with self._lock:
            return max(0.0, time.monotonic() - self._started_at)

    def set_ready(self, ready: bool) -> None:
        with self._lock:
            self._ready = bool(ready)

    def set_models(self, models: list[str]) -> None:
        with self._lock:
            self._models = list(models)

    def set_active_sessions(self, count: int) -> None:
        with self._lock:
            self._active_sessions = max(0, int(count))

    def to_dict(self) -> dict[str, Any]:
        """Instantánea consistente para serializar en la respuesta HTTP."""
        with self._lock:
            return {
                "status": "ok" if self._ready else "degraded",
                "ready": self._ready,
                "models": list(self._models),
                "active_sessions": self._active_sessions,
                "uptime_seconds": round(max(0.0, time.monotonic() - self._started_at), 3),
            }


def _make_handler(state: HealthState) -> type[BaseHTTPRequestHandler]:
    class HealthRequestHandler(BaseHTTPRequestHandler):
        server_version = "BabelPipeHealth/1.0"
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            if urlsplit(self.path).path.rstrip("/") != HEALTH_PATH.rstrip("/"):
                self._send_json(404, {"status": "error", "error": "not_found"})
                return
            self._send_json(200, state.to_dict())

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return HealthRequestHandler


class HealthApp:
    """Servidor HTTP de salud con ciclo de vida explícito."""

    def __init__(self, state: HealthState, host: str = "127.0.0.1", port: int = 0) -> None:
        self._state = state
        self._server = ThreadingHTTPServer((host, port), _make_handler(state))
        self._thread: threading.Thread | None = None

    @property
    def state(self) -> HealthState:
        return self._state

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="babelpipe-health",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)
        self._thread = None


def make_health_app(
    state: HealthState | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> HealthApp:
    """Crea la aplicación de salud; ``port=0`` selecciona un puerto efímero."""
    return HealthApp(state if state is not None else HealthState(), host=host, port=port)
