"""Tests unitarios del overlay flotante de subtítulos (TASK-018).

Todo corre en modo headless: no se importa ``gi``, no hace falta servidor
X/Wayland y no se tocan librerías del sistema. Las aserciones cubren la
sanitización del texto, las transiciones de estado, la barra VAD, el cierre sin
recursos colgados y los design tokens de ARCHITECTURE.md §5.
"""

from __future__ import annotations

import builtins
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

import client.overlay.subtitle_window as overlay_module
from client.overlay.subtitle_window import (
    DEFAULT_ERROR_MESSAGE,
    DESIGN_TOKENS,
    EMPTY_CTA,
    EMPTY_MESSAGE,
    LISTENING_MESSAGE,
    OVERLAY_CSS,
    SubtitleOverlay,
)


def _overlay(**kwargs: Any) -> SubtitleOverlay:
    """Construye un overlay headless con callbacks opcionales inyectados."""
    return SubtitleOverlay(headless=True, **kwargs)


def test_show_stores_literal_text_never_markup() -> None:
    """AC seguridad: '<b>' y '&' se guardan literalmente, sin interpretar markup."""
    overlay = _overlay()
    raw = '<b>Hola & "mundo"</b> <script>alert(1)</script>'

    overlay.show(raw, is_final=True)

    assert overlay.subtitle_text == raw
    assert overlay.is_final is True
    assert ("show", raw, True) in overlay.events
    # El módulo no contiene ninguna llamada de renderizado de markup.
    source = Path(overlay_module.__file__).read_text(encoding="utf-8")
    assert "set_markup" not in source
    assert "set_label(" in source
    overlay.close()


def test_state_transitions_with_messages_and_callbacks() -> None:
    """listening → empty → error → listening, con Retry y CTA cableados."""
    retries: list[str] = []
    views: list[str] = []
    overlay = _overlay(
        on_retry=lambda: retries.append("retry"),
        on_view_nodes=lambda: views.append("nodes"),
    )

    assert overlay.state == "listening"
    assert overlay.message == LISTENING_MESSAGE
    assert overlay.cta is None

    overlay.set_state("empty")
    assert overlay.state == "empty"
    assert overlay.message == EMPTY_MESSAGE
    assert overlay.cta == EMPTY_CTA
    overlay.view_nodes()
    assert views == ["nodes"]

    overlay.set_state("error", "SERVER_OFFLINE")
    assert overlay.state == "error"
    assert overlay.message == "SERVER_OFFLINE"
    assert overlay.cta is None
    overlay.retry()
    assert retries == ["retry"]

    overlay.set_state("listening", "reconectado")
    assert overlay.state == "listening"
    assert overlay.message == "reconectado"

    assert [event[0] for event in overlay.events] == [
        "state",
        "view_nodes",
        "state",
        "retry",
        "state",
    ]
    overlay.close()


def test_error_defaults_to_actionable_code() -> None:
    """El estado error sin mensaje usa el código SERVER_OFFLINE por defecto."""
    overlay = _overlay()
    overlay.set_state("error")
    assert overlay.message == DEFAULT_ERROR_MESSAGE
    overlay.close()


@pytest.mark.parametrize("bad_state", ["paused", "", "LISTENING"])
def test_invalid_state_is_rejected(bad_state: str) -> None:
    """La frontera rechaza estados fuera del contrato literal."""
    overlay = _overlay()
    with pytest.raises(ValueError):
        overlay.set_state(bad_state)  # type: ignore[arg-type]
    overlay.close()


def test_set_level_updates_bar_and_clear_keeps_window_open() -> None:
    """set_level actualiza la barra; clear limpia texto sin cerrar ni cambiar estado."""
    overlay = _overlay()
    overlay.show("Hola, ¿cómo estás?")

    overlay.set_level(0.75)
    assert overlay.level == pytest.approx(0.75)
    assert overlay.events[-1] == ("level", 0.75)

    overlay.set_level(2.5)
    assert overlay.level == 1.0  # recorte al rango de la barra

    overlay.clear()
    assert overlay.subtitle_text == ""
    assert overlay.is_final is False
    assert overlay.closed is False
    assert overlay.state == "listening"
    assert ("clear",) in overlay.events
    overlay.close()


@pytest.mark.parametrize("bad_level", [float("nan"), float("inf"), "0.5", True])
def test_invalid_level_is_rejected(bad_level: Any) -> None:
    """Niveles no numéricos, no finitos o booleanos se rechazan en la frontera."""
    overlay = _overlay()
    with pytest.raises(ValueError):
        overlay.set_level(bad_level)
    overlay.close()


def test_close_releases_resources_without_hanging_threads() -> None:
    """close() es idempotente, registra el evento y no deja hilos colgados."""
    threads_before = threading.active_count()
    overlay = _overlay()
    overlay.show("Adiós")

    overlay.close()
    overlay.close()

    assert overlay.closed is True
    assert ("close",) in overlay.events
    assert threading.active_count() == threads_before
    with pytest.raises(RuntimeError, match="cerrado"):
        overlay.show("ya cerrado")


def test_headless_mode_never_imports_gi(monkeypatch: pytest.MonkeyPatch) -> None:
    """La API headless funciona aunque cualquier import de ``gi`` esté bloqueado."""
    real_import = builtins.__import__

    def _block_gi(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "gi" or name.startswith("gi."):
            raise AssertionError("El overlay headless no debe importar gi")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_gi)
    before = set(sys.modules)

    overlay = SubtitleOverlay(headless=True)
    overlay.show("Texto")
    overlay.set_state("empty")
    overlay.set_level(0.3)
    overlay.close()

    assert not (set(sys.modules) - before) & {"gi", "gi.repository"}


def test_importing_module_does_not_import_gi() -> None:
    """El import perezoso garantiza que cargar el módulo no exige PyGObject."""
    assert "gi" not in sys.modules
    assert "gi.repository" not in sys.modules


def test_real_gtk_without_binding_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Si se exige GTK real y no está, show() falla con instrucciones concretas."""

    def _no_gtk() -> tuple[Any, Any, Any]:
        raise overlay_module._GtkUnavailable("GTK forzado ausente en el test")

    monkeypatch.setattr(overlay_module, "_load_gtk", _no_gtk)
    overlay = SubtitleOverlay(headless=False)

    assert overlay.is_headless is False
    with pytest.raises(RuntimeError, match=r"uv sync --extra ui"):
        overlay.show("Hola")
    overlay.close()


def test_css_tokens_match_architecture_design_system() -> None:
    """Los hexadecimales y escalas de ARCH §5 están presentes en el CSS."""
    expected_hex = {
        "bg_canvas": "#0B0F14",
        "bg_surface": "#151B23",
        "bg_elevated": "#1E2733",
        "border_subtle": "#2A3543",
        "border_strong": "#3D4B5C",
        "text_primary": "#E6EDF3",
        "text_secondary": "#B7C2D0",
        "text_muted": "#8B98A8",
        "accent_primary": "#22D3EE",
        "accent_foreground": "#04212B",
        "accent_hover": "#67E8F9",
        "status_success": "#34D399",
        "status_warning": "#FBBF24",
        "status_danger": "#F87171",
    }
    for key, hex_value in expected_hex.items():
        assert DESIGN_TOKENS[key] == hex_value
        assert hex_value in OVERLAY_CSS

    assert DESIGN_TOKENS["overlay_bg"] == "rgba(11,15,20,0.62)"
    assert "rgba(11,15,20,0.62)" in OVERLAY_CSS
    assert "--overlay-blur: 8px" in OVERLAY_CSS
    for font in ("Inter", "JetBrains Mono"):
        assert DESIGN_TOKENS["font_sans" if font == "Inter" else "font_mono"] == font
        assert font in OVERLAY_CSS
    for radius in ("6px", "10px", "14px"):
        assert radius in OVERLAY_CSS
    for spacing in ("4px", "8px", "12px", "16px", "24px", "32px"):
        assert spacing in OVERLAY_CSS


def test_input_boundaries_follow_subtitle_contract() -> None:
    """Texto > 2000 chars y callbacks no invocables se rechazan en la frontera."""
    overlay = _overlay()
    with pytest.raises(ValueError):
        overlay.show("x" * 2001)
    with pytest.raises(ValueError):
        overlay.show(123)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        _overlay(on_retry="no-callable")  # type: ignore[arg-type]
    overlay.close()


def test_context_manager_closes_overlay() -> None:
    """El uso como context manager libera el overlay al salir."""
    with _overlay() as overlay:
        assert overlay.closed is False
        overlay.show("Hola")
    assert overlay.closed is True
