"""Tests unitarios del panel de control GTK4 de BabelPipe (TASK-021).

Todo corre en modo headless: no se importa ``gi``, no hace falta servidor
X/Wayland y no se tocan librerías del sistema. Las aserciones cubren los
selectores y el deslizador, la validación inline, el ciclo de Apply con el
botón deshabilitado durante el envío, Start/Stop, los toggles de PTT/AEC, los
estados UI, la sanitización del texto externo, los design tokens de
ARCHITECTURE.md §5 y la ausencia de hilos/tareas colgadas.
"""

from __future__ import annotations

import builtins
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

import client.ui.control_panel as panel_module
from client.config import ClientSettings
from client.overlay.subtitle_window import EMPTY_CTA
from client.ui.control_panel import (
    DEFAULT_LANGUAGES,
    THEME_TOKENS,
    ControlPanel,
    load_theme_tokens,
    stylesheet,
)

_THEME_CSS: Path = Path(panel_module.__file__).with_name("theme.css")

_EXPECTED_HEX: dict[str, str] = {
    "bg-canvas": "#0B0F14",
    "bg-surface": "#151B23",
    "bg-elevated": "#1E2733",
    "border-subtle": "#2A3543",
    "border-strong": "#3D4B5C",
    "text-primary": "#E6EDF3",
    "text-secondary": "#B7C2D0",
    "text-muted": "#8B98A8",
    "accent-primary": "#22D3EE",
    "accent-foreground": "#04212B",
    "accent-hover": "#67E8F9",
    "status-success": "#34D399",
    "status-warning": "#FBBF24",
    "status-danger": "#F87171",
}


def _panel(**kwargs: Any) -> ControlPanel:
    """Construye un panel headless con callbacks y datos opcionales."""
    return ControlPanel(headless=True, **kwargs)


def test_selectors_and_slider_emit_valid_values() -> None:
    """Los selectores y el slider emiten valores válidos y los conservan."""
    panel = _panel(pipewire_nodes=["Firefox", "Telegram", "Firefox"])

    assert panel.nodes == ("Firefox", "Telegram")  # dedupe estable
    assert panel.selected_app == "Firefox"
    panel.set_app("Telegram")
    assert panel.selected_app == "Telegram"
    assert ("app", "Telegram") in panel.events

    panel.set_source_lang("de")
    panel.set_target_lang("fr")
    assert panel.source_lang == "de"
    assert panel.target_lang == "fr"
    assert ("language", "rx", "de") in panel.events
    assert ("language", "tx", "fr") in panel.events

    panel.set_duck_level(0.10)
    panel.set_duck_level(0.15)
    assert panel.duck_level == pytest.approx(0.15)
    assert panel.events[-1] == ("duck", 0.15)
    assert ("duck", 0.10) in panel.events
    assert "duck_level" not in panel.errors
    assert "en" in DEFAULT_LANGUAGES and "es" in DEFAULT_LANGUAGES
    panel.close()


@pytest.mark.parametrize("bad", [0.09, 0.16, 1.0, float("nan"), float("inf"), "0.12", True])
def test_duck_level_out_of_range_rejected_inline(bad: Any) -> None:
    """Fuera de 0.10–0.15: ValueError, error inline y valor inalterado."""
    panel = _panel()

    with pytest.raises(ValueError, match="duck_level"):
        panel.set_duck_level(bad)

    assert "duck_level" in panel.errors
    assert any(
        event[0] == "validation" and event[1] == "duck_level" and event[2]
        for event in panel.events
    )
    assert panel.duck_level == pytest.approx(0.12)
    panel.close()


@pytest.mark.parametrize("bad", ["e", "", "x" * 11, 123])
def test_invalid_language_rejected_inline(bad: Any) -> None:
    """La frontera rechaza idiomas fuera del contrato (2–10 caracteres)."""
    panel = _panel()

    with pytest.raises(ValueError, match="source_lang"):
        panel.set_source_lang(bad)

    assert "source_lang" in panel.errors
    assert panel.source_lang == "en"
    panel.close()


def test_unknown_app_is_rejected_inline() -> None:
    """Un nodo ausente de la lista se rechaza con error inline accionable."""
    panel = _panel(pipewire_nodes=["Firefox"])

    with pytest.raises(ValueError, match="nodos PipeWire"):
        panel.set_app("Telegram")

    assert "app" in panel.errors
    assert panel.selected_app == "Firefox"
    with pytest.raises(ValueError, match="Nombre de nodo"):
        panel.set_app("<b>Firefox</b>")
    panel.close()


def test_apply_calls_on_apply_with_valid_settings_and_disables_button() -> None:
    """Apply entrega ClientSettings válida y queda deshabilitado durante el envío."""
    captured: dict[str, Any] = {}
    holder: dict[str, ControlPanel] = {}

    def on_apply(settings: ClientSettings) -> None:
        current = holder["panel"]
        captured["settings"] = settings
        captured["apply_enabled"] = current.apply_enabled
        captured["busy"] = current.is_busy

    panel = _panel(on_apply=on_apply)
    holder["panel"] = panel
    panel.set_source_lang("de")
    panel.set_target_lang("fr")
    panel.set_duck_level(0.14)
    panel.set_ptt_enabled(True)
    panel.set_echo_cancel(False)

    panel.apply()

    settings = captured["settings"]
    assert isinstance(settings, ClientSettings)
    assert settings.pipeline.source_lang == "de"
    assert settings.pipeline.target_lang == "fr"
    assert settings.audio.duck_level == pytest.approx(0.14)
    assert settings.audio.ptt_enabled is True
    assert settings.audio.echo_cancel is False
    assert ("apply", settings) in panel.events
    assert captured["apply_enabled"] is False
    assert captured["busy"] is True
    assert panel.is_busy is False
    assert panel.apply_enabled is True
    assert [event for event in panel.events if event[0] == "busy"] == [
        ("busy", True),
        ("busy", False),
    ]
    panel.close()


def test_apply_callback_is_injected_at_construction() -> None:
    """El callback inyectable se registra en el constructor y recibe la settings."""
    received: list[ClientSettings] = []
    panel = _panel(on_apply=received.append)
    panel.set_duck_level(0.13)

    panel.apply()

    assert len(received) == 1
    assert isinstance(received[0], ClientSettings)
    assert received[0].audio.duck_level == pytest.approx(0.13)
    assert panel.settings is received[0]
    panel.close()


def test_apply_re_enables_button_after_callback_error() -> None:
    """Si el callback falla, Apply se rehabilita y el error queda inline."""
    def boom(_settings: ClientSettings) -> None:
        raise RuntimeError("boom")

    panel = _panel(on_apply=boom)

    with pytest.raises(RuntimeError, match="boom"):
        panel.apply()

    assert panel.is_busy is False
    assert panel.apply_enabled is True
    assert panel.errors["apply"] == "No se pudo aplicar la configuración"
    assert ("apply_error", "RuntimeError", "boom") in panel.events
    assert [event for event in panel.events if event[0] == "busy"] == [
        ("busy", True),
        ("busy", False),
    ]
    panel.close()


def test_apply_without_callback_still_validates() -> None:
    """Sin callback, Apply construye y conserva una ClientSettings válida."""
    panel = _panel()
    panel.apply()
    assert isinstance(panel.settings, ClientSettings)
    panel.close()


def test_apply_blocked_while_loading() -> None:
    """Los skeletons de modelos bloquean Apply y se liberan al terminar."""
    panel = _panel()
    panel.set_loading(True)
    assert panel.loading is True
    assert panel.apply_enabled is False
    assert ("loading", True) in panel.events
    with pytest.raises(RuntimeError, match="cargando"):
        panel.apply()

    panel.set_loading(False)
    assert panel.loading is False
    assert panel.apply_enabled is True
    panel.close()


def test_start_stop_callbacks_and_idempotence() -> None:
    """Start/Stop invocan sus callbacks, son idempotentes y alternan el botón."""
    starts: list[str] = []
    stops: list[str] = []
    panel = _panel(
        on_start=lambda: starts.append("start"),
        on_stop=lambda: stops.append("stop"),
    )

    panel.start()
    panel.start()
    assert starts == ["start"]
    assert panel.is_running is True
    assert ("running", True) in panel.events

    panel.toggle_start_stop()
    panel.stop()
    assert stops == ["stop"]
    assert panel.is_running is False
    assert ("running", False) in panel.events
    panel.close()


def test_ptt_and_aec_toggles_with_events() -> None:
    """Los toggles de PTT/AEC y el atajo Ctrl+P cambian el estado y emiten eventos."""
    panel = _panel()

    panel.toggle_ptt()
    assert panel.ptt_enabled is True
    assert ("toggle", "ptt_enabled", True) in panel.events
    panel.toggle_ptt()
    assert panel.ptt_enabled is False
    assert panel.events.count(("toggle", "ptt_enabled", False)) == 1

    panel.set_echo_cancel(False)
    panel.toggle_echo_cancel()
    assert panel.echo_cancel is True
    assert ("toggle", "echo_cancel", False) in panel.events
    assert ("toggle", "echo_cancel", True) in panel.events
    panel.close()


def test_ui_states_with_cta_and_retry_callbacks() -> None:
    """listening/empty/error con CTA, Retry y mensajes literales."""
    views: list[str] = []
    retries: list[str] = []
    panel = _panel(
        on_view_nodes=lambda: views.append("nodes"),
        on_retry=lambda: retries.append("retry"),
    )

    assert panel.state == "listening"
    panel.set_state("empty")
    assert panel.state == "empty"
    assert panel.cta == EMPTY_CTA
    panel.view_nodes()
    assert views == ["nodes"]

    panel.set_state("error", "SERVER_OFFLINE")
    assert panel.state == "error"
    assert panel.message == "SERVER_OFFLINE"
    assert panel.cta is None
    panel.retry()
    assert retries == ["retry"]

    panel.set_state("listening")
    assert panel.state == "listening"
    assert [event[0] for event in panel.events if event[0] == "state"] == [
        "state",
        "state",
        "state",
    ]
    panel.close()


@pytest.mark.parametrize("bad_state", ["paused", "", "LISTENING", 1])
def test_invalid_state_rejected(bad_state: Any) -> None:
    """La frontera rechaza estados fuera del contrato literal."""
    panel = _panel()
    with pytest.raises(ValueError, match="state inválido"):
        panel.set_state(bad_state)
    panel.close()


def test_sanitization_external_text_is_never_markup() -> None:
    """'<b>' externo se guarda literal: se usa set_label, jamás set_markup."""
    raw = '<b>SERVER_OFFLINE</b> & "drop" <script>alert(1)</script>'
    panel = _panel()

    panel.set_state("error", raw)

    assert panel.message == raw
    assert ("state", "error", raw) in panel.events
    source = Path(panel_module.__file__).read_text(encoding="utf-8")
    assert "set_markup" not in source
    assert "set_label(" in source
    panel.close()


def test_theme_tokens_match_architecture_design_system() -> None:
    """Los hexadecimales y escalas de ARCH §5 son exactos y están en theme.css."""
    css = _THEME_CSS.read_text(encoding="utf-8")

    for name, hex_value in _EXPECTED_HEX.items():
        assert THEME_TOKENS[name] == hex_value
        assert f"--{name}: {hex_value};" in css

    assert THEME_TOKENS["overlay-bg"] == "rgba(11,15,20,0.62)"
    assert "--overlay-bg: rgba(11,15,20,0.62);" in css
    assert THEME_TOKENS["overlay-blur"] == "8px"
    assert "--overlay-blur: 8px;" in css
    assert THEME_TOKENS["font-sans"] == "Inter"
    assert "--font-sans: Inter;" in css
    assert THEME_TOKENS["font-mono"] == "JetBrains Mono"
    assert "--font-mono: JetBrains Mono;" in css
    for name, radius in (("radius-sm", "6px"), ("radius-md", "10px"), ("radius-lg", "14px")):
        assert THEME_TOKENS[name] == radius
        assert f"--{name}: {radius};" in css
    for name, space in (
        ("space-1", "4px"),
        ("space-2", "8px"),
        ("space-3", "12px"),
        ("space-4", "16px"),
        ("space-6", "24px"),
        ("space-8", "32px"),
    ):
        assert THEME_TOKENS[name] == space
        assert f"--{name}: {space};" in css


def test_theme_css_is_the_only_source_of_panel_tokens() -> None:
    """El módulo Python no duplica hexadecimales: los deriva de theme.css."""
    source = Path(panel_module.__file__).read_text(encoding="utf-8")

    for hex_value in _EXPECTED_HEX.values():
        assert hex_value not in source

    assert load_theme_tokens(_THEME_CSS)["accent-primary"] == "#22D3EE"
    generated = stylesheet()
    assert "@define-color bg_canvas #0B0F14;" in generated
    assert "@define-color accent_primary #22D3EE;" in generated
    assert "@define-color status_danger #F87171;" in generated
    assert "focus-visible" in generated
    # Los hexadecimales aparecen UNA SOLA VEZ: en las directivas @define-color
    # generadas desde theme.css; el módulo Python no los repite en su código.
    for hex_value in _EXPECTED_HEX.values():
        assert generated.count(hex_value) == 1


def test_headless_mode_never_imports_gi(monkeypatch: pytest.MonkeyPatch) -> None:
    """La API headless funciona aunque cualquier import de ``gi`` esté bloqueado."""
    real_import = builtins.__import__

    def _block_gi(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "gi" or name.startswith("gi."):
            raise AssertionError("El panel headless no debe importar gi")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_gi)
    before = set(sys.modules)

    panel = ControlPanel(headless=True, pipewire_nodes=["Firefox"])
    panel.set_app("Firefox")
    panel.set_source_lang("en")
    panel.set_duck_level(0.12)
    panel.apply()
    panel.close()

    assert not (set(sys.modules) - before) & {"gi", "gi.repository"}


def test_importing_module_does_not_import_gi() -> None:
    """El import perezoso garantiza que cargar el módulo no exige PyGObject."""
    assert "gi" not in sys.modules
    assert "gi.repository" not in sys.modules


def test_mock_alias_forces_headless_backend() -> None:
    """El flag --mock se traduce al backend headless sin tocar GTK."""
    panel = ControlPanel(mock=True)
    assert panel.is_headless is True
    panel.close()


def test_forced_gtk_without_binding_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Si se exige GTK real y no está, present() falla con instrucciones concretas."""

    def _no_gtk() -> tuple[Any, Any, Any]:
        raise panel_module._GtkUnavailable("GTK forzado ausente en el test")

    monkeypatch.setattr(panel_module, "_load_gtk", _no_gtk)
    panel = ControlPanel(headless=False)

    assert panel.is_headless is False
    with pytest.raises(RuntimeError, match=r"uv sync --extra ui"):
        panel.present()
    panel.close()


def test_constructor_rejects_invalid_callbacks_and_flags() -> None:
    """Los callbacks y las banderas se validan en la frontera del constructor."""
    with pytest.raises(ValueError, match="on_apply"):
        ControlPanel(headless=True, on_apply="no-callable")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="headless y mock"):
        ControlPanel(headless=True, mock=False)
    with pytest.raises(ValueError, match="settings"):
        ControlPanel(headless=True, settings="no-settings")  # type: ignore[arg-type]


def test_close_is_idempotent_and_leaves_no_threads() -> None:
    """close() es idempotente, registra el evento y no deja hilos colgados."""
    before = threading.active_count()
    panel = _panel()
    panel.set_state("empty")
    panel.apply()

    panel.close()
    panel.close()

    assert panel.closed is True
    assert ("close",) in panel.events
    assert threading.active_count() == before
    with pytest.raises(RuntimeError, match="cerrado"):
        panel.apply()


def test_context_manager_closes_panel() -> None:
    """El uso como context manager libera el panel al salir."""
    with _panel() as panel:
        assert panel.closed is False
        panel.start()
    assert panel.closed is True
    assert ("close",) in panel.events
