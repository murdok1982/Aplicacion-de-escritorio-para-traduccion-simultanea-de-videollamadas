"""Panel de control GTK4 de BabelPipe (TASK-021, SPEC.md §4, ARCHITECTURE.md §5).

El panel expone en una sola ventana los controles operativos del daemon:

* selector de **app a enrutar** (lista de nodos PipeWire detectados),
* selectores de idioma **Rx** (entrada) y **Tx** (salida),
* deslizador de ``duck_level`` acotado al rango legal ``0.10``–``0.15``,
* interruptores de **PTT** y **AEC**,
* botón **Apply** (validación inline y deshabilitado mientras envía),
* botón **Start/Stop**,
* estados ``listening`` / ``empty`` / ``error`` con glifo, CTA y ``Retry``,
* indicador de carga (*skeleton* de modelos) con anchos que caben en 375 px,
* filas ``Gtk.FlowBox`` que envuelven etiquetas, CTA y botones al estrechar la
  ventana (idiomática GTK4, sin media queries),
* atajos de teclado: ``Ctrl+P`` (PTT), ``Ctrl+Return`` (Apply), ``Ctrl+S``
  (Start/Stop).

Modelo de datos
---------------
No se define ningún esquema nuevo: se reutiliza :class:`client.config.ClientSettings`
(TASK-022). Cada campo se valida en la frontera contra los esquemas canónicos
:class:`~client.config.AudioConfig` y :class:`~client.config.PipelineConfig`, de
modo que el panel y el daemon comparten exactamente las mismas reglas.

Design tokens
-------------
Los tokens viven **únicamente** en ``client/ui/theme.css`` (ARCHITECTURE.md §5).
Este módulo los lee al importarse y genera la hoja GTK4 con ``@define-color``;
no contiene ningún hexadecimal duplicado. El overlay tiene los suyos en su
propio módulo y no se replican aquí.

GTK perezoso / modo headless
----------------------------
Importar este módulo **nunca** importa ``gi``. Con ``headless=True`` (o ``mock=True``,
que es el modo usado por los tests con ``--mock``) se emplea un backend que
registra cada operación como evento, sin X/Wayland ni bibliotecas del sistema.
Si GTK no está disponible y se fuerza ``headless=False``, ``present()`` lanza un
:class:`RuntimeError` accionable que indica ``uv sync --extra ui``.

El módulo no crea hilos ni tareas ``asyncio``: el backend GTK solo programa
trabajo en el hilo de UI con ``GLib.idle_add`` cuando se llama desde otro hilo,
y :meth:`ControlPanel.close` libera ventana y aplicación de forma idempotente.
"""

from __future__ import annotations

import math
import re
import threading
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Literal, Protocol

from pydantic import ValidationError

from client.config import (
    _NODE_NAME_RE,
    DUCK_LEVEL_MAX,
    DUCK_LEVEL_MIN,
    AudioConfig,
    ClientSettings,
    PipelineConfig,
)
from client.overlay.subtitle_window import (
    DEFAULT_ERROR_MESSAGE,
    EMPTY_CTA,
    EMPTY_MESSAGE,
    ERROR_CTA,
    LISTENING_MESSAGE,
)

PanelState = Literal["listening", "empty", "error"]

_VALID_STATES: Final[frozenset[str]] = frozenset({"listening", "empty", "error"})
PANEL_STATE_VALUES: Final[tuple[str, ...]] = tuple(sorted(_VALID_STATES))
# Glifos de estado como TEXTO PLANO (nunca markup con contenido externo).
_STATE_GLYPH: Final[Mapping[str, str]] = MappingProxyType(
    {"listening": "◉", "empty": "◎", "error": "⚠"}
)
_MAX_EVENTS: Final[int] = 512
_MAX_TEXT_CHARS: Final[int] = 2000  # shared.models.SubtitleMessage.text
_GTK_INVALID_POSITION: Final[int] = 0xFFFFFFFF

DEFAULT_LANGUAGES: Final[tuple[str, ...]] = (
    "en",
    "es",
    "fr",
    "de",
    "it",
    "pt",
    "ja",
    "ko",
    "zh",
    "ru",
)

_THEME_PATH: Final[Path] = Path(__file__).with_name("theme.css")
_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"--([a-z][a-z0-9-]*)\s*:\s*([^;]+);")

_REQUIRED_TOKENS: Final[tuple[str, ...]] = (
    "bg-canvas",
    "bg-surface",
    "bg-elevated",
    "border-subtle",
    "border-strong",
    "text-primary",
    "text-secondary",
    "text-muted",
    "accent-primary",
    "accent-foreground",
    "accent-hover",
    "status-success",
    "status-warning",
    "status-danger",
    "overlay-bg",
    "overlay-blur",
    "font-sans",
    "font-mono",
    "radius-sm",
    "radius-md",
    "radius-lg",
    "space-1",
    "space-2",
    "space-3",
    "space-4",
    "space-6",
    "space-8",
)

_GTK_INSTALL_HINT: Final[str] = (
    "GTK4 no está disponible para el panel de control de BabelPipe. Instala el "
    "extra 'ui' con `uv sync --extra ui` (PyGObject>=3.50,<4) y las bibliotecas "
    "del sistema 'libgtk-4-1' y 'libgirepository-1.0-1'. Para tests usa "
    "`headless=True` (o el flag --mock de pytest)."
)

_HINT_BY_ERROR: Final[Mapping[str, str]] = {
    "extra_forbidden": "campo no permitido",
    "missing": "campo obligatorio ausente",
    "greater_than_equal": "valor por debajo del mínimo permitido",
    "less_than_equal": "valor por encima del máximo permitido",
    "string_type": "se esperaba texto",
    "string_too_short": "texto demasiado corto",
    "string_too_long": "texto demasiado largo",
    "string_pattern_mismatch": "formato de texto inválido",
    "float_type": "se esperaba un número decimal",
    "float_parsing": "se esperaba un número decimal",
    "bool_type": "se esperaba true o false",
    "model_type": "se esperaba una sección con sus campos",
}


def load_theme_tokens(path: Path | None = None) -> Mapping[str, str]:
    """Lee ``theme.css`` y devuelve los tokens canónicos indexados sin ``--``.

    Args:
        path: fichero alternativo (tests de los propios tokens). Por defecto,
            el ``theme.css`` que acompaña a este módulo.

    Raises:
        RuntimeError: el fichero no existe, no se puede leer o falta algún token
            obligatorio del contrato de ARCHITECTURE.md §5.
    """
    css_path = _THEME_PATH if path is None else path
    try:
        text = css_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(
            f"No se pudieron leer los design tokens de {css_path.name}: "
            f"{exc.strerror or 'error de E/S'}"
        ) from None

    tokens: dict[str, str] = {}
    for match in _TOKEN_RE.finditer(text):
        value = match.group(2).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        tokens[match.group(1)] = value

    missing = [name for name in _REQUIRED_TOKENS if name not in tokens]
    if missing:
        raise RuntimeError(f"Faltan design tokens en {css_path.name}: {', '.join(missing)}")
    return MappingProxyType(tokens)


THEME_TOKENS: Final[Mapping[str, str]] = load_theme_tokens()
"""Tokens canónicos leídos de ``theme.css`` (única fuente del panel)."""


def _px(token_name: str) -> int:
    """Convierte un token espacial ``<n>px`` a enteros para el layout GTK.

    El espaciado del backend GTK no se escribe a mano: se deriva de los mismos
    tokens que la hoja CSS, de modo que la escala de múltiplos de 4 px tenga una
    única fuente de verdad (ARCHITECTURE.md §5).
    """
    value = THEME_TOKENS[token_name]
    if not value.endswith("px"):
        raise RuntimeError(f"El token {token_name} no es una medida px: {value!r}")
    return int(value[:-2])


_SPACE_1: Final[int] = _px("space-1")
_SPACE_2: Final[int] = _px("space-2")
_SPACE_3: Final[int] = _px("space-3")
_SPACE_4: Final[int] = _px("space-4")
_SPACE_6: Final[int] = _px("space-6")
_SPACE_8: Final[int] = _px("space-8")
"""Espaciado GTK derivado de ``theme.css`` (4 / 8 / 12 / 16 / 24 / 32 px)."""

# Barras de skeleton con anchos escalonados que caben en 375 px incluso con el
# padding de 24 px a cada lado (288 + 48 = 336 < 375). Evita scroll horizontal.
_SKELETON_WIDTHS: Final[tuple[int, ...]] = (
    _SPACE_8 * 9,
    _SPACE_8 * 8,
    _SPACE_8 * 9 - _SPACE_4,
)
_SKELETON_HEIGHT: Final[int] = _SPACE_6
_INPUT_MIN_WIDTH: Final[int] = _SPACE_8 * 5
"""Ancho mínimo idiomático de entradas desplegables y deslizadores al envolver."""


def stylesheet() -> str:
    """Genera la hoja GTK4 a partir de :data:`THEME_TOKENS` (sin duplicar hex)."""
    token = THEME_TOKENS

    def define(name: str) -> str:
        return f"@define-color {name.replace('-', '_')} {token[name]};"

    excluded = ("space-", "radius-", "font-", "overlay-")
    colors = "\n".join(define(name) for name in _REQUIRED_TOKENS if not name.startswith(excluded))
    return f"""/* Generado desde client/ui/theme.css — no editar a mano. */
{colors}

.bp-panel {{
  background-color: @bg_canvas;
  color: @text_primary;
  font-family: {token['font-sans']}, sans-serif;
}}
.bp-panel-root {{ padding: {token['space-6']}; }}
.bp-title {{ color: @text_primary; font-size: 20px; font-weight: 700; }}
.bp-subtitle {{
  color: @text_muted;
  font-family: {token['font-mono']}, monospace;
  font-size: {token['space-3']};
}}
.bp-state {{
  background-color: @bg_surface;
  color: @text_secondary;
  border: 1px solid @border_subtle;
  border-radius: {token['radius-md']};
  padding: {token['space-3']};
  transition: border-color 180ms ease, background-color 180ms ease;
}}
.bp-state.empty {{ border-color: @accent_primary; color: @text_primary; }}
.bp-state.error {{ border-color: @status_danger; color: @status_danger; }}
.bp-state-icon {{
  color: @accent_primary;
  font-family: {token['font-mono']}, monospace;
  min-width: {token['space-6']};
}}

.bp-form {{
  background-color: @bg_surface;
  border: 1px solid @border_subtle;
  border-radius: {token['radius-lg']};
  padding: {token['space-4']};
}}
.bp-field-label {{ color: @text_secondary; font-weight: 600; }}
.bp-input {{
  background-color: @bg_elevated;
  color: @text_primary;
  border: 1px solid @border_subtle;
  border-radius: {token['radius-sm']};
  padding: {token['space-2']};
}}
.bp-input.is-invalid {{ border-color: @status_danger; }}
.bp-scale {{ color: @accent_primary; }}
.bp-scale trough {{
  min-height: {token['space-2']};
  background-color: @bg_elevated;
  border-radius: {token['radius-sm']};
}}
.bp-scale trough block.filled {{
  background-color: @accent_primary;
  border-radius: {token['radius-sm']};
}}
.bp-switch:checked {{ background-color: @accent_primary; }}
.bp-field-error {{
  color: @status_danger;
  font-family: {token['font-mono']}, monospace;
  font-size: {token['space-3']};
}}

.bp-skeleton {{
  background-color: @bg_elevated;
  border-radius: {token['radius-sm']};
  animation: bp-panel-pulse 1400ms ease-in-out infinite alternate;
}}
@keyframes bp-panel-pulse {{ from {{ opacity: 0.55; }} to {{ opacity: 1; }} }}

.bp-apply, .bp-start, .bp-cta {{
  border-radius: {token['radius-sm']};
  transition: background-color 140ms ease, border-color 140ms ease, color 140ms ease;
}}
.bp-apply {{
  background-color: @accent_primary;
  color: @accent_foreground;
  padding: {token['space-3']};
  font-weight: 700;
}}
.bp-apply:hover {{ background-color: @accent_hover; }}
.bp-apply:disabled {{ opacity: 0.5; }}
.bp-start {{
  background-color: @bg_elevated;
  color: @text_primary;
  border: 1px solid @border_strong;
  padding: {token['space-3']};
  font-weight: 700;
}}
.bp-start:hover {{ background-color: @border_subtle; }}
.bp-start.running {{ background-color: @status_success; color: @accent_foreground; }}
.bp-cta {{
  color: @accent_primary;
  background-color: transparent;
  border: 1px solid @border_strong;
  padding: {token['space-2']} {token['space-3']};
}}
.bp-cta:hover {{ background-color: @bg_elevated; border-color: @accent_primary; }}

.bp-apply:focus-visible, .bp-start:focus-visible, .bp-cta:focus-visible,
.bp-input:focus-visible, .bp-switch:focus-visible, .bp-scale:focus-visible {{
  outline: 2px solid @accent_primary;
  outline-offset: {token['space-1']};
}}
.bp-hint {{
  color: @text_muted;
  font-family: {token['font-mono']}, monospace;
  font-size: {token['space-3']};
}}
"""


class _GtkUnavailable(RuntimeError):
    """El binding GTK4 no se pudo cargar (error interno de frontera)."""


class _EventLog:
    """Registro acotado de eventos para assertions deterministas."""

    def __init__(self) -> None:
        self._records: deque[tuple[Any, ...]] = deque(maxlen=_MAX_EVENTS)

    def record(self, kind: str, *payload: Any) -> None:
        """Añade un evento ``(kind, *payload)``."""
        self._records.append((kind, *payload))

    def snapshot(self) -> tuple[tuple[Any, ...], ...]:
        """Devuelve una copia inmutable de los eventos registrados."""
        return tuple(self._records)


class _PanelBackend(Protocol):
    """Frontera de renderizado compartida por los tres backends."""

    def set_app(self, name: str) -> None: ...

    def set_language(self, stream: str, code: str) -> None: ...

    def set_duck_level(self, value: float) -> None: ...

    def set_toggle(self, name: str, active: bool) -> None: ...

    def set_validation(self, field: str, message: str | None) -> None: ...

    def set_busy(self, busy: bool) -> None: ...

    def set_running(self, running: bool) -> None: ...

    def set_loading(self, loading: bool) -> None: ...

    def set_state(self, state: PanelState, message: str) -> None: ...

    def present(self) -> None: ...

    def destroy(self) -> None: ...


class _HeadlessBackend:
    """Backend sin GTK: registra cada operación como evento (modo tests)."""

    def __init__(self, log: _EventLog) -> None:
        self._log = log

    def set_app(self, name: str) -> None:
        self._log.record("app", name)

    def set_language(self, stream: str, code: str) -> None:
        self._log.record("language", stream, code)

    def set_duck_level(self, value: float) -> None:
        self._log.record("duck", value)

    def set_toggle(self, name: str, active: bool) -> None:
        self._log.record("toggle", name, active)

    def set_validation(self, field: str, message: str | None) -> None:
        self._log.record("validation", field, message)

    def set_busy(self, busy: bool) -> None:
        self._log.record("busy", busy)

    def set_running(self, running: bool) -> None:
        self._log.record("running", running)

    def set_loading(self, loading: bool) -> None:
        self._log.record("loading", loading)

    def set_state(self, state: PanelState, message: str) -> None:
        self._log.record("state", state, message)

    def present(self) -> None:
        self._log.record("present")

    def destroy(self) -> None:
        """No hay recursos del sistema que liberar en modo headless."""


class _MissingGtkBackend:
    """Backend de fallo explícito: exige GTK real y lo reporta de forma accionable."""

    def __init__(self, log: _EventLog, reason: _GtkUnavailable) -> None:
        self._log = log
        self._reason = reason

    def _fail(self) -> None:
        raise RuntimeError(_GTK_INSTALL_HINT) from self._reason

    def set_app(self, name: str) -> None:
        self._fail()

    def set_language(self, stream: str, code: str) -> None:
        self._fail()

    def set_duck_level(self, value: float) -> None:
        self._fail()

    def set_toggle(self, name: str, active: bool) -> None:
        self._fail()

    def set_validation(self, field: str, message: str | None) -> None:
        self._fail()

    def set_busy(self, busy: bool) -> None:
        self._fail()

    def set_running(self, running: bool) -> None:
        self._fail()

    def set_loading(self, loading: bool) -> None:
        self._fail()

    def set_state(self, state: PanelState, message: str) -> None:
        self._fail()

    def present(self) -> None:
        self._fail()

    def destroy(self) -> None:
        """No hay ventana que cerrar: el binding nunca se pudo cargar."""


def _load_gtk() -> tuple[Any, Any, Any]:
    """Importa Gtk/Gdk/GLib solo cuando se pide un panel real.

    El ``import gi`` vive aquí dentro a propósito: importar el módulo nunca
    exige PyGObject ni las bibliotecas del sistema.
    """
    try:
        import gi

        gi.require_version("Gtk", "4.0")
        from gi.repository import Gdk, GLib, Gtk
    except (ImportError, ValueError, RuntimeError) as exc:
        raise _GtkUnavailable(_GTK_INSTALL_HINT) from exc
    return Gtk, Gdk, GLib


class _GtkBackend:
    """Backend GTK4 real: panel con validación inline, estados y atajos."""

    APP_ID: Final[str] = "dev.babelpipe.ControlPanel"

    def __init__(
        self,
        *,
        log: _EventLog,
        controller: ControlPanel,
        nodes: Sequence[str],
        languages: Sequence[str],
        initial: Mapping[str, Any],
    ) -> None:
        Gtk, Gdk, GLib = _load_gtk()
        self._Gtk = Gtk
        self._GLib = GLib
        self._log = log
        self._controller = controller
        self._languages = tuple(languages)
        self._ui_thread = threading.current_thread()
        self._field_errors: dict[str, Any] = {}
        self._field_widgets: dict[str, Any] = {}

        app = Gtk.Application(application_id=self.APP_ID)
        window = Gtk.ApplicationWindow(application=app)
        window.add_css_class("bp-panel")
        window.set_title("BabelPipe — Panel de control")
        window.set_default_size(440, 620)
        window.set_resizable(True)
        self._app = app
        self._window = window
        self._install_css(Gtk, Gdk)

        root = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=_SPACE_3)
        root.add_css_class("bp-panel-root")
        window.set_child(root)

        title = Gtk.Label(label="BABELPIPE · CONTROL")
        title.set_xalign(0.0)
        title.add_css_class("bp-title")
        root.append(title)
        subtitle = Gtk.Label(label="Rutado · idiomas · ducking · PTT")
        subtitle.set_xalign(0.0)
        subtitle.add_css_class("bp-subtitle")
        root.append(subtitle)

        # FlowBox: el mensaje y su CTA/Retry se redistribuyen en líneas al
        # estrechar la ventana (375 px), sin señales de layout ni scroll horizontal.
        state_row = self._make_row(Gtk, "bp-state-row")
        self._state_icon = Gtk.Label(label="")
        self._state_icon.add_css_class("bp-state-icon")
        self._state_icon.set_valign(Gtk.Align.CENTER)
        state_row.append(self._state_icon)
        self._state_label = Gtk.Label(label=str(initial["message"]))
        self._state_label.set_xalign(0.0)
        self._state_label.set_wrap(True)
        # Tope de medida legible: evita que el mensaje empuje el CTA fuera del
        # ancho disponible en 375 px antes de que FlowBox lo envuelva.
        self._state_label.set_max_width_chars(48)
        self._state_label.set_hexpand(True)
        self._state_label.set_valign(Gtk.Align.CENTER)
        self._state_label.add_css_class("bp-state")
        state_row.append(self._state_label)
        self._view_button = Gtk.Button(label=EMPTY_CTA)
        self._view_button.add_css_class("bp-cta")
        self._view_button.set_hexpand(False)
        self._view_button.set_valign(Gtk.Align.CENTER)
        self._view_button.connect("clicked", lambda *_: self._controller.view_nodes())
        state_row.append(self._view_button)
        self._retry_button = Gtk.Button(label=ERROR_CTA)
        self._retry_button.add_css_class("bp-cta")
        self._retry_button.set_hexpand(False)
        self._retry_button.set_valign(Gtk.Align.CENTER)
        self._retry_button.connect("clicked", lambda *_: self._controller.retry())
        state_row.append(self._retry_button)
        root.append(state_row)

        self._skeleton = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=_SPACE_2)
        for width in _SKELETON_WIDTHS:
            bar = Gtk.Label(label="")
            bar.set_size_request(width, _SKELETON_HEIGHT)
            bar.add_css_class("bp-skeleton")
            self._skeleton.append(bar)
        root.append(self._skeleton)

        form = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=_SPACE_3)
        form.add_css_class("bp-form")
        root.append(form)
        self._form = form

        self._node_model = Gtk.StringList.new(list(nodes))
        self._app_dropdown = Gtk.DropDown(model=self._node_model)
        self._app_dropdown.add_css_class("bp-input")
        self._app_dropdown.connect("notify::selected", self._on_app_changed)
        self._add_field(
            form,
            "App a enrutar",
            self._app_dropdown,
            "app",
            "bp-input",
            min_width=_INPUT_MIN_WIDTH,
        )

        self._rx_entry = Gtk.Entry()
        self._rx_entry.set_text(str(initial["source_lang"]))
        self._rx_entry.add_css_class("bp-input")
        self._attach_completion(self._rx_entry)
        self._rx_entry.connect(
            "notify::text",
            lambda *_: self._controller.set_source_lang(self._rx_entry.get_text()),
        )
        self._add_field(
            form,
            "Idioma Rx",
            self._rx_entry,
            "source_lang",
            "bp-input",
            min_width=_INPUT_MIN_WIDTH,
        )

        self._tx_entry = Gtk.Entry()
        self._tx_entry.set_text(str(initial["target_lang"]))
        self._tx_entry.add_css_class("bp-input")
        self._attach_completion(self._tx_entry)
        self._tx_entry.connect(
            "notify::text",
            lambda *_: self._controller.set_target_lang(self._tx_entry.get_text()),
        )
        self._add_field(
            form,
            "Idioma Tx",
            self._tx_entry,
            "target_lang",
            "bp-input",
            min_width=_INPUT_MIN_WIDTH,
        )

        adjustment = Gtk.Adjustment(
            value=float(initial["duck_level"]),
            lower=DUCK_LEVEL_MIN,
            upper=DUCK_LEVEL_MAX,
            step_increment=0.001,
            page_increment=0.005,
            page_size=0.0,
        )
        self._scale = Gtk.Scale.new(Gtk.Orientation.HORIZONTAL, adjustment)
        self._scale.set_digits(2)
        self._scale.set_value_pos(Gtk.PositionType.RIGHT)
        self._scale.add_css_class("bp-scale")
        self._scale.connect(
            "value-changed",
            lambda *_: self._controller.set_duck_level(float(self._scale.get_value())),
        )
        self._add_field(
            form,
            "Ducking (10–15 %)",
            self._scale,
            "duck_level",
            "bp-scale",
            min_width=_INPUT_MIN_WIDTH,
        )

        self._ptt_switch = Gtk.Switch()
        self._ptt_switch.set_active(bool(initial["ptt_enabled"]))
        self._ptt_switch.add_css_class("bp-switch")
        self._ptt_switch.connect(
            "notify::active",
            lambda *_: self._controller.set_ptt_enabled(bool(self._ptt_switch.get_active())),
        )
        self._add_field(form, "Push-to-Talk (Ctrl+P)", self._ptt_switch, "ptt_enabled", "bp-switch")

        self._aec_switch = Gtk.Switch()
        self._aec_switch.set_active(bool(initial["echo_cancel"]))
        self._aec_switch.add_css_class("bp-switch")
        self._aec_switch.connect(
            "notify::active",
            lambda *_: self._controller.set_echo_cancel(bool(self._aec_switch.get_active())),
        )
        self._add_field(
            form, "Cancelación de eco (AEC)", self._aec_switch, "echo_cancel", "bp-switch"
        )

        # Reserva de error global para fallos de apply (texto plano, nunca markup).
        apply_error = Gtk.Label(label="")
        apply_error.set_xalign(0.0)
        apply_error.add_css_class("bp-field-error")
        form.append(apply_error)
        self._field_errors["apply"] = apply_error

        actions = self._make_row(Gtk, "bp-actions")
        self._apply_button = Gtk.Button(label="Apply")
        self._apply_button.add_css_class("bp-apply")
        self._apply_button.set_sensitive(not bool(initial["busy"]))
        self._apply_button.connect("clicked", lambda *_: self._controller.apply())
        actions.append(self._apply_button)
        self._start_button = Gtk.Button(label="Stop" if initial["running"] else "Start")
        self._start_button.add_css_class("bp-start")
        if initial["running"]:
            self._start_button.add_css_class("running")
        self._start_button.connect("clicked", lambda *_: self._controller.toggle_start_stop())
        actions.append(self._start_button)
        root.append(actions)

        hint = Gtk.Label(label="Ctrl+P PTT · Ctrl+Return Apply · Ctrl+S Start/Stop")
        hint.set_xalign(0.0)
        hint.add_css_class("bp-hint")
        root.append(hint)

        self._add_shortcuts(Gtk, window)
        self.set_loading(bool(initial["loading"]))
        self.set_state(initial["state"], str(initial["message"]))

    @staticmethod
    def _make_row(Gtk: Any, css_class: str) -> Any:
        """Fila responsive: ``Gtk.FlowBox`` envuelve sus hijos al estrechar la ventana.

        Es el mismo contenedor idiomático que usa el overlay (misma ventana GTK4):
        sin media queries ni señales de layout, y sin scroll horizontal en 375 px.
        ``set_selection_mode(NONE)`` evita que los controles parezcan seleccionables.
        """
        row = Gtk.FlowBox()
        row.set_orientation(Gtk.Orientation.HORIZONTAL)
        row.set_column_spacing(_SPACE_2)
        row.set_row_spacing(_SPACE_2)
        row.set_selection_mode(Gtk.SelectionMode.NONE)
        row.set_activate_on_single_click(False)
        row.set_hexpand(True)
        if css_class:
            row.add_css_class(css_class)
        return row

    def _attach_completion(self, entry: Any) -> None:
        """Ofrece los idiomas disponibles como autocompletado del selector."""
        Gtk = self._Gtk
        store = Gtk.ListStore(str)
        for code in self._languages:
            store.append([code])
        completion = Gtk.EntryCompletion()
        completion.set_model(store)
        completion.set_text_column(0)
        completion.set_inline_completion(True)
        completion.set_popup_completion(True)
        entry.set_completion(completion)

    def _install_css(self, Gtk: Any, Gdk: Any) -> None:
        """Carga los tokens en la pantalla por defecto si existe."""
        display = Gdk.Display.get_default()
        if display is None:
            return
        self._css_provider = Gtk.CssProvider()
        self._css_provider.load_from_data(stylesheet().encode("utf-8"))
        Gtk.StyleContext.add_provider_for_display(
            display,
            self._css_provider,
            Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION,
        )

    def _add_field(
        self,
        parent: Any,
        caption: str,
        widget: Any,
        field: str,
        css_class: str,
        *,
        min_width: int | None = None,
    ) -> None:
        """Fila con etiqueta, control y línea de error inline (texto plano).

        La etiqueta se envuelve con ``FlowBox`` junto al control: en 1440 px
        comparten línea y en 375 px el control pasa a la suya sin recortes. Los
        controles anchos reciben un mínimo derivado de tokens para no colapsar.
        """
        Gtk = self._Gtk
        group = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=_SPACE_1)
        line = self._make_row(Gtk, "bp-field-line")
        label = Gtk.Label(label=caption)
        label.set_xalign(0.0)
        label.set_wrap(True)
        label.set_max_width_chars(28)
        label.set_valign(Gtk.Align.CENTER)
        label.add_css_class("bp-field-label")
        line.append(label)
        widget.set_hexpand(True)
        if min_width is not None:
            widget.set_size_request(min_width, -1)
        if css_class:
            widget.add_css_class(css_class)
        line.append(widget)
        group.append(line)
        error = Gtk.Label(label="")
        error.set_xalign(0.0)
        error.add_css_class("bp-field-error")
        group.append(error)
        self._field_errors[field] = error
        self._field_widgets[field] = widget
        parent.append(group)

    def _add_shortcuts(self, Gtk: Any, window: Any) -> None:
        """Registra los atajos globales del panel."""
        controller = Gtk.ShortcutController()
        controller.set_scope(Gtk.ShortcutScope.GLOBAL)
        bindings: tuple[tuple[str, Callable[[], None]], ...] = (
            ("<Control>p", self._controller.toggle_ptt),
            ("<Control>Return", self._controller.apply),
            ("<Control>s", self._controller.toggle_start_stop),
        )
        for accelerator, callback in bindings:
            trigger = Gtk.ShortcutTrigger.parse_string(accelerator)
            action = Gtk.CallbackAction.new(lambda *_cb, fn=callback: fn())
            controller.add_shortcut(Gtk.Shortcut.new(trigger, action))
        window.add_controller(controller)

    def _dispatch(self, work: Callable[[], None]) -> None:
        """Ejecuta ``work`` en el hilo de UI (GTK no es thread-safe)."""

        def _run() -> bool:
            work()
            return False

        if threading.current_thread() is self._ui_thread:
            work()
        else:
            self._GLib.idle_add(_run)

    def _on_app_changed(self, dropdown: Any, _param: Any) -> None:
        index = dropdown.get_selected()
        if index == _GTK_INVALID_POSITION or index >= self._node_model.get_n_items():
            return
        self._controller.set_app(self._node_model.get_string(index))

    def set_app(self, name: str) -> None:
        def _render() -> None:
            for index in range(self._node_model.get_n_items()):
                if self._node_model.get_string(index) == name:
                    self._app_dropdown.set_selected(index)
                    return
            self._app_dropdown.set_selected(_GTK_INVALID_POSITION)

        self._dispatch(_render)

    def set_language(self, stream: str, code: str) -> None:
        entry = self._rx_entry if stream == "rx" else self._tx_entry
        self._dispatch(lambda: entry.set_text(code))

    def set_duck_level(self, value: float) -> None:
        self._dispatch(lambda: self._scale.set_value(value))

    def set_toggle(self, name: str, active: bool) -> None:
        switch = self._ptt_switch if name == "ptt_enabled" else self._aec_switch
        self._dispatch(lambda: switch.set_active(active))

    def set_validation(self, field: str, message: str | None) -> None:
        def _render() -> None:
            label = self._field_errors.get(field)
            widget = self._field_widgets.get(field)
            if label is not None:
                label.set_label(message or "")  # texto externo SIEMPRE plano
            if widget is not None:
                if message:
                    widget.add_css_class("is-invalid")
                else:
                    widget.remove_css_class("is-invalid")

        self._dispatch(_render)

    def set_busy(self, busy: bool) -> None:
        def _render() -> None:
            self._apply_button.set_sensitive(not busy)
            if busy:
                self._apply_button.add_css_class("is-busy")
            else:
                self._apply_button.remove_css_class("is-busy")

        self._dispatch(_render)

    def set_running(self, running: bool) -> None:
        def _render() -> None:
            self._start_button.set_label("Stop" if running else "Start")
            if running:
                self._start_button.add_css_class("running")
            else:
                self._start_button.remove_css_class("running")

        self._dispatch(_render)

    def set_loading(self, loading: bool) -> None:
        def _render() -> None:
            self._skeleton.set_visible(loading)
            self._form.set_sensitive(not loading)

        self._dispatch(_render)

    def set_state(self, state: PanelState, message: str) -> None:
        def _render() -> None:
            self._state_label.set_label(message)  # siempre como texto plano
            self._state_icon.set_label(_STATE_GLYPH[state])  # icono literal, no externo
            for css_class in ("empty", "error", "listening"):
                self._state_label.remove_css_class(css_class)
            self._state_label.add_css_class(state)
            self._view_button.set_visible(state == "empty")
            self._retry_button.set_visible(state == "error")

        self._dispatch(_render)

    def present(self) -> None:
        self._dispatch(self._window.present)

    def destroy(self) -> None:
        def _destroy() -> None:
            self._window.close()
            self._app.quit()

        self._dispatch(_destroy)


def _validated_text(value: object, label: str) -> str:
    """Valida texto externo en la frontera (longitud del contrato de subtitle)."""
    if not isinstance(value, str):
        raise ValueError(f"{label} debe ser texto, recibido {type(value).__name__}")
    if len(value) > _MAX_TEXT_CHARS:
        raise ValueError(f"{label} excede {_MAX_TEXT_CHARS} caracteres")
    return value


def _validated_node(value: object) -> str:
    """Valida un nombre de nodo PipeWire con el esquema canónico de audio."""
    text = _validated_text(value, "app")
    # Pydantic `pattern` usa re.search: se exige además fullmatch con el patrón
    # canónico de client.config para que '<b>Firefox</b>' no pase colado.
    if re.fullmatch(_NODE_NAME_RE, text) is None:
        raise ValueError("Nombre de nodo PipeWire inválido (letras, dígitos, _, ., -)")
    AudioConfig.model_validate({"rx_sink": text})
    return text


def _validated_language(value: object, field: str) -> str:
    """Valida un idioma contra ``PipelineConfig`` (ISO 639-1 en el contrato)."""
    text = _validated_text(value, field)
    try:
        PipelineConfig.model_validate({field: text})
    except ValidationError:
        raise ValueError(f"{field} debe tener entre 2 y 10 caracteres (ISO 639-1)") from None
    return text


def _validated_duck(value: object) -> float:
    """Valida ``duck_level`` contra ``AudioConfig`` (rango legal 0.10–0.15)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(
            "duck_level debe ser un número decimal, recibido " f"{type(value).__name__}"
        )
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("duck_level debe ser finito")
    try:
        AudioConfig.model_validate({"duck_level": number})
    except ValidationError:
        raise ValueError(
            f"duck_level debe estar entre {DUCK_LEVEL_MIN:.2f} y {DUCK_LEVEL_MAX:.2f}"
        ) from None
    return number


def _validated_bool(value: object, field: str) -> bool:
    """Valida un booleano en la frontera (para campos no incluidos en AudioConfig)."""
    if not isinstance(value, bool):
        raise ValueError(f"{field} debe ser true o false, recibido {type(value).__name__}")
    return value


def _validated_audio_bool(value: object, field: str) -> bool:
    """Valida un booleano contra el esquema canónico ``AudioConfig``."""
    validated = _validated_bool(value, field)
    AudioConfig.model_validate({field: validated})
    return validated


def _summarize_validation(exc: ValidationError) -> str:
    """Resume errores de Pydantic sin incluir valores de entrada ni stack trace."""
    details: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", ())) or "configuración"
        kind = str(error.get("type", "invalid"))
        details.append(f"{location}: {_HINT_BY_ERROR.get(kind, 'valor inválido')}")
    return "; ".join(details)


class ControlPanel:
    """Panel de control con API única para GTK y modo headless.

    Args:
        on_apply: callback invocado con la :class:`~client.config.ClientSettings`
            resultante al pulsar ``Apply``.
        on_start: callback del botón ``Start``.
        on_stop: callback del botón ``Stop``.
        on_view_nodes: callback del CTA ``Ver nodos PipeWire`` (estado empty).
        on_retry: callback del botón ``Retry`` (estado error).
        headless: ``True`` fuerza el backend de eventos (tests), ``False`` exige
            GTK real y ``None`` (por defecto) detecta su disponibilidad.
        mock: alias de ``headless`` para el flag ``--mock`` de pytest.
        pipewire_nodes: nombres de nodos disponibles para el selector de app.
        languages: códigos ISO ofrecidos en los selectores de idioma.
        settings: configuración inicial semilla; el resto de secciones se
            conservan al construir la ``ClientSettings`` de ``Apply``.
    """

    def __init__(
        self,
        *,
        on_apply: Callable[[ClientSettings], None] | None = None,
        on_start: Callable[[], None] | None = None,
        on_stop: Callable[[], None] | None = None,
        on_view_nodes: Callable[[], None] | None = None,
        on_retry: Callable[[], None] | None = None,
        headless: bool | None = None,
        mock: bool | None = None,
        pipewire_nodes: Sequence[str] | None = None,
        languages: Sequence[str] | None = None,
        settings: ClientSettings | None = None,
    ) -> None:
        callbacks = {
            "on_apply": on_apply,
            "on_start": on_start,
            "on_stop": on_stop,
            "on_view_nodes": on_view_nodes,
            "on_retry": on_retry,
        }
        for name, callback in callbacks.items():
            if callback is not None and not callable(callback):
                raise ValueError(f"{name} debe ser invocable o None")
        if headless is not None and not isinstance(headless, bool):
            raise ValueError("headless debe ser bool o None")
        if mock is not None and not isinstance(mock, bool):
            raise ValueError("mock debe ser bool o None")
        if mock is not None:
            if headless is not None and headless != mock:
                raise ValueError("headless y mock no pueden tener valores distintos")
            headless = mock
        if settings is not None and not isinstance(settings, ClientSettings):
            raise ValueError("settings debe ser ClientSettings o None")

        self._on_apply = on_apply
        self._on_start = on_start
        self._on_stop = on_stop
        self._on_view_nodes = on_view_nodes
        self._on_retry = on_retry
        self._log = _EventLog()
        self._errors: dict[str, str] = {}
        self._state: PanelState = "listening"
        self._message = LISTENING_MESSAGE
        self._cta: str | None = None
        self._loading = False
        self._busy = False
        self._running = False
        self._closed = False

        if pipewire_nodes is None:
            self._nodes: tuple[str, ...] = ()
        else:
            seen_nodes: set[str] = set()
            cleaned_nodes: list[str] = []
            for raw_node in pipewire_nodes:
                node = _validated_node(raw_node)
                if node not in seen_nodes:
                    seen_nodes.add(node)
                    cleaned_nodes.append(node)
            self._nodes = tuple(cleaned_nodes)

        if languages is None:
            self._languages = DEFAULT_LANGUAGES
        else:
            seen_languages: set[str] = set()
            cleaned_languages: list[str] = []
            for raw_code in languages:
                code = _validated_language(raw_code, "source_lang")
                if code not in seen_languages:
                    seen_languages.add(code)
                    cleaned_languages.append(code)
            self._languages = tuple(cleaned_languages)

        self._settings = settings if settings is not None else ClientSettings()
        self._app: str | None = self._nodes[0] if self._nodes else None
        self._source_lang = self._settings.pipeline.source_lang
        self._target_lang = self._settings.pipeline.target_lang
        self._duck_level = self._settings.audio.duck_level
        self._ptt_enabled = self._settings.audio.ptt_enabled
        self._echo_cancel = self._settings.audio.echo_cancel

        self._backend: _PanelBackend
        if headless is True:
            self._backend = _HeadlessBackend(self._log)
            self._is_headless = True
        else:
            initial: dict[str, Any] = {
                "message": self._message,
                "state": self._state,
                "source_lang": self._source_lang,
                "target_lang": self._target_lang,
                "duck_level": self._duck_level,
                "ptt_enabled": self._ptt_enabled,
                "echo_cancel": self._echo_cancel,
                "busy": self._busy,
                "running": self._running,
                "loading": self._loading,
            }
            try:
                self._backend = _GtkBackend(
                    log=self._log,
                    controller=self,
                    nodes=self._nodes,
                    languages=self._languages,
                    initial=initial,
                )
                self._is_headless = False
            except _GtkUnavailable as exc:
                if headless is False:
                    self._backend = _MissingGtkBackend(self._log, exc)
                    self._is_headless = False
                else:
                    self._backend = _HeadlessBackend(self._log)
                    self._is_headless = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("El panel de control ya está cerrado")

    def _set_error(self, field: str, message: str | None) -> None:
        """Actualiza el error inline de un campo (el backend registra el evento)."""
        if message:
            if self._errors.get(field) == message:
                return
            self._errors[field] = message
        else:
            if field not in self._errors:
                return
            self._errors.pop(field, None)
        self._backend.set_validation(field, message)

    @property
    def events(self) -> tuple[tuple[Any, ...], ...]:
        """Copia de los eventos emitidos (para assertions y diagnóstico)."""
        return self._log.snapshot()

    @property
    def is_headless(self) -> bool:
        """``True`` cuando se usa el backend sin GTK."""
        return self._is_headless

    @property
    def state(self) -> PanelState:
        """Estado visible actual (``listening``/``empty``/``error``)."""
        return self._state

    @property
    def message(self) -> str:
        """Mensaje del estado actual, literal (sin markup)."""
        return self._message

    @property
    def cta(self) -> str | None:
        """Texto del CTA visible en el estado empty, si aplica."""
        return self._cta

    @property
    def errors(self) -> Mapping[str, str]:
        """Errores inline activos, indexados por campo."""
        return MappingProxyType(dict(self._errors))

    @property
    def nodes(self) -> tuple[str, ...]:
        """Nodos PipeWire ofrecidos por el selector de app."""
        return self._nodes

    @property
    def languages(self) -> tuple[str, ...]:
        """Códigos de idioma ofrecidos por los selectores Rx/Tx."""
        return self._languages

    @property
    def selected_app(self) -> str | None:
        """App enrutada seleccionada (``None`` si no hay nodos)."""
        return self._app

    @property
    def source_lang(self) -> str:
        """Idioma Rx (entrada)."""
        return self._source_lang

    @property
    def target_lang(self) -> str:
        """Idioma Tx (salida)."""
        return self._target_lang

    @property
    def duck_level(self) -> float:
        """Atenuación del audio original durante el doblaje."""
        return self._duck_level

    @property
    def ptt_enabled(self) -> bool:
        """Si Push-to-Talk está activado."""
        return self._ptt_enabled

    @property
    def echo_cancel(self) -> bool:
        """Si la cancelación de eco (AEC) está activada."""
        return self._echo_cancel

    @property
    def is_busy(self) -> bool:
        """``True`` mientras ``Apply`` está enviando la configuración."""
        return self._busy

    @property
    def apply_enabled(self) -> bool:
        """``False`` durante el envío o mientras se cargan los modelos."""
        return not self._busy and not self._loading

    @property
    def is_running(self) -> bool:
        """``True`` cuando el daemon está en marcha."""
        return self._running

    @property
    def loading(self) -> bool:
        """``True`` mientras se muestran los skeletons de modelos."""
        return self._loading

    @property
    def settings(self) -> ClientSettings:
        """Última configuración aplicada (o la semilla inicial)."""
        return self._settings

    @property
    def closed(self) -> bool:
        """``True`` después de :meth:`close`."""
        return self._closed

    def set_app(self, name: str) -> None:
        """Selecciona la app a enrutar; valida el nombre y la pertenencia."""
        self._ensure_open()
        try:
            validated = _validated_node(name)
        except ValueError as exc:
            self._set_error("app", str(exc))
            raise
        if validated not in self._nodes:
            self._set_error("app", "Selecciona una app de la lista de nodos PipeWire")
            raise ValueError("app no está en la lista de nodos PipeWire disponibles")
        self._set_error("app", None)
        self._app = validated
        self._backend.set_app(validated)

    def set_source_lang(self, code: str) -> None:
        """Fija el idioma Rx con validación inline (ISO 639-1)."""
        self._ensure_open()
        try:
            validated = _validated_language(code, "source_lang")
        except ValueError as exc:
            self._set_error("source_lang", str(exc))
            raise
        self._set_error("source_lang", None)
        self._source_lang = validated
        self._backend.set_language("rx", validated)

    def set_target_lang(self, code: str) -> None:
        """Fija el idioma Tx con validación inline (ISO 639-1)."""
        self._ensure_open()
        try:
            validated = _validated_language(code, "target_lang")
        except ValueError as exc:
            self._set_error("target_lang", str(exc))
            raise
        self._set_error("target_lang", None)
        self._target_lang = validated
        self._backend.set_language("tx", validated)

    def set_duck_level(self, value: float) -> None:
        """Fija ``duck_level`` rechazando fuera de rango con error inline."""
        self._ensure_open()
        try:
            validated = _validated_duck(value)
        except ValueError as exc:
            self._set_error("duck_level", str(exc))
            raise
        self._set_error("duck_level", None)
        self._duck_level = validated
        self._backend.set_duck_level(validated)

    def set_ptt_enabled(self, value: bool) -> None:
        """Activa/desactiva Push-to-Talk."""
        self._ensure_open()
        try:
            validated = _validated_audio_bool(value, "ptt_enabled")
        except ValueError as exc:
            self._set_error("ptt_enabled", str(exc))
            raise
        self._set_error("ptt_enabled", None)
        self._ptt_enabled = validated
        self._backend.set_toggle("ptt_enabled", validated)

    def set_echo_cancel(self, value: bool) -> None:
        """Activa/desactiva la cancelación de eco (AEC)."""
        self._ensure_open()
        try:
            validated = _validated_audio_bool(value, "echo_cancel")
        except ValueError as exc:
            self._set_error("echo_cancel", str(exc))
            raise
        self._set_error("echo_cancel", None)
        self._echo_cancel = validated
        self._backend.set_toggle("echo_cancel", validated)

    def toggle_ptt(self) -> None:
        """Atajo ``Ctrl+P``: invierte el interruptor de PTT."""
        self.set_ptt_enabled(not self._ptt_enabled)

    def toggle_echo_cancel(self) -> None:
        """Invierte el interruptor de AEC."""
        self.set_echo_cancel(not self._echo_cancel)

    def set_state(self, state: PanelState, message: str | None = None) -> None:
        """Cambia a ``listening``/``empty``/``error`` con su mensaje y acciones."""
        self._ensure_open()
        if state not in _VALID_STATES:
            raise ValueError(f"state inválido: {state!r}")
        detail = _validated_text(message, "message") if message is not None else ""
        if state == "empty":
            effective = detail or EMPTY_MESSAGE
            self._cta = EMPTY_CTA
        elif state == "error":
            effective = detail or DEFAULT_ERROR_MESSAGE
            self._cta = None
        else:
            effective = detail or LISTENING_MESSAGE
            self._cta = None
        self._state = state
        self._message = effective
        self._backend.set_state(state, effective)

    def set_loading(self, loading: bool) -> None:
        """Muestra/oculta los skeletons de modelos y bloquea el formulario."""
        self._ensure_open()
        validated = _validated_bool(loading, "loading")
        self._loading = validated
        self._backend.set_loading(validated)

    def view_nodes(self) -> None:
        """Acción del CTA ``Ver nodos PipeWire``."""
        self._ensure_open()
        self._log.record("view_nodes")
        if self._on_view_nodes is not None:
            self._on_view_nodes()

    def retry(self) -> None:
        """Acción del botón ``Retry`` del estado de error."""
        self._ensure_open()
        self._log.record("retry")
        if self._on_retry is not None:
            self._on_retry()

    def _build_settings(self) -> ClientSettings:
        """Construye ``ClientSettings`` reutilizando el esquema canónico."""
        seed = self._settings
        payload: dict[str, Any] = {
            "server": {
                "host": seed.server.host,
                "port": seed.server.port,
                "token": seed.server.token,
            },
            "audio": {
                "rx_sink": seed.audio.rx_sink,
                "tx_source": seed.audio.tx_source,
                "duck_level": self._duck_level,
                "echo_cancel": self._echo_cancel,
                "ptt_enabled": self._ptt_enabled,
            },
            "pipeline": {
                "source_lang": self._source_lang,
                "target_lang": self._target_lang,
                "stt": seed.pipeline.stt,
                "translator": seed.pipeline.translator,
                "tts": seed.pipeline.tts,
            },
            "model": {
                "stt_path": seed.model.stt_path,
                "translator_path": seed.model.translator_path,
                "tts_path": seed.model.tts_path,
            },
        }
        return ClientSettings.model_validate(payload)

    def apply(self) -> None:
        """Valida y envía la configuración; Apply queda deshabilitado mientras envía."""
        self._ensure_open()
        if self._busy:
            raise RuntimeError("Apply ya está en curso")
        if self._loading:
            raise RuntimeError("El panel está cargando modelos; Apply no disponible")
        try:
            settings = self._build_settings()
        except ValidationError as exc:
            detail = _summarize_validation(exc)
            self._set_error("apply", detail)
            raise ValueError(f"Configuración inválida — {detail}") from None
        self._set_error("apply", None)

        self._busy = True
        self._backend.set_busy(True)
        try:
            self._log.record("apply", settings)
            if self._on_apply is not None:
                self._on_apply(settings)
            self._settings = settings
            self._log.record("applied", settings)
        except Exception as exc:
            self._log.record("apply_error", type(exc).__name__, str(exc))
            self._set_error("apply", "No se pudo aplicar la configuración")
            raise
        finally:
            self._busy = False
            self._backend.set_busy(False)

    def start(self) -> None:
        """Arranca el daemon e invoca ``on_start`` (idempotente)."""
        self._ensure_open()
        if self._running:
            return
        self._running = True
        self._log.record("start")
        self._backend.set_running(True)
        if self._on_start is not None:
            self._on_start()

    def stop(self) -> None:
        """Detiene el daemon e invoca ``on_stop`` (idempotente)."""
        self._ensure_open()
        if not self._running:
            return
        self._running = False
        self._log.record("stop")
        self._backend.set_running(False)
        if self._on_stop is not None:
            self._on_stop()

    def toggle_start_stop(self) -> None:
        """Atajo ``Ctrl+S`` / botón único: alterna entre ``start`` y ``stop``."""
        if self._running:
            self.stop()
        else:
            self.start()

    def present(self) -> None:
        """Presenta la ventana real (registra evento; no-op headless)."""
        self._ensure_open()
        self._backend.present()

    def close(self) -> None:
        """Libera ventana y aplicación; idempotente y sin recursos colgados."""
        if self._closed:
            return
        self._closed = True
        self._log.record("close")
        self._backend.destroy()

    def __enter__(self) -> ControlPanel:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


__all__ = [
    "DEFAULT_LANGUAGES",
    "PANEL_STATE_VALUES",
    "THEME_TOKENS",
    "ControlPanel",
    "PanelState",
    "load_theme_tokens",
    "stylesheet",
]
