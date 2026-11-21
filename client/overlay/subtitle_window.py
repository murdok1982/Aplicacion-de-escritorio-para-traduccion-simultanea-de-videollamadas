"""Overlay flotante GTK4 de subtítulos de BabelPipe (TASK-018, SPEC.md §4).

El overlay es una ventana siempre visible (*always-on-top*), semitransparente y
redimensionable que **no captura pulsaciones** salvo en su botón de cierre. Expone
tres estados obligatorios (``listening`` con barra de nivel VAD, ``empty`` con
el mensaje de enrutado y el CTA de nodos PipeWire, y ``error`` con banner
recuperable y botón ``Retry``).

Sanitización (AC de seguridad): el texto externo **nunca** se interpreta como
markup. Llega a los widgets mediante ``set_label()`` / buffers de texto plano;
en este módulo no existe ninguna llamada de renderizado de markup. Los códigos
de error y los mensajes siguen la misma regla.

GTK se importa de forma **perezosa**: importar este módulo no requiere PyGObject.
Si el binding no está disponible (o se pide ``headless=True``) se usa un backend
que registra cada operación como evento, de modo que la API completa es
determinista y testeable sin servidor X/Wayland ni librerías del sistema. En
producción se instala el extra ``ui``: ``uv sync --extra ui`` (``PyGObject``),
además de ``libgtk-4-1`` y ``libgirepository-1.0-1``. Cuando se exige
explícitamente una ventana real (``headless=False``) y GTK falta, ``show()``
lanza un :class:`RuntimeError` accionable con esas instrucciones.

El módulo no crea hilos ni tareas asyncio: el backend GTK solo programa trabajo
en el hilo de UI con ``GLib.idle_add`` cuando se llama desde otro hilo, y
:meth:`SubtitleOverlay.close` libera ventana y aplicación de forma idempotente.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, Final, Literal, Protocol

OverlayState = Literal["listening", "empty", "error"]

_VALID_STATES: Final[frozenset[str]] = frozenset({"listening", "empty", "error"})
MAX_SUBTITLE_CHARS: Final[int] = 2000  # shared.models.SubtitleMessage.text

EMPTY_MESSAGE: Final[str] = "Sin audio detectado — enruta una app a Translator_Rx_Sink"
EMPTY_CTA: Final[str] = "Ver nodos PipeWire"
LISTENING_MESSAGE: Final[str] = "escuchando…"
ERROR_CTA: Final[str] = "Retry"
DEFAULT_ERROR_MESSAGE: Final[str] = "SERVER_OFFLINE"
ERROR_PREFIX: Final[str] = "ERROR"

_WINDOWS_OPACITY: Final[float] = 0.94
_MAX_EVENTS: Final[int] = 512
# FlowBox envuelve el CTA debajo del mensaje cuando el ancho escasea (375 px)
# sin depender de la señal `size-allocate`, que GTK4 ya no expone.
_ROW_SPACING: Final[int] = 12  # DESIGN_TOKENS["space_3"]
# Dos clases de entrada idénticas: alternarlas fuerza la reinvalidación de estilo
# que reinicia la animación CSS en cada subtítulo nuevo (GTK4 no expone un
# ``animation-play-state`` reiniciable desde Python).
_ENTER_A: Final[str] = "enter-a"
_ENTER_B: Final[str] = "enter-b"
_GTK_INSTALL_HINT: Final[str] = (
    "GTK4 no está disponible para el overlay de BabelPipe. Instala el extra 'ui' "
    "con `uv sync --extra ui` (PyGObject>=3.50,<4) y las bibliotecas del sistema "
    "'libgtk-4-1' y 'libgirepository-1.0-1'. Para tests usa `headless=True`."
)

# Design tokens canónicos (ARCHITECTURE.md §5). Fuente única para el CSS interno.
DESIGN_TOKENS: Final[Mapping[str, str]] = MappingProxyType(
    {
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
        "overlay_bg": "rgba(11,15,20,0.62)",
        "overlay_blur": "8px",
        "font_sans": "Inter",
        "font_mono": "JetBrains Mono",
        "radius_sm": "6px",
        "radius_md": "10px",
        "radius_lg": "14px",
        "space_1": "4px",
        "space_2": "8px",
        "space_3": "12px",
        "space_4": "16px",
        "space_6": "24px",
        "space_8": "32px",
    }
)

# RGB del token bg_canvas para derivar scrims sin duplicar el hexadecimal.
_CANVAS_RGB: Final[str] = ", ".join(
    str(int(DESIGN_TOKENS["bg_canvas"][index : index + 2], 16)) for index in (1, 3, 5)
)

OVERLAY_CSS: Final[str] = f"""
/* BabelPipe overlay — design tokens de ARCHITECTURE.md §5. */
@define-color bg_canvas {DESIGN_TOKENS["bg_canvas"]};
@define-color bg_surface {DESIGN_TOKENS["bg_surface"]};
@define-color bg_elevated {DESIGN_TOKENS["bg_elevated"]};
@define-color border_subtle {DESIGN_TOKENS["border_subtle"]};
@define-color border_strong {DESIGN_TOKENS["border_strong"]};
@define-color text_primary {DESIGN_TOKENS["text_primary"]};
@define-color text_secondary {DESIGN_TOKENS["text_secondary"]};
@define-color text_muted {DESIGN_TOKENS["text_muted"]};
@define-color accent_primary {DESIGN_TOKENS["accent_primary"]};
@define-color accent_foreground {DESIGN_TOKENS["accent_foreground"]};
@define-color accent_hover {DESIGN_TOKENS["accent_hover"]};
@define-color status_success {DESIGN_TOKENS["status_success"]};
@define-color status_warning {DESIGN_TOKENS["status_warning"]};
@define-color status_danger {DESIGN_TOKENS["status_danger"]};
@define-color overlay_bg {DESIGN_TOKENS["overlay_bg"]};

.overlay-window {{
  /* --overlay-blur: 8px; GTK4 no expone backdrop-filter: se aproxima con
     fondo rgba(11,15,20,0.62), opacidad de ventana y sombra difusa de 8px. */
  background-color: @overlay_bg;
  color: @text_primary;
  font-family: {DESIGN_TOKENS["font_sans"]}, sans-serif;
  font-size: {DESIGN_TOKENS["space_4"]}px;
  border: 1px solid @border_subtle;
  border-radius: {DESIGN_TOKENS["radius_md"]};
  /* Dos capas: sombra difusa de profundidad + brillo superior de superficie. */
  box-shadow: 0 {DESIGN_TOKENS["space_2"]} {DESIGN_TOKENS["space_8"]} rgba(0, 0, 0, 0.55),
              inset 0 1px 0 rgba(230, 237, 243, 0.06);
}}

.overlay-root {{
  background-color: transparent;
  border-radius: {DESIGN_TOKENS["radius_lg"]};
  padding: {DESIGN_TOKENS["space_3"]} {DESIGN_TOKENS["space_4"]};
}}

.overlay-title {{
  color: @text_muted;
  font-family: {DESIGN_TOKENS["font_mono"]}, monospace;
  font-size: {DESIGN_TOKENS["space_3"]}px;
  letter-spacing: {DESIGN_TOKENS["space_1"]};
}}

.overlay-close {{
  background-color: transparent;
  color: @text_secondary;
  border-radius: {DESIGN_TOKENS["radius_sm"]};
  border: 1px solid transparent;
  padding: {DESIGN_TOKENS["space_1"]};
  min-width: {DESIGN_TOKENS["space_6"]};
  min-height: {DESIGN_TOKENS["space_6"]};
}}
.overlay-close:hover {{ color: @accent_hover; border-color: @border_strong; }}
.overlay-close:focus-visible {{
  outline: 2px solid @accent_primary;
  outline-offset: {DESIGN_TOKENS["space_1"]};
}}

.overlay-subtitle {{
  color: @text_primary;
  font-family: {DESIGN_TOKENS["font_sans"]}, sans-serif;
  font-size: 20px;
  font-weight: 600;
  padding: {DESIGN_TOKENS["space_2"]} {DESIGN_TOKENS["space_1"]};
  border-left: {DESIGN_TOKENS["space_1"]} solid transparent;
  /* Scrim derivado de bg_canvas: mantiene AA incluso si el vídeo detrás es
     blanco puro (peor caso de composición: 3.6:1 → 5.3:1), sin volver opaca
     la ventana. La sombra refuerza la legibilidad periférica. */
  background-color: rgba({_CANVAS_RGB}, 0.28);
  border-radius: {DESIGN_TOKENS["radius_sm"]};
  text-shadow: 0 1px {DESIGN_TOKENS["space_1"]} rgba(0, 0, 0, 0.85);
  transition: border-left-color 180ms ease;
}}
.overlay-subtitle.is-final {{ border-left-color: @accent_primary; }}
/* Micro-interacción de aparición: fundido + elevación de 4 px. Alternar entre
   ambas clases reinicia la animación para cada cláusula, sin recrear widgets. */
.overlay-subtitle.{_ENTER_A},
.overlay-subtitle.{_ENTER_B} {{
  animation: bp-subtitle-in 240ms cubic-bezier(0.2, 0.7, 0.2, 1) both;
}}
@keyframes bp-subtitle-in {{
  from {{ opacity: 0; transform: translateY({DESIGN_TOKENS["space_1"]}); }}
  to {{ opacity: 1; transform: translateY(0); }}
}}

.overlay-listening, .overlay-empty, .overlay-error-banner {{
  padding-top: {DESIGN_TOKENS["space_2"]};
}}

.overlay-listening {{ color: @text_secondary; }}
.overlay-listening .overlay-hint {{
  font-family: {DESIGN_TOKENS["font_mono"]}, monospace;
  color: @accent_primary;
  animation: bp-listen-pulse 1600ms ease-in-out infinite alternate;
}}
/* El pulso se mantiene sutil (≥0.85) para no perder el carácter del acento
   cian; el contraste AA de ese token se mide sobre las superficies oscuras
   del sistema (9.58:1), no sobre un hipotético fotograma blanco puro. */
@keyframes bp-listen-pulse {{ from {{ opacity: 0.85; }} to {{ opacity: 1; }} }}

/* GTK4 pinta el nivel en `trough > block.filled / block.empty`; teñir solo el
   widget no colorea la barra, por eso se estilizan los subnodos reales. */
.overlay-vad-level {{
  min-width: {DESIGN_TOKENS["space_8"]};
  color: @accent_primary;
}}
.overlay-vad-level trough {{
  min-height: {DESIGN_TOKENS["space_2"]};
  background-color: @bg_elevated;
  border: 1px solid @border_subtle;
  border-radius: {DESIGN_TOKENS["radius_sm"]};
}}
.overlay-vad-level trough block.filled {{
  background-color: @accent_primary;
  border-radius: {DESIGN_TOKENS["radius_sm"]};
}}
.overlay-vad-level trough block.empty {{
  background-color: transparent;
}}

.overlay-empty {{ color: @text_secondary; }}

.overlay-error-banner {{
  background-color: @bg_surface;
  border: 1px solid @status_danger;
  border-radius: {DESIGN_TOKENS["radius_sm"]};
  padding: {DESIGN_TOKENS["space_2"]} {DESIGN_TOKENS["space_3"]};
  box-shadow: inset 0 1px 0 rgba(230, 237, 243, 0.04);
}}
.overlay-error-banner .overlay-error-code {{
  font-family: {DESIGN_TOKENS["font_mono"]}, monospace;
  color: @status_danger;
}}
.overlay-error-banner .overlay-message {{ color: @text_secondary; }}

.overlay-cta, .overlay-retry {{
  color: @accent_foreground;
  background-color: @accent_primary;
  border-radius: {DESIGN_TOKENS["radius_sm"]};
  padding: {DESIGN_TOKENS["space_2"]} {DESIGN_TOKENS["space_3"]};
  font-weight: 600;
  transition: background-color 140ms ease, transform 140ms ease;
}}
.overlay-cta:hover, .overlay-retry:hover {{ background-color: @accent_hover; }}
.overlay-cta:active, .overlay-retry:active {{ transform: scale(0.98); }}
.overlay-cta:focus-visible, .overlay-retry:focus-visible {{
  outline: 2px solid @accent_primary;
  outline-offset: {DESIGN_TOKENS["space_1"]};
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


class _OverlayBackend(Protocol):
    """Frontera de renderizado compartida por los tres backends."""

    def render_subtitle(self, text: str, is_final: bool) -> None: ...

    def render_state(
        self,
        state: OverlayState,
        message: str,
        *,
        cta: str | None,
        retry: bool,
    ) -> None: ...

    def render_level(self, level: float) -> None: ...

    def clear_subtitle(self) -> None: ...

    def destroy(self) -> None: ...


class _HeadlessBackend:
    """Backend sin GTK: registra cada render como evento (modo tests)."""

    def __init__(self, log: _EventLog) -> None:
        self._log = log

    def render_subtitle(self, text: str, is_final: bool) -> None:
        self._log.record("show", text, is_final)

    def render_state(
        self,
        state: OverlayState,
        message: str,
        *,
        cta: str | None,
        retry: bool,
    ) -> None:
        self._log.record("state", state, message, cta, retry)

    def render_level(self, level: float) -> None:
        self._log.record("level", level)

    def clear_subtitle(self) -> None:
        self._log.record("clear")

    def destroy(self) -> None:
        """No hay recursos del sistema que liberar en modo headless."""


class _MissingGtkBackend:
    """Backend de fallo explícito: exige GTK real y lo reporta de forma accionable."""

    def __init__(self, log: _EventLog, reason: _GtkUnavailable) -> None:
        self._log = log
        self._reason = reason

    def _fail(self) -> None:
        raise RuntimeError(_GTK_INSTALL_HINT) from self._reason

    def render_subtitle(self, text: str, is_final: bool) -> None:
        self._fail()

    def render_state(
        self,
        state: OverlayState,
        message: str,
        *,
        cta: str | None,
        retry: bool,
    ) -> None:
        self._fail()

    def render_level(self, level: float) -> None:
        self._fail()

    def clear_subtitle(self) -> None:
        self._fail()

    def destroy(self) -> None:
        """No hay ventana que cerrar: el binding nunca se pudo cargar."""


def _load_gtk() -> tuple[Any, Any, Any]:
    """Importa Gtk/Gdk/GLib solo cuando se pide una ventana real.

    El ``import gi`` vive aquí dentro a propósito: importar el módulo nunca
    exige PyGObject ni las librerías del sistema.
    """
    try:
        import gi

        gi.require_version("Gtk", "4.0")
        from gi.repository import Gdk, GLib, Gtk
    except (ImportError, ValueError, RuntimeError) as exc:
        raise _GtkUnavailable(_GTK_INSTALL_HINT) from exc
    return Gtk, Gdk, GLib


class _GtkBackend:
    """Backend GTK4 real: ventana always-on-top, translúcida y click-through."""

    APP_ID: Final[str] = "dev.babelpipe.SubtitleOverlay"

    def __init__(
        self,
        *,
        log: _EventLog,
        on_retry: Callable[[], None],
        on_view_nodes: Callable[[], None],
        on_close: Callable[[], None],
        keep_above: bool,
        decorated: bool,
        skip_taskbar: bool,
        initial_state: OverlayState,
        initial_message: str,
    ) -> None:
        Gtk, Gdk, GLib = _load_gtk()
        self._Gtk = Gtk
        self._Gdk = Gdk
        self._GLib = GLib
        self._log = log
        self._on_close = on_close
        self._ui_thread = threading.current_thread()
        # Empieza en A: el primer render alterna a B y dispara la animación.
        self._subtitle_enter_class = _ENTER_A

        self._app = Gtk.Application(application_id=self.APP_ID)
        self._window = Gtk.ApplicationWindow(application=self._app)
        self._window.add_css_class("overlay-window")
        self._window.set_title("BabelPipe — Subtítulos")
        self._window.set_keep_above(keep_above)
        self._window.set_decorated(decorated)
        self._window.set_resizable(True)
        self._window.set_default_size(480, 160)
        self._window.set_opacity(_WINDOWS_OPACITY)
        self._window.connect("realize", self._on_realize)
        if skip_taskbar:
            self._set_if_supported("set_skip_taskbar_hint", True)

        self._install_css(Gtk, Gdk)

        root = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        root.add_css_class("overlay-root")
        self._window.set_child(root)

        header = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        title = Gtk.Label(label="BABELPIPE · SUBTÍTULOS")
        title.set_xalign(0.0)
        title.add_css_class("overlay-title")
        header.append(title)

        spacer = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL)
        spacer.set_hexpand(True)
        header.append(spacer)

        self._close_button = Gtk.Button(label="✕")
        self._close_button.add_css_class("overlay-close")
        self._close_button.set_tooltip_text("Cerrar overlay")
        self._close_button.connect("clicked", self._on_close_clicked)
        header.append(self._close_button)
        root.append(header)

        self._subtitle_label = Gtk.Label(label="")
        self._subtitle_label.set_xalign(0.0)
        self._subtitle_label.set_wrap(True)
        self._subtitle_label.set_max_width_chars(72)
        self._subtitle_label.set_hexpand(True)
        self._subtitle_label.add_css_class("overlay-subtitle")
        root.append(self._subtitle_label)

        self._stack = Gtk.Stack()
        self._stack.set_transition_type(Gtk.StackTransitionType.CROSSFADE)
        self._stack.set_transition_duration(150)
        root.append(self._stack)

        listening = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        listening.add_css_class("overlay-listening")
        self._listening_label = Gtk.Label(label=LISTENING_MESSAGE)
        self._listening_label.set_xalign(0.0)
        self._listening_label.add_css_class("overlay-hint")
        listening.append(self._listening_label)
        self._level_bar = Gtk.LevelBar()
        self._level_bar.set_min_value(0.0)
        self._level_bar.set_max_value(1.0)
        self._level_bar.set_value(0.0)
        self._level_bar.set_mode(Gtk.LevelBarMode.CONTINUOUS)
        self._level_bar.set_hexpand(True)
        self._level_bar.add_css_class("overlay-vad-level")
        listening.append(self._level_bar)
        self._stack.add_titled(listening, "listening", "escuchando")

        empty = self._make_row(Gtk, "overlay-empty")
        self._empty_label = Gtk.Label(label=EMPTY_MESSAGE)
        self._empty_label.set_xalign(0.0)
        self._empty_label.set_wrap(True)
        # Tope de medida legible; también evita que el mensaje empuje el CTA
        # fuera del ancho disponible en 375 px antes de que FlowBox lo envuelva.
        self._empty_label.set_max_width_chars(30)
        self._empty_label.set_valign(Gtk.Align.CENTER)
        self._empty_label.add_css_class("overlay-message")
        empty.append(self._empty_label)
        self._view_button = Gtk.Button(label=EMPTY_CTA)
        self._view_button.add_css_class("overlay-cta")
        self._view_button.set_hexpand(False)
        self._view_button.set_valign(Gtk.Align.CENTER)
        self._view_button.connect("clicked", self._on_view_clicked)
        empty.append(self._view_button)
        self._stack.add_titled(empty, "empty", "sin audio")
        self._on_view_nodes = on_view_nodes

        error = self._make_row(Gtk, "overlay-error-banner")
        error_code = Gtk.Label(label=ERROR_PREFIX)
        error_code.add_css_class("overlay-error-code")
        error_code.set_valign(Gtk.Align.CENTER)
        error.append(error_code)
        self._error_label = Gtk.Label(label="")
        self._error_label.set_xalign(0.0)
        self._error_label.set_wrap(True)
        self._error_label.set_max_width_chars(30)
        self._error_label.set_valign(Gtk.Align.CENTER)
        self._error_label.add_css_class("overlay-message")
        error.append(self._error_label)
        self._retry_button = Gtk.Button(label=ERROR_CTA)
        self._retry_button.add_css_class("overlay-retry")
        self._retry_button.set_hexpand(False)
        self._retry_button.set_valign(Gtk.Align.CENTER)
        self._retry_button.connect("clicked", self._on_retry_clicked)
        error.append(self._retry_button)
        self._stack.add_titled(error, "error", "error")
        self._on_retry = on_retry

        self._stack.set_hexpand(True)
        self._stack.set_visible_child_name(initial_state)
        self._listening_label.set_label(initial_message)

    def _install_css(self, Gtk: Any, Gdk: Any) -> None:
        """Carga los tokens en la pantalla por defecto si existe."""
        display = Gdk.Display.get_default()
        if display is None:
            return
        self._css_provider = Gtk.CssProvider()
        self._css_provider.load_from_data(OVERLAY_CSS.encode("utf-8"))
        Gtk.StyleContext.add_provider_for_display(
            display,
            self._css_provider,
            Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION,
        )

    def _set_if_supported(self, method: str, value: Any) -> None:
        """Aplica una propiedad de ventana solo si el toolkit la expone."""
        setter = getattr(self._window, method, None)
        if callable(setter):
            setter(value)

    @staticmethod
    def _make_row(Gtk: Any, css_class: str) -> Any:
        """Fila que envuelve su CTA debajo del mensaje cuando no cabe en ancho.

        ``Gtk.FlowBox`` es el contenedor responsive nativo de GTK4: redistribuye
        sus hijos en líneas al redimensionar la ventana, sin señales de layout.
        """
        row = Gtk.FlowBox()
        row.set_orientation(Gtk.Orientation.HORIZONTAL)
        row.set_column_spacing(_ROW_SPACING)
        row.set_row_spacing(_ROW_SPACING)
        row.set_selection_mode(Gtk.SelectionMode.NONE)
        row.set_activate_on_single_click(False)
        row.set_hexpand(True)
        row.add_css_class(css_class)
        return row

    def _dispatch(self, work: Callable[[], None]) -> None:
        """Ejecuta ``work`` en el hilo de UI (GTK no es thread-safe)."""

        def _run() -> bool:
            work()
            return False

        if threading.current_thread() is self._ui_thread:
            work()
        else:
            self._GLib.idle_add(_run)

    def _on_realize(self, _widget: Any) -> None:
        self._apply_input_region()

    def _apply_input_region(self) -> None:
        """Deja capturando pulsaciones únicamente el área del botón de cierre.

        Requiere que el compositor exponga ``set_input_region`` en la superficie;
        en compositores que no lo soportan se mantiene la ventana utilizable sin
        fallar (degradación graceful, documentada para ``@ui-artisan``).
        """
        surface = self._window.get_surface()
        if surface is None:
            return
        region_setter = getattr(surface, "set_input_region", None)
        if not callable(region_setter):
            return
        try:
            allocation = self._close_button.get_allocation()
            region = self._Gdk.Region.new()
            rect = self._Gdk.Rectangle()
            rect.x = max(0, allocation.x - 4)
            rect.y = max(0, allocation.y - 4)
            rect.width = allocation.width + 8
            rect.height = allocation.height + 8
            region.union(rect)
            region_setter(region)
        except (AttributeError, TypeError, RuntimeError, OSError):
            return

    def _on_close_clicked(self, _button: Any) -> None:
        self._on_close()

    def _on_retry_clicked(self, _button: Any) -> None:
        self._on_retry()

    def _on_view_clicked(self, _button: Any) -> None:
        self._on_view_nodes()

    def render_subtitle(self, text: str, is_final: bool) -> None:
        self._log.record("show", text, is_final)

        def _render() -> None:
            # Texto externo SIEMPRE como texto plano, jamás como markup.
            self._subtitle_label.set_label(text)
            # Alternar clases reinicia el fundido de entrada en cada cláusula.
            next_class = _ENTER_B if self._subtitle_enter_class == _ENTER_A else _ENTER_A
            self._subtitle_label.remove_css_class(self._subtitle_enter_class)
            self._subtitle_label.add_css_class(next_class)
            self._subtitle_enter_class = next_class
            if is_final:
                self._subtitle_label.add_css_class("is-final")
            else:
                self._subtitle_label.remove_css_class("is-final")

        self._dispatch(_render)

    def render_state(
        self,
        state: OverlayState,
        message: str,
        *,
        cta: str | None,
        retry: bool,
    ) -> None:
        self._log.record("state", state, message, cta, retry)

        def _render() -> None:
            if state == "listening":
                self._listening_label.set_label(message)
            elif state == "empty":
                self._empty_label.set_label(message)
            else:
                self._error_label.set_label(message)
            self._view_button.set_visible(cta is not None)
            self._retry_button.set_visible(retry)
            self._stack.set_visible_child_name(state)

        self._dispatch(_render)

    def render_level(self, level: float) -> None:
        self._log.record("level", level)
        self._dispatch(lambda: self._level_bar.set_value(level))

    def clear_subtitle(self) -> None:
        self._log.record("clear")

        def _clear() -> None:
            self._subtitle_label.set_label("")
            self._subtitle_label.remove_css_class(_ENTER_A)
            self._subtitle_label.remove_css_class(_ENTER_B)

        self._dispatch(_clear)

    def destroy(self) -> None:
        def _destroy() -> None:
            self._window.close()
            self._app.quit()

        self._dispatch(_destroy)

    def present(self) -> None:
        """Muestra la ventana (se llama desde :meth:`SubtitleOverlay.show`)."""
        self._dispatch(self._window.present)


def _validate_text(value: str, label: str) -> str:
    """Valida texto externo en la frontera (longitud del contrato de subtitle)."""
    if not isinstance(value, str):
        raise ValueError(f"{label} debe ser str, recibido {type(value).__name__}")
    if len(value) > MAX_SUBTITLE_CHARS:
        raise ValueError(f"{label} excede {MAX_SUBTITLE_CHARS} caracteres")
    return value


def _validate_bool(value: bool, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} debe ser bool, recibido {type(value).__name__}")
    return value


def _validate_level(value: float) -> float:
    """Valida y acota el nivel VAD a ``[0.0, 1.0]`` para la barra de nivel."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"vad_level debe ser float, recibido {type(value).__name__}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("vad_level debe ser finito")
    return min(1.0, max(0.0, number))


class SubtitleOverlay:
    """Ventana de subtítulos con API única para GTK y modo headless.

    Args:
        on_retry: callback del botón ``Retry`` del estado de error.
        on_view_nodes: callback del CTA ``Ver nodos PipeWire`` del estado empty.
        headless: ``True`` fuerza el backend de eventos (tests), ``False`` exige
            GTK real y ``None`` (por defecto) detecta su disponibilidad.
        keep_above: mantiene la ventana siempre visible por encima del resto.
        decorated: si ``True`` conserva la decoración nativa (por defecto no).
        skip_taskbar: oculta la ventana de la barra de tareas cuando es posible.
    """

    def __init__(
        self,
        *,
        on_retry: Callable[[], None] | None = None,
        on_view_nodes: Callable[[], None] | None = None,
        headless: bool | None = None,
        keep_above: bool = True,
        decorated: bool = False,
        skip_taskbar: bool = True,
    ) -> None:
        if on_retry is not None and not callable(on_retry):
            raise ValueError("on_retry debe ser invocable o None")
        if on_view_nodes is not None and not callable(on_view_nodes):
            raise ValueError("on_view_nodes debe ser invocable o None")
        if headless is not None and not isinstance(headless, bool):
            raise ValueError("headless debe ser bool o None")
        for name, flag in (
            ("keep_above", keep_above),
            ("decorated", decorated),
            ("skip_taskbar", skip_taskbar),
        ):
            _validate_bool(flag, name)

        self._on_retry = on_retry
        self._on_view_nodes = on_view_nodes
        self._log = _EventLog()
        self._state: OverlayState = "listening"
        self._message = LISTENING_MESSAGE
        self._subtitle_text = ""
        self._is_final = False
        self._level = 0.0
        self._cta: str | None = None
        self._closed = False

        self._backend: _OverlayBackend
        if headless is True:
            self._backend = _HeadlessBackend(self._log)
            self._is_headless = True
        else:
            try:
                self._backend = _GtkBackend(
                    log=self._log,
                    on_retry=self.retry,
                    on_view_nodes=self.view_nodes,
                    on_close=self.close,
                    keep_above=keep_above,
                    decorated=decorated,
                    skip_taskbar=skip_taskbar,
                    initial_state=self._state,
                    initial_message=self._message,
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
            raise RuntimeError("El overlay de subtítulos ya está cerrado")

    @property
    def events(self) -> tuple[tuple[Any, ...], ...]:
        """Copia de los eventos emitidos (para assertions y diagnóstico)."""
        return self._log.snapshot()

    @property
    def is_headless(self) -> bool:
        """``True`` cuando se usa el backend sin GTK."""
        return self._is_headless

    @property
    def state(self) -> OverlayState:
        """Estado visible actual."""
        return self._state

    @property
    def message(self) -> str:
        """Mensaje del estado actual (o del último subtitle)."""
        return self._message

    @property
    def subtitle_text(self) -> str:
        """Último subtítulo recibido, literal (sin interpretar como markup)."""
        return self._subtitle_text

    @property
    def is_final(self) -> bool:
        """Si el último subtítulo era una cláusula cerrada por VAD."""
        return self._is_final

    @property
    def level(self) -> float:
        """Nivel VAD actual, en ``[0.0, 1.0]``."""
        return self._level

    @property
    def cta(self) -> str | None:
        """Texto del CTA visible en el estado empty, si aplica."""
        return self._cta

    @property
    def closed(self) -> bool:
        """``True`` después de :meth:`close`."""
        return self._closed

    def present(self) -> None:
        """Presenta la ventana real (no-op en modo headless)."""
        self._ensure_open()
        if isinstance(self._backend, _GtkBackend):
            self._backend.present()

    def show(self, subtitle: str, *, is_final: bool = False) -> None:
        """Muestra un subtítulo como texto plano; ``is_final`` marca la cláusula."""
        self._ensure_open()
        text = _validate_text(subtitle, "subtitle")
        final = _validate_bool(is_final, "is_final")
        self._subtitle_text = text
        self._is_final = final
        self._backend.render_subtitle(text, final)

    def set_state(self, state: OverlayState, message: str | None = None) -> None:
        """Cambia a ``listening``/``empty``/``error`` con su mensaje y acciones."""
        self._ensure_open()
        if state not in _VALID_STATES:
            raise ValueError(f"state inválido: {state!r}")
        detail = _validate_text(message, "message") if message is not None else ""
        if state == "empty":
            effective = detail or EMPTY_MESSAGE
            cta: str | None = EMPTY_CTA
        elif state == "error":
            effective = detail or DEFAULT_ERROR_MESSAGE
            cta = None
        else:
            effective = detail or LISTENING_MESSAGE
            cta = None
        self._state = state
        self._message = effective
        self._cta = cta
        self._backend.render_state(state, effective, cta=cta, retry=state == "error")

    def set_level(self, vad_level: float) -> None:
        """Actualiza la barra de nivel VAD (valor recortado a ``[0, 1]``)."""
        self._ensure_open()
        level = _validate_level(vad_level)
        self._level = level
        self._backend.render_level(level)

    def clear(self) -> None:
        """Limpia el texto del subtítulo sin cerrar la ventana ni cambiar estado."""
        self._ensure_open()
        self._subtitle_text = ""
        self._is_final = False
        self._backend.clear_subtitle()

    def retry(self) -> None:
        """Acción del botón ``Retry``: invoca el callback inyectable."""
        self._ensure_open()
        self._log.record("retry")
        if self._on_retry is not None:
            self._on_retry()

    def view_nodes(self) -> None:
        """Acción del CTA ``Ver nodos PipeWire``: invoca su callback."""
        self._ensure_open()
        self._log.record("view_nodes")
        if self._on_view_nodes is not None:
            self._on_view_nodes()

    def close(self) -> None:
        """Libera ventana y aplicación; idempotente y sin recursos colgados."""
        if self._closed:
            return
        self._closed = True
        self._log.record("close")
        self._backend.destroy()

    def __enter__(self) -> SubtitleOverlay:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


__all__ = [
    "DEFAULT_ERROR_MESSAGE",
    "DESIGN_TOKENS",
    "EMPTY_CTA",
    "EMPTY_MESSAGE",
    "ERROR_CTA",
    "LISTENING_MESSAGE",
    "MAX_SUBTITLE_CHARS",
    "OVERLAY_CSS",
    "SubtitleOverlay",
    "OverlayState",
]
