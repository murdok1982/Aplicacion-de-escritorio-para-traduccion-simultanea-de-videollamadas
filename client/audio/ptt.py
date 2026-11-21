"""Push-to-Talk con gating de frames y hotkey inyectable (TASK-007).

Contrato (SPEC.md REQ-06 / AC-11, ARCHITECTURE.md §4.2):

* :class:`PTTState` es el estado mostrado por el panel: ``PTT_IDLE`` o
  ``PTT_ACTIVE``.
* Con PTT desactivado el canal Tx está abierto: el productor de audio solo
  omite el envío cuando el VAD detecta silencio explícito (responsabilidad del
  pipeline, no de este gate). Con PTT activado se envían frames de voz solo si
  la tecla está pulsada (``active``) o si la cabecera trae ``PTT_ACTIVE``.
* El hotkey se enlaza mediante una interfaz inyectable: nada graba teclas a
  nivel de sistema, por lo que los tests son deterministas y sin hardware.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from enum import Enum
from typing import Protocol

from shared.protocol import Flags

_FLAGS_MAX: int = 0xFF
_KEY_RE = re.compile(r"[A-Za-z0-9_+ -]{1,64}\Z")


class PTTState(str, Enum):
    """Estado del panel de control para el modo Push-to-Talk."""

    PTT_IDLE = "PTT_IDLE"
    PTT_ACTIVE = "PTT_ACTIVE"


class HotkeyBinder(Protocol):
    """Frontera inyectable para enlazar la tecla de PTT.

    La implementación real vive en la capa de UI (GTK recibe las pulsaciones y
    llama a los callbacks). Los tests inyectan un binder que solo registra.
    """

    def bind(
        self,
        key: str,
        on_press: Callable[[], None],
        on_release: Callable[[], None],
    ) -> None:
        """Asocia ``key`` con los callbacks de pulsación y liberación."""
        ...


class CallbackHotkeyBinder:
    """Puente real entre eventos de teclado entregados por la UI y el PTT.

    No graba teclas del sistema: la capa de UI llama a :meth:`key_down` y
    :meth:`key_up` cuando recibe el evento. Es determinista, no abre
    dispositivos y está completamente cableado a los callbacks de ``PushToTalk``.
    """

    def __init__(self) -> None:
        self._bindings: dict[str, tuple[Callable[[], None], Callable[[], None]]] = {}

    def bind(
        self,
        key: str,
        on_press: Callable[[], None],
        on_release: Callable[[], None],
    ) -> None:
        """Registra los callbacks de una tecla (la valida antes de guardar)."""
        _validate_key(key)
        if not callable(on_press) or not callable(on_release):
            raise ValueError("on_press y on_release deben ser invocables")
        self._bindings[key] = (on_press, on_release)

    def key_down(self, key: str) -> None:
        """Notifica pulsación: invoca el callback de pulsación si hay binding."""
        binding = self._bindings.get(key)
        if binding is not None:
            binding[0]()

    def key_up(self, key: str) -> None:
        """Notifica liberación: invoca el callback de liberación si hay binding."""
        binding = self._bindings.get(key)
        if binding is not None:
            binding[1]()

    @property
    def bound_keys(self) -> tuple[str, ...]:
        """Teclas enlazadas, en orden de registro."""
        return tuple(self._bindings)


def _validate_key(key: str) -> str:
    """Valida el identificador de tecla en la frontera de configuración."""
    if not isinstance(key, str):
        raise ValueError(f"key debe ser str, recibido {type(key).__name__}")
    if _KEY_RE.fullmatch(key) is None:
        raise ValueError(
            f"key contiene caracteres no permitidos: {key!r}. "
            "Solo letras, dígitos, guion, guion bajo, espacio y '+'."
        )
    return key


def _validate_bool(value: bool, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} debe ser bool, recibido {type(value).__name__}")
    return value


def _validate_flags(flags: int) -> int:
    """Valida la máscara uint8 de la cabecera ``!2sBBHIIH``."""
    if isinstance(flags, bool) or not isinstance(flags, int):
        raise ValueError(f"flags debe ser un entero uint8, recibido {type(flags).__name__}")
    value = int(flags)
    if not 0 <= value <= _FLAGS_MAX:
        raise ValueError(f"flags fuera de uint8: {value!r}")
    return value


class PushToTalk:
    """Estado y gating de Push-to-Talk para el canal Tx.

    Uso típico::

        ptt = PushToTalk(enabled=True)
        ptt.bind_hotkey("Space")
        # UI: binder.key_down("Space") / binder.key_up("Space")
        if ptt.should_send(int(flags)):
            enviar_frame_de_voz(...)
        else:
            enviar_frame_de_silencio(...)
    """

    def __init__(
        self,
        *,
        enabled: bool = False,
        hotkey_binder: HotkeyBinder | None = None,
    ) -> None:
        self._enabled = _validate_bool(enabled, "enabled")
        self._active = False
        self._key: str | None = None
        self._binder: HotkeyBinder = (
            hotkey_binder if hotkey_binder is not None else CallbackHotkeyBinder()
        )

    @property
    def enabled(self) -> bool:
        """``True`` cuando el modo PTT está activado en la configuración."""
        return self._enabled

    @property
    def active(self) -> bool:
        """``True`` mientras la tecla de PTT esté pulsada."""
        return self._active

    @property
    def state(self) -> PTTState:
        """Estado para el panel: solo ACTIVE si PTT está activado y pulsado."""
        if self._enabled and self._active:
            return PTTState.PTT_ACTIVE
        return PTTState.PTT_IDLE

    @property
    def hotkey(self) -> str | None:
        """Tecla enlazada, o ``None`` si aún no se ha enlazado ninguna."""
        return self._key

    @property
    def binder(self) -> HotkeyBinder:
        """Binder inyectado (o el puente por defecto) para conectar la UI."""
        return self._binder

    def set_enabled(self, enabled: bool) -> None:
        """Activa/desactiva el modo PTT; al desactivar vuelve a ``PTT_IDLE``."""
        self._enabled = _validate_bool(enabled, "enabled")
        if not self._enabled:
            self._active = False

    def set_active(self, active: bool) -> None:
        """Fija el estado de pulsación de la tecla."""
        self._active = _validate_bool(active, "active")

    def press(self) -> None:
        """Callback de pulsación: equivale a ``set_active(True)``."""
        self._active = True

    def release(self) -> None:
        """Callback de liberación: equivale a ``set_active(False)``."""
        self._active = False

    def toggle(self) -> None:
        """Invierte el estado de pulsación."""
        self._active = not self._active

    def bind_hotkey(self, key: str) -> None:
        """Enlaza ``key`` al binder inyectado y conecta press/release.

        No se graba ninguna tecla real: el binder decide cómo entregar los
        eventos (GTK en producción, un registrador en los tests).
        """
        validated = _validate_key(key)
        self._binder.bind(validated, self.press, self.release)
        self._key = validated

    def should_send(self, flags: int = 0) -> bool:
        """Decide si el frame de voz debe enviarse (AC-11).

        * PTT desactivado: el canal está siempre abierto; el silencio
          explícito lo decide el VAD, no este gate.
        * PTT activado: se envía solo si la tecla está pulsada (``active``) o
          si la cabecera trae ``Flags.PTT_ACTIVE`` (un productor remoto puede
          forzar el envío). En caso contrario se emite un frame de silencio.
        """
        header_ptt = bool(_validate_flags(flags) & int(Flags.PTT_ACTIVE))
        if not self._enabled:
            return True
        return self._active or header_ptt
