"""Tests unitarios de Push-to-Talk (TASK-007, AC-11).

El hotkey se inyecta: ningún test graba teclas reales ni abre dispositivos.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from client.audio.ptt import CallbackHotkeyBinder, PTTState, PushToTalk
from shared.protocol import Flags

_ON_PRESS = Callable[[], None]


class RecordingBinder:
    """Binder inyectable que solo registra llamadas y callbacks."""

    def __init__(self) -> None:
        self.bound_keys: list[str] = []
        self.on_press: _ON_PRESS | None = None
        self.on_release: _ON_PRESS | None = None

    def bind(self, key: str, on_press: _ON_PRESS, on_release: _ON_PRESS) -> None:
        self.bound_keys.append(key)
        self.on_press = on_press
        self.on_release = on_release


def test_ac11_ptt_gating() -> None:
    """AC-11: con PTT activado y sin pulsar, no se envían frames de voz."""
    ptt = PushToTalk(enabled=True)

    assert ptt.enabled is True
    assert ptt.active is False
    assert ptt.state is PTTState.PTT_IDLE
    assert ptt.should_send(0) is False
    assert ptt.should_send(int(Flags.FINAL)) is False

    ptt.set_active(True)
    assert ptt.active is True
    assert ptt.state is PTTState.PTT_ACTIVE
    assert ptt.should_send(0) is True

    ptt.set_active(False)
    assert ptt.state is PTTState.PTT_IDLE
    assert ptt.should_send(0) is False


def test_flag_ptt_active_overrides_local_idle() -> None:
    """El flag PTT_ACTIVE de la cabecera habilita el envío aunque no se pulse."""
    ptt = PushToTalk(enabled=True)

    assert ptt.should_send(int(Flags.PTT_ACTIVE)) is True
    assert ptt.should_send(int(Flags.FINAL | Flags.DUCK_START | Flags.PTT_ACTIVE)) is True
    assert ptt.should_send(int(Flags.FINAL | Flags.DUCK_START)) is False
    assert ptt.state is PTTState.PTT_IDLE  # el flag no cambia el estado local


def test_ptt_disabled_keeps_tx_channel_open() -> None:
    """Con PTT desactivado el canal Tx está abierto; el VAD decide el silencio."""
    ptt = PushToTalk(enabled=False)

    assert ptt.enabled is False
    assert ptt.state is PTTState.PTT_IDLE
    assert ptt.should_send(0) is True
    assert ptt.should_send(int(Flags.PTT_ACTIVE)) is True

    ptt.set_active(True)
    assert ptt.active is True
    assert ptt.should_send(0) is True


def test_toggle_and_set_enabled() -> None:
    """toggle invierte la pulsación; desactivar PTT fuerza PTT_IDLE."""
    ptt = PushToTalk(enabled=True)

    ptt.toggle()
    assert ptt.active is True
    assert ptt.state is PTTState.PTT_ACTIVE
    ptt.toggle()
    assert ptt.active is False
    assert ptt.state is PTTState.PTT_IDLE

    ptt.set_active(True)
    ptt.set_enabled(False)
    assert ptt.active is False
    assert ptt.state is PTTState.PTT_IDLE
    assert ptt.should_send(0) is True


def test_hotkey_is_injected_and_wired() -> None:
    """bind_hotkey delega en el binder y conecta press/release al estado."""
    binder = RecordingBinder()
    ptt = PushToTalk(enabled=True, hotkey_binder=binder)

    ptt.bind_hotkey("Space")
    assert binder.bound_keys == ["Space"]
    assert ptt.hotkey == "Space"
    assert binder.on_press is not None and binder.on_release is not None

    binder.on_press()
    assert ptt.active is True
    assert ptt.state is PTTState.PTT_ACTIVE
    assert ptt.should_send(0) is True

    binder.on_release()
    assert ptt.active is False
    assert ptt.state is PTTState.PTT_IDLE
    assert ptt.should_send(0) is False


def test_default_hotkey_bridge_delivers_events() -> None:
    """El puente por defecto lleva eventos de teclado de la UI al PTT."""
    ptt = PushToTalk(enabled=True)
    ptt.bind_hotkey("KeyT")

    bridge = ptt.binder
    assert isinstance(bridge, CallbackHotkeyBinder)
    assert bridge.bound_keys == ("KeyT",)

    bridge.key_down("KeyT")
    assert ptt.active is True
    assert ptt.should_send(0) is True
    bridge.key_up("KeyT")
    assert ptt.active is False
    assert ptt.should_send(0) is False

    # Teclas no enlazadas no alteran el estado.
    bridge.key_down("Unbound")
    assert ptt.active is False


@pytest.mark.parametrize("flags", [-1, 256, 0x10000])
def test_invalid_flags_are_rejected(flags: int) -> None:
    """Los flags de la cabecera deben ser uint8."""
    ptt = PushToTalk(enabled=True)
    with pytest.raises(ValueError, match="flags"):
        ptt.should_send(flags)
    with pytest.raises(ValueError, match="flags"):
        ptt.should_send(True)  # bool no es un uint8 válido en la frontera


def test_strict_boundary_validation() -> None:
    """Tipos y claves inválidos se rechazan antes de tocar el estado."""
    with pytest.raises(ValueError, match="enabled"):
        PushToTalk(enabled="yes")  # type: ignore[arg-type]
    ptt = PushToTalk()
    with pytest.raises(ValueError, match="active"):
        ptt.set_active(1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="enabled"):
        ptt.set_enabled("true")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="key"):
        ptt.bind_hotkey("")
    with pytest.raises(ValueError, match="key"):
        ptt.bind_hotkey("Space;rm -rf /")
