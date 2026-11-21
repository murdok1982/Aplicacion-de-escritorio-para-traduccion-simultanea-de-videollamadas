"""Tests de integración del AEC (TASK-007, AC-10) sin hardware.

La atenuación del eco se mide con ``tests/fixtures/echo_48k.wav`` y el modelo
determinista :func:`client.audio.echo_cancel.suppress_echo`. La gestión del
módulo usa un ``CommandRunner`` en memoria: ``subprocess.run`` está parcheado
para que cualquier comando real haga fallar el test.
"""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

import numpy as np
import pytest
import soundfile as sf

from client.audio.echo_cancel import (
    ECHO_CANCEL_MODULE,
    CommandResult,
    EchoCancelError,
    EchoCancelManager,
    build_aec_args,
    suppress_echo,
)

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
ECHO_WAV = FIXTURES_DIR / "echo_48k.wav"
SAMPLE_RATE = 48_000
DELAY_MS = 120.0
DELAY_SAMPLES = round(SAMPLE_RATE * DELAY_MS / 1000.0)


def _rms(signal: np.ndarray) -> float:
    """RMS en las unidades de la señal (float normalizado aquí)."""
    return float(np.sqrt(np.mean(np.square(signal, dtype=np.float64))))


def _attenuation_db(before: np.ndarray, after: np.ndarray) -> float:
    """Atenuación en dB entre dos porciones de eco."""
    return 20.0 * float(np.log10(_rms(before) / _rms(after)))


class FakeCommandRunner:
    """Simula ``pactl`` sin ejecutar ni un solo comando real."""

    def __init__(self, *, module_id: int = 42, load_fails: bool = False) -> None:
        self.module_id = module_id
        self.load_fails = load_fails
        self.commands: list[tuple[str, ...]] = []

    def run(self, args: Sequence[str]) -> CommandResult:
        argv = tuple(args)
        self.commands.append(argv)

        if argv[0] != "pactl":
            raise AssertionError(f"Comando inesperado (podría afectar al sistema): {argv}")
        if len(argv) < 3:
            return CommandResult(stderr="argv de pactl inválido", returncode=1)

        action = argv[1]
        if action == "load-module":
            assert argv[2] == ECHO_CANCEL_MODULE, f"Módulo inesperado: {argv[2]}"
            assert all("sudo" not in item and item != "sh" for item in argv)
            if self.load_fails:
                return CommandResult(stderr="módulo no disponible", returncode=1)
            return CommandResult(stdout=f"{self.module_id}\n", returncode=0)
        if action == "unload-module":
            assert argv[2] == str(self.module_id), f"Se descargó un módulo ajeno: {argv[2]}"
            return CommandResult(returncode=0)
        raise AssertionError(f"Acción pactl no soportada: {action}")


@pytest.fixture(autouse=True)
def forbid_real_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """Red de seguridad: cualquier subprocess.real rompe el test."""

    def fail(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(f"Se intentó ejecutar un comando real: {args} {kwargs}")

    monkeypatch.setattr(subprocess, "run", fail)


def test_build_aec_args_webrtc_options() -> None:
    """La cadena usa WebRTC AEC, la referencia del monitor y los flags fijados."""
    args = build_aec_args(
        "Physical_Mic",
        "alsa_output.pci-0000_00_1f.3.analog-stereo",
        noise_suppression=True,
        analog_agc=False,
        digital_agc=False,
    )

    assert "source.name=Physical_Mic" in args
    assert "sink.name=alsa_output.pci-0000_00_1f.3.analog-stereo.monitor" in args
    assert "noise_suppression=1" in args
    assert "analog_gain_control=0" in args
    assert "digital_gain_control=0" in args
    assert 'aec_args="analog_gain_control=0 digital_gain_control=0 noise_suppression=1"' in args


def test_build_aec_args_flags_and_monitor_idempotent() -> None:
    """Los AGC se activan cuando se piden y un monitor no se duplica."""
    enabled = build_aec_args(
        "Mic",
        "Sink.monitor",
        noise_suppression=False,
        analog_agc=True,
        digital_agc=True,
    )
    assert "sink.name=Sink.monitor " in enabled
    assert "sink.name=Sink.monitor.monitor" not in enabled
    assert "analog_gain_control=1" in enabled
    assert "digital_gain_control=1" in enabled
    assert "noise_suppression=0" in enabled


def test_build_aec_args_rejects_unsafe_names() -> None:
    """La frontera rechaza nombres con opciones inyectadas y tipos incorrectos."""
    with pytest.raises(ValueError, match="source_name"):
        build_aec_args("Mic sink.name=Otro aec_args=x", "Sink")
    with pytest.raises(ValueError, match="sink_name"):
        build_aec_args("Mic", "Sink;rm -rf /")
    with pytest.raises(ValueError, match="noise_suppression"):
        build_aec_args("Mic", "Sink", noise_suppression=1)  # type: ignore[arg-type]


def test_ac10_echo_attenuation_db() -> None:
    """AC-10: el eco del interlocutor queda atenuado >= 20 dB.

    Se usa ``echo_48k.wav`` como señal del monitor del sink físico (far-end).
    El micrófono simulado contiene voz cercana en los primeros 120 ms y una
    copia retardada del far-end a ganancia unitaria a partir de ahí; solo se
    compara el RMS de la porción de eco antes/después de la cancelación.
    """
    info = sf.info(str(ECHO_WAV))
    assert info.samplerate == SAMPLE_RATE
    assert info.channels == 1
    reference, samplerate = sf.read(str(ECHO_WAV), dtype="float64")
    assert samplerate == SAMPLE_RATE
    assert reference.ndim == 1
    assert reference.size > DELAY_SAMPLES

    mic = np.zeros(reference.size + DELAY_SAMPLES, dtype=np.float64)
    near_end_t = np.arange(DELAY_SAMPLES, dtype=np.float64) / SAMPLE_RATE
    mic[:DELAY_SAMPLES] = 0.4 * np.sin(2.0 * np.pi * 180.0 * near_end_t)
    mic[DELAY_SAMPLES:] += reference

    before = mic[DELAY_SAMPLES:]
    cleaned = suppress_echo(mic, reference, delay_ms=DELAY_MS, attenuation_db=20.0)
    after = cleaned[DELAY_SAMPLES:]

    measured = _attenuation_db(before, after)
    print(f"\nAtenuación del eco medida: {measured:.4f} dB (objetivo >= 20 dB)")
    assert _rms(before) > 0.0
    assert _rms(after) < _rms(before) / 10.0
    assert measured >= 20.0
    # La voz cercana (anterior al retardo) no se toca.
    assert np.array_equal(cleaned[:DELAY_SAMPLES], mic[:DELAY_SAMPLES])


def test_suppress_echo_preserves_int16_dtype_and_validates() -> None:
    """El modelo conserva el dtype LSB y rechaza entradas floats/NaN."""
    delay_samples = round(SAMPLE_RATE * 1.0 / 1000.0)  # 48 muestras a 48 kHz
    reference = np.full(256, 2000, dtype=np.int16)
    mic = np.zeros(512, dtype=np.int16)
    mic[delay_samples : delay_samples + reference.size] += reference

    cleaned = suppress_echo(mic, reference, delay_ms=1.0, attenuation_db=20.0)
    assert cleaned.dtype == np.dtype(np.int16)
    assert cleaned.shape == mic.shape
    assert int(np.max(np.abs(cleaned[delay_samples : delay_samples + reference.size]))) < int(
        np.max(np.abs(mic[delay_samples : delay_samples + reference.size]))
    )

    with pytest.raises(ValueError, match="mic"):
        suppress_echo([0.0, 1.0], reference)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="no finitas"):
        suppress_echo(np.array([0.0, np.nan]), reference)
    with pytest.raises(ValueError, match="attenuation_db"):
        suppress_echo(mic, reference, attenuation_db=-1.0)


def test_enable_disable_only_own_aec_module_is_idempotent() -> None:
    """enable/disable cargan y descargan solo el módulo AEC propio, una vez."""
    runner = FakeCommandRunner()
    manager = EchoCancelManager(
        runner,
        source_name="Physical_Mic",
        sink_name="alsa_output.pci",
    )

    assert manager.status().enabled is False
    assert manager.status().module_id is None
    first_id = manager.enable()
    assert first_id == 42
    assert manager.enable() == 42  # idempotente: no carga dos veces

    status = manager.status()
    assert status.enabled is True
    assert status.module_id == 42
    assert status.module_name == ECHO_CANCEL_MODULE
    assert "sink.name=alsa_output.pci.monitor" in status.aec_args

    manager.disable()
    manager.disable()  # idempotente: no descarga dos veces
    assert manager.status().enabled is False
    assert manager.status().module_id is None

    expected_args = shlex.split(
        build_aec_args("Physical_Mic", "alsa_output.pci")
    )
    assert runner.commands == [
        ("pactl", "load-module", ECHO_CANCEL_MODULE, *expected_args),
        ("pactl", "unload-module", "42"),
    ]
    assert not any("module-null-sink" in item for command in runner.commands for item in command)


def test_enable_failure_does_not_register_module() -> None:
    """Si la carga falla, el estado sigue desactivado y no hay descarga."""
    runner = FakeCommandRunner(load_fails=True)
    manager = EchoCancelManager(runner)

    with pytest.raises(EchoCancelError, match="NO_PIPEWIRE"):
        manager.enable()
    assert manager.status().enabled is False
    manager.disable()  # no emite comandos
    assert len(runner.commands) == 1
