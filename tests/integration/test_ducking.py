"""Tests de integración del mezclador con ducking (REQ-05 / AC-08, ADR-002).

Todas las señales son fixtures WAV sintéticos o PCM generado con
:mod:`client.audio.resampler`: no se toca PipeWire ni hardware de audio. Esto
es precisamente la garantía de ADR-002 (mezclador determinista y testeable
sin el grafo del sistema).
"""

from __future__ import annotations

import wave
from pathlib import Path

import numpy as np
import pytest
from numpy.typing import NDArray

from client.audio.ducking import (
    DEFAULT_DUCK_LEVEL,
    DUCK_LEVEL_MAX,
    DUCK_LEVEL_MIN,
    DuckingMixer,
)
from client.audio.resampler import resample_to_48k
from shared.constants import PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE
from shared.protocol import Flags, decode_frame, encode_frame

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
RAMP_SAMPLES = round(PIPEWIRE_SAMPLE_RATE * 30 / 1000)  # 1440 muestras = 30 ms
MAX_DISCONTINUITY = 0.1
RMS_TOLERANCE = 0.01


def _read_wav(name: str) -> tuple[NDArray[np.float64], int]:
    """Lee un fixture WAV y devuelve PCM normalizado float64 y su sample rate."""
    with wave.open(str(FIXTURES_DIR / name), "rb") as handle:
        rate = handle.getframerate()
        raw = handle.readframes(handle.getnframes())
    pcm = np.frombuffer(raw, dtype="<i2").astype(np.float64) / 32768.0
    return pcm, rate


def _rms(signal: NDArray[np.float32] | NDArray[np.float64]) -> float:
    """RMS en unidades normalizadas [-1, 1]."""
    if signal.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(np.square(signal, dtype=np.float64))))


def _tone_48k() -> NDArray[np.float32]:
    pcm, rate = _read_wav("tone_1k_48k.wav")
    assert rate == PIPEWIRE_SAMPLE_RATE
    return pcm.astype(np.float32)


def _voice_48k() -> NDArray[np.float32]:
    pcm, rate = _read_wav("voice_es_16k.wav")
    assert rate == SAMPLE_RATE
    return resample_to_48k(pcm, src_rate=rate, dtype=np.float32)


def _silence_48k() -> NDArray[np.float32]:
    pcm, rate = _read_wav("silence_16k.wav")
    assert rate == SAMPLE_RATE
    return resample_to_48k(pcm, src_rate=rate, dtype=np.float32)


def _echo_48k() -> NDArray[np.float32]:
    pcm, rate = _read_wav("echo_48k.wav")
    assert rate == PIPEWIRE_SAMPLE_RATE
    return pcm.astype(np.float32)


def _duck_start_frame_flags() -> int:
    """Flags ``DUCK_START`` extraídos de un frame real del protocolo."""
    frame = encode_frame(
        stream_id=1,
        sequence=1,
        flags=int(Flags.DUCK_START),
        pcm=b"\x00\x00" * 320,
    )
    return decode_frame(frame).flags


def _duck_stop_frame_flags() -> int:
    """Flags ``DUCK_STOP`` extraídos de un frame real del protocolo."""
    frame = encode_frame(
        stream_id=1,
        sequence=2,
        flags=int(Flags.DUCK_STOP),
        pcm=b"\x00\x00" * 320,
    )
    return decode_frame(frame).flags


def test_default_duck_level_and_boundaries() -> None:
    """El default es 0.12 y los extremos 0.10/0.15 son legales."""
    assert DuckingMixer().duck_level == DEFAULT_DUCK_LEVEL == 0.12
    assert DuckingMixer(duck_level=DUCK_LEVEL_MIN).duck_level == 0.10
    assert DuckingMixer(duck_level=DUCK_LEVEL_MAX).duck_level == 0.15


@pytest.mark.parametrize("level", [0.09, 0.16, 0.0, 1.0, -0.12, float("nan"), True, "0.12"])
def test_duck_level_out_of_range_raises_value_error(level: object) -> None:
    """AC-08 / frontera: cualquier valor fuera de [0.10, 0.15] se rechaza."""
    with pytest.raises(ValueError):
        DuckingMixer(duck_level=level)  # type: ignore[arg-type]


def test_ac08_ducked_original_rms_is_duck_level() -> None:
    """AC-08: con DUCK_START el tono queda a 0.12 ± 0.01 de su RMS."""
    mixer = DuckingMixer()
    tone = _tone_48k()
    silence = _silence_48k()
    assert tone.size == silence.size == PIPEWIRE_SAMPLE_RATE

    mixer.apply_flags(_duck_start_frame_flags())
    assert mixer.ducking_active is True
    assert mixer.flags & int(Flags.DUCK_START)
    assert not mixer.flags & int(Flags.DUCK_STOP)
    assert mixer.target_gain == mixer.duck_level

    # La rampa de ataque se descarta: la medición es en régimen estacionario.
    mixer.process(tone[:RAMP_SAMPLES], silence[:RAMP_SAMPLES], ducking_active=True)
    assert mixer.gain == pytest.approx(mixer.duck_level, abs=1e-6)
    assert mixer.ramp_samples == RAMP_SAMPLES

    mixed = mixer.process(tone[RAMP_SAMPLES:], silence[RAMP_SAMPLES:], ducking_active=True)
    assert isinstance(mixed, np.ndarray)
    measured = _rms(mixed) / _rms(tone[RAMP_SAMPLES:])
    assert measured == pytest.approx(mixer.duck_level, abs=RMS_TOLERANCE)
    print(f"\nAC-08 duck RMS ratio = {measured:.4f} (objetivo {mixer.duck_level})")


def test_ac08_duck_stop_restores_full_level_within_100ms() -> None:
    """AC-08: con DUCK_STOP el original vuelve a 1.0 ± 0.01 en < 100 ms."""
    mixer = DuckingMixer()
    tone = _tone_48k()
    silence = _silence_48k()

    mixer.on_duck_start()
    mixer.process(tone[:RAMP_SAMPLES], silence[:RAMP_SAMPLES], ducking_active=True)
    assert mixer.gain == pytest.approx(0.12, abs=1e-6)

    mixer.apply_flags(_duck_stop_frame_flags())
    assert mixer.ducking_active is False
    assert mixer.flags & int(Flags.DUCK_STOP)
    assert not mixer.flags & int(Flags.DUCK_START)
    assert mixer.target_gain == 1.0
    assert mixer.ramp_samples < 0.100 * PIPEWIRE_SAMPLE_RATE  # < 100 ms

    # Un solo bloque de rampa (30 ms) basta para recuperar ganancia completa.
    release = mixer.process(
        tone[RAMP_SAMPLES : 2 * RAMP_SAMPLES],
        silence[RAMP_SAMPLES : 2 * RAMP_SAMPLES],
        ducking_active=False,
    )
    assert isinstance(release, np.ndarray)
    assert mixer.gain == pytest.approx(1.0, abs=1e-6)

    restored = mixer.process(
        tone[2 * RAMP_SAMPLES :], silence[2 * RAMP_SAMPLES :], ducking_active=False
    )
    assert isinstance(restored, np.ndarray)
    measured = _rms(restored) / _rms(tone[2 * RAMP_SAMPLES :])
    assert measured == pytest.approx(1.0, abs=RMS_TOLERANCE)
    print(f"\nAC-08 restore RMS ratio = {measured:.4f} tras {mixer.ramp_samples} muestras (30 ms)")


def test_tts_is_mixed_at_unity_gain() -> None:
    """El TTS se reproduce a ganancia 1.0: se preserva su RMS."""
    mixer = DuckingMixer()
    silence = _silence_48k()
    voice = _voice_48k()
    assert voice.size == 2 * PIPEWIRE_SAMPLE_RATE  # 2 s a 16 kHz → 2 s a 48 kHz

    mixed = mixer.process(silence[: voice.size], voice, ducking_active=True)
    assert isinstance(mixed, np.ndarray)
    measured = _rms(mixed) / _rms(voice)
    assert measured == pytest.approx(1.0, abs=RMS_TOLERANCE)
    print(f"\nTTS RMS ratio = {measured:.4f} (RMS TTS={_rms(voice):.6f})")


def test_no_clipping_on_loud_combination() -> None:
    """Tono + eco supera 1.0 antes del limitador: la salida nunca recorta."""
    mixer = DuckingMixer()
    tone = _tone_48k()
    echo = _echo_48k()[: tone.size]
    assert float(np.max(np.abs(tone + echo))) > 1.0  # la combinación excede FS

    mixed = mixer.process(tone, echo, ducking_active=False)
    assert isinstance(mixed, np.ndarray)
    assert mixed.dtype == np.float32
    peak = float(np.max(np.abs(mixed)))
    assert peak <= 1.0
    assert np.all(np.isfinite(mixed))
    print(f"\nPeak antes limitador = {float(np.max(np.abs(tone + echo))):.4f}; salida = {peak:.4f}")


def test_no_abrupt_jumps_during_attack_and_release() -> None:
    """La rampa de 30 ms evita saltos > 0.1 entre muestras consecutivas."""
    mixer = DuckingMixer()
    tone = _tone_48k()
    silence = _silence_48k()

    mixer.on_duck_start()
    attack = mixer.process(tone, silence, ducking_active=True)
    assert isinstance(attack, np.ndarray)
    attack_jump = float(np.max(np.abs(np.diff(attack))))
    assert attack_jump <= MAX_DISCONTINUITY

    mixer.on_duck_stop()
    release = mixer.process(tone, silence, ducking_active=False)
    assert isinstance(release, np.ndarray)
    release_jump = float(np.max(np.abs(np.diff(release))))
    assert release_jump <= MAX_DISCONTINUITY
    print(f"\nSalto máx ataque = {attack_jump:.4f}; liberación = {release_jump:.4f}")


def test_silence_original_yields_only_tts() -> None:
    """Si el original es silencio, la mezcla es exactamente el TTS."""
    mixer = DuckingMixer()
    silence = _silence_48k()
    voice = _voice_48k()[: silence.size]

    mixed = mixer.process(silence, voice, ducking_active=True)
    assert isinstance(mixed, np.ndarray)
    np.testing.assert_allclose(mixed, voice, atol=1e-6, rtol=0.0)
    assert _rms(mixed) > 0.0


def test_protocol_flags_control_ducking_state() -> None:
    """DUCK_START / DUCK_STOP de shared.protocol cambian el estado correcto."""
    mixer = DuckingMixer()
    assert mixer.flags == 0
    assert mixer.ducking_active is False
    assert mixer.gain == 1.0

    mixer.apply_flags(int(Flags.DUCK_START) | int(Flags.PTT_ACTIVE))
    assert mixer.ducking_active is True
    assert mixer.flags == int(Flags.DUCK_START)  # PTT no afecta al estado de ducking
    assert mixer.target_gain == 0.12

    mixer.apply_flags(int(Flags.DUCK_START) | int(Flags.DUCK_STOP))
    assert mixer.ducking_active is False  # STOP prevalece por seguridad
    assert mixer.flags == int(Flags.DUCK_STOP)
    assert mixer.target_gain == 1.0

    # El argumento de process sincroniza los flags del protocolo.
    mixer.process(_tone_48k()[:RAMP_SAMPLES], _silence_48k()[:RAMP_SAMPLES], ducking_active=True)
    assert mixer.ducking_active is True
    assert mixer.flags & int(Flags.DUCK_START)

    mixer.reset()
    assert mixer.flags == 0
    assert mixer.ducking_active is False
    assert mixer.gain == 1.0
    assert mixer.target_gain == 1.0


@pytest.mark.parametrize("bad_flags", [-1, 256, 1.5, True, "duck"])
def test_apply_flags_rejects_invalid_values(bad_flags: object) -> None:
    """La frontera de flags también valida su rango uint8."""
    with pytest.raises(ValueError):
        DuckingMixer().apply_flags(bad_flags)  # type: ignore[arg-type]


def test_bytes_io_returns_int16_le_bytes() -> None:
    """PCM bytes a 48 kHz entra y sale como bytes int16 LE para PipeWire."""
    mixer = DuckingMixer()
    tone = _tone_48k()
    silence = _silence_48k()
    mixer.on_duck_start()
    mixer.process(tone[:RAMP_SAMPLES], silence[:RAMP_SAMPLES], ducking_active=True)

    original_bytes = (tone[RAMP_SAMPLES:] * 32768.0).astype("<i2").tobytes()
    tts_bytes = (silence[RAMP_SAMPLES:] * 32768.0).astype("<i2").tobytes()
    out = mixer.process(original_bytes, tts_bytes, ducking_active=True)
    assert isinstance(out, bytes)
    assert len(out) == len(original_bytes)
    pcm = np.frombuffer(out, dtype="<i2").astype(np.float64) / 32768.0
    measured = _rms(pcm) / _rms(tone[RAMP_SAMPLES:])
    assert measured == pytest.approx(mixer.duck_level, abs=RMS_TOLERANCE)


def test_short_tts_is_zero_padded() -> None:
    """Bloques de distinta longitud se alinean con ceros sin perder muestras."""
    mixer = DuckingMixer()
    tone = _tone_48k()
    short_tts = np.zeros(RAMP_SAMPLES, dtype=np.float32)
    mixed = mixer.process(tone, short_tts, ducking_active=False)
    assert isinstance(mixed, np.ndarray)
    assert mixed.size == tone.size
    np.testing.assert_allclose(mixed[RAMP_SAMPLES:], tone[RAMP_SAMPLES:], atol=1e-6, rtol=0.0)
