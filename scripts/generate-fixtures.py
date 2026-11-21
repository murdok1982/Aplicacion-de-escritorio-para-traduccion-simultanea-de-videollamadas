#!/usr/bin/env python3
"""Generador determinista de fixtures WAV para BabelPipe.

Produce los siete WAV mono int16 LE usados por los tests de audio sin hardware.
No descarga nada: toda la senal se sintetiza con numpy y se escribe con el
modulo ``wave`` de la biblioteca estandar.

Uso:
    uv run python scripts/generate-fixtures.py            # generar
    uv run python scripts/generate-fixtures.py --verify   # validar los WAV
"""

from __future__ import annotations

import argparse
import sys
import wave
from dataclasses import dataclass
from pathlib import Path

import numpy as np

SAMPLE_RATE_16K = 16_000
SAMPLE_RATE_48K = 48_000
TONE_FREQ_HZ = 1_000.0
TONE_AMPLITUDE = 0.70
NOISE_AMPLITUDE = 0.70
VOICE_AMPLITUDE = 0.60
NOISE_SEED = 42
VOICE_SEED = 42

CLAUSE_DURATION_S = 2.0
GAP_DURATION_S = 0.400
ECHO_DELAY_S = 0.120
ECHO_ATTENUATION = 0.35
ECHO_DURATION_S = CLAUSE_DURATION_S + ECHO_DELAY_S

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures"

INT16_FULL_SCALE = 32767.0
RMS_SILENCE_THRESHOLD = 200.0  # LSB int16 (~ -44 dBFS)

# (F1, F2, F3) en Hz para cinco vocales; bases formanticas de la sintesis.
VOWEL_FORMANTS: tuple[tuple[float, float, float], ...] = (
    (730.0, 1090.0, 2440.0),  # /a/
    (530.0, 1840.0, 2480.0),  # /e/
    (270.0, 2290.0, 3010.0),  # /i/
    (570.0, 840.0, 2410.0),  # /o/
    (300.0, 870.0, 2240.0),  # /u/
)
FORMANT_BANDWIDTHS = (80.0, 110.0, 160.0)
FORMANT_GAINS = (1.00, 0.70, 0.40)
SYLLABLE_DURATION_S = 0.180
SYLLABLE_PAUSE_S = 0.040
SYLLABLE_ATTACK_S = 0.015
SYLLABLE_RELEASE_S = 0.045
VIBRATO_HZ = 5.0
HARMONIC_CEILING_HZ = 5000.0


@dataclass(frozen=True)
class FixtureSpec:
    """Contrato verificable de un fixture WAV."""

    filename: str
    sample_rate: int
    duration_s: float
    kind: str
    tone_hz: float | None = None


FIXTURES: tuple[FixtureSpec, ...] = (
    FixtureSpec("tone_1k_48k.wav", SAMPLE_RATE_48K, 1.0, "tone", TONE_FREQ_HZ),
    FixtureSpec("tone_1k_16k.wav", SAMPLE_RATE_16K, 1.0, "tone", TONE_FREQ_HZ),
    FixtureSpec("silence_16k.wav", SAMPLE_RATE_16K, 1.0, "silence"),
    FixtureSpec("noise_16k.wav", SAMPLE_RATE_16K, 1.0, "noise"),
    FixtureSpec("voice_es_16k.wav", SAMPLE_RATE_16K, CLAUSE_DURATION_S, "voice"),
    FixtureSpec(
        "two_clauses_16k.wav",
        SAMPLE_RATE_16K,
        2.0 * CLAUSE_DURATION_S + GAP_DURATION_S,
        "two_clauses",
    ),
    FixtureSpec("echo_48k.wav", SAMPLE_RATE_48K, ECHO_DURATION_S, "echo"),
)


def frame_count(sample_rate: int, duration_s: float) -> int:
    """Numero entero de muestras que corresponde a una duracion."""
    return round(sample_rate * duration_s)


def synthesize_tone(sample_rate: int, duration_s: float) -> np.ndarray:
    """Tono senoidal puro a ``TONE_FREQ_HZ`` con amplitud normalizada."""
    n = frame_count(sample_rate, duration_s)
    t = np.arange(n, dtype=np.float64) / sample_rate
    return TONE_AMPLITUDE * np.sin(2.0 * np.pi * TONE_FREQ_HZ * t)


def synthesize_noise(sample_rate: int, duration_s: float, seed: int = NOISE_SEED) -> np.ndarray:
    """Ruido blanco gaussiano determinista (semilla por defecto 42)."""
    n = frame_count(sample_rate, duration_s)
    rng = np.random.default_rng(seed)
    raw = rng.standard_normal(n)
    peak = float(np.max(np.abs(raw)))
    return (NOISE_AMPLITUDE / peak) * raw if peak > 0.0 else raw


def _glottal_source(
    duration_s: float, sample_rate: int, f0_hz: float, rng: np.random.Generator
) -> np.ndarray:
    """Fuente harmonica con vibrato y fases iniciales deterministas."""
    n = frame_count(sample_rate, duration_s)
    t = np.arange(n, dtype=np.float64) / sample_rate
    vibrato = 1.0 + 0.015 * np.sin(2.0 * np.pi * VIBRATO_HZ * t + rng.uniform(0.0, 2.0 * np.pi))
    phase = 2.0 * np.pi * f0_hz * np.cumsum(vibrato) / sample_rate
    source = np.zeros(n, dtype=np.float64)
    harmonics = max(1, int(HARMONIC_CEILING_HZ / f0_hz))
    for h in range(1, harmonics + 1):
        source += (1.0 / h**1.1) * np.sin(h * phase + rng.uniform(0.0, 2.0 * np.pi))
    return source


def _syllable_envelope(duration_s: float, sample_rate: int) -> np.ndarray:
    """Ataque corto, sustain con leve decaimiento y liberacion suave."""
    n = frame_count(sample_rate, duration_s)
    t = np.arange(n, dtype=np.float64) / sample_rate
    attack = np.clip(t / SYLLABLE_ATTACK_S, 0.0, 1.0)
    release = np.clip((duration_s - t) / SYLLABLE_RELEASE_S, 0.0, 1.0)
    decay = 1.0 - 0.25 * (t / duration_s)
    return attack * release * decay


def _apply_formants(
    source: np.ndarray, sample_rate: int, formants: tuple[float, ...]
) -> np.ndarray:
    """Colora la fuente con tres picos espectrales (formantes vocalicos)."""
    n = source.size
    freqs = np.fft.rfftfreq(n, 1.0 / sample_rate)
    gains = np.full(freqs.shape, 0.05, dtype=np.float64)
    for center_hz, bandwidth_hz, gain in zip(
        formants, FORMANT_BANDWIDTHS, FORMANT_GAINS, strict=True
    ):
        gains += gain * np.exp(-0.5 * ((freqs - center_hz) / bandwidth_hz) ** 2)
    return np.fft.irfft(np.fft.rfft(source) * gains, n=n)


def synthesize_voice(
    sample_rate: int, duration_s: float, seed: int = VOICE_SEED
) -> np.ndarray:
    """Senal sintetica parecida a voz: harmonicos + formantes + silabas.

    No usa TTS externo ni modelos: cada silaba es una fuente glotal con
    vibrato, filtrada por formantes y multiplicada por una envolvente con
    ataque/sostenimiento/liberacion, separada por pausas breves.
    """
    rng = np.random.default_rng(seed)
    total = frame_count(sample_rate, duration_s)
    signal = np.zeros(total, dtype=np.float64)
    total_s = total / sample_rate
    syllable_nominal_s = min(SYLLABLE_DURATION_S, total_s)
    syllable_count = max(1, round(total_s / (SYLLABLE_DURATION_S + SYLLABLE_PAUSE_S)))
    step_s = (total_s - syllable_nominal_s) / (syllable_count - 1) if syllable_count > 1 else 0.0
    syllable_nominal = frame_count(sample_rate, syllable_nominal_s)

    for syllable_index in range(syllable_count):
        position = frame_count(sample_rate, syllable_index * step_s)
        syllable_n = min(syllable_nominal, total - position)
        syllable_s = syllable_n / sample_rate
        formants = VOWEL_FORMANTS[syllable_index % len(VOWEL_FORMANTS)]
        f0_hz = 110.0 + 12.0 * np.sin(syllable_index * 0.9) + rng.uniform(-4.0, 4.0)
        source = _glottal_source(syllable_s, sample_rate, f0_hz, rng)
        source *= _syllable_envelope(syllable_s, sample_rate)
        signal[position : position + syllable_n] += _apply_formants(source, sample_rate, formants)

    peak = float(np.max(np.abs(signal)))
    return (VOICE_AMPLITUDE / peak) * signal if peak > 0.0 else signal


def synthesize_two_clauses(sample_rate: int) -> np.ndarray:
    """Dos rafagas identicas de voz separadas por 400 ms de silencio."""
    clause = synthesize_voice(sample_rate, CLAUSE_DURATION_S)
    gap = np.zeros(frame_count(sample_rate, GAP_DURATION_S), dtype=np.float64)
    return np.concatenate((clause, gap, clause))


def synthesize_echo(sample_rate: int) -> np.ndarray:
    """Voz a 48 kHz mas una copia atenuada 120 ms mas tarde (AEC)."""
    voice = synthesize_voice(sample_rate, CLAUSE_DURATION_S)
    delay = frame_count(sample_rate, ECHO_DELAY_S)
    result = np.zeros(voice.size + delay, dtype=np.float64)
    result[: voice.size] += voice
    result[delay:] += ECHO_ATTENUATION * voice
    return result


def generate_fixture(spec: FixtureSpec, fixtures_dir: Path = FIXTURES_DIR) -> Path:
    """Genera un fixture y devuelve la ruta del WAV escrito."""
    if spec.kind == "tone":
        signal = synthesize_tone(spec.sample_rate, spec.duration_s)
    elif spec.kind == "silence":
        signal = np.zeros(frame_count(spec.sample_rate, spec.duration_s), dtype=np.float64)
    elif spec.kind == "noise":
        signal = synthesize_noise(spec.sample_rate, spec.duration_s)
    elif spec.kind == "voice":
        signal = synthesize_voice(spec.sample_rate, spec.duration_s)
    elif spec.kind == "two_clauses":
        signal = synthesize_two_clauses(spec.sample_rate)
    elif spec.kind == "echo":
        signal = synthesize_echo(spec.sample_rate)
    else:
        raise ValueError(f"Tipo de fixture desconocido: {spec.kind!r}")

    return write_wav(fixtures_dir / spec.filename, signal, spec.sample_rate)


def write_wav(path: Path, signal: np.ndarray, sample_rate: int) -> Path:
    """Escribe PCM int16 LE mono con el modulo ``wave`` (sin dependencias extra)."""
    pcm = np.clip(np.rint(signal * INT16_FULL_SCALE), -32768, 32767).astype("<i2", copy=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(pcm.tobytes())
    return path


def read_wav(path: Path) -> tuple[np.ndarray, tuple[int, int, int, int]]:
    """Lee un WAV y devuelve (pcm int16 LE, (canales, bytes/muestra, rate, frames))."""
    with wave.open(str(path), "rb") as handle:
        metadata = (
            handle.getnchannels(),
            handle.getsampwidth(),
            handle.getframerate(),
            handle.getnframes(),
        )
        raw = handle.readframes(metadata[3])
    return np.frombuffer(raw, dtype="<i2").copy(), metadata


def rms(signal: np.ndarray) -> float:
    """RMS en unidades LSB int16 (0.0 para silencio)."""
    if signal.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(np.square(signal, dtype=np.float64))))


def dominant_frequency(pcm: np.ndarray, sample_rate: int) -> float:
    """Frecuencia dominante por FFT de la senal completa (1 s de resolucion)."""
    samples = pcm.astype(np.float64) / 32768.0
    spectrum = np.abs(np.fft.rfft(samples))
    freqs = np.fft.rfftfreq(samples.size, 1.0 / sample_rate)
    return float(freqs[int(np.argmax(spectrum))])


def verify(fixtures_dir: Path = FIXTURES_DIR) -> bool:
    """Valida formato, duracion y contenido de todos los fixtures. Devuelve exito."""
    print(f"Verificando fixtures en {fixtures_dir}")
    print(f"{'fixture':24s} {'rate':>7s} {'ch':>3s} {'dtype':>7s} {'dur(s)':>7s}  contenido")
    print("-" * 82)
    all_ok = True
    passed = 0

    for spec in FIXTURES:
        path = fixtures_dir / spec.filename
        problems: list[str] = []

        if not path.is_file():
            print(f"{spec.filename:24s} {'--':>7s} {'--':>3s} {'--':>7s} {'--':>7s}  FALTA")
            all_ok = False
            continue

        pcm, (channels, sample_width, rate, frames) = read_wav(path)
        expected_frames = frame_count(spec.sample_rate, spec.duration_s)
        if channels != 1:
            problems.append(f"canales={channels}")
        if sample_width != 2:
            problems.append(f"sampwidth={sample_width}")
        if rate != spec.sample_rate:
            problems.append(f"rate={rate}")
        if frames != expected_frames or pcm.size != expected_frames:
            problems.append(f"frames={frames}")

        content = spec.kind
        if spec.tone_hz is not None and not problems:
            measured = dominant_frequency(pcm, rate)
            if abs(measured - spec.tone_hz) > 20.0:
                problems.append(f"f0={measured:.1f}Hz")
            content = f"tono {measured:.1f} Hz"

        if spec.kind == "silence" and not problems and rms(pcm) > 0.0:
            problems.append("silencio no nulo")

        if spec.kind == "noise" and not problems:
            expected = synthesize_noise(spec.sample_rate, spec.duration_s)
            expected_pcm = np.rint(expected * INT16_FULL_SCALE).astype("<i2")
            if not np.array_equal(pcm, expected_pcm):
                problems.append("ruido no determinista")
            content = "ruido seed=42"

        if spec.kind == "voice" and not problems:
            active = _active_ratio(pcm, rate)
            if active < 0.30 or active > 0.95:
                problems.append(f"actividad={active:.2f}")
            content = f"voz VAD={active:.0%}"

        if spec.kind == "two_clauses" and not problems:
            gap_start = frame_count(rate, CLAUSE_DURATION_S)
            gap_end = gap_start + frame_count(rate, GAP_DURATION_S)
            gap_rms = rms(pcm[gap_start:gap_end])
            before = rms(pcm[:gap_start])
            after = rms(pcm[gap_end:])
            if gap_rms > RMS_SILENCE_THRESHOLD or min(before, after) < 1000.0:
                problems.append("gap/clausulas invalidos")
            content = f"gap {GAP_DURATION_S * 1000:.0f} ms (RMS={gap_rms:.1f})"

        if spec.kind == "echo" and not problems:
            voice = synthesize_voice(rate, CLAUSE_DURATION_S)
            delay = frame_count(rate, ECHO_DELAY_S)
            residual = pcm[: voice.size].astype(np.float64) / INT16_FULL_SCALE - voice
            expected_tail = ECHO_ATTENUATION * voice[: voice.size - delay]
            tail_error = float(
                np.max(np.abs(residual[delay:] - expected_tail))
            )
            if tail_error > 2e-4:
                problems.append(f"eco error={tail_error:.4f}")
            content = f"eco {ECHO_DELAY_S * 1000:.0f} ms x{ECHO_ATTENUATION}"

        duration = frames / rate if rate else float("nan")
        status = "OK" if not problems else "ERROR: " + ", ".join(problems)
        print(
            f"{spec.filename:24s} {rate:7d} {channels:3d} {'int16':>7s} "
            f"{duration:7.3f}  {status} ({content})"
        )
        if problems:
            all_ok = False
        else:
            passed += 1

    print("-" * 82)
    print(f"Resumen: {passed}/{len(FIXTURES)} fixtures validos")
    print("Resultado: " + ("TODOS LOS FIXTURES VALIDOS" if all_ok else "FALLOS DETECTADOS"))
    return all_ok


def _active_ratio(pcm: np.ndarray, sample_rate: int) -> float:
    """Fraccion de ventanas de 20 ms con energia por encima del umbral de silencio."""
    window = frame_count(sample_rate, 0.020)
    count = 0
    windows = 0
    for start in range(0, pcm.size - window + 1, window):
        windows += 1
        if rms(pcm[start : start + window]) > RMS_SILENCE_THRESHOLD:
            count += 1
    return count / windows if windows else 0.0


def main(argv: list[str] | None = None) -> int:
    """Punto de entrada CLI: genera los fixtures o verifica los existentes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="validar los WAV existentes en lugar de regenerarlos",
    )
    args = parser.parse_args(argv)

    if args.verify:
        return 0 if verify() else 1

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    for spec in FIXTURES:
        path = generate_fixture(spec)
        pcm, metadata = read_wav(path)
        print(
            f"Generado {path.relative_to(FIXTURES_DIR.parent.parent)}: "
            f"{metadata[2]} Hz, mono int16, {metadata[3] / metadata[2]:.3f} s, "
            f"{path.stat().st_size} bytes, RMS={rms(pcm):.1f}"
        )
    print(f"{len(FIXTURES)} fixtures listos en {FIXTURES_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
