"""E2E del modo vídeo (TASK-017 / AC-08 / AC-09), sin hardware ni red externa.

* :class:`VideoMockTransport` es un :class:`~client.audio.transport.MockAudioTransport`
  que expone un programa PCM determinista (3 kHz para medir el ducking por banda
  estrecha) a través de la lectura de monitor y registra todo lo escrito en el
  sink físico. El TTS del mock está en 180-460 Hz, por lo que la banda de 3 kHz
  permite medir la ganancia del original sin contaminación.
* :class:`~server.pipeline.mock.MockInferenceServer` cierra la cláusula por VAD
  y devuelve TTS determinista; se fijan las frases del mock al valor canónico
  del AC-08 (“Hello, how are you?” → “Hola, ¿cómo estás?”) solo dentro del test.
* :class:`FakeGraphRunner` simula ``pw-dump``/``pactl`` en memoria. Cualquier
  ``subprocess`` real hace fallar el test (ver ``forbid_real_commands``).
"""

from __future__ import annotations

import asyncio
import math
import subprocess
import wave
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

import numpy as np
import pytest
from numpy.typing import NDArray

from client.audio.pipewire_graph import CommandResult, PipeWireGraph
from client.audio.resampler import SampleInput, resample_to_48k
from client.audio.transport import MockAudioTransport
from client.main import (
    SubtitleEvent,
    VideoAudioSettings,
    VideoModeSettings,
    VideoPipelineSettings,
    run_video_mode,
)
from client.net.ws_client import WSClient
from server.pipeline.mock import MockInferenceServer, MockSTT, MockVAD
from shared.constants import PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
TIMEOUT = 10.0
CAPTURE_SAMPLES = 960  # 20 ms @48 kHz
TONE_HZ = 3000.0
TONE_AMPLITUDE = 8000.0
BED_AMPLITUDE = 140.0
"""Lecho de 3 kHz por debajo del umbral del VAD (RMS ~99 LSB < 120)."""
SECRET = "babelpipe-token-task-017-no-logear"
REFERENCE_CHUNK = 5
MUSIC_CHUNKS = 250  # 5 s @48 kHz / 960 muestras por chunk
VAD_CLOSE_CHUNKS = 13  # ceil(250 ms / 20 ms): cierre del VAD mock


def _read_wav(name: str, expected_rate: int) -> NDArray[np.int16]:
    path = FIXTURES / name
    assert path.is_file(), f"falta el fixture {path}"
    with wave.open(str(path), "rb") as wav:
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == expected_rate
        raw = wav.readframes(wav.getnframes())
    return np.frombuffer(raw, dtype="<i2").copy()


def _tone(duration_s: float, amplitude: float = TONE_AMPLITUDE) -> NDArray[np.int16]:
    """Tono de 3 kHz: banda separada del TTS determinista del mock."""
    count = round(PIPEWIRE_SAMPLE_RATE * duration_s)
    phase = np.arange(count, dtype=np.float64) / PIPEWIRE_SAMPLE_RATE
    return np.rint(amplitude * np.sin(2.0 * math.pi * TONE_HZ * phase)).astype(np.int16)


def _rms(samples: NDArray[np.int16] | bytes) -> float:
    values = (
        np.frombuffer(samples, dtype="<i2").astype(np.float64)
        if isinstance(samples, bytes)
        else samples.astype(np.float64)
    )
    if values.size == 0:
        return 0.0
    return float(math.sqrt(float(np.mean(np.square(values)))))


def _band_rms(pcm: bytes, target_hz: float) -> float:
    """RMS de la componente `target_hz` (FFT con ventana Hann)."""
    samples = np.frombuffer(pcm, dtype="<i2").astype(np.float64)
    window = np.hanning(samples.size)
    spectrum = np.fft.rfft(samples * window)
    resolution = PIPEWIRE_SAMPLE_RATE / samples.size
    index = int(round(target_hz / resolution))
    amplitude = 2.0 * float(abs(spectrum[index])) / float(window.sum())
    return amplitude / math.sqrt(2.0)


def _low_band_rms(pcm: bytes, cutoff_hz: float = 1000.0) -> float:
    """RMS agregado de las componentes por debajo de `cutoff_hz`."""
    samples = np.frombuffer(pcm, dtype="<i2").astype(np.float64)
    window = np.hanning(samples.size)
    spectrum = np.fft.rfft(samples * window)
    resolution = PIPEWIRE_SAMPLE_RATE / samples.size
    last = max(1, int(math.floor(cutoff_hz / resolution)))
    energy = float(np.sum(np.square(np.abs(spectrum[1 : last + 1]))))
    return math.sqrt(energy) * 2.0 / (float(window.sum()) * math.sqrt(2.0))


class VideoMockTransport(MockAudioTransport):
    """MockAudioTransport con programa determinista en el monitor del Rx."""

    def __init__(self, program: NDArray[np.int16]) -> None:
        super().__init__(
            near_end=np.empty(0, dtype=np.int16),
            far_end=np.empty(0, dtype=np.int16),
            tail_samples=0,
        )
        self.program = program
        self._offset = 0
        self.sink_writes: list[bytes] = []

    def start(self) -> None:
        super().start()
        self._offset = 0
        self.sink_writes.clear()

    def read_physical_sink_monitor_48k(self, num_samples: int) -> NDArray[np.int16]:
        """Devuelve el programa y, agotado, silencio para drenar el pipeline."""
        if isinstance(num_samples, bool) or not isinstance(num_samples, int):
            raise ValueError("num_samples debe ser int")
        if not 0 < num_samples <= PIPEWIRE_SAMPLE_RATE:
            raise ValueError(f"num_samples fuera de rango: {num_samples}")
        count = min(num_samples, max(0, self.program.size - self._offset))
        chunk = self.program[self._offset : self._offset + count]
        self._offset += count
        if chunk.size < num_samples:
            chunk = np.pad(chunk, (0, num_samples - chunk.size))
        return chunk.astype(np.int16, copy=True)

    def write_physical_sink_48k(self, samples: SampleInput) -> int:
        """Conserva el PCM real escrito para medir RMS y presencia de TTS."""
        pcm = resample_to_48k(samples, src_rate=PIPEWIRE_SAMPLE_RATE, dtype=np.int16)
        self.sink_writes.append(pcm.tobytes())
        self.sink_written_samples += int(pcm.size)
        return int(pcm.size)


class FakeGraphRunner:
    """Simula ``pw-dump`` y ``pactl`` sin ejecutar ningún comando real."""

    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []

    def run(self, args: Sequence[str]) -> CommandResult:
        argv = tuple(args)
        self.commands.append(argv)
        assert all("sudo" not in item and item not in ("sh", "bash") for item in argv)
        if argv[0] == "pw-dump":
            return CommandResult(stdout="[]")
        if argv[:2] == ("pactl", "load-module"):
            return CommandResult(stdout="42\n")
        if argv[:2] == ("pactl", "unload-module"):
            return CommandResult()
        raise AssertionError(f"comando inesperado: {argv}")


@pytest.fixture(autouse=True)
def forbid_real_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """Red de seguridad: cualquier subprocess real hace fallar el test."""

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"Se intentó ejecutar un comando real: {args} {kwargs}")

    monkeypatch.setattr(subprocess, "run", fail)
    monkeypatch.setattr(subprocess, "Popen", fail)


async def _wait_for(predicate: Callable[[], bool], timeout: float = TIMEOUT) -> None:
    """Espera cooperativa con deadline en el reloj del event loop."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("timeout esperando al pipeline de modo vídeo")
        await asyncio.sleep(0.002)


async def _assert_no_hanging_tasks() -> None:
    """Comprueba que no quede ninguna tarea del pipeline viva."""
    for _ in range(20):
        await asyncio.sleep(0)
        alive = [
            task.get_name()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith(
                ("babelpipe-video-", "babelpipe-ws-", "babelpipe-rx-", "mock-ws-", "mock-pipeline")
            )
        ]
        if not alive:
            return
    assert alive == [], f"tareas colgadas: {alive}"


def _settings(*, ramp_ms: float = 5.0) -> VideoModeSettings:
    return VideoModeSettings(
        audio=VideoAudioSettings(duck_level=0.12, ramp_ms=ramp_ms),
        pipeline=VideoPipelineSettings(source_lang="en", target_lang="es"),
        create_rx_sink=True,
        queue_maxsize=4096,  # sin descartes: índice de render == índice del programa
    )


def _client(server: MockInferenceServer) -> WSClient:
    return WSClient(
        host="127.0.0.1",
        port=server.port,
        token=SECRET,
        mode="rx",
        source_lang="en",
        target_lang="es",
        queue_maxsize=4096,
        ping_interval_s=None,
    )


@pytest.mark.asyncio
async def test_ac08_youtube_dubbing(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-08: voz en `en` → TTS en `es`, original al 12 % y subtítulo final."""
    monkeypatch.setattr(MockSTT, "PHRASES", ("Hello, how are you?",))
    server = MockInferenceServer(min_silence_ms=250, queue_maxsize=4096)
    await server.start()

    # Lecho inicial de 0.4 s (referencia al 100 %) + 2 s de "voz" + pausa de
    # 250 ms redondeada al chunk canónico (13 chunks = 260 ms) + 0.8 s de lecho
    # para medir la restauración. El lecho está por debajo del umbral del VAD,
    # así que el pipeline lo trata como silencio pero el sink lo reproduce.
    program = np.concatenate(
        (
            _tone(0.4, BED_AMPLITUDE),
            _tone(2.0),
            _tone(0.3, BED_AMPLITUDE),
            _tone(0.8, BED_AMPLITUDE),
        )
    )
    transport = VideoMockTransport(program)
    runner = FakeGraphRunner()
    graph = PipeWireGraph(runner, register_handlers=False)
    client = _client(server)
    subtitles: list[SubtitleEvent] = []
    mixed: list[tuple[int, bytes, bool]] = []

    def on_mixed(pcm: bytes, index: int, had_tts: bool) -> None:
        mixed.append((index, pcm, had_tts))

    orchestrator = run_video_mode(
        _settings(),
        transport,
        client,
        on_subtitle=subtitles.append,
        on_mixed=on_mixed,
        graph=graph,
    )
    task = asyncio.create_task(orchestrator.run())
    try:
        await client.connect()
        await _wait_for(
            lambda: orchestrator.metrics.duck_starts >= 1
            and orchestrator.metrics.duck_stops >= 1
            and orchestrator.metrics.tts_rendered_chunks >= 2
        )
        stop_render = orchestrator.metrics.rendered_chunks
        await _wait_for(lambda: orchestrator.metrics.rendered_chunks >= stop_render + 20)
    finally:
        orchestrator.stop()
        await asyncio.wait_for(task, TIMEOUT)
        await client.close()
        orchestrator.cleanup()
        await server.stop()
    await _assert_no_hanging_tasks()

    assert any(
        event.kind == "subtitle"
        and event.is_final
        and event.text == "Hello, how are you?"
        and event.lang == "en"
        for event in subtitles
    )
    assert any(
        event.kind == "translated"
        and event.is_final
        and event.text == "Hola, ¿cómo estás?"
        and event.lang == "es"
        for event in subtitles
    )
    assert orchestrator.metrics.tts_chunks >= 2
    assert orchestrator.metrics.duck_starts == 1
    assert orchestrator.metrics.duck_stops == 1
    assert ("pactl", "unload-module", "42") in runner.commands

    tts_chunks = [item for item in mixed if item[2]]
    assert len(tts_chunks) >= 2

    def original_band(index: int) -> float:
        """RMS de 3 kHz del original en el mismo chunk de renderización."""
        start = index * CAPTURE_SAMPLES
        block = program[start : start + CAPTURE_SAMPLES]
        return _band_rms(block.tobytes(), TONE_HZ)

    def measured_ratio(item: tuple[int, bytes, bool]) -> float:
        expected = original_band(item[0])
        if expected <= 1.0:
            return 0.0
        return _band_rms(item[1], TONE_HZ) / expected

    ducked_items = [item for item in tts_chunks if original_band(item[0]) > 1.0]
    assert ducked_items, "no había original medible durante el doblaje"
    duck_ratio = measured_ratio(ducked_items[-1])
    restore_ratio = measured_ratio(mixed[-1])
    tts_low = _low_band_rms(ducked_items[-1][1])
    reference_low = _low_band_rms(mixed[REFERENCE_CHUNK][1])
    assert tts_low > reference_low * 5.0, "el sink no contiene TTS en español"

    latency_ms = orchestrator.metrics.tts_latency_ms
    print(
        f"\n[AC-08] RMS ducking={duck_ratio:.4f} (objetivo 0.12), "
        f"RMS restaurado={restore_ratio:.4f}, latencia={latency_ms:.1f} ms, "
        f"TTS low-band={tts_low:.1f} vs ref={reference_low:.1f} LSB"
    )
    assert 0.10 <= duck_ratio <= 0.15, f"ducking fuera de rango: {duck_ratio:.4f}"
    assert abs(restore_ratio - 1.0) <= 0.02, f"original no restaurado: {restore_ratio:.4f}"
    assert orchestrator.mixer.gain == pytest.approx(1.0, abs=1e-6)
    assert latency_ms < 1500.0, f"latencia excedida: {latency_ms:.1f} ms"
    # El último callback `on_mixed` puede cancelarse con el stop: se admite un
    # desfase de una muestra frente a las escrituras reales del sink.
    assert len(transport.sink_writes) >= len(mixed)
    assert len(transport.sink_writes) - len(mixed) <= 1


@pytest.mark.asyncio
async def test_ac09_music_no_false_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-09: 5 s de ruido → 0 subtítulos, 0 ducking y original al 100 %."""
    # El VAD determinista del mock es por energía; se configura su umbral por
    # encima del pico del fixture de ruido (22937 LSB) para modelar un detector
    # de no-voz. Lo que se verifica es el comportamiento del orquestador ante
    # cero eventos: no publica nada, no atenúa y copia el original íntegro.
    monkeypatch.setattr(MockVAD, "SPEECH_ON_RMS", 30_000.0)
    monkeypatch.setattr(MockVAD, "SPEECH_OFF_RMS", 25_000.0)
    server = MockInferenceServer(min_silence_ms=250, queue_maxsize=4096)
    await server.start()

    noise = _read_wav("noise_16k.wav", SAMPLE_RATE)
    program = cast(
        "NDArray[np.int16]",
        resample_to_48k(np.tile(noise, 5), src_rate=SAMPLE_RATE, dtype=np.int16),
    )
    assert program.size == MUSIC_CHUNKS * CAPTURE_SAMPLES
    transport = VideoMockTransport(program)
    runner = FakeGraphRunner()
    graph = PipeWireGraph(runner, register_handlers=False)
    client = _client(server)
    subtitles: list[SubtitleEvent] = []

    orchestrator = run_video_mode(
        _settings(),
        transport,
        client,
        on_subtitle=subtitles.append,
        graph=graph,
    )
    task = asyncio.create_task(orchestrator.run())
    try:
        await client.connect()
        await _wait_for(lambda: orchestrator.metrics.rendered_chunks >= MUSIC_CHUNKS)
    finally:
        orchestrator.stop()
        await asyncio.wait_for(task, TIMEOUT)
        await client.close()
        orchestrator.cleanup()
        await server.stop()
    await _assert_no_hanging_tasks()

    assert orchestrator.metrics.chunks >= MUSIC_CHUNKS
    assert orchestrator.metrics.subtitles == 0
    assert orchestrator.metrics.translated == 0
    assert orchestrator.metrics.tts_chunks == 0
    assert orchestrator.metrics.duck_starts == 0
    assert orchestrator.metrics.duck_stops == 0
    assert subtitles == []
    assert server.stats["subtitles_final"] == 0
    assert server.stats["tts_chunks"] == 0
    assert orchestrator.mixer.gain == pytest.approx(1.0, abs=1e-6)

    input_rms = _rms(program)
    output_rms = _rms(b"".join(transport.sink_writes))
    ratio = output_rms / input_rms
    print(
        f"\n[AC-09] RMS original={input_rms:.1f} LSB, sink={output_rms:.1f} LSB, "
        f"ratio={ratio:.4f}, subtítulos=0, ducking=0"
    )
    assert abs(ratio - 1.0) <= 0.02, f"original no quedó al 100 %: {ratio:.4f}"
    assert len(transport.sink_writes) >= MUSIC_CHUNKS
