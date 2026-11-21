"""E2E de llamada bidireccional completa (TASK-020 / AC-10).

Sin hardware, sin red externa y sin ejecutar un solo comando real:

* :class:`~client.audio.transport.MockAudioTransport` (subclase que registra el
  sink físico sin realimentar su monitor artificial) reproduce el grafo: voz
  cercana en español, far-end del interlocutor y eco retardado 120 ms en el mic.
* :class:`~server.pipeline.mock.MockInferenceServer` cierra ambas cláusulas por
  VAD y emite TTS determinista en cada dirección.
* :func:`client.call_mode.run_call_mode` arranca Rx y Tx a la vez con un único
  :class:`~client.audio.echo_cancel.EchoCancelManager` y un ``CommandRunner``
  en memoria.

Se verifican: simultaneidad de streams, "See you tomorrow" en
``Translator_Tx_Mic``, la traducción del interlocutor en el sink físico con
ducking, atenuación del eco ≥ 20 dB, latencia < 1500 ms en **ambos** sentidos,
PTT activo/idle conviviendo con Rx, limpieza del grafo y del event loop, y que
el token nunca aparece en logs.

La subclase del transporte **no** inyecta el doblaje en el monitor simulado:
en el grafo real el AEC recibe ese monitor, pero en el mock hacerlo desplazaría
las muestras de eco y convertiría la medida de atenuación en un artefacto del
fixture. El sink sí recibe y registra toda la salida doblada.
"""

from __future__ import annotations

import asyncio
import logging
import math
import shlex
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import pytest_asyncio
from numpy.typing import NDArray

from client.audio.echo_cancel import CommandResult, build_aec_args
from client.audio.ptt import PTTState, PushToTalk
from client.audio.resampler import SampleInput, resample_to_16k, resample_to_48k
from client.audio.transport import MockAudioTransport
from client.call_mode import CAPTURE_SAMPLES, run_call_mode
from client.net.ws_client import WSClient
from server.pipeline.mock import MockInferenceServer, MockSTT, MockTranslator
from shared.constants import PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE, STREAM_TX
from shared.models import SubtitleMessage
from shared.protocol import AudioFrame, Flags, decode_frame, encode_frame

TIMEOUT = 15.0
TOTAL_FRAMES = 120  # 2.4 s de audio en bloques de 20 ms
TAIL_START = 102  # ventana eco-only: ya no hay voz cercana
TAIL_END = 110
SECRET = "babelpipe-token-task-020-no-logear"
MIC_SOURCE = "Physical_Mic"
PHYSICAL_SINK = "alsa_output.pci-0000_00_1f.3.analog-stereo"
EXPECTED_TX = "See you tomorrow"
EXPECTED_RX = "Hola, ¿cómo estás?"


def _rms(samples: NDArray[Any]) -> float:
    values = np.asarray(samples, dtype=np.float64)
    return float(math.sqrt(float(np.mean(np.square(values)))))


def _pcm_rms(pcm: bytes) -> float:
    return _rms(np.frombuffer(pcm, dtype="<i2"))


class FakePactlRunner:
    """Simula ``pactl``; nunca ejecuta nada y solo registra comandos."""

    def __init__(self, *, module_id: int = 42) -> None:
        self.module_id = module_id
        self.commands: list[tuple[str, ...]] = []

    def run(self, args: Sequence[str]) -> CommandResult:
        argv = tuple(args)
        self.commands.append(argv)
        assert argv[0] == "pactl", f"comando inesperado: {argv}"
        assert all("sudo" not in item and item != "sh" for item in argv)
        if argv[1] == "load-module":
            return CommandResult(stdout=f"{self.module_id}\n")
        if argv[1] == "unload-module":
            assert argv[2] == str(self.module_id), "se descargó un módulo ajeno"
            return CommandResult()
        raise AssertionError(f"acción pactl no soportada: {argv[1]}")


class RecordingWSClient(WSClient):
    """WSClient real que conserva los frames enviados para medir el canal Tx."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.sent: list[AudioFrame] = []

    def send_frame(self, frame: AudioFrame | bytes) -> Awaitable[None]:
        raw = (
            frame
            if isinstance(frame, bytes)
            else encode_frame(
                stream_id=frame.stream_id,
                sequence=frame.sequence,
                flags=frame.flags,
                pcm=frame.pcm,
                sample_rate=frame.sample_rate,
                num_samples=frame.num_samples,
            )
        )
        self.sent.append(decode_frame(raw))
        return super().send_frame(frame)


class SinkRecordingTransport(MockAudioTransport):
    """MockAudioTransport que registra el doblaje sin realimentar el monitor."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.sink_writes: list[NDArray[np.int16]] = []

    def start(self) -> None:
        super().start()
        self.sink_writes.clear()

    def write_physical_sink_48k(self, samples: SampleInput) -> int:
        pcm = cast(
            "NDArray[np.int16]",
            resample_to_48k(
                samples, src_rate=PIPEWIRE_SAMPLE_RATE, dtype=np.int16
            ),
        )
        self.sink_writes.append(pcm.copy())
        self.sink_written_samples += int(pcm.size)
        return int(pcm.size)


@pytest.fixture(autouse=True)
def forbid_real_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """Red de seguridad: cualquier subprocess real hace fallar el test."""

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"Se intentó ejecutar un comando real: {args} {kwargs}")

    monkeypatch.setattr(subprocess, "run", fail)


@pytest_asyncio.fixture
async def call_server() -> AsyncIterator[MockInferenceServer]:
    """Servidor mock en puerto efímero: dos sesiones, dos colas, sin hardware."""
    server = MockInferenceServer(min_silence_ms=250, queue_maxsize=4096)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


def _make_clients(server: MockInferenceServer) -> tuple[RecordingWSClient, RecordingWSClient]:
    common: dict[str, Any] = {
        "host": "127.0.0.1",
        "port": server.port,
        "token": SECRET,
        "queue_maxsize": 4096,
        "ping_interval_s": None,
    }
    rx = RecordingWSClient(mode="rx", source_lang="en", target_lang="es", **common)
    tx = RecordingWSClient(mode="tx", source_lang="es", target_lang="en", **common)
    return rx, tx


def _make_settings(*, ptt_enabled: bool = False) -> SimpleNamespace:
    """Objeto de configuración de aplicación (validado por Pydantic en la frontera)."""
    return SimpleNamespace(
        audio=SimpleNamespace(
            ptt_enabled=ptt_enabled,
            echo_cancel=True,
            duck_level=0.12,
            mic_source=MIC_SOURCE,
            physical_sink=PHYSICAL_SINK,
            aec_delay_ms=120.0,
            aec_attenuation_db=20.0,
        ),
        pipeline=SimpleNamespace(
            rx_source_lang="en",
            rx_target_lang="es",
            tx_source_lang="es",
            tx_target_lang="en",
        ),
    )


async def _wait_for(predicate: Callable[[], bool], timeout: float = TIMEOUT) -> None:
    """Espera cooperativa con deadline."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("timeout esperando a la llamada bidireccional")
        await asyncio.sleep(0.002)


async def _assert_clean(server: MockInferenceServer) -> None:
    """Sin tareas colgadas, sockets cerrados y módulo AEC descargado."""
    await server.stop()
    for _ in range(20):
        await asyncio.sleep(0)
        alive = [
            task.get_name()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith(
                (
                    "babelpipe-call-",
                    "babelpipe-rx-",
                    "babelpipe-tx-",
                    "babelpipe-ws-",
                    "mock-ws-",
                    "mock-pipeline",
                )
            )
        ]
        if not alive:
            return
    assert alive == [], f"tareas colgadas: {alive}"


@pytest.mark.asyncio
async def test_ac10_bidirectional_call_with_aec(
    call_server: MockInferenceServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-10: Rx y Tx simultáneos, eco ≥20 dB y <1500 ms en ambos sentidos."""
    monkeypatch.setattr(
        MockSTT, "PHRASES", ("Nos vemos mañana", "Hello, how are you?")
    )
    monkeypatch.setitem(MockTranslator.TRANSLATIONS, "en", EXPECTED_TX)
    monkeypatch.setitem(MockTranslator.TRANSLATIONS, "es", EXPECTED_RX)
    caplog.set_level(logging.DEBUG, logger="babelpipe.client.call_mode")

    transport = SinkRecordingTransport()
    runner = FakePactlRunner()
    rx_client, tx_client = _make_clients(call_server)
    stop = asyncio.Event()
    subtitles: list[SubtitleMessage] = []
    run_task = asyncio.create_task(
        run_call_mode(
            _make_settings(),
            transport,
            rx_client,
            tx_client,
            on_subtitle=subtitles.append,
            aec_runner=runner,
            stop_event=stop,
        ),
        name="test-run-call-mode",
    )
    try:
        # Se espera al cierre natural de ambas cláusulas (silencio del VAD) y a
        # que todos los chunks TTS estén escritos: así la parada no intercepta
        # ningún ``_on_tts_frame`` en vuelo.
        await _wait_for(
            lambda: len(rx_client.sent) >= TOTAL_FRAMES
            and len(tx_client.sent) >= TOTAL_FRAMES
        )
        await _wait_for(lambda: call_server.stats["tts_chunks"] >= 4)
        await _wait_for(lambda: len(transport.tx_writes) >= 2)
        await _wait_for(lambda: len(transport.sink_writes) >= 2)
        await asyncio.sleep(0.05)
    finally:
        stop.set()
        metrics = await asyncio.wait_for(run_task, TIMEOUT)

    # Dos streams realmente concurrentes y con eventos en cada dirección.
    assert metrics.rx_frames >= TOTAL_FRAMES
    assert metrics.tx_frames >= TOTAL_FRAMES
    assert metrics.rx_subtitles > 0 and metrics.tx_subtitles > 0
    assert any(item.stream == "rx" for item in subtitles)
    assert any(item.stream == "tx" for item in subtitles)
    assert EXPECTED_RX in metrics.rx_translations
    assert EXPECTED_TX in metrics.tx_translations

    # Translator_Tx_Mic recibe la voz traducida del usuario (es → en).
    assert metrics.tx_samples > 0
    assert len(transport.tx_writes) >= 2
    tx_pcm = b"".join(chunk.tobytes() for chunk in transport.tx_writes)
    assert _pcm_rms(tx_pcm) > 0.0, "Translator_Tx_Mic recibió silencio"

    # El sink físico recibe la traducción del interlocutor (en → es) con ducking.
    assert len(transport.sink_writes) >= 2
    sink_pcm = b"".join(chunk.tobytes() for chunk in transport.sink_writes)
    assert _pcm_rms(sink_pcm) > 0.0, "el sink físico recibió silencio"
    assert metrics.duck_starts >= 1 and metrics.duck_stops >= 1
    assert metrics.sink_samples > 0

    # Un único AEC con referencia del monitor del sink físico, cargado/descargado.
    expected_args = build_aec_args(MIC_SOURCE, PHYSICAL_SINK)
    assert metrics.aec_args == expected_args
    assert "sink.name=alsa_output.pci-0000_00_1f.3.analog-stereo.monitor" in expected_args
    assert (
        'aec_args="analog_gain_control=0 digital_gain_control=0 noise_suppression=1"'
        in expected_args
    )
    assert runner.commands == [
        (
            "pactl",
            "load-module",
            "libpipewire-module-echo-cancel",
            *shlex.split(expected_args),
        ),
        ("pactl", "unload-module", "42"),
    ]
    assert transport.monitor_source_name.endswith(".monitor")
    assert transport.monitor_reads >= TOTAL_FRAMES
    assert metrics.aec_enabled is False
    assert metrics.transport_running is False
    assert metrics.ptt_state == "PTT_IDLE"

    # Atenuación medida sobre los frames Tx realmente enviados en la ventana eco-only.
    assert len(tx_client.sent) >= TAIL_END
    assert all(frame.stream_id == STREAM_TX for frame in tx_client.sent)
    assert all(frame.sample_rate == SAMPLE_RATE for frame in tx_client.sent)
    raw_echo = transport.mic_pcm[TAIL_START * CAPTURE_SAMPLES : TAIL_END * CAPTURE_SAMPLES]
    before = _rms(resample_to_16k(raw_echo, src_rate=PIPEWIRE_SAMPLE_RATE))
    after = _pcm_rms(
        b"".join(frame.pcm for frame in tx_client.sent[TAIL_START:TAIL_END])
    )
    assert before > 0.0 and after > 0.0
    measured_db = 20.0 * math.log10(before / after)
    print(
        f"\n[AC-10] eco antes={before:.1f} LSB, después={after:.1f} LSB, "
        f"atenuación={measured_db:.2f} dB (métrica Tx={metrics.echo_attenuation_db:.2f} dB)"
    )
    assert measured_db >= 20.0, f"atenuación insuficiente: {measured_db:.2f} dB"

    # Presupuesto de latencia por cláusula en ambos sentidos (SPEC §7).
    print(
        f"[AC-10] latencia Rx={metrics.rx_latency_ms:.1f} ms, "
        f"Tx={metrics.tx_latency_ms:.1f} ms, combinada={metrics.latency_ms:.1f} ms"
    )
    assert 0.0 <= metrics.rx_latency_ms < 1500.0
    assert 0.0 <= metrics.tx_latency_ms < 1500.0
    assert metrics.latency_ms < 1500.0

    assert SECRET not in caplog.text
    assert "Traceback" not in caplog.text
    await _assert_clean(call_server)


@pytest.mark.asyncio
async def test_ptt_idle_con_rx_simultaneo(
    call_server: MockInferenceServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PTT en idle no envía voz en Tx mientras Rx sigue doblando en el sink."""
    monkeypatch.setattr(
        MockSTT, "PHRASES", ("Nos vemos mañana", "Hello, how are you?")
    )
    monkeypatch.setitem(MockTranslator.TRANSLATIONS, "en", EXPECTED_TX)
    monkeypatch.setitem(MockTranslator.TRANSLATIONS, "es", EXPECTED_RX)

    transport = SinkRecordingTransport()
    rx_client, tx_client = _make_clients(call_server)
    ptt = PushToTalk(enabled=True)
    stop = asyncio.Event()
    run_task = asyncio.create_task(
        run_call_mode(
            _make_settings(ptt_enabled=True),
            transport,
            rx_client,
            tx_client,
            aec_runner=FakePactlRunner(),
            ptt=ptt,
            stop_event=stop,
        ),
        name="test-run-call-mode-ptt",
    )
    try:
        await _wait_for(lambda: len(tx_client.sent) >= 30)
        idle_frames = tx_client.sent[:30]
        assert idle_frames
        assert all(_pcm_rms(frame.pcm) == 0.0 for frame in idle_frames)
        assert all(not frame.flags & int(Flags.PTT_ACTIVE) for frame in idle_frames)
        assert ptt.state is PTTState.PTT_IDLE

        # Al pulsar la tecla se reabre Tx con voz y flag PTT_ACTIVE. Se hace
        # enseguida para que la fixture de voz cercana todavía esté presente.
        idle_count = len(tx_client.sent)
        ptt.press()
        assert ptt.state is PTTState.PTT_ACTIVE
        await _wait_for(lambda: len(tx_client.sent) >= idle_count + 10)
        opened = tx_client.sent[idle_count:]
        assert any(_pcm_rms(frame.pcm) > 0.0 for frame in opened)
        assert any(frame.flags & int(Flags.PTT_ACTIVE) for frame in opened)

        # Rx permanece activa aunque el canal Tx estuviera inicialmente en idle.
        await _wait_for(lambda: any(chunk.size > 0 for chunk in transport.sink_writes))
        assert len(rx_client.sent) > 0
        ptt.release()
    finally:
        stop.set()
        metrics = await asyncio.wait_for(run_task, TIMEOUT)

    assert metrics.ptt_state == "PTT_IDLE"
    assert metrics.rx_frames > 0 and metrics.tx_frames > 0
    assert metrics.sink_samples > 0
    assert metrics.transport_running is False
    assert metrics.aec_enabled is False
    await _assert_clean(call_server)
