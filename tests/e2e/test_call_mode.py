"""E2E del modo llamada: canal Tx hacia ``Translator_Tx_Mic`` (TASK-019).

Cubre los criterios AC-10 y AC-11 sin hardware, sin red externa y sin ejecutar
ni un solo comando real:

* :class:`~client.audio.transport.MockAudioTransport` reproduce el grafo con
  ``voice_es_16k.wav`` (voz cercana) y ``echo_48k.wav`` (far-end del
  interlocutor): el monitor del sink físico devuelve exactamente ese far-end y
  el micrófono contiene una copia retardada 120 ms del mismo.
* :class:`~server.pipeline.mock.MockInferenceServer` cierra la cláusula por VAD
  y devuelve TTS determinista. Se fijan sus frases mock al valor canónico del
  AC-10 (“Nos vemos mañana” → “See you tomorrow”) solo dentro del test.
* :class:`~client.audio.echo_cancel.EchoCancelManager` se construye con un
  ``CommandRunner`` en memoria; ``subprocess.run`` está parcheado para fallar.

La atenuación del eco se mide sobre los frames **realmente enviados** por
:class:`~client.net.stream.TxStream`, en una ventana donde solo hay eco del
interlocutor (sin voz cercana), y la latencia se lee de los timestamps que el
propio pipeline registra con el reloj simulado del mock.
"""

from __future__ import annotations

import asyncio
import logging
import math
import shlex
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import pytest_asyncio

from client.audio.echo_cancel import (
    CommandResult,
    EchoCancelManager,
    build_aec_args,
)
from client.audio.ptt import PTTState, PushToTalk
from client.audio.resampler import resample_to_16k
from client.audio.transport import MockAudioTransport
from client.net.stream import TxStream
from client.net.ws_client import WSClient
from server.pipeline.mock import MockInferenceServer, MockSTT, MockTranslator
from shared.constants import CHUNK_SAMPLES, PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE, STREAM_TX
from shared.models import TranslatedMessage
from shared.protocol import AudioFrame, Flags, decode_frame, encode_frame

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
TIMEOUT = 10.0
CAPTURE_SAMPLES = CHUNK_SAMPLES * 3  # 960 muestras @48 kHz = 20 ms @16 kHz
TOTAL_FRAMES = 120  # 2.4 s: 2 s de voz + 400 ms de eco sin voz cercana
TAIL_START = 102  # ventana eco-only, ya sin voz cercana y antes del fin del mic
TAIL_END = 110
SECRET = "babelpipe-token-task-019-no-logear"
MIC_SOURCE = "Physical_Mic"
PHYSICAL_SINK = "alsa_output.pci-0000_00_1f.3.analog-stereo"


def _rms(samples: np.ndarray) -> float:
    """RMS en LSB int16 de una señal 1-D."""
    values = np.asarray(samples, dtype=np.float64)
    return float(math.sqrt(float(np.mean(np.square(values)))))


def _pcm_rms(pcm: bytes) -> float:
    return _rms(np.frombuffer(pcm, dtype="<i2"))


class FakePactlRunner:
    """Simula ``pactl``; solo registra comandos, nunca los ejecuta."""

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
    """WSClient real que conserva copia de los frames enviados para medir."""

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


@pytest.fixture(autouse=True)
def forbid_real_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """Red de seguridad: cualquier subprocess real hace fallar el test."""

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"Se intentó ejecutar un comando real: {args} {kwargs}")

    monkeypatch.setattr(subprocess, "run", fail)


@pytest_asyncio.fixture
async def call_server() -> AsyncIterator[MockInferenceServer]:
    """Servidor mock en puerto efímero, sin hardware y sin pérdida de eventos."""
    # El productor alimenta 2.4 s de audio en un solo tick de CPU: la cola debe
    # admitir todo el lote para no emitir OVERRUN que desplace los tts_chunk.
    server = MockInferenceServer(min_silence_ms=250, queue_maxsize=4096)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


def _make_client(server: MockInferenceServer) -> RecordingWSClient:
    return RecordingWSClient(
        host="127.0.0.1",
        port=server.port,
        token=SECRET,
        mode="tx",
        source_lang="es",
        target_lang="en",
        queue_maxsize=4096,
        ping_interval_s=None,
    )


def _make_aec(runner: FakePactlRunner) -> EchoCancelManager:
    return EchoCancelManager(runner, source_name=MIC_SOURCE, sink_name=PHYSICAL_SINK)


def _new_stream(
    transport: MockAudioTransport,
    *,
    aec: EchoCancelManager,
    ptt: PushToTalk | None = None,
    on_translated: Callable[[TranslatedMessage], None] | None = None,
    on_tts: Callable[[bytes], None] | None = None,
) -> TxStream:
    return TxStream(
        transport,
        aec=aec,
        ptt=ptt,
        capture_samples=CAPTURE_SAMPLES,
        on_translated=on_translated,
        on_tts=on_tts,
    )


async def _wait_for(predicate: Callable[[], bool], timeout: float = TIMEOUT) -> None:
    """Espera cooperativa a que el pipeline alcance un estado."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("timeout esperando al pipeline Tx")
        await asyncio.sleep(0.002)


async def _assert_no_hanging_tasks(server: MockInferenceServer) -> None:
    """Cierra el servidor mock y comprueba que no quede ninguna tarea viva."""
    await server.stop()
    for _ in range(20):
        await asyncio.sleep(0)
        alive = [
            task.get_name()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith(
                ("babelpipe-tx-", "babelpipe-ws-", "mock-ws-", "mock-pipeline")
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
    """AC-10: voz traducida en Tx, eco atenuado >=20 dB y latencia <1500 ms."""
    monkeypatch.setattr(MockSTT, "PHRASES", ("Nos vemos mañana",))
    monkeypatch.setitem(MockTranslator.TRANSLATIONS, "en", "See you tomorrow")
    caplog.set_level(logging.DEBUG, logger="babelpipe.client.net")

    transport = MockAudioTransport()
    runner = FakePactlRunner()
    aec = _make_aec(runner)
    assert aec.aec_args == build_aec_args(MIC_SOURCE, PHYSICAL_SINK)
    assert "sink.name=alsa_output.pci-0000_00_1f.3.analog-stereo.monitor" in aec.aec_args
    assert (
        'aec_args="analog_gain_control=0 digital_gain_control=0 noise_suppression=1"'
        in aec.aec_args
    )

    translated: list[TranslatedMessage] = []
    tts_done = asyncio.Event()
    client = _make_client(call_server)
    stream = _new_stream(
        transport,
        aec=aec,
        on_translated=translated.append,
        on_tts=lambda _: tts_done.set(),
    )
    run_task = asyncio.create_task(stream.run(client))
    try:
        await client.connect()
        await _wait_for(lambda: stream.metrics.chunks >= TOTAL_FRAMES)
        await asyncio.wait_for(tts_done.wait(), TIMEOUT)
        await _wait_for(lambda: stream.metrics.tts_chunks >= 2)
    finally:
        stream.stop()
        await asyncio.wait_for(run_task, TIMEOUT)
        await client.close()
        for _ in range(5):
            await asyncio.sleep(0)

    # La referencia del AEC es el monitor del sink físico, no el stream Tx.
    assert transport.monitor_source_name.endswith(".monitor")
    assert transport.monitor_reads >= TOTAL_FRAMES
    assert aec.status().enabled is False  # el manager descargó su único módulo
    assert runner.commands == [
        (
            "pactl",
            "load-module",
            "libpipewire-module-echo-cancel",
            *shlex.split(aec.aec_args),
        ),
        ("pactl", "unload-module", "42"),
    ]

    # El micrófono virtual recibe la voz traducida (TTS del servidor mock).
    assert translated, "el servidor no envió ningún evento translated"
    assert any(
        message.text == "See you tomorrow" and message.lang == "en" and message.is_final
        for message in translated
    )
    assert stream.metrics.tts_chunks >= 2
    assert len(transport.tx_writes) >= 2
    tx_pcm = b"".join(pcm.tobytes() for pcm in transport.tx_writes)
    assert len(tx_pcm) >= 2 * CHUNK_SAMPLES * 2
    assert _pcm_rms(tx_pcm) > 0.0, "Translator_Tx_Mic recibió silencio"

    # Atenuación medida sobre los frames Tx reales en la ventana eco-only.
    assert len(client.sent) >= TAIL_END
    assert all(frame.stream_id == STREAM_TX for frame in client.sent)
    assert all(frame.sample_rate == SAMPLE_RATE for frame in client.sent)
    raw_echo = transport.mic_pcm[
        TAIL_START * CAPTURE_SAMPLES : TAIL_END * CAPTURE_SAMPLES
    ]
    raw_16k = resample_to_16k(raw_echo, src_rate=PIPEWIRE_SAMPLE_RATE)
    cleaned_pcm = b"".join(frame.pcm for frame in client.sent[TAIL_START:TAIL_END])
    before = _rms(raw_16k)
    after = _pcm_rms(cleaned_pcm)
    assert before > 0.0
    assert after > 0.0, "la ventana eco-only quedó en silencio"
    attenuation_db = 20.0 * math.log10(before / after)
    print(
        f"\n[AC-10] eco antes={before:.1f} LSB, después={after:.1f} LSB, "
        f"atenuación={attenuation_db:.2f} dB"
    )
    assert attenuation_db >= 20.0, f"atenuación insuficiente: {attenuation_db:.2f} dB"

    latency_ms = stream.metrics.tts_latency_ms
    print(f"[AC-10] latencia voz->Translator_Tx_Mic={latency_ms:.1f} ms")
    assert latency_ms < 1500.0, f"latencia excedida: {latency_ms:.1f} ms"

    assert SECRET not in caplog.text
    assert "Traceback" not in caplog.text
    await _assert_no_hanging_tasks(call_server)


@pytest.mark.asyncio
async def test_ac11_ptt_idle_no_envia_frames_de_voz(
    call_server: MockInferenceServer,
) -> None:
    """AC-11: con PTT activo y sin pulsar, solo silencio y estado PTT_IDLE."""
    transport = MockAudioTransport()
    ptt = PushToTalk(enabled=True)
    client = _make_client(call_server)
    stream = _new_stream(transport, aec=_make_aec(FakePactlRunner()), ptt=ptt)
    run_task = asyncio.create_task(stream.run(client))
    try:
        await client.connect()
        await _wait_for(lambda: stream.metrics.silence_frames >= 30)
        assert ptt.state is PTTState.PTT_IDLE
        idle_frames = client.sent[:30]
        assert idle_frames
        assert all(_pcm_rms(frame.pcm) == 0.0 for frame in idle_frames)
        assert all(not frame.flags & int(Flags.PTT_ACTIVE) for frame in idle_frames)
        assert stream.metrics.chunks == stream.metrics.silence_frames
        assert call_server.stats["subtitles_final"] == 0

        # Al pulsar la tecla se reabre el canal y los frames llevan el flag.
        idle_count = len(client.sent)
        ptt.press()
        assert ptt.state is PTTState.PTT_ACTIVE
        await _wait_for(lambda: len(client.sent) >= idle_count + 10)
        opened = client.sent[idle_count:]
        assert any(_pcm_rms(frame.pcm) > 0.0 for frame in opened)
        assert any(frame.flags & int(Flags.PTT_ACTIVE) for frame in opened)
        ptt.release()
    finally:
        stream.stop()
        await asyncio.wait_for(run_task, TIMEOUT)
        await client.close()
        for _ in range(5):
            await asyncio.sleep(0)

    assert ptt.state is PTTState.PTT_IDLE
    await _assert_no_hanging_tasks(call_server)
