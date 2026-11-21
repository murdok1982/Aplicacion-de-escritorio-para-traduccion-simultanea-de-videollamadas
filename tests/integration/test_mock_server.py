"""Tests de integración del servidor mock determinista (TASK-010 / AC-05).

Ejercitan el servidor WS sobre un puerto efímero con los fixtures WAV reales
de ``tests/fixtures`` en chunks canónicos de 640 bytes. No hay red externa,
modelos ni hardware: toda la carga se genera localmente con el protocolo de
``shared``. Las aserciones cubren validación de frames, dedup por ``sequence``,
backpressure sin bloqueo, orden de eventos, determinismo y reconexión.
"""

from __future__ import annotations

import asyncio
import json
import struct
import wave
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
import websockets
from websockets.legacy.client import WebSocketClientProtocol

from server.pipeline.mock import MOCK_MODELS, MockInferenceServer
from shared.constants import (
    CHUNK_BYTES,
    CHUNK_SAMPLES,
    HEADER_SIZE,
    HEADER_STRUCT,
    MAGIC,
    SAMPLE_RATE,
    STREAM_RX,
    VERSION,
)
from shared.models import (
    AudioConfigMessage,
    ConfigMessage,
    ControlMessage,
    DuckMessage,
    ErrorMessage,
    PingMessage,
    PongMessage,
    ReadyMessage,
    ReconnectMessage,
    ResetMessage,
    StreamName,
    SubtitleMessage,
    TranslatedMessage,
    TtsChunkMessage,
    parse_control_message,
)
from shared.protocol import Flags, decode_frame, encode_frame, iter_pcm_chunks

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
TIMEOUT = 10.0
Item = ControlMessage | bytes


def _pcm_from_wav(name: str) -> bytes:
    """Lee un WAV canónico 16 kHz mono int16 LE de ``tests/fixtures``."""
    path = FIXTURES_DIR / name
    assert path.is_file(), f"falta el fixture {path}"
    with wave.open(str(path), "rb") as wav:
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == SAMPLE_RATE
        return wav.readframes(wav.getnframes())


def _chunks(name: str) -> list[bytes]:
    """Divide el WAV en chunks exactos de 640 bytes (20 ms)."""
    chunks = list(iter_pcm_chunks(_pcm_from_wav(name), CHUNK_SAMPLES))
    assert chunks
    assert all(len(chunk) == CHUNK_BYTES for chunk in chunks)
    return chunks


def _config(
    session_id: UUID,
    *,
    mode: StreamName = "rx",
    source_lang: str = "en",
    target_lang: str = "es",
) -> str:
    return ConfigMessage(
        session_id=session_id,
        mode=mode,
        source_lang=source_lang,
        target_lang=target_lang,
        vad="mock-vad",
        stt="mock-stt",
        translator="mock-translator",
        tts="mock-tts",
        audio_config=AudioConfigMessage(
            sample_rate=16_000, channels=1, format="pcm_s16le", chunk_ms=20
        ),
    ).model_dump_json()


def _raw_frame(
    *,
    magic: bytes = MAGIC,
    version: int = VERSION,
    sample_rate: int = SAMPLE_RATE,
    payload: bytes | None = None,
    sequence: int = 1,
) -> bytes:
    pcm = payload if payload is not None else b"\x01\x00" * CHUNK_SAMPLES
    return (
        struct.pack(
            HEADER_STRUCT, magic, version, 0, STREAM_RX, sequence, sample_rate, CHUNK_SAMPLES
        )
        + pcm
    )


async def _recv(websocket: WebSocketClientProtocol, timeout: float = TIMEOUT) -> Item:
    raw = await asyncio.wait_for(websocket.recv(), timeout)
    if isinstance(raw, bytes):
        return raw
    return parse_control_message(raw)


async def _collect_until(
    websocket: WebSocketClientProtocol,
    predicate: Callable[[Item], bool],
    *,
    count: int = 1,
    timeout: float = TIMEOUT,
) -> list[Item]:
    """Lee eventos hasta ver ``count`` coincidencias (incluidos binarios)."""
    collected: list[Item] = []
    matched = 0
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while matched < count:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise AssertionError(f"timeout esperando {count} evento(s) coincidente(s)")
        item = await _recv(websocket, remaining)
        collected.append(item)
        if predicate(item):
            matched += 1
    return collected


def _is_duck_stop(item: Item) -> bool:
    return isinstance(item, DuckMessage) and item.action == "stop"


def _is_pong(item: Item) -> bool:
    return isinstance(item, PongMessage)


def _messages(items: list[Item]) -> list[ControlMessage]:
    return [item for item in items if not isinstance(item, bytes)]


async def _send_wav(
    websocket: WebSocketClientProtocol,
    chunks: list[bytes],
    *,
    start_seq: int = 0,
    final: bool = True,
) -> int:
    """Envía el WAV en chunks y devuelve la última ``sequence`` usada."""
    last_seq = start_seq + len(chunks) - 1
    for sequence, pcm in enumerate(chunks, start=start_seq):
        flags = int(Flags.FINAL) if final and sequence == last_seq else 0
        await websocket.send(
            encode_frame(stream_id=STREAM_RX, sequence=sequence, flags=flags, pcm=pcm)
        )
    return last_seq


async def _wait_stat(server: MockInferenceServer, key: str, minimum: int) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    while loop.time() < deadline:
        if server.stats[key] >= minimum:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"el contador {key} no alcanzó {minimum}")


@pytest_asyncio.fixture
async def mock_server() -> AsyncIterator[MockInferenceServer]:
    server = MockInferenceServer(queue_maxsize=50, min_silence_ms=250)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_ciclo_de_vida_control_y_puerto_liberado() -> None:
    server = MockInferenceServer()
    assert server.is_ready is False
    await server.start()
    try:
        assert server.is_ready is True
        assert server.port > 0
        assert server.url == f"ws://127.0.0.1:{server.port}/ws/translate"
        session_id = uuid4()
        async with websockets.connect(server.url) as websocket:
            await websocket.send(_config(session_id))
            ready = await _recv(websocket)
            assert isinstance(ready, ReadyMessage)
            assert ready.session_id == session_id
            assert ready.models == list(MOCK_MODELS)

            await websocket.send(PingMessage(ts=99).model_dump_json())
            pong = await _recv(websocket)
            assert isinstance(pong, PongMessage)
            assert pong.ts == 99

            await websocket.send(ResetMessage(session_id=session_id).model_dump_json())
            await websocket.send(
                ReconnectMessage(
                    retry_after_ms=0, session_id=session_id, last_seq=7
                ).model_dump_json()
            )
            ack = await _recv(websocket)
            assert isinstance(ack, ReconnectMessage)
            assert ack.last_seq == 7

            await websocket.send(PingMessage(ts=100).model_dump_json())
            pong = await _recv(websocket)
            assert isinstance(pong, PongMessage)
            assert pong.ts == 100
    finally:
        await server.stop()

    assert server.is_ready is False
    with pytest.raises(OSError):
        await asyncio.wait_for(websockets.connect(server.url), timeout=2.0)


@pytest.mark.asyncio
async def test_frames_y_json_invalidos_devuelven_error_sin_stack_trace(
    mock_server: MockInferenceServer,
) -> None:
    async with websockets.connect(mock_server.url) as websocket:
        await websocket.send(_config(uuid4()))
        assert isinstance(await _recv(websocket), ReadyMessage)

        malformed = (
            (_raw_frame(magic=b"XX"), "SCHEMA"),
            (_raw_frame(version=9), "SCHEMA"),
            (_raw_frame(sample_rate=48_000), "FORMAT"),
            (_raw_frame(payload=b"\x00" * 10), "SCHEMA"),
            (b"BP\x01", "SCHEMA"),
        )
        for frame, expected_code in malformed:
            await websocket.send(frame)
            error = await _recv(websocket)
            assert isinstance(error, ErrorMessage)
            assert error.code == expected_code
            assert "Traceback" not in error.message
            assert 'File "' not in error.message

        await websocket.send("{json invalido")
        error = await _recv(websocket)
        assert isinstance(error, ErrorMessage)
        assert error.code == "SCHEMA"

        await websocket.send(json.dumps({"type": "ping", "ts": 1, "extra": True}))
        error = await _recv(websocket)
        assert isinstance(error, ErrorMessage)
        assert error.code == "SCHEMA"

        await websocket.send(PingMessage(ts=5).model_dump_json())
        pong = await _recv(websocket)
        assert isinstance(pong, PongMessage)
        assert pong.ts == 5
    assert mock_server.stats["frames_invalid"] == len(malformed)


@pytest.mark.asyncio
async def test_dedup_por_sequence_y_wav_repetido(
    mock_server: MockInferenceServer,
) -> None:
    chunks = _chunks("voice_es_16k.wav")
    async with websockets.connect(mock_server.url) as websocket:
        await websocket.send(_config(uuid4()))
        assert isinstance(await _recv(websocket), ReadyMessage)

        await _send_wav(websocket, chunks)
        first = await _collect_until(websocket, _is_duck_stop)
        finals = [
            event
            for event in _messages(first)
            if isinstance(event, SubtitleMessage) and event.is_final
        ]
        assert len(finals) == 1

        # Reenvío del WAV completo con las mismas sequences: todo se descarta.
        await _send_wav(websocket, chunks)
        await websocket.send(PingMessage(ts=77).model_dump_json())
        after = await _collect_until(websocket, _is_pong)
        assert not [
            event for event in _messages(after) if isinstance(event, SubtitleMessage)
        ]

    stats = mock_server.stats
    assert stats["frames_received"] == 2 * len(chunks)
    assert stats["frames_processed"] == len(chunks)
    assert stats["frames_duplicate"] == len(chunks)
    print(
        f"[stats] dedup: chunks={len(chunks)} procesados={stats['frames_processed']} "
        f"duplicados={stats['frames_duplicate']} descartados_overrun="
        f"{stats['frames_dropped_overrun']}"
    )


@pytest.mark.asyncio
async def test_eventos_ordenados_subtitulo_final_ducking_y_tts(
    mock_server: MockInferenceServer,
) -> None:
    chunks = _chunks("voice_es_16k.wav")
    async with websockets.connect(mock_server.url) as websocket:
        await websocket.send(_config(uuid4(), target_lang="es"))
        assert isinstance(await _recv(websocket), ReadyMessage)
        await _send_wav(websocket, chunks)
        items = await _collect_until(websocket, _is_duck_stop)

    events = _messages(items)
    subtitles = [event for event in events if isinstance(event, SubtitleMessage)]
    assert subtitles
    assert any(not event.is_final for event in subtitles), "debe haber subtítulos parciales"
    assert sum(1 for event in subtitles if event.is_final) == 1
    assert subtitles[-1].is_final is True
    assert subtitles[-1].stream == "rx"

    translated = [event for event in events if isinstance(event, TranslatedMessage)]
    assert len(translated) == 1
    assert translated[0].is_final is True
    assert translated[0].lang == "es"

    idx_final = next(
        index
        for index, event in enumerate(events)
        if isinstance(event, SubtitleMessage) and event.is_final
    )
    idx_translated = next(
        index for index, event in enumerate(events) if isinstance(event, TranslatedMessage)
    )
    idx_duck_start = next(
        index
        for index, event in enumerate(events)
        if isinstance(event, DuckMessage) and event.action == "start"
    )
    idx_tts = [
        index for index, event in enumerate(events) if isinstance(event, TtsChunkMessage)
    ]
    idx_duck_stop = len(events) - 1
    assert idx_final < idx_translated < idx_duck_start < min(idx_tts) < idx_duck_stop
    assert all(
        isinstance(event, DuckMessage) and event.stream == "rx"
        for event in events
        if isinstance(event, DuckMessage)
    )

    binaries = [item for item in items if isinstance(item, bytes)]
    assert len(binaries) == len(idx_tts) == 2
    for raw in binaries:
        assert len(raw) >= HEADER_SIZE
        frame = decode_frame(raw)
        assert frame.num_samples == CHUNK_SAMPLES
        assert frame.stream_id == STREAM_RX


@pytest.mark.asyncio
async def test_mismo_wav_genera_mismos_subtitulos(
    mock_server: MockInferenceServer,
) -> None:
    chunks = _chunks("voice_es_16k.wav")
    runs: list[
        tuple[list[tuple[str, bool]], list[tuple[str, str]]]
    ] = []
    for _ in range(2):
        async with websockets.connect(mock_server.url) as websocket:
            await websocket.send(_config(uuid4()))
            assert isinstance(await _recv(websocket), ReadyMessage)
            await _send_wav(websocket, chunks)
            items = await _collect_until(websocket, _is_duck_stop)
        events = _messages(items)
        runs.append(
            (
                [
                    (event.text, event.is_final)
                    for event in events
                    if isinstance(event, SubtitleMessage)
                ],
                [
                    (event.text, event.lang)
                    for event in events
                    if isinstance(event, TranslatedMessage)
                ],
            )
        )
    assert runs[0] == runs[1]
    print(
        f"[stats] determinismo: ejecuciones=2 chunks={len(chunks)} "
        f"eventos_por_ejecucion={len(runs[0][0])} identical=True"
    )


@pytest.mark.asyncio
async def test_two_clauses_cierra_dos_clausulas_sin_corte_interno(
    mock_server: MockInferenceServer,
) -> None:
    chunks = _chunks("two_clauses_16k.wav")
    async with websockets.connect(mock_server.url) as websocket:
        await websocket.send(_config(uuid4()))
        assert isinstance(await _recv(websocket), ReadyMessage)
        await _send_wav(websocket, chunks)
        items = await _collect_until(websocket, _is_duck_stop, count=2)

    finals = [
        event
        for event in _messages(items)
        if isinstance(event, SubtitleMessage) and event.is_final
    ]
    assert len(finals) == 2
    assert mock_server.stats["subtitles_final"] == 2
    print(
        f"[stats] two_clauses: chunks={len(chunks)} finales=2 "
        f"procesados={mock_server.stats['frames_processed']}"
    )


@pytest.mark.asyncio
async def test_backpressure_descarta_antiguos_y_no_se_cuelga() -> None:
    server = MockInferenceServer(queue_maxsize=4, processing_delay_ms=5.0)
    await server.start()
    try:
        chunks = _chunks("voice_es_16k.wav")[:40]
        assert len(chunks) == 40
        async with websockets.connect(server.url) as websocket:
            await websocket.send(_config(uuid4()))
            assert isinstance(await _recv(websocket), ReadyMessage)

            async def blast() -> None:
                for sequence, pcm in enumerate(chunks):
                    flags = int(Flags.FINAL) if sequence == len(chunks) - 1 else 0
                    await websocket.send(
                        encode_frame(
                            stream_id=STREAM_RX,
                            sequence=sequence,
                            flags=flags,
                            pcm=pcm,
                        )
                    )

            await asyncio.wait_for(blast(), timeout=5.0)
            items = await _collect_until(websocket, _is_duck_stop, timeout=8.0)

        overruns = [
            event
            for event in _messages(items)
            if isinstance(event, ErrorMessage) and event.code == "OVERRUN"
        ]
        assert overruns
        stats = server.stats
        assert stats["frames_received"] == len(chunks)
        assert stats["frames_accepted"] == len(chunks)
        assert stats["frames_processed"] + stats["frames_dropped_overrun"] <= len(chunks)
        assert stats["frames_dropped_overrun"] >= 1
        assert stats["subtitles_final"] >= 1
        assert all("Traceback" not in event.message for event in overruns)
        print(
            f"[stats] backpressure: chunks={len(chunks)} "
            f"aceptados={stats['frames_accepted']} "
            f"procesados={stats['frames_processed']} "
            f"descartados={stats['frames_dropped_overrun']} "
            f"overruns={len(overruns)}"
        )
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_reconexion_con_last_seq_no_duplica_subtitulos(
    mock_server: MockInferenceServer,
) -> None:
    chunks = _chunks("voice_es_16k.wav")
    session_id = uuid4()
    split = len(chunks) // 2

    # Primera conexión: enviamos la mitad y cortamos a mitad de cláusula.
    async with websockets.connect(mock_server.url) as websocket:
        await websocket.send(_config(session_id))
        assert isinstance(await _recv(websocket), ReadyMessage)
        for sequence, pcm in enumerate(chunks[:split]):
            await websocket.send(
                encode_frame(stream_id=STREAM_RX, sequence=sequence, pcm=pcm)
            )
    await _wait_stat(mock_server, "subtitles_final", 1)
    finals_before = mock_server.stats["subtitles_final"]

    # Segunda conexión: misma sesión + last_seq, con el último chunk repetido.
    async with websockets.connect(mock_server.url) as websocket:
        await websocket.send(_config(session_id))
        assert isinstance(await _recv(websocket), ReadyMessage)
        await websocket.send(
            ReconnectMessage(
                retry_after_ms=0, session_id=session_id, last_seq=split - 1
            ).model_dump_json()
        )
        ack = await _recv(websocket)
        assert isinstance(ack, ReconnectMessage)
        assert ack.last_seq == split - 1

        for sequence in range(split - 1, len(chunks)):
            flags = int(Flags.FINAL) if sequence == len(chunks) - 1 else 0
            await websocket.send(
                encode_frame(
                    stream_id=STREAM_RX,
                    sequence=sequence,
                    flags=flags,
                    pcm=chunks[sequence],
                )
            )
        items = await _collect_until(websocket, _is_duck_stop)

    finals = [
        event
        for event in _messages(items)
        if isinstance(event, SubtitleMessage) and event.is_final
    ]
    assert len(finals) == 1
    assert mock_server.stats["subtitles_final"] == finals_before + 1
    assert mock_server.stats["frames_duplicate"] >= 1
    print(
        f"[stats] reconexion: chunks={len(chunks)} last_seq={split - 1} "
        f"duplicados_descartados={mock_server.stats['frames_duplicate']} "
        f"finales_nuevos={len(finals)}"
    )


@pytest.mark.asyncio
async def test_mock_expone_estado_ready() -> None:
    server = MockInferenceServer()
    async with server:
        assert server.is_ready is True
        assert server.stats["frames_received"] == 0
    assert server.is_ready is False
