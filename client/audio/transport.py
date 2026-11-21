"""Transporte PCM a 48 kHz del canal Tx de BabelPipe (TASK-019 / REQ-06).

Contrato (SPEC.md REQ-06 / AC-10 / AC-11, ARCHITECTURE.md §2.1 y §4.2):

* :class:`AudioTransport` es la frontera de E/S de audio del grafo PipeWire.
  Toda señal que cruza esta interfaz es ``pcm_s16le`` mono a 48 kHz
  (``PIPEWIRE_SAMPLE_RATE``), el formato nativo del grafo.
* :class:`PipeWireAudioTransport` es la implementación real: lanza
  ``pw-record`` para el micrófono y para el monitor del sink físico, y
  ``pw-play`` para `Translator_Tx_Mic` (a través de su sink maestro
  ``Translator_Tx_Master``) y para el sink físico. Nunca usa ``shell=True``,
  ``sudo`` ni comandos construidos con datos sin validar: el objeto que crea
  los procesos es un :class:`ProcessRunner` inyectable.
* :class:`MockAudioTransport` es una **implementación de transporte para
  tests**, no un stub de producción: reproduce un grafo completo en memoria a
  partir de ``tests/fixtures/voice_es_16k.wav`` (voz cercana) y
  ``tests/fixtures/echo_48k.wav`` (far-end del interlocutor). Lo que se escribe
  en el sink físico aparece en su monitor y la señal del micrófono contiene
  una copia retardada de ese far-end, de modo que el AEC puede medirse sin
  dispositivos. No ejecuta ningún comando externo.

La interfaz expone cinco operaciones obligatorias (``start``, ``stop``,
``read_mic_48k``, ``write_tx_48k``, ``write_physical_sink_48k``) más
:meth:`AudioTransport.read_physical_sink_monitor_48k`, necesaria para obtener
la referencia del AEC desde el monitor del sink físico y no desde el stream Tx.
"""

from __future__ import annotations

import re
import subprocess
import wave
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import IO, Protocol, cast

import numpy as np
from numpy.typing import NDArray

from client.audio.echo_cancel import DEFAULT_DELAY_MS
from client.audio.resampler import SampleInput, resample_to_48k
from shared.constants import PIPEWIRE_SAMPLE_RATE, SAMPLE_RATE

DEFAULT_FIXTURES_DIR: Path = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
MAX_READ_SAMPLES: int = PIPEWIRE_SAMPLE_RATE
"""Tamaño máximo de una lectura (1 s a 48 kHz); evita asignaciones desbocadas."""

DEFAULT_TIMEOUT_S: float = 5.0
DEFAULT_TX_SINK: str = "Translator_Tx_Master"
DEFAULT_TAIL_SAMPLES: int = 19_200
"""400 ms de eco sin voz cercana para medir la atenuación del AEC."""

_NODE_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
_PIPEWIRE_PLACEHOLDERS = frozenset(
    {"@DEFAULT_SOURCE@", "@DEFAULT_SINK@", "@DEFAULT_MONITOR_SOURCE@"}
)


class TransportError(RuntimeError):
    """Fallo del transporte con código estable (sin trazas ni secretos)."""

    def __init__(self, message: str, *, code: str = "NO_PIPEWIRE") -> None:
        self.code = code
        super().__init__(f"[{code}] {message}")


class AudioProcess(Protocol):
    """Proceso ``pw-record``/``pw-play`` con el mínimo surface que necesitamos."""

    stdin: IO[bytes] | None
    stdout: IO[bytes] | None
    stderr: IO[bytes] | None

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class ProcessRunner(Protocol):
    """Frontera inyectable para crear procesos; los tests nunca la usan."""

    def spawn(self, args: Sequence[str]) -> AudioProcess:
        """Lanza ``args`` sin shell y devuelve el proceso creado."""
        ...


class SubprocessProcessRunner:
    """Implementación real de :class:`ProcessRunner` sobre ``subprocess``."""

    def __init__(self, timeout: float = DEFAULT_TIMEOUT_S) -> None:
        if isinstance(timeout, bool) or not isinstance(timeout, int | float):
            raise ValueError("timeout debe ser un número real")
        if not np.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout debe ser finito y > 0")
        self.timeout = float(timeout)

    def spawn(self, args: Sequence[str]) -> AudioProcess:
        argv = tuple(args)
        if not argv:
            raise TransportError("lista de comandos vacía")
        try:
            process: AudioProcess = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
            )
        except OSError as exc:
            raise TransportError(
                f"No se pudo iniciar '{argv[0]}': comprueba 'pw-record' y 'pw-play'."
            ) from exc
        return process

    def wait(self, process: AudioProcess) -> None:
        """Espera con timeout y escalate a ``TransportError`` si se cuelga."""
        try:
            process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            with suppress(OSError):
                process.kill()
            raise TransportError(
                f"El proceso de audio tardó más de {self.timeout:g} s en terminar"
            ) from exc


class AudioTransport(ABC):
    """Frontera de audio 48 kHz: captura, monitor de referencia y salida Tx."""

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """``True`` entre :meth:`start` y :meth:`stop`."""

    @abstractmethod
    def start(self) -> None:
        """Abre los dispositivos/flujos; idempotente mientras esté activo."""

    @abstractmethod
    def stop(self) -> None:
        """Cierra los flujos; idempotente y sustituible por una señal."""

    @abstractmethod
    def read_mic_48k(self, num_samples: int) -> NDArray[np.int16]:
        """Lee ``num_samples`` del micrófono físico (puede contener eco)."""

    @abstractmethod
    def read_physical_sink_monitor_48k(self, num_samples: int) -> NDArray[np.int16]:
        """Lee ``num_samples`` del monitor del sink físico (referencia AEC)."""

    @abstractmethod
    def write_tx_48k(self, samples: SampleInput) -> int:
        """Escribe PCM en `Translator_Tx_Mic`; devuelve muestras escritas."""

    @abstractmethod
    def write_physical_sink_48k(self, samples: SampleInput) -> int:
        """Escribe el far-end en el sink físico; devuelve muestras escritas."""


def _validate_count(num_samples: int, name: str = "num_samples") -> int:
    if isinstance(num_samples, bool) or not isinstance(num_samples, int):
        raise ValueError(f"{name} debe ser int, recibido {type(num_samples).__name__}")
    if not 0 < num_samples <= MAX_READ_SAMPLES:
        raise ValueError(f"{name} fuera de rango (0, {MAX_READ_SAMPLES}]: {num_samples}")
    return num_samples


def _validate_node(name: str, label: str) -> str:
    if not isinstance(name, str):
        raise ValueError(f"{label} debe ser str, recibido {type(name).__name__}")
    if name not in _PIPEWIRE_PLACEHOLDERS and _NODE_NAME_RE.fullmatch(name) is None:
        raise ValueError(f"{label} contiene caracteres no permitidos: {name!r}")
    return name


def _as_int16(samples: SampleInput, name: str) -> NDArray[np.int16]:
    """Valida PCM en la frontera y lo normaliza a int16 @48 kHz."""
    try:
        normalized = resample_to_48k(
            samples, src_rate=PIPEWIRE_SAMPLE_RATE, dtype=np.int16
        )
    except ValueError as exc:
        raise TransportError(f"{name} inválido: {exc}", code="FORMAT") from exc
    return cast("NDArray[np.int16]", normalized)


def _read_exact(stream: IO[bytes] | None, count: int, label: str) -> bytes:
    if stream is None:
        raise TransportError(f"flujo de {label} no disponible")
    parts: list[bytes] = []
    remaining = count
    while remaining > 0:
        part = stream.read(remaining)
        if not part:
            raise TransportError(f"el flujo de {label} terminó antes de {count} bytes")
        parts.append(part)
        remaining -= len(part)
    return b"".join(parts)


class PipeWireAudioTransport(AudioTransport):
    """Transporte PipeWire real mediante ``pw-record`` / ``pw-play``.

    ``Translator_Tx_Mic`` es un source virtual: se alimenta reproduciendo en
    su sink maestro ``Translator_Tx_Master`` (ARCHITECTURE.md §2.1). El
    monitor del sink físico se captura con un segundo ``pw-record`` para que
    el AEC reciba exactamente la señal que el interlocutor está escuchando.
    """

    def __init__(
        self,
        runner: ProcessRunner | None = None,
        *,
        mic_source: str = "@DEFAULT_SOURCE@",
        tx_sink: str = DEFAULT_TX_SINK,
        physical_sink: str = "@DEFAULT_SINK@",
    ) -> None:
        self.mic_source = _validate_node(mic_source, "mic_source")
        self.tx_sink = _validate_node(tx_sink, "tx_sink")
        self.physical_sink = _validate_node(physical_sink, "physical_sink")
        self._runner: ProcessRunner = runner if runner is not None else SubprocessProcessRunner()
        self._processes: list[AudioProcess] = []
        self._mic: AudioProcess | None = None
        self._monitor: AudioProcess | None = None
        self._tx: AudioProcess | None = None
        self._sink: AudioProcess | None = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return
        record_args = (
            "--format=s16",
            f"--rate={PIPEWIRE_SAMPLE_RATE}",
            "--channels=1",
        )
        play_args = (
            "--format=s16",
            f"--rate={PIPEWIRE_SAMPLE_RATE}",
            "--channels=1",
        )
        try:
            self._mic = self._runner.spawn(
                ("pw-record", *record_args, "--target", self.mic_source)
            )
            self._monitor = self._runner.spawn(
                ("pw-record", *record_args, "--target", f"{self.physical_sink}.monitor")
            )
            self._tx = self._runner.spawn(("pw-play", *play_args, "--target", self.tx_sink))
            self._sink = self._runner.spawn(
                ("pw-play", *play_args, "--target", self.physical_sink)
            )
        except TransportError:
            self._close_all()
            raise
        self._processes = [self._mic, self._monitor, self._tx, self._sink]
        self._running = True

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._close_all()

    def read_mic_48k(self, num_samples: int) -> NDArray[np.int16]:
        count = _validate_count(num_samples)
        self._require_running()
        mic = self._require_process(self._mic, "micrófono")
        raw = _read_exact(mic.stdout, count * 2, "micrófono")
        return np.frombuffer(raw, dtype="<i2").copy()

    def read_physical_sink_monitor_48k(self, num_samples: int) -> NDArray[np.int16]:
        count = _validate_count(num_samples)
        self._require_running()
        monitor = self._require_process(self._monitor, "monitor del sink físico")
        raw = _read_exact(monitor.stdout, count * 2, "monitor del sink físico")
        return np.frombuffer(raw, dtype="<i2").copy()

    def write_tx_48k(self, samples: SampleInput) -> int:
        self._require_running()
        return self._write(self._tx, samples, "Translator_Tx_Mic")

    def write_physical_sink_48k(self, samples: SampleInput) -> int:
        self._require_running()
        return self._write(self._sink, samples, "sink físico")

    def _write(self, process: AudioProcess | None, samples: SampleInput, label: str) -> int:
        pcm = _as_int16(samples, label)
        if process is None or process.stdin is None:
            raise TransportError(f"flujo de salida de {label} no disponible")
        try:
            process.stdin.write(pcm.tobytes())
            process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise TransportError(f"se cerró el flujo de salida de {label}") from exc
        return int(pcm.size)

    def _require_running(self) -> None:
        if not self._running:
            raise TransportError("el transporte no está iniciado; llame a start()")

    @staticmethod
    def _require_process(process: AudioProcess | None, label: str) -> AudioProcess:
        if process is None:
            raise TransportError(f"proceso de {label} no disponible")
        return process

    def _close_all(self) -> None:
        runner = self._runner
        for process in self._processes:
            if process.stdin is not None:
                with suppress(OSError):
                    process.stdin.close()
            with suppress(OSError):
                process.terminate()
        for process in self._processes:
            if isinstance(runner, SubprocessProcessRunner):
                runner.wait(process)
            else:
                with suppress(OSError):
                    process.wait(timeout=DEFAULT_TIMEOUT_S)
        self._processes.clear()
        self._mic = self._monitor = self._tx = self._sink = None


class MockAudioTransport(AudioTransport):
    """Transporte en memoria para tests E2E: fixture de voz + eco determinista.

    Es una implementación completa de :class:`AudioTransport`: el micrófono
    contiene voz cercana (``voice_es_16k.wav`` resampleado a 48 kHz) más una
    copia retardada del far-end (``echo_48k.wav``) y el monitor del sink
    físico devuelve exactamente ese far-end. No abre dispositivos ni ejecuta
    comandos; ``start()`` reinicia todos los cursores y búferes de salida.
    """

    def __init__(
        self,
        fixtures_dir: Path | None = None,
        *,
        near_end: NDArray[np.int16] | None = None,
        far_end: NDArray[np.int16] | None = None,
        echo_delay_ms: float = DEFAULT_DELAY_MS,
        tail_samples: int = DEFAULT_TAIL_SAMPLES,
    ) -> None:
        directory = fixtures_dir if fixtures_dir is not None else DEFAULT_FIXTURES_DIR
        self.near_end = (
            near_end.copy() if near_end is not None else self._load_near_end(directory)
        )
        self.far_end = far_end.copy() if far_end is not None else self._load_far_end(directory)
        if not isinstance(echo_delay_ms, int | float) or isinstance(echo_delay_ms, bool):
            raise ValueError("echo_delay_ms debe ser un número real")
        if not np.isfinite(echo_delay_ms) or not 0 <= echo_delay_ms <= 5000:
            raise ValueError("echo_delay_ms debe estar en [0, 5000]")
        if isinstance(tail_samples, bool) or not isinstance(tail_samples, int):
            raise ValueError("tail_samples debe ser int")
        if not 0 <= tail_samples <= PIPEWIRE_SAMPLE_RATE * 10:
            raise ValueError("tail_samples fuera de rango")

        delay = round(float(echo_delay_ms) * PIPEWIRE_SAMPLE_RATE / 1000.0)
        total = max(int(self.near_end.size), int(self.far_end.size) + delay) + tail_samples
        mic = np.zeros(total, dtype=np.int64)
        mic[: self.near_end.size] += self.near_end.astype(np.int64)
        echo_start = delay
        echo_end = min(delay + self.far_end.size, total)
        mic[echo_start:echo_end] += self.far_end[: echo_end - echo_start].astype(np.int64)
        limits = np.iinfo(np.int16)
        self.mic_pcm: NDArray[np.int16] = np.clip(
            mic, limits.min, limits.max
        ).astype(np.int16)

        self._monitor: deque[int] = deque(maxlen=PIPEWIRE_SAMPLE_RATE * 5)
        self._mic_offset = 0
        self._running = False
        self.tx_writes: list[NDArray[np.int16]] = []
        self.sink_written_samples = 0
        self.mic_reads = 0
        self.monitor_reads = 0

    @staticmethod
    def _read_wav(path: Path, expected_rate: int) -> NDArray[np.int16]:
        if not path.is_file():
            raise TransportError(f"falta el fixture {path.name}", code="FIXTURE")
        with wave.open(str(path), "rb") as handle:
            if handle.getnchannels() != 1 or handle.getsampwidth() != 2:
                raise TransportError(f"formato inválido en {path.name}", code="FIXTURE")
            if handle.getframerate() != expected_rate:
                raise TransportError(f"tasa inválida en {path.name}", code="FIXTURE")
            raw = handle.readframes(handle.getnframes())
        return np.frombuffer(raw, dtype="<i2").copy()

    @classmethod
    def _load_near_end(cls, directory: Path) -> NDArray[np.int16]:
        raw = cls._read_wav(directory / "voice_es_16k.wav", SAMPLE_RATE)
        upsampled = resample_to_48k(raw, src_rate=SAMPLE_RATE, dtype=np.int16)
        return cast("NDArray[np.int16]", upsampled)

    @classmethod
    def _load_far_end(cls, directory: Path) -> NDArray[np.int16]:
        return cls._read_wav(directory / "echo_48k.wav", PIPEWIRE_SAMPLE_RATE)

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def monitor_source_name(self) -> str:
        """Nombre lógico del monitor del sink físico usado como referencia AEC."""
        return "@DEFAULT_SINK@.monitor"

    def start(self) -> None:
        self._running = True
        self._mic_offset = 0
        self._monitor.clear()
        self.tx_writes.clear()
        self.sink_written_samples = 0
        self.mic_reads = 0
        self.monitor_reads = 0

    def stop(self) -> None:
        self._running = False
        self._monitor.clear()

    def read_mic_48k(self, num_samples: int) -> NDArray[np.int16]:
        count = _validate_count(num_samples)
        self._require_running()
        start = self._mic_offset
        end = min(start + count, self.mic_pcm.size)
        chunk = self.mic_pcm[start:end]
        self._mic_offset = end
        self.mic_reads += 1
        # El interlocutor sigue sonando: lo que se reproduce en el sink físico
        # alimenta su monitor y, con retardo, el eco capturado por el mic.
        # Cuando el micrófono se agota se sigue alimentando silencio para que
        # la ventana deslizante del AEC se vacíe y el VAD pueda cerrar.
        if start < self.mic_pcm.size and self.far_end.size:
            echo = np.arange(start, end) % self.far_end.size
            self._monitor.extend(int(value) for value in self.far_end[echo])
        else:
            self._monitor.extend(0 for _ in range(count))
        if chunk.size < count:
            chunk = np.pad(chunk, (0, count - chunk.size))
        return chunk.astype(np.int16, copy=True)

    def read_physical_sink_monitor_48k(self, num_samples: int) -> NDArray[np.int16]:
        count = _validate_count(num_samples)
        self._require_running()
        self.monitor_reads += 1
        values = [self._monitor.popleft() if self._monitor else 0 for _ in range(count)]
        return np.asarray(values, dtype=np.int16)

    def write_tx_48k(self, samples: SampleInput) -> int:
        self._require_running()
        pcm = _as_int16(samples, "Translator_Tx_Mic")
        self.tx_writes.append(pcm.copy())
        return int(pcm.size)

    def write_physical_sink_48k(self, samples: SampleInput) -> int:
        self._require_running()
        pcm = _as_int16(samples, "sink físico")
        self._monitor.extend(int(value) for value in pcm)
        self.sink_written_samples += int(pcm.size)
        return int(pcm.size)

    def _require_running(self) -> None:
        if not self._running:
            raise TransportError("el transporte mock no está iniciado; llame a start()")


__all__ = [
    "AudioProcess",
    "AudioTransport",
    "MockAudioTransport",
    "PipeWireAudioTransport",
    "ProcessRunner",
    "SubprocessProcessRunner",
    "TransportError",
]
