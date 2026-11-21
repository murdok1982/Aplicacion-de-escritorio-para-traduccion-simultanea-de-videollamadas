"""AEC (WebRTC) y referencia del monitor del sink físico (TASK-007).

Contrato (SPEC.md REQ-06 / AC-10, ARCHITECTURE.md §2.1):

* En modo llamada, el micrófono físico se captura y el eco del interlocutor se
  elimina con ``libpipewire-module-echo-cancel`` (WebRTC AEC). La referencia
  de eco es el **monitor del sink físico**, no el stream Tx.
* :func:`build_aec_args` construye la cadena de argumentos del módulo con
  ``analog_gain_control=0 digital_gain_control=0 noise_suppression=1``.
* :class:`EchoCancelManager` carga y descarga **únicamente** el módulo AEC que
  él mismo creó. Es idempotente y no ejecuta shell ni ``sudo``: todos los
  comandos pasan por un :class:`CommandRunner` inyectable.
* :func:`suppress_echo` es un modelo determinista para tests sin hardware.
  **No sustituye al módulo WebRTC en producción**: solo replica la resta de
  la referencia retardada para poder medir la atenuación del eco con WAV.

La frontera de comandos es local (``Protocol`` propio) para no acoplar este
módulo a ``client.audio.pipewire_graph``; ambas clases son estructuralmente
compatibles, así que el runner real de ese módulo puede inyectarse aquí.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
from numpy.typing import NDArray

from shared.constants import PIPEWIRE_SAMPLE_RATE

# El módulo PipeWire nativo y los placeholders estándar del grafo.
ECHO_CANCEL_MODULE: str = "libpipewire-module-echo-cancel"
DEFAULT_SOURCE_NAME: str = "@DEFAULT_SOURCE@"
DEFAULT_SINK_NAME: str = "@DEFAULT_SINK@"

# Parámetros del modelo determinista (48 kHz es la tasa del grafo PipeWire).
DEFAULT_DELAY_MS: float = 120.0
DEFAULT_ATTENUATION_DB: float = 20.0
MAX_ATTENUATION_DB: float = 120.0
MAX_DELAY_MS: float = 5_000.0

_NODE_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
_MODULE_ID_RE = re.compile(r"[0-9]+\Z")
_PIPEWIRE_PLACEHOLDERS = frozenset(
    {"@DEFAULT_SOURCE@", "@DEFAULT_SINK@", "@DEFAULT_MONITOR_SOURCE@"}
)
_ONE = 1
_ZERO = 0


class EchoCancelError(RuntimeError):
    """Fallo al cargar/descargar el módulo AEC, con código estable."""

    def __init__(self, message: str, *, code: str = "NO_PIPEWIRE") -> None:
        self.code = code
        super().__init__(f"[{code}] {message}")


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Resultado normalizado de un comando externo (sin shell)."""

    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


class CommandRunner(Protocol):
    """Frontera inyectable para ejecutar ``pactl`` sin shell y sin sudo."""

    def run(self, args: Sequence[str]) -> CommandResult:
        """Ejecuta ``args`` y devuelve su resultado."""
        ...


class SubprocessCommandRunner:
    """Implementación real: ``subprocess.run`` sin shell, sin sudo, con timeout."""

    def __init__(self, timeout: float = 10.0) -> None:
        if isinstance(timeout, bool) or not isinstance(timeout, int | float):
            raise ValueError("timeout debe ser un número real")
        if not np.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout debe ser finito y > 0")
        self.timeout = float(timeout)

    def run(self, args: Sequence[str]) -> CommandResult:
        """Ejecuta el comando y normaliza stdout/stderr/código de salida."""
        argv = list(args)
        if not argv:
            raise EchoCancelError("Se intentó ejecutar una lista de comandos vacía")
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout,
                shell=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise EchoCancelError(
                f"El comando '{argv[0]}' tardó más de {self.timeout:g} s; "
                "comprueba 'pactl info'."
            ) from exc
        return CommandResult(
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
            returncode=completed.returncode,
        )


@dataclass(frozen=True, slots=True)
class EchoCancelStatus:
    """Estado observable del módulo AEC propiedad de esta instancia."""

    enabled: bool
    module_id: int | None
    module_name: str
    source_name: str
    sink_name: str
    aec_args: str


def _validate_node_name(value: str, label: str) -> str:
    """Valida un nombre de nodo PipeWire o un placeholder estándar."""
    if not isinstance(value, str):
        raise ValueError(f"{label} debe ser str, recibido {type(value).__name__}")
    if value in _PIPEWIRE_PLACEHOLDERS:
        return value
    if _NODE_NAME_RE.fullmatch(value) is None:
        raise ValueError(
            f"{label} contiene caracteres no permitidos: {value!r}. "
            "Solo letras, dígitos, guion, guion bajo y punto (o un placeholder @DEFAULT_*)."
        )
    return value


def _validate_bool(value: bool, label: str) -> bool:
    """Valida una opción booleana de la frontera de configuración."""
    if not isinstance(value, bool):
        raise ValueError(f"{label} debe ser bool, recibido {type(value).__name__}")
    return value


def _bit(value: bool) -> int:
    return _ONE if value else _ZERO


def build_aec_args(
    source_name: str,
    sink_name: str,
    *,
    noise_suppression: bool = True,
    analog_agc: bool = False,
    digital_agc: bool = False,
) -> str:
    """Construye los argumentos de ``libpipewire-module-echo-cancel``.

    Devuelve una cadena con la forma
    ``source.name=<mic> sink.name=<sink>.monitor aec_args="..."``. El canal de
    referencia es el **monitor del sink físico**: si ``sink_name`` ya termina
    en ``.monitor`` se respeta tal cual; si no, se añade el sufijo.

    Los tres controles WebRTC se emiten siempre en el mismo orden para que la
    configuración sea reproducible y verificable::
        analog_gain_control=<0|1> digital_gain_control=<0|1> noise_suppression=<0|1>

    La validación de nombres evita inyectar opciones arbitrarias en la cadena
    de argumentos (no se usa shell, pero la frontera sigue siendo strict).
    """
    source = _validate_node_name(source_name, "source_name")
    sink = _validate_node_name(sink_name, "sink_name")
    suppression = _validate_bool(noise_suppression, "noise_suppression")
    analog = _validate_bool(analog_agc, "analog_agc")
    digital = _validate_bool(digital_agc, "digital_agc")

    reference = sink if sink.endswith(".monitor") else f"{sink}.monitor"
    webrtc_args = (
        f"analog_gain_control={_bit(analog)} "
        f"digital_gain_control={_bit(digital)} "
        f"noise_suppression={_bit(suppression)}"
    )
    return f'source.name={source} sink.name={reference} aec_args="{webrtc_args}"'


def _split_module_args(arguments: str) -> list[str]:
    """Tokeniza los argumentos; ``shlex`` no ejecuta nada (nunca hay shell)."""
    tokens = shlex.split(arguments)
    if not tokens:
        raise EchoCancelError("La cadena de argumentos AEC está vacía")
    return tokens


class EchoCancelManager:
    """Carga/descarga idempotente del único módulo AEC de BabelPipe.

    ``enable()`` devuelve el ID de módulo creado; llamadas posteriores no
    emiten comandos. ``disable()`` descarga ese ID y solo ese ID; una segunda
    llamada es inofensiva. ``status()`` expone el estado sin tocar PipeWire.
    """

    def __init__(
        self,
        runner: CommandRunner | None = None,
        *,
        source_name: str = DEFAULT_SOURCE_NAME,
        sink_name: str = DEFAULT_SINK_NAME,
        noise_suppression: bool = True,
        analog_agc: bool = False,
        digital_agc: bool = False,
    ) -> None:
        self.source_name = _validate_node_name(source_name, "source_name")
        self.sink_name = _validate_node_name(sink_name, "sink_name")
        self.noise_suppression = _validate_bool(noise_suppression, "noise_suppression")
        self.analog_agc = _validate_bool(analog_agc, "analog_agc")
        self.digital_agc = _validate_bool(digital_agc, "digital_agc")
        self._runner: CommandRunner = runner if runner is not None else SubprocessCommandRunner()
        self._aec_args = build_aec_args(
            self.source_name,
            self.sink_name,
            noise_suppression=self.noise_suppression,
            analog_agc=self.analog_agc,
            digital_agc=self.digital_agc,
        )
        self._module_id: int | None = None

    @property
    def module_id(self) -> int | None:
        """ID del módulo AEC cargado por esta instancia, o ``None``."""
        return self._module_id

    @property
    def aec_args(self) -> str:
        """Cadena de argumentos enviada al cargar el módulo."""
        return self._aec_args

    def status(self) -> EchoCancelStatus:
        """Devuelve el estado actual sin ejecutar ningún comando."""
        return EchoCancelStatus(
            enabled=self._module_id is not None,
            module_id=self._module_id,
            module_name=ECHO_CANCEL_MODULE,
            source_name=self.source_name,
            sink_name=self.sink_name,
            aec_args=self._aec_args,
        )

    def enable(self) -> int:
        """Carga el módulo AEC si no está ya cargado; devuelve su ID."""
        if self._module_id is not None:
            return self._module_id

        args = (ECHO_CANCEL_MODULE, *_split_module_args(self._aec_args))
        result = self._execute(("pactl", "load-module", *args))
        if result.returncode != 0:
            raise EchoCancelError(
                f"No se pudo cargar {ECHO_CANCEL_MODULE}: "
                f"{result.stderr.strip() or 'sin detalle'}. Comprueba 'pactl info'."
            )
        stdout = result.stdout.strip()
        if _MODULE_ID_RE.fullmatch(stdout) is None:
            raise EchoCancelError(
                f"'pactl load-module' devolvió un ID inesperado ({stdout!r}); "
                "no se puede garantizar una descarga selectiva."
            )
        self._module_id = int(stdout)
        return self._module_id

    def disable(self) -> None:
        """Descarga solo el módulo creado por esta instancia (idempotente)."""
        if self._module_id is None:
            return
        module_id = self._module_id
        result = self._execute(("pactl", "unload-module", str(module_id)))
        if result.returncode != 0:
            raise EchoCancelError(
                f"No se pudo descargar el módulo AEC {module_id}: "
                f"{result.stderr.strip() or 'sin detalle'}."
            )
        self._module_id = None

    def _execute(self, args: Sequence[str]) -> CommandResult:
        """Ejecuta un comando y convierte fallos de sistema en EchoCancelError."""
        try:
            return self._runner.run(args)
        except FileNotFoundError as exc:
            raise EchoCancelError(
                f"No se encontró '{args[0]}'. Instala PipeWire y verifica "
                "'pactl info' con tu usuario. BabelPipe nunca ejecuta sudo."
            ) from exc
        except OSError as exc:
            raise EchoCancelError(f"Fallo del sistema al ejecutar '{args[0]}': {exc}") from exc


def _validate_signal(signal: NDArray[Any], label: str) -> None:
    """Valida un bloque PCM 1-D, numérico y finito en la frontera."""
    if not isinstance(signal, np.ndarray):
        raise ValueError(f"{label} debe ser un numpy.ndarray, recibido {type(signal).__name__}")
    if signal.ndim != 1:
        raise ValueError(f"{label} debe ser unidimensional, recibido {signal.ndim} dimensiones")
    if signal.size == 0:
        raise ValueError(f"{label} no puede estar vacío")
    if not np.issubdtype(signal.dtype, np.number):
        raise ValueError(f"{label} debe tener dtype numérico, recibido {signal.dtype}")
    if not bool(np.all(np.isfinite(signal))):
        raise ValueError(f"{label} contiene muestras no finitas")


def _validate_real(value: float, label: str, *, low: float, high: float) -> float:
    """Valida un parámetro real acotado."""
    if isinstance(value, bool) or not isinstance(value, int | float | np.floating):
        raise ValueError(f"{label} debe ser un número real, recibido {type(value).__name__}")
    number = float(value)
    if not np.isfinite(number) or not low <= number <= high:
        raise ValueError(f"{label} fuera de rango [{low:g}, {high:g}]: {value!r}")
    return number


def suppress_echo(
    mic: NDArray[Any],
    reference: NDArray[Any],
    *,
    delay_ms: float = DEFAULT_DELAY_MS,
    attenuation_db: float = DEFAULT_ATTENUATION_DB,
) -> NDArray[Any]:
    """Modelo determinista de cancelación para tests sin hardware (NO producción).

    Resta de ``mic`` una copia de ``reference`` retardada ``delay_ms`` y
    escalada con ``1 - 10**(-attenuation_db/20)``; por tanto
    ``attenuation_db=20`` deja un 10 % del eco original (−20 dB). ``reference``
    es la señal del monitor del sink físico (far-end) y ``mic`` la captura del
    micrófono con el eco ya mezclado. Ambas se esperan en las mismas unidades
    (float normalizado o LSB int16) y la salida conserva el dtype de ``mic``.

    El retardo se calcula a 48 kHz, la tasa canónica del grafo PipeWire.
    **Este helper no sustituye al módulo WebRTC en producción**: en la ruta real
    el AEC lo hace ``libpipewire-module-echo-cancel``; aquí solo se modela la
    resta para poder verificar AC-10 con ``tests/fixtures/echo_48k.wav`` sin
    dispositivos de audio.
    """
    _validate_signal(mic, "mic")
    _validate_signal(reference, "reference")
    delay = _validate_real(delay_ms, "delay_ms", low=_ZERO, high=MAX_DELAY_MS)
    attenuation = _validate_real(
        attenuation_db, "attenuation_db", low=_ZERO, high=MAX_ATTENUATION_DB
    )

    delay_samples = int(round(delay * PIPEWIRE_SAMPLE_RATE / 1000.0))
    cancel_gain = 1.0 - 10.0 ** (-attenuation / 20.0)

    result = mic.astype(np.float64, copy=True)
    overlap = min(max(mic.size - delay_samples, 0), reference.size)
    if overlap > 0:
        result[delay_samples : delay_samples + overlap] -= (
            cancel_gain * reference[:overlap].astype(np.float64)
        )

    if np.issubdtype(mic.dtype, np.integer):
        limits = np.iinfo(mic.dtype)
        return np.clip(np.rint(result), limits.min, limits.max).astype(mic.dtype)
    return result.astype(mic.dtype, copy=False)
