"""Gestión idempotente de nodos PipeWire virtuales para BabelPipe (TASK-004).

Contrato (SPEC.md REQ-01 / AC-01 / AC-02, ARCHITECTURE.md §2.1):

* ``Translator_Rx_Sink``: sink virtual 48 kHz estéreo over ``module-null-sink``.
* ``Translator_Tx_Mic``: source virtual estable construida con un
  ``module-null-sink`` maestro y un ``module-remap-source`` desde su
  ``.monitor``.
* La existencia previa se comprueba con ``pw-dump`` antes de cargar módulos.
* ``destroy_all()`` descarga únicamente los IDs de módulo cargados por esta
  instancia. Nunca toca módulos de otras aplicaciones ni del sistema.

El comando se ejecuta siempre sin shell y sin ``sudo``: los permisos de
PipeWire son los del usuario de sesión (ARCHITECTURE.md §6).
"""

from __future__ import annotations

import atexit
import json
import re
import signal
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import FrameType
from typing import Protocol, cast

RX_SINK_NAME = "Translator_Rx_Sink"
TX_SOURCE_NAME = "Translator_Tx_Mic"
TX_MASTER_SINK_NAME = "Translator_Tx_Master"
PIPEWIRE_RATE = 48_000
PIPEWIRE_CHANNELS = 2

_NODE_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
_MODULE_ID_RE = re.compile(r"[0-9]+\Z")

_PreviousHandler = Callable[[int, FrameType | None], object] | int | None


class PipeWireError(RuntimeError):
    """Error de PipeWire con un código estable y un mensaje accionable.

    ``code`` replica la semántica de SPEC.md §5 (``NO_PIPEWIRE``) para que la
    capa de UI pueda mostrar el banner recuperable sin parsear texto libre.
    """

    def __init__(self, message: str, *, code: str = "NO_PIPEWIRE") -> None:
        self.code = code
        super().__init__(f"[{code}] {message}")


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Resultado normalizado de un comando externo."""

    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


class CommandRunner(Protocol):
    """Frontera inyectable para ejecutar ``pactl``/``pw-dump``.

    Los tests sustituyen esto por un runner en memoria; el código de producción
    usa ``SubprocessCommandRunner``. No hay mocks en la ruta de producción.
    """

    def run(self, args: Sequence[str]) -> CommandResult:
        """Ejecuta ``args`` sin shell y devuelve su resultado."""
        ...


class SubprocessCommandRunner:
    """Implementación real: ``subprocess.run`` sin shell, sin sudo, con timeout."""

    def __init__(self, timeout: float = 10.0) -> None:
        if timeout <= 0:
            raise ValueError("El timeout del runner debe ser positivo")
        self.timeout = timeout

    def run(self, args: Sequence[str]) -> CommandResult:
        """Ejecuta el comando y normaliza stdout/stderr/código de salida."""
        argv = list(args)
        if not argv:
            raise PipeWireError("Se intentó ejecutar una lista de comandos vacía")
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise PipeWireError(
                f"El comando '{argv[0]}' tardó más de {self.timeout:g} s. "
                "Comprueba que PipeWire responda con 'pactl info'."
            ) from exc
        return CommandResult(
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
            returncode=completed.returncode,
        )


class PipeWireGraph:
    """Crea y destruye de forma idempotente los nodos virtuales de BabelPipe."""

    def __init__(
        self,
        runner: CommandRunner | None = None,
        *,
        rx_sink_name: str = RX_SINK_NAME,
        tx_source_name: str = TX_SOURCE_NAME,
        tx_master_name: str = TX_MASTER_SINK_NAME,
        register_handlers: bool = True,
    ) -> None:
        for label, name in (
            ("rx_sink_name", rx_sink_name),
            ("tx_source_name", tx_source_name),
            ("tx_master_name", tx_master_name),
        ):
            if _NODE_NAME_RE.fullmatch(name) is None:
                raise ValueError(
                    f"{label} contiene caracteres no permitidos: {name!r}. "
                    "Solo se aceptan letras, dígitos, guion, guion bajo y punto."
                )

        self.rx_sink_name = rx_sink_name
        self.tx_source_name = tx_source_name
        self.tx_master_name = tx_master_name
        self._runner: CommandRunner = runner if runner is not None else SubprocessCommandRunner()
        self._module_ids: list[int] = []
        self._previous_handlers: dict[int, _PreviousHandler] = {}
        if register_handlers:
            self._register_handlers()

    @property
    def loaded_module_ids(self) -> tuple[int, ...]:
        """IDs de módulo cargados por esta instancia y aún no descargados."""
        return tuple(self._module_ids)

    def create_rx_sink(self) -> list[int]:
        """Crea ``Translator_Rx_Sink`` si no existe; devuelve los IDs cargados."""
        payload = self._dump_objects()
        if self._count_nodes(payload, self.rx_sink_name) > 0:
            return []
        return [
            self._load_module(
                "module-null-sink",
                f"sink_name={self.rx_sink_name}",
                f"sink_properties=device.description={self.rx_sink_name}",
                f"rate={PIPEWIRE_RATE}",
                f"channels={PIPEWIRE_CHANNELS}",
            )
        ]

    def create_tx_source(self) -> list[int]:
        """Crea ``Translator_Tx_Mic`` si no existe; devuelve los IDs cargados.

        El source virtual se construye con un sink maestro auxiliar y un
        ``module-remap-source`` que lee el ``.monitor`` de dicho sink. Si el
        maestro ya existiera (p. ej. de una sesión anterior), no se vuelve a
        cargar y, por tanto, no se registrará para su descarga: solo se
        descargan los módulos que esta instancia ha creado realmente.
        """
        payload = self._dump_objects()
        if self._count_nodes(payload, self.tx_source_name) > 0:
            return []

        created: list[int] = []
        if self._count_nodes(payload, self.tx_master_name) == 0:
            created.append(
                self._load_module(
                    "module-null-sink",
                    f"sink_name={self.tx_master_name}",
                    f"sink_properties=device.description={self.tx_master_name}",
                    f"rate={PIPEWIRE_RATE}",
                    f"channels={PIPEWIRE_CHANNELS}",
                )
            )
        created.append(
            self._load_module(
                "module-remap-source",
                f"master={self.tx_master_name}.monitor",
                f"source_name={self.tx_source_name}",
                f"source_properties=device.description={self.tx_source_name}",
                f"rate={PIPEWIRE_RATE}",
                f"channels={PIPEWIRE_CHANNELS}",
            )
        )
        return created

    def destroy_all(self) -> None:
        """Descarga solo los módulos creados por esta instancia (idempotente)."""
        atexit.unregister(self.destroy_all)
        if not self._module_ids:
            return

        failed: list[int] = []
        detail = "sin detalle"
        for module_id in reversed(self._module_ids):
            result = self._execute(("pactl", "unload-module", str(module_id)))
            if result.returncode != 0:
                failed.append(module_id)
                if result.stderr.strip():
                    detail = result.stderr.strip()
        self._module_ids = failed
        if failed:
            raise PipeWireError(
                f"No se pudieron descargar los módulos creados por BabelPipe: {failed}. "
                f"Detalle de 'pactl': {detail}. Comprueba 'pactl list short modules'."
            )

    def _register_handlers(self) -> None:
        """Registra limpieza en atexit y en SIGINT/SIGTERM (AC-02)."""
        atexit.register(self.destroy_all)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._previous_handlers[int(sig)] = signal.getsignal(sig)
            signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        """Limpia el grafo antes de propagar la terminación del proceso."""
        cleanup_error: PipeWireError | None = None
        try:
            self.destroy_all()
        except PipeWireError as exc:
            cleanup_error = exc

        previous = self._previous_handlers.get(signum)
        if callable(previous):
            previous(signum, frame)

        if cleanup_error is not None:
            raise cleanup_error
        # 128 + señal es el código de salida POSIX convencional (SIGTERM=143).
        raise SystemExit(128 + signum)

    def _execute(self, args: Sequence[str]) -> CommandResult:
        """Ejecuta un comando y convierte fallos de sistema en PipeWireError."""
        try:
            return self._runner.run(args)
        except FileNotFoundError as exc:
            raise PipeWireError(
                f"No se encontró el ejecutable '{args[0]}'. Instala PipeWire en el "
                "espacio de usuario ('sudo apt install pipewire wireplumber "
                "libpipewire-0.3-utils') y verifica 'pactl info' con tu usuario. "
                "BabelPipe nunca ejecuta sudo."
            ) from exc
        except OSError as exc:
            raise PipeWireError(
                f"Fallo del sistema al ejecutar '{args[0]}': {exc}. "
                "Comprueba los permisos del sockets de PipeWire de tu sesión."
            ) from exc

    def _dump_objects(self) -> list[object]:
        """Obtiene y valida el JSON de ``pw-dump``."""
        result = self._execute(("pw-dump",))
        if result.returncode != 0:
            raise PipeWireError(
                f"'pw-dump' terminó con código {result.returncode}: "
                f"{result.stderr.strip() or 'sin detalle'}. Asegúrate de que "
                "PipeWire esté activo ('pactl info')."
            )
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise PipeWireError(
                "'pw-dump' no devolvió JSON válido; actualiza libpipewire y "
                "comprueba 'pw-dump' en una terminal."
            ) from exc
        if not isinstance(payload, list):
            raise PipeWireError(
                "Formato inesperado de 'pw-dump': se esperaba una lista de objetos."
            )
        return cast(list[object], payload)

    @staticmethod
    def _count_nodes(payload: list[object], name: str) -> int:
        """Cuenta nodos cuyo nombre, node.name o device.description coincide."""
        return sum(1 for obj in payload if name in PipeWireGraph._names_of(obj))

    @staticmethod
    def _names_of(obj: object) -> set[str]:
        """Extrae los identificadores de nombre de un objeto de ``pw-dump``."""
        if not isinstance(obj, dict):
            return set()
        info = obj.get("info")
        if not isinstance(info, dict):
            return set()
        properties = info.get("properties")
        if not isinstance(properties, dict):
            properties = {}
        candidates = (
            info.get("name"),
            properties.get("node.name"),
            properties.get("device.description"),
        )
        return {value for value in candidates if isinstance(value, str)}

    def _load_module(self, module: str, *args: str) -> int:
        """Carga un módulo pactl, registra su ID y valida la respuesta."""
        result = self._execute(("pactl", "load-module", module, *args))
        if result.returncode != 0:
            raise PipeWireError(
                f"'pactl load-module {module}' falló: "
                f"{result.stderr.strip() or 'sin detalle'}. "
                "Comprueba 'pactl info' y los permisos del usuario."
            )
        stdout = result.stdout.strip()
        if _MODULE_ID_RE.fullmatch(stdout) is None:
            raise PipeWireError(
                f"'pactl load-module {module}' devolvió un ID de módulo inesperado "
                f"({stdout!r}); no es seguro continuar porque no se podría "
                "garantizar una limpieza selectiva. Revisa 'pactl list short modules'."
            )
        module_id = int(stdout)
        self._module_ids.append(module_id)
        return module_id
