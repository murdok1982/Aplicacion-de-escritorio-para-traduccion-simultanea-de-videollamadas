"""Tests de integración del grafo PipeWire (TASK-004, AC-01 y AC-02).

Aislamiento total: se inyecta un ``CommandRunner`` en memoria y se parchea
``subprocess.run`` para que cualquier comando real haga fallar el test.
"""

from __future__ import annotations

import json
import signal
import subprocess
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import NoReturn

import pytest

from client.audio.pipewire_graph import (
    CommandResult,
    PipeWireError,
    PipeWireGraph,
)

RX = "Translator_Rx_Sink"
TX = "Translator_Tx_Mic"
MASTER = "Translator_Tx_Master"
FOREIGN = "Unrelated_App_Sink"


@dataclass
class _Module:
    module: str
    args: tuple[str, ...]
    nodes: list[dict[str, object]] = field(default_factory=list)


class FakeCommandRunner:
    """Simula ``pw-dump``/``pactl`` sin ejecutar ni un solo comando real."""

    def __init__(self, *, pactl_missing: bool = False) -> None:
        self.pactl_missing = pactl_missing
        self.modules: dict[int, _Module] = {}
        self.commands: list[tuple[str, ...]] = []
        self._next_id = 1

    def run(self, args: Sequence[str]) -> CommandResult:
        argv = tuple(args)
        self.commands.append(argv)

        if argv[0] == "pw-dump":
            return CommandResult(stdout=json.dumps(self._dump_payload()), returncode=0)

        if argv[0] != "pactl":
            raise AssertionError(f"Comando inesperado (podría afectar al sistema): {argv}")

        if self.pactl_missing:
            raise FileNotFoundError(2, "No such file or directory", "pactl")
        if len(argv) < 2:
            return CommandResult(stderr="argv de pactl inválido", returncode=1)

        action = argv[1]
        if action == "info":
            server_string = "Server String: unix:/run/user/1000/pipewire-0\n"
            return CommandResult(stdout=server_string, returncode=0)
        if action == "load-module":
            return self._load(argv)
        if action == "unload-module":
            return self._unload(argv)
        return CommandResult(stderr=f"acción pactl no soportada: {action}", returncode=1)

    def _load(self, argv: tuple[str, ...]) -> CommandResult:
        if len(argv) < 3:
            return CommandResult(stderr="load-module sin nombre de módulo", returncode=1)
        module = argv[2]
        options = argv[3:]
        if module == "module-remap-source":
            node_name = self._option(options, "source_name")
            media_class = "Audio/Source"
        elif module == "module-null-sink":
            node_name = self._option(options, "sink_name")
            media_class = "Audio/Sink"
        else:
            return CommandResult(stderr=f"módulo no simulado: {module}", returncode=1)
        if node_name is None:
            return CommandResult(stderr="nombre de nodo ausente", returncode=1)

        module_id = self._next_id
        self._next_id += 1
        node: dict[str, object] = {
            "id": 100 + module_id,
            "type": "PipeWire:Interface:Node",
            "info": {
                "name": node_name,
                "properties": {
                    "node.name": node_name,
                    "device.description": node_name,
                    "media.class": media_class,
                },
            },
        }
        self.modules[module_id] = _Module(module=module, args=options, nodes=[node])
        return CommandResult(stdout=f"{module_id}\n", returncode=0)

    def _unload(self, argv: tuple[str, ...]) -> CommandResult:
        try:
            module_id = int(argv[2])
        except (IndexError, ValueError):
            return CommandResult(stderr="ID de módulo inválido", returncode=1)
        if module_id not in self.modules:
            return CommandResult(stderr=f"el módulo {module_id} no existe", returncode=1)
        del self.modules[module_id]
        return CommandResult(returncode=0)

    @staticmethod
    def _option(options: tuple[str, ...], key: str) -> str | None:
        prefix = f"{key}="
        for option in options:
            if option.startswith(prefix):
                return option[len(prefix) :]
        return None

    def _dump_payload(self) -> list[dict[str, object]]:
        return [node for module in self.modules.values() for node in module.nodes]

    def node_names(self) -> list[str]:
        names: list[str] = []
        for node in self._dump_payload():
            info = node["info"]
            assert isinstance(info, dict)
            name = info["name"]
            assert isinstance(name, str)
            names.append(name)
        return names

    def add_foreign_module(self) -> int:
        """Simula un nodo de otra aplicación que jamás debe ser descargado."""
        module_id = self._next_id
        self._next_id += 1
        node: dict[str, object] = {
            "id": 100 + module_id,
            "type": "PipeWire:Interface:Node",
            "info": {
                "name": FOREIGN,
                "properties": {"node.name": FOREIGN, "media.class": "Audio/Sink"},
            },
        }
        self.modules[module_id] = _Module(
            module="module-null-sink",
            args=(f"sink_name={FOREIGN}",),
            nodes=[node],
        )
        return module_id


@pytest.fixture(autouse=True)
def forbid_real_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    """Red de seguridad: cualquier subprocess.real rompe el test."""

    def fail(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(f"Se intentó ejecutar un comando real: {args} {kwargs}")

    monkeypatch.setattr(subprocess, "run", fail)


@pytest.fixture
def restore_signals() -> Iterator[None]:
    """Restaura los manejadores de señal del proceso tras cada test."""
    targets = (signal.SIGINT, signal.SIGTERM)
    saved = {sig: signal.getsignal(sig) for sig in targets}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


def test_ac01_idempotent_nodes(mock_mode: None) -> None:
    """AC-01: dos arranques consecutivos no duplican ningún nodo."""
    runner = FakeCommandRunner()
    graph = PipeWireGraph(runner, register_handlers=False)

    first_rx = graph.create_rx_sink()
    second_rx = graph.create_rx_sink()
    first_tx = graph.create_tx_source()
    second_tx = graph.create_tx_source()

    assert len(first_rx) == 1
    assert second_rx == []
    assert len(first_tx) == 2  # sink maestro + remap-source
    assert second_tx == []

    names = runner.node_names()
    assert names.count(RX) == 1, f"Rx duplicado o ausente: {names}"
    assert names.count(TX) == 1, f"Tx duplicado o ausente: {names}"
    assert names.count(MASTER) == 1, f"Maestro duplicado o ausente: {names}"

    remap_modules = [m for m in runner.modules.values() if m.module == "module-remap-source"]
    assert len(remap_modules) == 1
    assert any(f"source_name={TX}" == arg for arg in remap_modules[0].args)
    assert any(f"master={MASTER}.monitor" == arg for arg in remap_modules[0].args)

    # Ningún comando real: todo pasó por el runner en memoria.
    assert all(argv[0] in {"pw-dump", "pactl"} for argv in runner.commands)
    assert any(argv[:2] == ("pactl", "load-module") for argv in runner.commands)


def test_ac02_cleanup_on_signal(mock_mode: None, restore_signals: None) -> None:
    """AC-02: SIGTERM descarga solo los módulos creados por esta instancia."""
    runner = FakeCommandRunner()
    graph = PipeWireGraph(runner)
    graph.create_rx_sink()
    graph.create_tx_source()

    created_ids = set(runner.modules)
    assert created_ids, "El setup no creó ningún módulo"
    foreign_id = runner.add_foreign_module()

    sigterm_handler = signal.getsignal(signal.SIGTERM)
    assert callable(sigterm_handler), "No se registró el manejador de SIGTERM"
    assert callable(signal.getsignal(signal.SIGINT)), "No se registró el de SIGINT"

    with pytest.raises(SystemExit) as exit_info:
        sigterm_handler(signal.SIGTERM, None)
    assert exit_info.value.code == 143

    assert set(runner.modules) == {foreign_id}, "Se descargó un módulo ajeno o quedó uno propio"
    assert runner.node_names() == [FOREIGN]
    assert graph.loaded_module_ids == ()

    unload_commands = [
        argv
        for argv in runner.commands
        if len(argv) == 3 and argv[:2] == ("pactl", "unload-module")
    ]
    assert {int(argv[2]) for argv in unload_commands} == created_ids
    assert all(int(argv[2]) != foreign_id for argv in unload_commands)

    # Una segunda limpieza es inofensiva: no emite nuevos comandos.
    before = len(runner.commands)
    graph.destroy_all()
    assert len(runner.commands) == before


def test_missing_pactl_raises_pipewire_error(mock_mode: None) -> None:
    """pactl ausente se traduce en PipeWireError accionable, nunca en FileNotFoundError."""
    runner = FakeCommandRunner(pactl_missing=True)
    graph = PipeWireGraph(runner, register_handlers=False)

    with pytest.raises(PipeWireError) as exc_info:
        graph.create_rx_sink()

    assert exc_info.value.code == "NO_PIPEWIRE"
    assert "pactl" in str(exc_info.value)
    assert "PipeWire" in str(exc_info.value)
    assert runner.node_names() == []
