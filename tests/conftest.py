"""Configuración global de pytest para BabelPipe.

Registra ``--mock``: los tests de integración que simulan PipeWire usan el modo
mock por defecto, porque NUNCA se debe tocar el grafo real durante los tests.
La bandera sigue admitiéndose explícitamente para mayor claridad en CI.
"""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Añade la opción que habilita los tests con grafo PipeWire simulado."""
    parser.addoption(
        "--mock",
        action="store_true",
        default=True,
        help="Ejecuta los tests de PipeWire con un CommandRunner en memoria (sin hardware)",
    )


@pytest.fixture
def mock_mode(request: pytest.FixtureRequest) -> None:
    """Marca los tests simulados; el modo mock es el valor por defecto.

    No falla cuando no se pasa la bandera: ``default=True`` garantiza el
    aislamiento tanto en la suite diaria como en los gates sin flags.
    """
    assert request.config.getoption("--mock") is True, (
        "El modo mock está desactivado: los tests no deben ejecutar comandos reales"
    )
