"""Tests del verificador de entorno PipeWire (TASK-002).

Todos los casos usan ``--dry-run`` con fixtures embebidas: no requieren
hardware, ni PipeWire real, ni los binarios del sistema.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK_SCRIPT = REPO_ROOT / "scripts" / "check-pipewire.sh"


def run_check(
    *args: str,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Ejecuta el script de forma aislada del entorno PipeWire de la máquina."""
    env = os.environ.copy()
    env.pop("BABELPIPE_DRY_RUN_MISSING", None)
    env.pop("BABELPIPE_DRY_RUN_INACTIVE", None)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash", str(CHECK_SCRIPT), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_script_exists() -> None:
    assert CHECK_SCRIPT.is_file(), f"No existe el script {CHECK_SCRIPT}"


def test_dry_run_ok() -> None:
    result = run_check("--dry-run")
    assert result.returncode == 0, f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    assert "ENTORNO OK" in result.stdout


def test_missing_component() -> None:
    result = run_check(
        "--dry-run",
        extra_env={"BABELPIPE_DRY_RUN_MISSING": "pactl"},
    )
    assert result.returncode == 1, f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    output = result.stdout + result.stderr
    assert "pactl" in output, f"El mensaje no menciona 'pactl': {output!r}"


def test_json_mode() -> None:
    result = run_check("--dry-run", "--json")
    assert result.returncode == 0, f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    payload = json.loads(result.stdout)
    assert {"ok", "components", "errors"}.issubset(payload), payload
    assert payload["ok"] is True
    assert isinstance(payload["components"], dict)
    assert isinstance(payload["errors"], list)
    assert payload["errors"] == []
    for binary in ("pactl", "pw-cli", "pw-dump", "pipewire", "wireplumber"):
        assert binary in payload["components"]


def test_json_mode_reports_missing_component() -> None:
    result = run_check(
        "--dry-run",
        "--json",
        extra_env={"BABELPIPE_DRY_RUN_MISSING": "pactl"},
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["components"]["pactl"]["status"] == "missing"
    assert any("pactl" in error for error in payload["errors"])


def test_inactive_pipewire_returns_exit_two() -> None:
    result = run_check(
        "--dry-run",
        extra_env={"BABELPIPE_DRY_RUN_INACTIVE": "1"},
    )
    assert result.returncode == 2, f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
    assert "no activo" in result.stdout


def test_help() -> None:
    result = run_check("--help")
    assert result.returncode == 0
    for option in ("--dry-run", "--json", "--help"):
        assert option in result.stdout
