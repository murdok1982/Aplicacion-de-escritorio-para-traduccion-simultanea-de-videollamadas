"""Estado persistente ligero del cliente BabelPipe (REQ-07, AC-12).

Contrato (ARCHITECTURE.md §3, §6):
* :class:`AppState` guarda preferencias de perfil — idiomas Rx/Tx, ``duck_level``,
  PTT y geometría del overlay — con Pydantic v2 estricto (``extra="forbid"``).
* **No contiene secretos**: el token vive exclusivamente en ``client.config``.
* Ruta por defecto ``$XDG_CONFIG_HOME/babelpipe/state.json`` (o
  ``~/.config/babelpipe/state.json``). Directorio ``0o700``, fichero ``0o600``.
* Escritura atómica: temporal en el mismo directorio + :func:`os.replace`.
* Un fichero corrupto NO rompe la app: se avisa con ``UserWarning`` y se usa
  el estado por defecto.
"""

from __future__ import annotations

import os
import tempfile
import warnings
from collections.abc import Mapping
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from client.config import (
    DEFAULT_DUCK_LEVEL,
    DEFAULT_SOURCE_LANG,
    DEFAULT_TARGET_LANG,
    DUCK_LEVEL_MAX,
    DUCK_LEVEL_MIN,
)

STATE_FILENAME: str = "state.json"
DEFAULT_PROFILE_NAME: str = "default"
_ERROR_HINTS: dict[str, str] = {
    "extra_forbidden": "campo desconocido en el estado guardado",
    "greater_than_equal": "valor por debajo del mínimo permitido",
    "less_than_equal": "valor por encima del máximo permitido",
    "int_type": "se esperaba un número entero",
    "float_type": "se esperaba un número decimal",
    "bool_type": "se esperaba true o false",
    "string_type": "se esperaba texto",
    "model_type": "se esperaba un objeto JSON",
    "json_invalid": "JSON mal formado",
}


class _StrictModel(BaseModel):
    """Base común: rechaza campos desconocidos, es inmutable y es estricta."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class AppState(_StrictModel):
    """Preferencias persistentes del perfil activo, sin secretos."""

    profile_name: str = Field(default=DEFAULT_PROFILE_NAME, min_length=1, max_length=64)
    last_rx_lang: str = Field(default=DEFAULT_SOURCE_LANG, min_length=2, max_length=10)
    last_tx_lang: str = Field(default=DEFAULT_TARGET_LANG, min_length=2, max_length=10)
    duck_level: float = Field(default=DEFAULT_DUCK_LEVEL, ge=DUCK_LEVEL_MIN, le=DUCK_LEVEL_MAX)
    ptt_enabled: bool = False
    overlay_x: int | None = Field(default=None, ge=0, le=100_000)
    overlay_y: int | None = Field(default=None, ge=0, le=100_000)
    overlay_width: int | None = Field(default=None, ge=1, le=100_000)
    overlay_height: int | None = Field(default=None, ge=1, le=100_000)

    def save(self, path: Path | None = None) -> Path:
        """Persiste el estado de forma atómica y devuelve la ruta escrita.

        El directorio se crea con ``0o700`` y el fichero temporal se fija a
        ``0o600`` antes de reemplazar el destino, evitando ventanas de lectura
        con permisos permisivos.
        """
        target = path if path is not None else default_state_path()
        parent = target.parent
        _ensure_private_dir(parent)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=parent)
        tmp_path = Path(tmp_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(self.model_dump_json(indent=2))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        return target

    @classmethod
    def load(cls, path: Path | None = None) -> AppState:
        """Carga el estado; si está corrupto avisa y devuelve el estado inicial.

        Un fichero ausente es un caso normal (primer arranque) y no genera
        advertencia. Nunca se propaga una excepción por contenido inválido.
        """
        target = path if path is not None else default_state_path()
        try:
            raw = target.read_text(encoding="utf-8")
        except FileNotFoundError:
            return cls()
        except OSError as exc:
            warnings.warn(
                f"No se pudo leer el estado en {target}: {exc.strerror or 'error de E/S'}. "
                "Se usa el estado por defecto.",
                UserWarning,
                stacklevel=2,
            )
            return cls()
        try:
            return cls.model_validate_json(raw)
        except ValidationError as exc:
            warnings.warn(
                f"Estado corrupto en {target}: {_describe_validation(exc)}. "
                "Se usa el estado por defecto.",
                UserWarning,
                stacklevel=2,
            )
            return cls()

    def reset(self, path: Path | None = None) -> AppState:
        """Vuelve al estado por defecto, lo persiste y lo devuelve."""
        fresh = AppState()
        fresh.save(path)
        return fresh


def default_state_path(env: Mapping[str, str] | None = None) -> Path:
    """Ruta ``$XDG_CONFIG_HOME/babelpipe/state.json`` (o ``~/.config/...``).

    ``env`` permite aislar tests del ``HOME`` real sin ``monkeypatch`` global.
    """
    environ: Mapping[str, str] = os.environ if env is None else env
    xdg = environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "babelpipe" / STATE_FILENAME


def _ensure_private_dir(directory: Path) -> None:
    """Crea (si hace falta) el directorio con permisos ``0o700``."""
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)


def _describe_validation(exc: ValidationError) -> str:
    """Resume errores sin incluir los valores de entrada del fichero leído."""
    details: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", ())) or "estado"
        hint = _ERROR_HINTS.get(str(error.get("type", "")), "valor inválido")
        details.append(f"{location}: {hint}")
    return "; ".join(details)


__all__ = ["AppState", "default_state_path"]
