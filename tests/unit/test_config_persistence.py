"""Tests de persistencia de configuración y perfiles (TASK-022 / AC-12).

Cobertura:
* Capas defaults < TOML < BABELPIPE_* < overrides, con errores accionables.
* Token como ``SecretStr``: ausente en ``repr``, ``to_toml`` y mensajes de error.
* ``config.toml.example`` / ``.env.example`` sin secretos reales.
* ``AppState`` roundtrip, corrupción tolerada, permisos 0o600/0o700 y reset.
* Sin instalación global de paquetes: el código propio no invoca el instalador.
"""

from __future__ import annotations

import stat
import tomllib
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from client.config import (
    DEFAULT_DUCK_LEVEL,
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_RX_SINK,
    DEFAULT_SOURCE_LANG,
    DEFAULT_TARGET_LANG,
    DEFAULT_TTS,
    ClientSettings,
    ConfigError,
    dump_example,
    load_settings,
)
from client.state import AppState, default_state_path

REPO_ROOT = Path(__file__).resolve().parents[2]
SECRET = "token-super-secreto-no-commitear-9f3a"

# Entorno deliberadamente vacío: ningún test depende del entorno de la máquina.
EMPTY_ENV: dict[str, str] = {}


def _write_toml(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# AC-12 — carga por capas
# ---------------------------------------------------------------------------


def test_defaults_layer() -> None:
    settings = load_settings(env=EMPTY_ENV)
    assert settings.server.host == DEFAULT_HOST
    assert settings.server.port == DEFAULT_PORT
    assert settings.server.token is None
    assert settings.audio.rx_sink == DEFAULT_RX_SINK
    assert settings.audio.duck_level == DEFAULT_DUCK_LEVEL == 0.12
    assert settings.audio.echo_cancel is True
    assert settings.audio.ptt_enabled is False
    assert settings.pipeline.source_lang == DEFAULT_SOURCE_LANG
    assert settings.pipeline.target_lang == DEFAULT_TARGET_LANG
    assert settings.pipeline.tts == DEFAULT_TTS
    assert settings.model.stt_path == ""


def test_toml_overrides_defaults(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
[server]
port = 9999
[audio]
duck_level = 0.15
[pipeline]
source_lang = "fr"
[model]
stt_path = "/modelos/whisper"
""",
    )
    settings = load_settings(config_path=path, env=EMPTY_ENV)
    assert settings.server.host == DEFAULT_HOST  # no tocado por el TOML
    assert settings.server.port == 9999
    assert settings.audio.duck_level == 0.15
    assert settings.pipeline.source_lang == "fr"
    assert settings.pipeline.target_lang == DEFAULT_TARGET_LANG
    assert settings.model.stt_path == "/modelos/whisper"


def test_env_overrides_toml(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
[audio]
duck_level = 0.15
[pipeline]
source_lang = "fr"
""",
    )
    env = {
        "BABELPIPE_AUDIO_DUCK_LEVEL": "0.10",
        "BABELPIPE_PIPELINE_SOURCE_LANG": "de",
        "BABELPIPE_AUDIO_PTT_ENABLED": "true",
    }
    settings = load_settings(config_path=path, env=env)
    assert settings.audio.duck_level == 0.10
    assert settings.pipeline.source_lang == "de"
    assert settings.audio.ptt_enabled is True


def test_explicit_overrides_env_and_toml(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
[audio]
duck_level = 0.15
[pipeline]
source_lang = "fr"
""",
    )
    env = {
        "BABELPIPE_AUDIO_DUCK_LEVEL": "0.10",
        "BABELPIPE_PIPELINE_SOURCE_LANG": "de",
    }
    overrides = {"audio": {"duck_level": 0.13}, "pipeline": {"source_lang": "it"}}
    settings = load_settings(config_path=path, env=env, overrides=overrides)
    assert settings.audio.duck_level == 0.13
    assert settings.pipeline.source_lang == "it"
    assert settings.pipeline.target_lang == DEFAULT_TARGET_LANG


def test_duck_level_out_of_range_is_actionable(tmp_path: Path) -> None:
    path = _write_toml(tmp_path, "[audio]\nduck_level = 0.42\n")
    with pytest.raises(ConfigError) as caught:
        load_settings(config_path=path, env=EMPTY_ENV)
    message = str(caught.value)
    assert "duck_level" in message
    assert "máximo" in message or "mínimo" in message
    assert "Traceback" not in message


def test_extra_field_rejected(tmp_path: Path) -> None:
    path = _write_toml(tmp_path, "[audio]\nduck_level = 0.12\ninventado = 1\n")
    with pytest.raises(ConfigError) as caught:
        load_settings(config_path=path, env=EMPTY_ENV)
    message = str(caught.value)
    assert "audio.inventado" in message
    assert "no permitido" in message
    assert "Traceback" not in message


def test_bad_toml_and_unreadable_path(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Sintaxis TOML"):
        load_settings(config_path=_write_toml(tmp_path, "[audio\n"), env=EMPTY_ENV)
    with pytest.raises(ConfigError, match="No se pudo leer"):
        load_settings(config_path=tmp_path / "no-existe.toml", env=EMPTY_ENV)


def test_invalid_env_value_has_no_stack_trace() -> None:
    with pytest.raises(ConfigError) as caught:
        load_settings(env={"BABELPIPE_AUDIO_DUCK_LEVEL": "no-numero"})
    message = str(caught.value)
    assert "BABELPIPE_AUDIO_DUCK_LEVEL" in message
    assert "Traceback" not in message


def test_remote_host_requires_token() -> None:
    with pytest.raises(ConfigError, match="server.token"):
        load_settings(env=EMPTY_ENV, overrides={"server": {"host": "192.168.1.50"}})
    ok = load_settings(
        env=EMPTY_ENV,
        overrides={"server": {"host": "192.168.1.50", "token": SECRET}},
    )
    assert isinstance(ok.server.token, SecretStr)


# ---------------------------------------------------------------------------
# Secretos: SecretStr, repr, serialización y ejemplos
# ---------------------------------------------------------------------------


def _settings_with_secret(**overrides: Any) -> ClientSettings:
    return load_settings(env=EMPTY_ENV, overrides={"server": {"token": SECRET}, **overrides})


def test_token_is_secretstr_and_masked_everywhere() -> None:
    settings = _settings_with_secret()
    assert isinstance(settings.server.token, SecretStr)
    assert settings.server.token.get_secret_value() == SECRET
    assert SECRET not in repr(settings)
    assert SECRET not in str(settings)
    assert SECRET not in repr(settings.server.token)
    assert "**********" in repr(settings.server.token)


def test_token_never_serialized_to_toml() -> None:
    settings = _settings_with_secret()
    toml_text = settings.to_toml()
    assert SECRET not in toml_text
    assert "token" not in toml_text.lower()

    parsed = tomllib.loads(toml_text)
    assert "token" not in parsed["server"]
    assert parsed["server"]["port"] == DEFAULT_PORT
    assert parsed["audio"]["duck_level"] == DEFAULT_DUCK_LEVEL


def test_token_not_in_validation_error_messages() -> None:
    with pytest.raises(ConfigError) as caught:
        load_settings(
            env=EMPTY_ENV,
            overrides={"server": {"token": SECRET, "port": "no-entero"}},
        )
    message = str(caught.value)
    assert SECRET not in message
    assert "server.port" in message
    assert "Traceback" not in message


def test_examples_contain_no_real_secrets() -> None:
    example_toml = tomllib.loads((REPO_ROOT / "config.toml.example").read_text(encoding="utf-8"))
    assert example_toml["server"]["token"] == ""

    env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    token_lines = [line for line in env_example if line.startswith("BABELPIPE_SERVER_TOKEN=")]
    assert token_lines == ["BABELPIPE_SERVER_TOKEN="]
    assert SECRET not in "\n".join(env_example)


def test_dump_example_is_pure_and_secret_free(tmp_path: Path) -> None:
    target = tmp_path / "config.toml.example"
    text = dump_example()
    assert not target.exists(), "dump_example() no debe escribir ficheros"
    parsed = tomllib.loads(text)
    assert "token" not in parsed["server"]
    assert SECRET not in text


# ---------------------------------------------------------------------------
# AppState — persistencia, permisos, corrupción y reset
# ---------------------------------------------------------------------------


def test_state_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    original = AppState(
        profile_name="llamadas",
        last_rx_lang="es",
        last_tx_lang="en",
        duck_level=0.14,
        ptt_enabled=True,
        overlay_x=40,
        overlay_y=80,
        overlay_width=640,
        overlay_height=120,
    )
    written = original.save(path)
    assert written == path
    assert AppState.load(path) == original
    assert "token" not in path.read_text(encoding="utf-8").lower()


def test_state_missing_file_returns_default_without_warning(tmp_path: Path) -> None:
    assert AppState.load(tmp_path / "nada.json") == AppState()


def test_state_corrupt_json_falls_back_to_default(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{ esto no es JSON", encoding="utf-8")
    with pytest.warns(UserWarning, match="corrupto"):
        restored = AppState.load(path)
    assert restored == AppState()


def test_state_unknown_field_falls_back_to_default(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text('{"token": "fuga", "duck_level": 0.12}', encoding="utf-8")
    with pytest.warns(UserWarning, match="corrupto"):
        restored = AppState.load(path)
    assert restored == AppState()


def test_state_permissions_and_xdg_path(tmp_path: Path) -> None:
    path = default_state_path(env={"XDG_CONFIG_HOME": str(tmp_path)})
    assert path == tmp_path / "babelpipe" / "state.json"
    AppState().save(path)
    mode_dir = stat.S_IMODE(path.parent.stat().st_mode)
    mode_file = stat.S_IMODE(path.stat().st_mode)
    assert mode_dir == 0o700, f"permisos de directorio: {oct(mode_dir)}"
    assert mode_file == 0o600, f"permisos de fichero: {oct(mode_file)}"


def test_state_save_is_atomic(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    AppState(profile_name="uno").save(path)
    AppState(profile_name="dos").save(path)
    # No quedan temporales de la escritura atómica.
    assert list(tmp_path.glob(".state.json.*.tmp")) == []
    assert AppState.load(path).profile_name == "dos"


def test_state_reset_persists_defaults(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    modified = AppState(profile_name="temporal", ptt_enabled=True, duck_level=0.15)
    modified.save(path)
    reset_state = modified.reset(path)
    assert reset_state == AppState()
    assert AppState.load(path) == AppState()


# ---------------------------------------------------------------------------
# Higiene de dependencias (sin pip global)
# ---------------------------------------------------------------------------


def test_no_global_pip_install_in_owned_source() -> None:
    marker = "pip" + " install"
    for name in ("client/config.py", "client/state.py", "tests/unit/test_config_persistence.py"):
        source = (REPO_ROOT / name).read_text(encoding="utf-8").lower()
        assert marker not in source, f"instalación global detectada en {name}"
