# BabelPipe — Traducción Simultánea Universal sobre PipeWire

BabelPipe ofrece **traducción simultánea bidireccional** con **dobladaje y subtítulos en vivo** directamente sobre el grafo de audio de PipeWire. En modo vídeo traduce YouTube/VLC/navegador; en modo llamada actúa como micrófono virtual para Telegram, Teams, Meet o Discord. El procesamiento pesado vive en un servidor de inferencia independiente; el escritorio solo ejecuta un daemon ligero.

## Requisitos

| Requisito | Detalle |
| :--- | :--- |
| Sistema | Kali Linux Rolling u otra distro moderna con **PipeWire + WirePlumber** (`>=1.0`) |
| Audio | `pactl`, `pw-cli`, `pw-dump`, `libpipewire-module-echo-cancel` |
| Python | **3.12** (rango soportado `>=3.12,<3.14`) |
| Gestor | **`uv`** (aislamiento PEP 668; prohibido `pip` global) |
| Opcional | GTK 4 + PyGObject para el overlay y el panel de control |

> No se requiere `git` para verificar, ejecutar ni empaquetar BabelPipe.

## Setup

```bash
# 1. Entorno base (cliente + herramientas de desarrollo)
uv sync

# 2. Solo en la máquina que corre el servidor de inferencia
uv sync --extra inference

# 3. Opcional: overlay GTK4 / panel de control
uv sync --extra ui

# 4. Comprobar PipeWire (--dry-run funciona sin hardware)
bash scripts/check-pipewire.sh
```

Copia las plantillas antes de arrancar y rellena el token si el servidor no está en `127.0.0.1`:

```bash
cp .env.example .env
cp config.toml.example config.toml
```

## Variables de entorno

Todas usan el prefijo `BABELPIPE_*`. La tabla completa está en `.env.example` y su espejo en `config.toml.example`; los secretos nunca se versionan.

| Variable | Default | Descripción |
| :--- | :--- | :--- |
| `BABELPIPE_SERVER_HOST` | `127.0.0.1` | Host del servidor WS; en LAN usar `wss://` + token |
| `BABELPIPE_SERVER_PORT` | `8765` | Puerto WebSocket (`1024-65535`) |
| `BABELPIPE_SERVER_TOKEN` | *(vacío)* | Bearer token; obligatorio si `host != 127.0.0.1` |
| `BABELPIPE_PIPELINE_SOURCE_LANG` | `en` | Idioma de entrada (ISO 639-1) |
| `BABELPIPE_PIPELINE_TARGET_LANG` | `es` | Idioma de salida (ISO 639-1) |
| `BABELPIPE_AUDIO_RX_SINK` | `Translator_Rx_Sink` | Bus de entrada (multimedia) |
| `BABELPIPE_AUDIO_TX_SOURCE` | `Translator_Tx_Mic` | Bus de salida (micrófono virtual) |
| `BABELPIPE_AUDIO_DUCK_LEVEL` | `0.12` | Atenuación del original (`0.10-0.15`) |
| `BABELPIPE_AUDIO_PTT_ENABLED` | `false` | Activa Push-to-Talk |
| `BABELPIPE_AUDIO_ECHO_CANCEL` | `true` | WebRTC AEC en el canal Tx |
| `BABELPIPE_MODEL_STT_PATH` | *(vacío)* | Ruta al modelo STT (vacío = caché por defecto) |
| `BABELPIPE_MODEL_TRANSLATOR_PATH` | *(vacío)* | Ruta al modelo de traducción |
| `BABELPIPE_MODEL_TTS_PATH` | *(vacío)* | Ruta al modelo TTS |

También están disponibles `BABELPIPE_PIPELINE_STT`, `BABELPIPE_PIPELINE_TRANSLATOR` y `BABELPIPE_PIPELINE_TTS` para seleccionar el adaptador.

## Ejecución

Comandos instalados por `uv sync` (entry points declarados en `pyproject.toml`):

```bash
# Cliente: crear nodos, ejecutar y limpiar al salir
babelpipe-client --setup
babelpipe-client --run
babelpipe-client --teardown

# Servidor de inferencia
babelpipe-server
```

Equivalentes sin instalar los scripts (útiles durante el desarrollo):

```bash
uv run python -m client.main --setup
uv run python -m client.main --run
uv run python -m client.main --teardown
uv run python -m server.main
```

El flag `--run` también está disponible en el AppImage: `./dist/BabelPipe-x86_64.AppImage --run`.

## Arquitectura

Dos buses lógicos estables en el grafo PipeWire y **dos procesos** separados por un WebSocket binario (`/ws/translate`, cabecera de 16 bytes + chunks `pcm_s16le` 16 kHz mono):

| Bus | Función |
| :--- | :--- |
| `Translator_Rx_Sink` | Sink virtual 48 kHz. Su `.monitor` alimenta el pipeline de traducción y el mezclador con ducking. |
| `Translator_Tx_Mic` | Source virtual 48 kHz que las apps de llamada ven como micrófono; recibe el TTS traducido. |

```text
YouTube/VLC ──> Translator_Rx_Sink(.monitor) ──> [cliente] ──WS──> [servidor]
                          │                          │              VAD → STT → MT → TTS
                          └── original 12% ──> ducking ──> sink físico <── doblaje
Mic físico ──> AEC (ref. sink físico) ──> [cliente] ──WS──> [servidor] ──> Translator_Tx_Mic ──> Telegram/Teams/Meet
```

El cliente hace captura/resampleo 48↔16 kHz, ducking, AEC, PTT y overlay GTK; el servidor ejecuta Silero VAD, faster-whisper, Opus-MT/LLM y Kokoro/Qwen3-TTS. Presupuesto extremo a extremo: **< 1500 ms por cláusula**.

## Modos de uso

- **Vídeo (YouTube, VLC, navegador):** en Ajustes de audio de la app selecciona `Translator_Rx_Sink` como salida. El doblaje se mezcla en el sink físico con el original atenuado al 12 % y los subtítulos aparecen en el overlay flotante. Sin voz (solo música) no se activa ni ducking ni subtítulos.
- **Llamadas (Telegram, Teams, Meet, Discord):** selecciona `Translator_Tx_Mic` como micrófono de la app. El canal Tx pasa por **AEC** con referencia del monitor del sink físico, de modo que la voz del interlocutor no se reenvía (≥ 20 dB de atenuación del eco).
- **Push-to-Talk:** con `BABELPIPE_AUDIO_PTT_ENABLED=true` el canal Tx envía solo silencio hasta que se pulsa la tecla de atajo; el indicador pasa a `PTT_IDLE`/`PTT_ACTIVE`. Recomendado si el AEC no converge (`ECHO_DETECTED`).

## Tests

```bash
uv run pytest tests/ -q
uv run ruff check .
uv run mypy client server shared
uv run pip-audit
```

## Licencias

Atribuciones completas en `NOTICE` y matriz normativa en `.factory/SPEC.md §6`.

| Componente | Licencia | Uso |
| :--- | :--- | :--- |
| Silero VAD, faster-whisper, CTranslate2, Misaki | MIT | Permitido |
| Kokoro-82M, Qwen3-TTS | Apache-2.0 | Permitido |
| Opus-MT | CC-BY-4.0 | Permitido con atribución en `NOTICE` |
| llama.cpp / Ollama / vLLM | MIT / MIT / Apache-2.0 | Permitido (endpoint LLM) |

**Excluidos deliberadamente:** NLLB-200 (CC-BY-NC-4.0, no comercial), XTTS v2 (CPML no comercial), F5-TTS (CC-BY-NC), Piper (GPL-3.0, copyleft viral) y espeak-ng/phonemizer (GPL-3.0; sustituido por Misaki). El gate `LICENSE_VIOLATION` falla la build si aparecen en `uv.lock`.

## Empaquetado

Verificación rápida (no construye, no necesita herramientas de build ni `git`):

```bash
bash scripts/package.sh --check
```

Construcción real de artefactos:

```bash
# Requiere appimagetool en el PATH; genera dist/BabelPipe-x86_64.AppImage
bash scripts/package.sh --appimage

# Requiere flatpak-builder y flatpak; genera dist/babelpipe.flatpak
bash scripts/package.sh --flatpak
```

Si falta alguna herramienta, el script muestra el comando de instalación y termina con error; **nunca** instala nada ni ejecuta `sudo` por su cuenta.

## Troubleshooting

| Código | Causa | Acción |
| :--- | :--- | :--- |
| `NO_PIPEWIRE` | `pactl info` / `pw-cli` no disponibles | Instala `pipewire wireplumber libpipewire-0.3-utils` y ejecuta `bash scripts/check-pipewire.sh`. |
| `AUTH_FAILED` | Token ausente o inválido en el handshake | Rellena `BABELPIPE_SERVER_TOKEN` en `.env`/`config.toml` con el mismo valor en cliente y servidor. |
| `SERVER_OFFLINE` | TCP rechazado o timeout | Arranca `babelpipe-server`, revisa host/puerto y espera el backoff exponencial (250 ms → 8 s). |
| `MODEL_MISSING` | Faltan pesos de inferencia | Ejecuta `uv sync --extra inference` en la máquina del servidor y configura las rutas `BABELPIPE_MODEL_*`. |
| `ECHO_DETECTED` | AEC no convergido; se oye al interlocutor en Tx | Verifica la referencia del sink físico y activa PTT (`BABELPIPE_AUDIO_PTT_ENABLED=true`). |
