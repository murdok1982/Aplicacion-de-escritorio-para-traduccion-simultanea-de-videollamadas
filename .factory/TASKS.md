# AUTONOMOUS EXECUTION BACKLOG

- **Project Name:** BabelPipe — Traducción Simultánea Universal sobre PipeWire
- **Current Phase:** `COMPLETED`
- **Progress:** `24 / 24` tasks verified (`100%`)
- **State Legend:**
  - `- [ ]` Pending (Listo para ejecutar)
  - `- [~]` In Progress (Asignado a subagente activo)
  - `- [x]` Verified Done (`@qa-verifier` aprobó build + tests + AC)
  - `- [!]` Blocked (Alcanzó 3/3 reintentos — requiere intervención de `@architect`)

> ✅ **ZERO-CODE GATE APROBADO** por el usuario el 2026-11-20. Se autoriza el bucle de implementación de subagentes.

---

## PHASE 0: SCAFFOLDING, TOOLING & SECURITY BASELINE

- [x] **TASK-001:** Bootstrap del repo: `uv init`, `pyproject.toml` con dependencias fijadas, `ruff`, `mypy`, `pytest`, `.env.example`, `config.toml.example`, `.gitignore`, `NOTICE`.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `NONE`
  - **Spec Ref:** `ARCHITECTURE.md §1-2`, `SPEC.md §6`
  - **Target Files:** `pyproject.toml`, `uv.lock`, `.env.example`, `config.toml.example`, `.gitignore`, `NOTICE`
  - **Verification:** `uv sync && uv run ruff check . && uv run mypy client server shared`
  - **Retries:** `0/3`

- [x] **TASK-002:** Script de verificación de entorno PipeWire en Kali (`pactl info`, `pw-cli info`, `pw-dump`, WirePlumber) con modo `--dry-run` para tests sin hardware.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-001`
  - **Spec Ref:** `REQ-01`, `AC-01`
  - **Target Files:** `scripts/check-pipewire.sh`, `tests/unit/test_env_check.py`
  - **Verification:** `bash scripts/check-pipewire.sh --dry-run && uv run pytest tests/unit/test_env_check.py -q`
  - **Retries:** `0/3`

- [x] **TASK-003:** Generador de fixtures WAV sintéticos (48 kHz y 16 kHz): tono 1 kHz, voz sintética, silencio, ruido y eco simulado; sin archivos binarios grandes commiteados.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-001`
  - **Spec Ref:** `SPEC.md §1.2`, `ARCHITECTURE.md §2.2`
  - **Target Files:** `scripts/generate-fixtures.py`, `tests/unit/test_fixtures.py`
  - **Verification:** `uv run python scripts/generate-fixtures.py --verify && uv run pytest tests/unit/test_fixtures.py -q`
  - **Retries:** `0/3`

---

## PHASE 1: SOLO AUDIO (SIN IA)

- [x] **TASK-004:** Gestión idempotente de nodos: crear/limpiar `Translator_Rx_Sink` y `Translator_Tx_Mic`, manejo de `SIGINT/SIGTERM`, sin duplicados.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-002`
  - **Spec Ref:** `REQ-01`, `AC-01`, `AC-02`
  - **Target Files:** `client/audio/pipewire_graph.py`, `scripts/setup-nodes.sh`, `tests/integration/test_pipewire_graph.py`
  - **Verification:** `uv run pytest tests/integration/test_pipewire_graph.py -q --mock`
  - **Retries:** `0/3`

- [x] **TASK-005:** Resampleo explícito 48 kHz ↔ 16 kHz con `soxr`, chunks exactos de 320 muestras int16 LE y verificación SNR de roundtrip.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-003`
  - **Spec Ref:** `REQ-02`, `AC-03`, `ARCHITECTURE.md §7 ADR-005`
  - **Target Files:** `shared/constants.py`, `client/audio/resampler.py`, `tests/unit/test_resampler.py`
  - **Verification:** `uv run pytest tests/unit/test_resampler.py -q`
  - **Retries:** `0/3`

- [x] **TASK-006:** Loopback PCM y mezclador con ducking configurable (10-15 %, default 12 %), sin glitches ni eco en señal sintética.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-004, TASK-005`
  - **Spec Ref:** `REQ-05`, `AC-08`, `ARCHITECTURE.md §7 ADR-002`
  - **Target Files:** `client/audio/transport.py`, `client/audio/ducking.py`, `tests/integration/test_ducking.py`
  - **Verification:** `uv run pytest tests/integration/test_ducking.py -q`
  - **Retries:** `0/3`

- [x] **TASK-007:** Integración de AEC (`libpipewire-module-echo-cancel`, WebRTC) y Push-to-Talk con gating de frames; prueba con fixture de eco.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-006`
  - **Spec Ref:** `REQ-06`, `AC-10`, `AC-11`
  - **Target Files:** `client/audio/echo_cancel.py`, `client/audio/ptt.py`, `tests/integration/test_echo_cancel.py`, `tests/unit/test_ptt.py`
  - **Verification:** `uv run pytest tests/integration/test_echo_cancel.py -q && uv run pytest tests/unit/test_ptt.py -q`
  - **Retries:** `0/3`

---

## PHASE 2: PROTOCOLO Y SERVIDOR (MOCK + REAL)

- [x] **TASK-008:** Contrato canónico de protocolo en `shared/`: cabecera binaria 16 bytes (`!2sBBHIIH`), modelos Pydantic de control y constantes.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-001`
  - **Spec Ref:** `REQ-03`, `AC-04`, `ARCHITECTURE.md §4`
  - **Target Files:** `shared/protocol.py`, `shared/models.py`, `shared/constants.py`, `tests/unit/test_protocol.py`
  - **Verification:** `uv run pytest tests/unit/test_protocol.py -q`
  - **Retries:** `0/3`

- [x] **TASK-009:** Servidor WebSocket real: handshake con token, `config`, `ping/pong`, `ready`, `error` sin stack trace, `/healthz`.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-008`
  - **Spec Ref:** `ARCHITECTURE.md §4, §6`, `AC-04`
  - **Target Files:** `server/main.py`, `server/config.py`, `server/health.py`, `tests/integration/test_ws_server.py`
  - **Verification:** `uv run pytest tests/integration/test_ws_server.py -q`
  - **Retries:** `0/3`

- [x] **TASK-010:** Servidor mock determinista y prueba de carga con WAV: validación de frames, backpressure y dedup por `sequence`.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-009`
  - **Spec Ref:** `ARCHITECTURE.md §4.4`, `AC-05`
  - **Target Files:** `server/pipeline/mock.py`, `tests/integration/test_mock_server.py`
  - **Verification:** `uv run pytest tests/integration/test_mock_server.py -q`
  - **Retries:** `0/3`

- [x] **TASK-011:** Adaptador VAD Silero incremental sobre chunks de 20 ms; segmentación por pausas naturales, sin ventanas ciegas.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-008`
  - **Spec Ref:** `REQ-04`, `AC-06`
  - **Target Files:** `server/pipeline/base.py`, `server/pipeline/vad.py`, `tests/unit/test_vad.py`
  - **Verification:** `uv run pytest tests/unit/test_vad.py -q`
  - **Retries:** `0/3`

- [x] **TASK-012:** Adaptador STT (faster-whisper / whisper.cpp) con transcripción incremental y fallback determinista en tests.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-011`
  - **Spec Ref:** `REQ-04`, `AC-07`
  - **Target Files:** `server/pipeline/stt.py`, `tests/unit/test_stt.py`
  - **Verification:** `uv run pytest tests/unit/test_stt.py -q`
  - **Retries:** `0/3`

- [x] **TASK-013:** Adaptador de traducción intercambiable: Opus-MT sobre CTranslate2 y endpoint LLM compatible OpenAI; **NLLB excluido por licencia**.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-012`
  - **Spec Ref:** `REQ-04`, `SPEC.md §6`, `ADR-003`
  - **Target Files:** `server/pipeline/translator.py`, `tests/unit/test_translator.py`
  - **Verification:** `uv run pytest tests/unit/test_translator.py -q && uv run pip-audit`
  - **Retries:** `0/3`

- [x] **TASK-014:** Adaptador TTS streaming Kokoro-82M / Qwen3-TTS con G2P Misaki (MIT), chunks de salida y guard de licencias.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-013`
  - **Spec Ref:** `REQ-04`, `SPEC.md §6`, `ADR-004`
  - **Target Files:** `server/pipeline/tts.py`, `tests/unit/test_tts.py`
  - **Verification:** `uv run pytest tests/unit/test_tts.py -q`
  - **Retries:** `0/3`

- [x] **TASK-015:** Orquestador por sesión con colas no bloqueantes y métricas de latencia por etapa (< 1500 ms totales).
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-011, TASK-012, TASK-013, TASK-014`
  - **Spec Ref:** `AC-07`, `SPEC.md §7`
  - **Target Files:** `server/pipeline/orchestrator.py`, `tests/integration/test_pipeline_latency.py`
  - **Verification:** `uv run pytest tests/integration/test_pipeline_latency.py -q`
  - **Retries:** `0/3`

---

## PHASE 3: MODO UNIDIRECCIONAL (YOUTUBE / VÍDEOS)

- [x] **TASK-016:** Cliente WS: stream Rx, envío de chunks 16 kHz, reconexión exponencial y reanudación por `session_id`/`last_seq`.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-006, TASK-009`
  - **Spec Ref:** `REQ-03`, `AC-05`
  - **Target Files:** `client/net/ws_client.py`, `client/net/stream.py`, `tests/integration/test_client_rx.py`
  - **Verification:** `uv run pytest tests/integration/test_client_rx.py -q`
  - **Retries:** `0/3`

- [x] **TASK-017:** Integración Rx extremo a extremo con audio sintético: doblaje + ducking + eventos `subtitle`/`translated`.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-015, TASK-016`
  - **Spec Ref:** `REQ-05`, `AC-08`, `AC-09`
  - **Target Files:** `client/main.py`, `tests/e2e/test_video_mode.py`
  - **Verification:** `uv run pytest tests/e2e/test_video_mode.py -q`
  - **Retries:** `0/3`

- [x] **TASK-018:** Overlay flotante GTK4 siempre visible y semitransparente con estados listening/empty/error y sanitización de texto.
  - **Owner:** `@builder` -> `@ui-artisan` -> `@qa-verifier`
  - **Depends On:** `TASK-017`
  - **Spec Ref:** `SPEC.md §4`, `ARCHITECTURE.md §5`
  - **Target Files:** `client/overlay/subtitle_window.py`, `tests/unit/test_overlay.py`
  - **Verification:** `uv run pytest tests/unit/test_overlay.py -q && uv run ruff check client/overlay`
  - **Retries:** `0/3`

---

## PHASE 4: MODO BIDIRECCIONAL (LLAMADAS)

- [x] **TASK-019:** Canal Tx: captura de mic → AEC → PTT → WS → TTS → inyección en `Translator_Tx_Mic`.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-007, TASK-016`
  - **Spec Ref:** `REQ-06`, `AC-10`, `AC-11`
  - **Target Files:** `client/audio/transport.py` (ABC de transporte + implementación Mock, pendiente de TASK-006), `client/net/stream.py`, `client/audio/echo_cancel.py`, `tests/e2e/test_call_mode.py`
  - **Verification:** `uv run pytest tests/e2e/test_call_mode.py -q`
  - **Retries:** `0/3`

- [x] **TASK-020:** Llamada bidireccional completa con dos streams simultáneos, AEC convergido y latencia < 1500 ms por cláusula.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-017, TASK-019`
  - **Spec Ref:** `AC-10`, `SPEC.md §7`
  - **Target Files:** `tests/e2e/test_bidirectional.py`
  - **Verification:** `uv run pytest tests/e2e/test_bidirectional.py -q`
  - **Retries:** `0/3`

---

## PHASE 5: INTERFAZ Y EMPAQUETADO

- [x] **TASK-021:** Panel de control completo: selector de app a enrutar, idiomas Rx/Tx, volumen de ducking, PTT, design tokens y estados UI.
  - **Owner:** `@builder` -> `@ui-artisan` -> `@qa-verifier`
  - **Depends On:** `TASK-018, TASK-020`
  - **Spec Ref:** `SPEC.md §4`, `ARCHITECTURE.md §5`
  - **Target Files:** `client/ui/control_panel.py`, `client/ui/theme.css`, `tests/unit/test_control_panel.py`
  - **Verification:** `uv run ruff check client/ui && uv run pytest tests/unit/test_control_panel.py -q`
  - **Retries:** `0/3`

- [x] **TASK-022:** Persistencia de configuración y perfiles (`.env`/`config.toml`), validación Pydantic y sin secretos en repo.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-021`
  - **Spec Ref:** `REQ-07`, `AC-12`
  - **Target Files:** `client/config.py`, `client/state.py`, `tests/unit/test_config_persistence.py`
  - **Verification:** `uv run pytest tests/unit/test_config_persistence.py -q`
  - **Retries:** `0/3`

- [x] **TASK-023:** Script de empaquetado AppImage/Flatpak y `README.md` con setup, variables de entorno y comandos exactos.
  - **Owner:** `@builder` -> `@qa-verifier`
  - **Depends On:** `TASK-022`
  - **Spec Ref:** `AC-13`
  - **Target Files:** `scripts/package.sh`, `README.md`
  - **Verification:** `bash scripts/package.sh --check && test -f README.md`
  - **Retries:** `0/3`

- [x] **TASK-024:** Aceptación final: suite E2E completa de todos los `AC-XX`, auditoría AppSec y gate de licencias.
  - **Owner:** `@qa-verifier` + `@redteam-auditor`
  - **Depends On:** `TASK-023`
  - **Spec Ref:** `SPEC.md §3, §5, §6`, `SECURITY_AUDIT.md §1-5`
  - **Target Files:** `.factory/SPEC.md`, `.factory/SECURITY_AUDIT.md`
  - **Verification:** `uv run pytest tests/e2e -q && uv run ruff check . && uv run mypy client server shared && grep -q "APPROVED" .factory/SECURITY_AUDIT.md`
  - **Retries:** `0/3`

---

## DYNAMIC SECURITY & QA REMEDIATION QUEUE

> `@redteam-auditor` o `@qa-verifier` añaden aquí tareas `SEC-FIX-XXX` si detectan fallos antes del Exit Gate.

*(Vacío inicialmente)*

---

## CIRCUIT BREAKER & FAILURE ESCALATION LOG

| Task ID | Attempt | Failure Symptom / Stack Trace Summary | Root Cause & Architectural Fix (`@architect`) |
| :--- | :--- | :--- | :--- |
| — | — | — | — |

---

## PROTOCOLO DE EJECUCIÓN PARA SUBAGENTES (BUCLE AUTÓNOMO)

> Estas reglas se activan SOLO tras la aprobación humana de los tres contratos.

1. **Leer contratos primero:** Todo subagente lee `.factory/SPEC.md`, `.factory/ARCHITECTURE.md` y `.factory/TASKS.md` antes de tocar cualquier archivo.
2. **Una tarea por iteración:** Ejecuta UNA sola tarea atómica por iteración, en orden de dependencias.
3. **Marcar `[~]` al iniciar:** Cambia el estado de la tarea a `- [~]` antes de escribir código.
4. **Verificación obligatoria:** No puede marcar `[x]` si no ejecuta y supera el **comando exacto de verificación** de la tarea **y** los tests de regresión (`uv run pytest tests/ -q`).
5. **Contrato antes que código:** Si descubre un cambio necesario en el contrato de audio o protocolo, actualiza primero `.factory/ARCHITECTURE.md` y registra la decisión (nuevo ADR) antes de modificar el código.
6. **Circuit Breaker:** Si una tarea falla 3 veces (Retries `3/3`), se marca `- [!]`, se detiene y `@architect` diagnostica y re_plantea la aproximación en `ARCHITECTURE.md` + `TASKS.md`.
7. **Gate de UI:** Toda tarea que toque archivos de `client/ui/` o `client/overlay/` pasa por `@ui-artisan` antes de `@qa-verifier`.
8. **Gate de seguridad:** Una vez todas las tareas funcionales en `- [x]`, `@redteam-auditor` audita todo el repo; cualquier hallazgo `CRITICAL`/`HIGH` crea una tarea `SEC-FIX-XXX` y bloquea la finalización.
