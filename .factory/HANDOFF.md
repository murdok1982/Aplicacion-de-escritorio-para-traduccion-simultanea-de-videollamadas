# HANDOFF — BabelPipe (continuación del bucle autónomo)

> Creado el 2026-11-20 por solicitud del usuario: **detener el trabajo y dejar un esquema de continuidad**.
> No hay subagentes en vuelo. No hay commits (el workspace no es repo git).

---

## 1. ESTADO ACTUAL (al detener)

- **Progreso oficial en `TASKS.md`: `16 / 24 (67%)`.**
- **16 tareas `[x]`** (TASK-001…014, TASK-016, TASK-019).
- **2 tareas verificadas por `@qa-verifier` en este handoff (antes `[~]`):**
  - `TASK-014` → `[x]` — TTS streaming (22 tests propios; chunks 640 B int16 LE @16 kHz, primer chunk 2.192 ms ≤ 250 ms, guard `LICENSE_VIOLATION` para xtts/f5/piper/espeak, Misaki `fallback=None` sin espeak, `MODEL_MISSING` sin red).
  - `TASK-019` → `[x]` — Canal Tx / `Translator_Tx_Mic` (2 tests E2E AC-10/AC-11; eco 20.00 dB ≥ 20 dB, latencia voz→Tx 0.1 ms < 1500 ms, AEC con ref `.monitor`, "See you tomorrow" en Tx; PTT idle → 0 frames de voz y `PTT_IDLE`; sin token en logs, sin tareas colgadas, `shell=False`).
  - Nota: el orquestador documentó un fix de flake en `client/net/ws_client.py` (`_write_loop` captura `ConnectionClosed`/`WebSocketException` como `_read_loop`); es corrección de bug de TASK-016 (ya `[x]`).
- **8 tareas `[ ]` pendientes:** TASK-015, 017, 018, 020, 021, 022, 023, 024.
- **Suite completa ejecutada en la verificación:** `uv run pytest tests/ -q` → **275 passed in 8.85 s**, 0 errores.
- `uv run ruff check .` → *All checks passed!*; `uv run mypy client server shared` → *Success: no issues found in 28 source files*.
- `uv run pip-audit` → *No known vulnerabilities found* (`babelpipe` skip: paquete local no publicado en PyPI; no CVE).

### Archivos creados por TASK-019 que aún no existían en el árbol original
- `client/audio/transport.py` (`AudioTransport` ABC + `PipeWireAudioTransport` + `MockAudioTransport`).
- `tests/e2e/test_call_mode.py`.
- `client/net/stream.py` ahora contiene `RxStream` **y** `TxStream`.

---

## 2. PASO INMEDIATO (resume aquí)

**Acción 1 — QA de TASK-014 y TASK-019** (`@qa-verifier`):
```bash
cd "/home/fsociety/Proyectos personales factorydev/Aplicacion de traduccion instantanea"
uv run pytest tests/unit/test_tts.py -q && uv run ruff check server/pipeline/tts.py tests/unit/test_tts.py && uv run mypy server/pipeline/tts.py
uv run pytest tests/e2e/test_call_mode.py -q && uv run ruff check client/audio/transport.py client/net/stream.py tests/e2e/test_call_mode.py && uv run mypy client/audio/transport.py client/net/stream.py
uv run pytest tests/ -q    # debe 275 passed
```
Si pasa → marcar TASK-014 y TASK-019 como `[x]`, progreso **`16 / 24 (67%)`**. Si falla → NO marcar, incrementar `Retries: 1/3` y reenviar al builder con el stack trace.

---

## 3. HOJA DE RUTA RESTANTE (en orden de dependencias)

| Orden | Tarea | Verificación exacta | Notas |
|:--|:--|:--|:--|
| 1 | **TASK-015** — Orquestador por sesión + métricas de latencia (<1500 ms, AC-07) | `uv run pytest tests/integration/test_pipeline_latency.py -q` | Cablear `VAD→STT→Translator→TTS` en `server/pipeline/orchestrator.py` con colas `asyncio.Queue` y métricas por etapa (SPEC §7). **Revisar el workaround del builder de TASK-019**: en `client/net/stream.py` hay un ajuste local del handler de texto para que el anuncio `tts_chunk` no contamine el dedup global y descarte el frame binario de TTS; idealmente mover esa corrección a `client/net/ws_client.py` de forma controlada. |
| 2 | **TASK-017** — Integración Rx E2E (doblaje + ducking + subtítulos) | `uv run pytest tests/e2e/test_video_mode.py -q` | AC-08 / AC-09 (YouTube con voz y con solo música). Usa `MockInferenceServer` + `MockAudioTransport`. |
| 3 | **TASK-018** — Overlay GTK4 flotante | `uv run pytest tests/unit/test_overlay.py -q && uv run ruff check client/overlay` | Pasa por **`@ui-artisan`** antes de QA (SPEC §4, ARCH §5). Requiere `gi` (extra `ui`); los tests deben correr con un stub si no hay GTK del sistema. |
| 4 | **TASK-020** — Llamada bidireccional completa | `uv run pytest tests/e2e/test_bidirectional.py -q` | AC-10: dos streams simultáneos, AEC convergido, <1500 ms. |
| 5 | **TASK-021** — Panel de control + design tokens | `uv run ruff check client/ui && uv run pytest tests/unit/test_control_panel.py -q` | **`@ui-artisan`** → QA. Selector de app, idiomas Rx/Tx, `duck_level`, PTT. |
| 6 | **TASK-022** — Persistencia config/perfiles | `uv run pytest tests/unit/test_config_persistence.py -q` | AC-12: sin secretos en repo, Pydantic. |
| 7 | **TASK-023** — Empaquetado + README | `bash scripts/package.sh --check && test -f README.md` | AC-13: AppImage/Flatpak. |
| 8 | **TASK-024** — Gate final | `uv run pytest tests/e2e -q && uv run ruff check . && uv run mypy client server shared && grep -q "APPROVED" .factory/SECURITY_AUDIT.md` | **`@redteam-auditor`** → auditoría AppSec en `.factory/SECURITY_AUDIT.md`; cualquier CRITICAL/HIGH crea `SEC-FIX-XXX` y bloquea. Verificar AC-01…AC-13 y matriz de licencias. |

---

## 4. REGLAS DEL BUCLE (no olvidar)

1. Leer `.factory/SPEC.md`, `.factory/ARCHITECTURE.md`, `.factory/TASKS.md` antes de tocar código.
2. Una sola tarea atómica por iteración; marcar `[~]` al iniciar.
3. Solo `@qa-verifier` puede marcar `[x]` (comando exacto + regresión `uv run pytest tests/ -q`).
4. Si hay cambio de contrato de audio/protocolo → actualizar `ARCHITECTURE.md` (nuevo ADR) **antes** del código.
5. 3 fallos → `- [!]`, circuit breaker, `@architect` re-plantea.
6. Toda tarea UI/overlay pasa por `@ui-artisan`.
7. Logs de estado: `.factory/SECURITY_AUDIT.md` debe terminar en `Ready for Production Delivery: APPROVED`.

---

## 5. DECISIONES Y TRAMPAS YA CONOCIDAS

- **ADR-003:** NLLB-200 **excluido** (CC-BY-NC) pese a mencionarse en el enunciado → Opus-MT (CC-BY-4.0) + LLM local.
- **ADR-004:** espeak-ng/phonemizer (GPL) **prohibidos** → G2P con Misaki (MIT) vía `generate_from_tokens` para que Kokoro no ejecute su espeak interno.
- **ADR-005:** PCM canónico `pcm_s16le` 16 kHz mono, 20 ms, 320 samples / 640 bytes, cabecera `!2sBBHIIH` de 16 bytes en `shared/`.
- **ADR-006:** Eventos no fatales (`OVERRUN`, `DUPLICATE`, `ECHO_DETECTED`) viajan como `error` con código no fatal y socket abierto (no existe tipo `warning` en `shared.models`).
- `--mock` en `tests/conftest.py` es **default True**: la suite nunca ejecuta comandos reales a PipeWire.
- `reconnect` usa `ReconnectMessage(session_id, last_seq)`; `ConfigMessage` tiene `extra="forbid"` y no admite `last_seq`.
- `Translator_Tx_Master` es un nodo auxiliar interno para el remap de `Translator_Tx_Mic` (no un tercer bus).
- El token de inferencia es `SecretStr`; nunca debe aparecer en logs, URI ni excepciones.
- SSRF: `LLMTranslator` solo permite loopback o LAN con allowlist explícita; bloquea `169.254.169.254`, `file://`, hosts públicos.
- Dependencias vetadas excluidas en `[tool.uv] exclude-dependencies` de `pyproject.toml`.
- `pip-audit` limpio tras subir pytest/pytest-asyncio; no revertir esos pins.
- No hay target de commit git (workspace no es repo).
