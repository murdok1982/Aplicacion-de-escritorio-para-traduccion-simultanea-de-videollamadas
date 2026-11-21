# ADVERSARIAL SECURITY AUDIT REPORT

- **Audit Status:** `PASS (0 CRITICAL / 0 HIGH)`
- **Auditor Agent:** `@redteam-auditor`
- **Target Commit / Iteration:** `TASK-024 — gate de seguridad final (post-implementación, 23/24 tareas funcionales verificadas)`
- **Audit Date:** `2026-11-21`
- **Summary Counts:**
  - `CRITICAL`: 0
  - `HIGH`: 0
  - `MEDIUM`: 0
  - `LOW`: 0

> **EXIT GATE RULE:** El proyecto NO puede darse por finalizado mientras exista cualquier hallazgo `CRITICAL` o `HIGH` en estado `OPEN`.

> **RESULTADO DE LA AUDITORÍA:** Se ejecutó el checklist completo de §2 y el tooling de §4 sobre todo el repositorio. **0 hallazgos** en todas las severidades. Los mecanismos de defensa (validación de frontera, auth con `hmac.compare_digest`, SSRF guard, `SecretStr`, permisos de archivo, guard de licencias, subprocess sin shell) están implementados y verificados con evidencia ejecutable. No se requieren tareas `SEC-FIX-XXX`.

---

## 1. ATTACK SURFACE & THREAT MODEL

- **External Entry Points:** WebSocket `ws(s)://host:8765/ws/translate` (audio binario + JSON de control), endpoint `GET /healthz` (JSON, stdlib `http.server`), scripts `check-pipewire.sh`, `setup-nodes.sh`, `package.sh`, `generate-fixtures.py`, archivos `.env`/`config.toml` y rutas de modelos locales.
- **Authentication Mechanism:** Token Bearer compartido (query `?token=` o cabecera `Authorization: Bearer`) verificado con `hmac.compare_digest`; `ServerSettings` exige token cuando `host` no es localhost; sin token → cierre `1008`. Cliente: `SecretStr`, URI redactada en logs (`uri` con `***`), header Bearer.
- **Sensitive Data Assets:** Token de inferencia y clave LLM (`SecretStr`, nunca en `repr`/logs/URI/excepciones/`to_toml`), configuración de idiomas/modelos, muestras PCM efímeras en memoria (no persistidas), rutas de modelos locales, `state.json` (sin secretos, 0o600).
- **Trust Boundaries:** Cliente ligero (espacio usuario, PipeWire) → WS LAN → Servidor de inferencia → Modelos locales / endpoint LLM. Entradas externas: frames binarios PCM, mensajes JSON de control, parámetros de `base_url` del LLM, rutas de nodos PipeWire, texto de subtítulos renderizado en GTK.

---

## 2. MANDATORY APPSEC VERIFICATION CHECKLIST

### 2.1 Injection & Input Validation
- [x] Frames binarios validan magic `BP`, versión (`VERSION=1`), `stream_id`, `sample_rate==16000`, `num_samples>0`, `len(pcm)==num_samples*2` y `MAX_FRAME_BYTES=8192` (`shared/protocol.py:155-202`, `AudioFrame.__post_init__:73-91`). El error distingue `SCHEMA` vs `FORMAT`.
- [x] Todo JSON de control pasa por Pydantic v2 con `extra="forbid"` y unión discriminada por `type` (`shared/models.py:48-195`); `parse_control_message` rechaza UTF-8 inválido, JSON inválido, `type` desconocido y campos extra sin stack trace.
- [x] Sin `eval`, `exec`, `shell=True`, `os.system` ni `os.popen` en código de aplicación (grep: único hit es un comentario en `client/audio/transport.py:11` que documenta que NUNCA usa `shell=True`). Todo `subprocess.run`/`Popen` usa `shell=False`, listas de argv y timeout.
- [x] Scripts shell (`setup-nodes.sh`, `check-pipewire.sh`, `package.sh`) sin interpolación insegura: rutas derivadas de `BASH_SOURCE` y normalizadas con `pwd -P`, `set -euo pipefail`, sin `eval`, sin `sudo`, comandos constants y IDs validados con regex `[0-9]+`.

### 2.2 Authentication, Sessions & Access Control
- [x] Token Bearer verificado en handshake con `hmac.compare_digest`; único bypass es `guest` → `GET /healthz` (que no expone secretos: solo `status`/`ready`/`models`/`active_sessions`/`uptime`). Cierre `1008` para ruta inválida, auth fallida y límite de sesiones (`server/main.py:111-130,242-255`).
- [x] Rate limiting por sesión: ventana deslizante de 1 s sobre TODOS los mensajes (binarios y de control), `rate_limit_per_second=200`, código `RATE_LIMIT` sin cerrar socket (`server/main.py:143-151,259-266`).
- [x] Límite de sesiones concurrentes (`max_sessions=32`), dedup por `sequence` por stream, `max_size` de frame en `websockets.serve`, y limpieza de sesión en `finally` al desconectar (`server/main.py:154-187,234-239,271-274,287-293`).
- [x] Cierre de sesión y limpieza de nodos PipeWire al desconectar / `SIGINT`/`SIGTERM` / `atexit` (`client/audio/pipewire_graph.py:195-238`, `client/main.py:703-718`).

### 2.3 Secrets, Configuration & HTTP Hardening
- [x] `.env` en `.gitignore`; `.env.example` y `config.toml.example` sin secretos reales (token vacío); sin fichero `.env` presente en el repo.
- [x] Token como `SecretStr` en `ServerSettings`, `ClientSettings`, `WSClientSettings` y `LLMTranslator`; nunca en logs (`uri` redactada, `_fail` logea solo `reason` de la excepción sin valor), nunca en URI de logs, nunca en excepciones y omitido por completo en `to_toml()` (`client/config.py:181-198`, `client/net/ws_client.py:429-447`).
- [x] Los mensajes de error de configuración/validación se construyen solo con ruta de campo y tipo de error, jamás con `input`/`ctx`/valores (`client/config.py:302-314`, `shared/models.py:163-168`, `server/main.py:137-140`). Sin stack traces al cliente.
- [x] CORS: no se usa `Access-Control-Allow-Origin: *` ni ninguna cabecera CORS (grep: 0 coincidencias). La única superficie HTTP es `/healthz`, un endpoint JSON bound a `127.0.0.1` por defecto que no sirve HTML, por lo que CSP/HSTS/X-Frame-Options no son aplicables; no hay contenido navegable inyectable.
- [x] Restricción de licencias (SPEC §6) verificada en `uv.lock`: 0 paquetes vetados en bloques `[[package]]`; `exclude-dependencies`/`[manifest].excludes` presentes (`espeakng-loader`, `phonemizer`, `phonemizer-fork`); `NOTICE` con atribuciones CC-BY-4.0 y Apache-2.0. Guard de licencias en `server/pipeline/translator.py:69-79,104-120` y `tts.py`.

### 2.4 SSRF, File Handling & Dependencies
- [x] `LLMTranslator._validate_base_url` bloquea SSRF verificado en ejecución: `169.254.169.254` (link-local), hostnames de metadata (`metadata.google.internal`, etc.), esquema `file://` (solo http/https), hosts públicos no allowlisteados, IPs públicas aunque estén en allowlist, credenciales embebidas, query/fragmento, y rangos unspecified/link-local/multicast/reserved. Solo loopback e IPs privadas de LAN allowlisteadas (`server/pipeline/translator.py:148-220`).-La clave LLM se envía como `SecretStr`, las excepciones del proveedor NO se registran porque pueden incluir URL y clave (`server/pipeline/translator.py:483-490`).
- [x] Fixtures WAV: 7 archivos sintéticos generados localmente por `scripts/generate-fixtures.py` con `FIXTURES_DIR = Path(__file__).resolve().parents[1]/"tests"/"fixtures"`, nombres fijos, sin descargas ni contenido malicioso; verificados por `--verify`.
- [x] `pip-audit` sin CVEs críticos/altos (0 vulnerabilidades conocidas; único skip: el propio paquete local `babelpipe`, no publicado en PyPI — no es un hallazgo).

### 2.5 Superficie Específica (GTK / archivos / rutas)
- [x] Sanitización de subtítulos en GTK: texto externo siempre vía `set_label()` / buffers de texto plano; **ninguna llamada `set_markup`** en el repositorio (grep: 0 coincidencias). Longitud acotada a 2000 chars (`client/overlay/subtitle_window.py:598-614,665-671`, `client/ui/control_panel.py:844-895`).
- [x] `state.json` con fichero `0o600` (`os.fchmod` antes de `os.replace`, escritura atómica) y directorio `0o700` (`mkdir(mode=0o700)` + `os.chmod`), sin secretos (`client/state.py:66-89,140-143`).
- [x] Sin path traversal en `config_path`/`model_path`: ambas son rutas locales provistas por el operador (CLI/config/.env), nunca alcanzables desde el WS ni desde mensajes de control; `OpusMTTranslator` exige directorio existente y valida dispositivo/compute_type/beam_size; los nombres de nodo PipeWire y `rx_sink` se restringen con `[A-Za-z0-9_.-]{1,128}` fullmatch antes de cualquier subprocess (`client/config.py:140-141`, `client/audio/transport.py:173-178`, `client/main.py:104-109`). Los imports dinámicos usan constantes hardcoded (`ctranslate2`, `openai`, `faster_whisper`, `kokoro`, `qwen_tts`, `qwen3_tts`), nunca strings del cliente.
- [x] Sin secretos ni placeholders inseguros en fixtures, plantillas ni documentación (secret scan: 0 coincidencias de AWS keys, private keys, JWTs, tokens GH/OAuth ni literales de token).
- [x] Sin `TODO` / `FIXME` / `implement later` accionables (grep: únicamente falsos positivos de la palabra española "todo" en docstrings; 0 marcadores reales).

---

## 3. VULNERABILITY FINDINGS LOG

> Registrar aquí cada hallazgo detectado. Por cada `CRITICAL` o `HIGH`, añadir inmediatamente una tarea `SEC-FIX-XXX` en `.factory/TASKS.md`.

**Resultado: 0 hallazgos.** Tras auditar todo el repositorio como atacante (frontera WS, auth, SSRF, inyección, secretos, GTK, permisos de archivo, scripts shell, dependencias y licencias) no se identificó ninguna vulnerabilidad `CRITICAL`, `HIGH`, `MEDIUM` ni `LOW`. No se han creado tareas `SEC-FIX-XXX` ni bloqueos.

| ID | Severidad | Categoría | Ubicación | Estado |
| :--- | :--- | :--- | :--- | :--- |
| — | — | — | — | Sin hallazgos |

---

## 4. AUTOMATED SECURITY TOOLING LOG

| Tool / Command | Scope | Exit Code | Findings Summary |
| :--- | :--- | :--- | :--- |
| `uv run pip-audit` | Dependencias (`uv.lock` + entorno) | 0 | `No known vulnerabilities found`. Skip informativo: `babelpipe` (paquete local 0.1.0 no en PyPI). 0 CVEs. |
| `uv run ruff check .` | Todo el repo (lint) | 0 | `All checks passed!` |
| `uv run mypy client server shared` | Typecheck estricto | 0 | `Success: no issues found in 35 source files` |
| `uv run pytest tests/ -q` | Suite completa (unit + integration + e2e) | 0 | `364 passed in 20.34s` |
| `bash scripts/package.sh --check` | Empaquetado / AC-13 / lockfile | 0 | `VERIFICACIÓN CORRECTA — AC-13 cubierto`. 0 dependencias vetadas. |
| `grep -rniE 'TODO\|FIXME\|implement later' client server shared scripts` | Código fuente | 0 (matches) | Solo falsos positivos de "todo" en español; 0 marcadores accionables. |
| Secret scan (`grep` de AWS key / private key / JWT / tokens + inspección `.env*` / `config*.example`) | Todo el repo | 0 | 0 secretos; `.gitignore` cubre `.env`; plantillas sin valores reales. |
| License scan (SPEC §6): `awk` sobre bloques `[[package]]` de `uv.lock` + verificación de `exclude-dependencies` / `NOTICE` | `uv.lock`, `pyproject.toml`, `NOTICE` | 0 | 0 paquetes vetados (NLLB/XTTS/F5/Piper/espeak/phonemizer); excludes y NOTICE presentes. |
| SSRF guard exercise (`_validate_base_url` vs 9 vectores) | `server/pipeline/translator.py` | 0 | 6 vectores bloqueados (metadata IP/host, `file://`, host/IP público, credenciales); 3 legítimos permitidos (loopback, localhost, LAN allowlisteada). |
| `grep -rniE 'set_markup\|Access-Control-Allow-Origin\|shell=True...'` | Superficie GTK / CORS / ejecución | 0 (sin hallazgos) | 0 `set_markup`, 0 CORS `*`, 0 `shell=True`/`eval`/`exec` reales. |

---

## 5. FINAL RELEASE SIGN-OFF

- **All `CRITICAL` findings resolved & verified:** `YES` (0 hallazgos CRITICAL — nada que remeditar)
- **All `HIGH` findings resolved & verified:** `YES` (0 hallazgos HIGH — nada que remeditar)
- **All `MEDIUM` / `LOW` findings resolved & verified:** `YES` (0 hallazgos)
- **Checklist completo §2 ejecutado y verificado:** `YES`
- **Tooling §4 ejecutado con exit code 0:** `YES` (7 comandos/escaneos, todos en 0)
- **0 CVEs (pip-audit) y 0 licencias prohibidas (uv.lock):** `YES`
- **Sin secretos expuestos ni stack traces al cliente:** `YES`
- **UI/GTK con texto plano (sin markup) y permisos de estado 0o600/0o700:** `YES`
- **Ready for Production Delivery:** `APPROVED`
