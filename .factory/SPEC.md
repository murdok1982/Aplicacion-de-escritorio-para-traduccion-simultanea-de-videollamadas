# PROJECT SPECIFICATION (SINGLE SOURCE OF TRUTH)

- **Project Name:** BabelPipe — Traducción Simultánea Universal sobre PipeWire
- **Status:** `DRAFT` (pendiente de aprobación humana — ZERO-CODE GATE activo)
- **Target Stack:** Python 3.12 (aislado con `uv`) + PipeWire/WirePlumber + WebSocket binario + faster-whisper/CTranslate2 + Kokoro-82M + GTK4 overlay
- **Last Updated By:** `@architect`

---

## 1. SCOPE & BOUNDARIES

### 1.1 In-Scope (Entregables Obligatorios)

**MVP — Fase 1 (Solo audio, sin IA):**
- Creación/limpieza idempotente de los nodos virtuales `Translator_Rx_Sink` (sink) y `Translator_Tx_Mic` (source).
- Loopback PCM: captura 48 kHz → resampleo 16 kHz → devolución 48 kHz sin pérdida ni glitches.
- Mezclador con **audio ducking** configurable (atenuar original al 10-15 %) y verificación sin eco.

**MVP — Fase 2 (Protocolo + servidor mock/real):**
- Protocolo WebSocket binario (chunks PCM + frames JSON de control) con reconexión y backpressure.
- Servidor mock para tests sin hardware; conexión progresiva VAD → STT → Traducción → TTS.
- Presupuesto de latencia extremo a extremo medible y < 1500 ms por cláusula.

**MVP — Fase 3 (Modo unidireccional — YouTube/VLC/navegador):**
- Enrutamiento de audio multimedia a `Translator_Rx_Sink`, doblaje con ducking y subtítulos flotantes en vivo.

**MVP — Fase 4 (Modo bidireccional — Telegram/Teams/Meet/Discord):**
- Canal Tx hacia `Translator_Tx_Mic`, WebRTC AEC vía `libpipewire-module-echo-cancel`, Push-to-Talk opcional, prueba de llamada completa.

**Fase 5 (Interfaz y empaquetado):** panel de control, persistencia de configuración y script AppImage/Flatpak.

### 1.2 Out-of-Scope (Exclusiones Explícitas)

- Aplicaciones móviles iOS/Android y versiones web de escritorio.
- Integraciones propietarias por API (Telegram Bot API, Google Translate API, APIs de Teams/Meet, credenciales de terceros). El sistema opera **exclusivamente** sobre el grafo de audio del sistema.
- Modelos con licencia no comercial o copyleft viral en el core (ver §6).
- Drivers de kernel, ALSA como capa de ruteo principal o PulseAudio heredado como objetivo (PipeWire nativo; `pactl` solo como herramienta de verificación).
- Transcripción persistenta en disco, historial de conversaciones en la nube o telemetría.

---

## 2. USER ROLES & ACCESS MATRIX

> El sistema no tiene cuentas de usuario: la autenticación es por **token compartido** en la frontera WebSocket LAN.

| Role | Description | Auth Required | Allowed Resources / Actions |
| :--- | :--- | :--- | :--- |
| `guest` | Cliente sin token / sonda de salud | No | `GET /healthz` únicamente; sin negociación de stream |
| `client` | Cliente ligero autorizado | Sí (Bearer token en `config.toml`/`.env`) | Abrir WS, enviar PCM 16 kHz, recibir TTS y subtítulos |
| `admin` | Operador del servidor de inferencia | Sí (token con flag `admin`) | Recargar modelos, consultar métricas de latencia, reiniciar pipelines |

---

## 3. FUNCTIONAL REQUIREMENTS & ACCEPTANCE CRITERIA

> **Regla para `@qa-verifier`:** Ningún `AC-XX` puede marcarse `[x]` sin un test automatizado o comando de verificación ejecutado con éxito.

### Feature 1: Captura y ruteo PipeWire (Rx/Tx)

- **REQ-01:** El daemon cliente crea `Translator_Rx_Sink` (sink virtual 48 kHz stereo) y `Translator_Tx_Mic` (source virtual 48 kHz mono/stereo) de forma idempotente y los elimina al salir, incluso tras señales `SIGINT/SIGTERM`. El script `scripts/check-pipewire.sh` verifica `pactl info` y `pw-cli info`.
  - [ ] **AC-01:** **GIVEN** un entorno Kali con PipeWire activo y los nodos ausentes, **WHEN** se ejecuta `uv run python -m client.main --setup`, **ENTONCES** aparecen exactamente un `Translator_Rx_Sink` y un `Translator_Tx_Mic` en `pw-dump`, y un segundo arranque no crea duplicados (idempotencia).
    - *Verification Test:* `tests/integration/test_pipewire_graph.py::test_ac01_idempotent_nodes`
  - [ ] **AC-02:** **GIVEN** el daemon detenido con `SIGTERM`, **WHEN** termina el proceso, **ENTONCES** no quedan nodos `Translator_*` huérfanos en `pw-dump`.
    - *Verification Test:* `tests/integration/test_pipewire_graph.py::test_ac02_cleanup_on_signal`

- **REQ-02:** Conversión explícita y testeada: PipeWire 48 kHz (float32 interleaved) → pipeline 16 kHz mono (`pcm_s16le`, int16 LE, 20 ms = 320 samples = 640 bytes/chunk) → salida TTS 16 kHz → 48 kHz. Prohibido cortar por ventanas ciegas sin VAD.
  - [ ] **AC-03:** **GIVEN** un fixture WAV de 48 kHz con tono de 1 kHz, **WHEN** se resamplea a 16 kHz y de vuelta a 48 kHz, **ENTONCES** la frecuencia se conserva con desviación < 1 % y SNR > 40 dB, y el chunk resultante es exactamente 320 muestras int16 LE.
    - *Verification Test:* `tests/unit/test_resampler.py::test_ac03_roundtrip_snr`

### Feature 2: Protocolo WebSocket y resiliencia

- **REQ-03:** Un único WebSocket (`/ws/translate`) multiplexa frames binarios PCM (cabecera de 16 bytes: magic `BP`, versión, flags, stream_id, sequence, sample_rate, num_samples) y eventos JSON de control (`config`, `subtitle`, `duck`, `ptt`, `error`, `reconnect`, `ping/pong`).
  - [ ] **AC-04:** **GIVEN** un frame PCM de 640 bytes con `sequence=42`, **WHEN** se codifica y decodifica, **ENTONCES** cabecera y payload son byte-idénticos y los mensajes JSON inválidos son rechazados con `{"type":"error","code":"SCHEMA"}` sin cerrar el socket.
    - *Verification Test:* `tests/unit/test_protocol.py::test_ac04_binary_roundtrip`
  - [ ] **AC-05:** **GIVEN** una desconexión del servidor a mitad de cláusula, **WHEN** el cliente reintenta con backoff exponencial (250 ms → 8 s), **ENTONCES** reanuda con `session_id` + `last_seq`, sin duplicar subtítulos ni perder más de un chunk.
    - *Verification Test:* `tests/integration/test_client_rx.py::test_ac05_reanuda_con_last_seq_sin_duplicar_subtitulos`

### Feature 3: Pipeline de IA en streaming

- **REQ-04:** Pipeline modular con interfaces `VAD`, `STT`, `Translator`, `TTS` intercambiables por configuración. VAD Silero segmenta por pausas naturales (`min_silence_ms=250`), STT incremental, traducción vía adaptador (Opus-MT local o LLM compatible OpenAI) y TTS Kokoro-82M/Qwen3-TTS con salida por chunks.
  - [ ] **AC-06:** **GIVEN** un stream sintético con dos cláusulas separadas por 400 ms de silencio, **WHEN** pasa por el pipeline mock, **ENTONCES** se emiten exactamente 2 segmentos finales con `is_final=true` y ningún corte dentro de una cláusula.
    - *Verification Test:* `tests/unit/test_vad.py::test_ac06_no_blind_windows`
  - [ ] **AC-07:** **GIVEN** el pipeline completo con modelo mock y RTT LAN simulado de 30 ms, **WHEN** se procesa una cláusula de 8 palabras, **ENTONCES** el primer chunk de TTS llega al cliente en < 1500 ms y el desglose por etapa queda registrado en métricas (VAD ≤ 300, STT ≤ 400, MT ≤ 350, TTS ≤ 250, red ≤ 30, render ≤ 50, captura/resample ≤ 20 ms).
    - *Verification Test:* `tests/integration/test_pipeline_latency.py::test_ac07_latency_budget`

### Feature 4: Modo vídeo (YouTube) — doblaje y subtítulos

- **REQ-05:** El audio multimedia se enruta a `Translator_Rx_Sink`; su `.monitor` se traduce y se mezcla en el sink físico aplicando ducking (original al 12 % por defecto, rango 10-15 %) mientras suena el doblaje. Overlay GTK siempre visible y semitransparente.
  - [ ] **AC-08 (GIVEN/WHEN/THEN):** **GIVEN** un navegador reproduciendo YouTube en `en` con salida en `Translator_Rx_Sink`, objetivo `es`, servidor saludable, **WHEN** el hablante dice "Hello, how are you?" y hace una pausa de 250 ms, **ENTONCES** en < 1500 ms el sink físico contiene voz sintetizada en español, el audio original está atenuado al 12 % durante el doblaje y vuelve al 100 % al terminar, y el overlay muestra "Hola, ¿cómo estás?" con `is_final=true`.
    - *Verification Test:* `tests/e2e/test_video_mode.py::test_ac08_youtube_dubbing`
  - [ ] **AC-09:** **GIVEN** un stream multimedia sin voz (solo música), **WHEN** transcurren 5 s, **ENTONCES** no se emiten subtítulos ni ducking y el audio original permanece al 100 %.
    - *Verification Test:* `tests/e2e/test_video_mode.py::test_ac09_music_no_false_trigger`

### Feature 5: Modo llamada bidireccional

- **REQ-06:** El micrófono físico se captura, pasa por AEC (referencia = monitor del sink físico), VAD/STT/Traducción/TTS, y se inyecta en `Translator_Tx_Mic` para que Telegram/Teams/Meet lo vean como micrófono. PTT opcional con `flags` en la cabecera.
  - [ ] **AC-10 (GIVEN/WHEN/THEN):** **GIVEN** una llamada en Teams con `Translator_Tx_Mic` seleccionado como micrófono, idioma usuario `es` → interlocutor `en`, **WHEN** el usuario dice "Nos vemos mañana" con AEC activo y el interlocutor habla simultáneamente por el sink físico, **ENTONCES** `Translator_Tx_Mic` emite "See you tomorrow" en < 1500 ms y el eco del interlocutor en la señal Tx queda atenuado ≥ 20 dB respecto al micrófono crudo.
    - *Verification Test:* `tests/e2e/test_call_mode.py::test_ac10_bidirectional_call_with_aec`
  - [ ] **AC-11:** **GIVEN** PTT activado, **WHEN** el usuario no pulsa la tecla, **ENTONCES** no se envían frames PCM de voz al servidor (solo silence frames) y el state del panel muestra `PTT_IDLE`.
    - *Verification Test:* `tests/unit/test_ptt.py::test_ac11_ptt_gating`

### Feature 6: Configuración, higiene y empaquetado

- **REQ-07:** Configuración por `.env`/`config.toml` (host, puerto, token, idiomas, rutas de modelos, `duck_level`, `ptt`, `echo_cancel`), sin secretos en el repo. Python aislado con `uv` (PEP 668).
  - [ ] **AC-12:** **GIVEN** un `config.toml.example` sin secretos reales, **WHEN** se valida con Pydantic y se ejecuta `uv sync`, **ENTONCES** no se usa `pip` global y `pip-audit` reporta 0 CVEs críticos/altos.
    - *Verification Test:* `tests/unit/test_config_persistence.py::test_examples_contain_no_real_secrets` + `bash scripts/package.sh --check` (`pip-audit` limpio)
  - [ ] **AC-13:** **GIVEN** el repositorio completo, **WHEN** se ejecuta el script de empaquetado, **ENTONCES** se genera un artefacto AppImage/Flatpak verificable y `README.md` contiene setup, variables de entorno y comandos de ejecución exactos.
    - *Verification Test:* `bash scripts/package.sh --check`

---

## 4. UI/UX & AESTHETIC CONTRACT (`@ui-artisan` Gate)

- **Visual Direction:** Minimalismo técnico oscuro, "HUD" de estudio de grabación: superficies en capas, acento cian/ámbar, legibilidad periférica.
- **Design Tokens (CSS / theme.css):**
  - Background Primary / Surface: `#0B0F14` / `#151B23`
  - Text Primary / Muted: `#E6EDF3` / `#8B98A8`
  - Accent Primary / Hover: `#22D3EE` / `#67E8F9`
  - Semantic (Success / Warning / Danger): `#34D399` / `#FBBF24` / `#F87171`
  - Typography (Display / Body / Mono): `Inter`, `Inter`, `JetBrains Mono`
  - Overlay: fondo `rgba(11,15,20,0.62)`, `backdrop-filter: blur(8px)`, borde 1 px `--border-subtle`, esquinas `--radius-md: 10px`.
- **Mandatory UI States (100% de vistas con datos):**
  - [ ] **Loading State:** Overlay muestra indicador de "escuchando…" con barra de nivel VAD; panel con skeletons de modelos.
  - [ ] **Empty State:** "Sin audio detectado — enruta una app a Translator_Rx_Sink" + CTA "Ver nodos PipeWire".
  - [ ] **Error State:** Banner recuperable con código (`SERVER_OFFLINE`, `AUTH_FAILED`, `NO_PIPEWIRE`) y botón `Retry`.
- **Responsiveness & Accessibility:**
  - [ ] Overlay redimensionable y funcional en `375px`, `768px`, `1440px`; sin scroll horizontal en el panel de control.
  - [ ] Contraste WCAG AA (`≥ 4.5:1`) y navegación por teclado (`focus-visible`), atajo global de PTT.

---

## 5. EDGE CASES & ERROR HANDLING MATRIX

| Scenario | Trigger | Expected System Behavior | Código / Estado |
| :--- | :--- | :--- | :--- |
| Frame WS malformado | Magic/versión incorrectos o payload < num_samples | Descartar frame, enviar `error SCHEMA`, mantener socket | `error SCHEMA` |
| JSON de control inválido | Pydantic `ValidationError` | Rechazo con campo y código, sin stack trace | `error SCHEMA` |
| Token ausente/inválido | Handshake sin Bearer | Cerrar con `1008 Policy Violation` | `AUTH_FAILED` |
| Servidor de inferencia caído | TCP refusado / timeout | Backoff exponencial, reanudar con `session_id`, UI en estado `error` | `SERVER_OFFLINE` |
| PipeWire no disponible | `pactl info` falla | Abortar setup con mensaje accionable, sin crash en bucle | `NO_PIPEWIRE` |
| Rate de muestreo incorrecto | Frame `sample_rate != 16000` | Resamplear o rechazar con `error FORMAT`, nunca mezclar rates | `error FORMAT` |
| Eco excesivo en Tx | AEC no convergido | Atenuar Tx, mostrar aviso `ECHO_DETECTED`, sugerir PTT | `warning ECHO` |
| Cola de audio saturada | Backpressure > N chunks | Descartar chunks más antiguos con `OVERRUN` y métrica, sin bloquear | `warning OVERRUN` |
| Licencia prohibida detectada | Dependencia XTTS/F5/Piper/NLLB en lockfile | Fallar CI/build con mensaje de licencia | `LICENSE_VIOLATION` |

---

## 6. LICENSE MATRIX (restricción legal estricta)

| Componente | Licencia | Uso en core | Notas |
| :--- | :--- | :--- | :--- |
| Silero VAD | MIT | ✅ PERMITIDO | Segmentación, sin restricción comercial |
| faster-whisper / CTranslate2 | MIT | ✅ PERMITIDO | STT incremental sobre motor CT2 |
| whisper.cpp | MIT | ✅ PERMITIDO | Alternativa STT |
| Kokoro-82M (`kokoro`) | Apache-2.0 | ✅ PERMITIDO | TTS principal, streaming |
| Qwen3-TTS | Apache-2.0 | ✅ PERMITIDO | Alternativa TTS |
| Misaki (G2P) | MIT | ✅ PERMITIDO | **Usar en lugar de espeak-ng** (evita GPL) |
| Opus-MT | CC-BY-4.0 | ✅ PERMITIDO | Atribución obligatoria en `NOTICE` |
| llama.cpp / Ollama / vLLM | MIT / MIT / Apache-2.0 | ✅ PERMITIDO | Endpoint LLM compatible OpenAI |
| **NLLB-200** | **CC-BY-NC-4.0** | ❌ EXCLUIDO | No comercial: se documenta desviación vs. sugerencia del enunciado; usar Opus-MT |
| **XTTS v2** | CPML (no comercial) | ❌ PROHIBIDO | Restricción explícita del usuario |
| **F5-TTS** | CC-BY-NC | ❌ PROHIBIDO | Restricción explícita del usuario |
| **Piper** | GPL-3.0 | ❌ PROHIBIDO | Copyleft viral |
| **espeak-ng / phonemizer-espeak** | GPL-3.0 | ❌ PROHIBIDO | Sustituido por Misaki |

> **Gate:** `uv run pip-audit` + escaneo de licencias en TASK-024 deben devolver 0 hallazgos `CRITICAL/HIGH` y 0 licencias prohibidas.

---

## 7. LATENCY BUDGET (presupuesto por etapa)

| Etapa | Presupuesto (ms) | Medición |
| :--- | :--- | :--- |
| Captura + resampleo 48→16 kHz | ≤ 20 | `tests/integration/test_pipeline_latency.py` |
| VAD (detección de fin de cláusula) | ≤ 300 | Silero con `min_silence_ms=250` |
| STT incremental | ≤ 400 | faster-whisper `tiny/base` int8 |
| Traducción (MT/LLM) | ≤ 350 | Opus-MT CT2 / LLM local |
| TTS (primer chunk) | ≤ 250 | Kokoro-82M streaming |
| Red (RTT LAN / localhost) | ≤ 30 | WebSocket binario |
| Render overlay + ducking | ≤ 50 | GTK + mezclador |
| **TOTAL extremo a extremo** | **< 1500** | **AC-07 / AC-08 / AC-10** |
