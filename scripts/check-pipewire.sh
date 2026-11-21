#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# BabelPipe — Verificación del entorno PipeWire en Kali Linux.
#
# Comprueba que las herramientas de PipeWire estén instaladas y operativas:
#   pactl, pw-cli, pw-dump, pipewire y wireplumber presentes en $PATH
#   pactl info, pw-cli info y pw-dump (JSON válido), además del proceso activo.
#
# Modo --dry-run: usa fixtures embebidas; NO requiere hardware ni PipeWire real.
# Variables de simulación (solo afectan a --dry-run):
#   BABELPIPE_DRY_RUN_MISSING=pactl[,pw-cli,...]  -> componente(s) ausente(s)
#   BABELPIPE_DRY_RUN_INACTIVE=1                  -> PipeWire inactivo
#
# Códigos de salida: 0 = entorno OK, 1 = componente faltante, 2 = inactivo.
# Este script NUNCA ejecuta sudo NI instala paquetes: los mensajes de
# instalación son únicamente recomendaciones accionables para el operador.
# ---------------------------------------------------------------------------
set -euo pipefail

readonly EXIT_OK=0
readonly EXIT_MISSING=1
readonly EXIT_INACTIVE=2
readonly INSTALL_HINT="Instala: sudo apt install pipewire wireplumber libpipewire-0.3-utils"
readonly -a REQUIRED_BINARIES=(pactl pw-cli pw-dump pipewire wireplumber)

DRY_RUN=0
JSON_OUTPUT=0
MISSING_OVERRIDE="${BABELPIPE_DRY_RUN_MISSING:-}"
INACTIVE_OVERRIDE="${BABELPIPE_DRY_RUN_INACTIVE:-}"

NAMES=()
STATUSES=()
DETAILS=()
CATEGORIES=()
ERRORS=()
declare -A SIM_MISSING=()

exit_code="$EXIT_OK"

usage() {
  cat <<'EOF'
Uso: bash scripts/check-pipewire.sh [OPCIONES]

Verifica que el entorno PipeWire de Kali esté listo para BabelPipe.

Opciones:
  --dry-run   Usa fixtures embebidas (sin hardware ni PipeWire real).
  --json      Emite un informe JSON en stdout con: ok, components, errors.
  -h, --help  Muestra esta ayuda.

Variables de simulación (solo con --dry-run):
  BABELPIPE_DRY_RUN_MISSING=pactl[,pw-cli,...]  Marca componentes como ausentes.
  BABELPIPE_DRY_RUN_INACTIVE=1                  Simula PipeWire inactivo.

Códigos de salida:
  0  Entorno OK
  1  Componente faltante o no operativo
  2  PipeWire no activo

Este script no ejecuta sudo ni instala paquetes: los mensajes de instalación
son solo recomendaciones para el operador.
EOF
}

parse_args() {
  while (( $# > 0 )); do
    case "$1" in
      --dry-run) DRY_RUN=1 ;;
      --json) JSON_OUTPUT=1 ;;
      -h | --help)
        usage
        exit "$EXIT_OK"
        ;;
      *)
        printf 'Error: opción desconocida "%s".\n' "$1" >&2
        usage >&2
        exit "$EXIT_MISSING"
        ;;
    esac
    shift
  done
}

parse_sim_overrides() {
  local token
  local -a tokens=()
  if [[ -n "$MISSING_OVERRIDE" ]]; then
    IFS=',' read -r -a tokens <<<"$MISSING_OVERRIDE"
    for token in "${tokens[@]}"; do
      token="${token//[[:space:]]/}"
      if [[ -n "$token" ]]; then
        SIM_MISSING["$token"]=1
      fi
    done
  fi
}

# Fixtures embebidas: salidas representativas, sin depender del sistema real.
dry_pactl_info() {
  cat <<'EOF'
Server String: unix:/run/user/1000/pipewire-0
Library Version: 1.0.5
Protocol Version: 9
Default Sink: Translator_Rx_Sink
Default Source: Translator_Tx_Mic
Cookie: 3141592653
EOF
}

dry_pwcli_info() {
  cat <<'EOF'
id 0, type PipeWire:Interface:Core/5
 	version = 5
 	name = "pipewire-0"
 	user-name = "babelpipe"
EOF
}

dry_pwdump() {
  cat <<'EOF'
[
  {"id": 0, "type": "PipeWire:Interface:Core", "info": {"version": 5, "name": "pipewire-0"}},
  {"id": 1, "type": "PipeWire:Interface:Node", "info": {"name": "Translator_Rx_Sink", "media.class": "Audio/Sink"}},
  {"id": 2, "type": "PipeWire:Interface:Node", "info": {"name": "Translator_Tx_Mic", "media.class": "Audio/Source"}}
]
EOF
}

# Devuelve 0 si el nombre está ausente (simulado o real), 1 si está presente.
is_missing() {
  local name="$1"
  if (( DRY_RUN )); then
    if [[ -n "${SIM_MISSING[$name]:-}" ]]; then
      return 0
    fi
    return 1
  fi
  if command -v "$name" >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

# Valida JSON con el intérprete Python del stack (no con jq, ausente en Kali base).
# Devuelve 2 si no hay validador disponible.
is_valid_json() {
  local payload="$1"
  local py=""
  if command -v python3 >/dev/null 2>&1; then
    py="python3"
  elif command -v python >/dev/null 2>&1; then
    py="python"
  else
    return 2
  fi
  printf '%s' "$payload" | "$py" -c 'import json, sys; json.load(sys.stdin)' >/dev/null 2>&1
}

process_active() {
  local proc="$1"
  if command -v pgrep >/dev/null 2>&1; then
    if pgrep -x "$proc" >/dev/null 2>&1; then
      return 0
    fi
    return 1
  fi
  if command -v ps >/dev/null 2>&1; then
    if ps -eo comm= | grep -Fxq -- "$proc"; then
      return 0
    fi
    return 1
  fi
  return 1
}

add_result() {
  local name="$1" status="$2" detail="$3" category="$4"
  NAMES+=("$name")
  STATUSES+=("$status")
  DETAILS+=("$detail")
  CATEGORIES+=("$category")
}

add_error() {
  ERRORS+=("$1")
}

check_binary() {
  local name="$1"
  if is_missing "$name"; then
    add_result "$name" "missing" "no encontrado en \$PATH" "binary"
    add_error "Falta el ejecutable '$name' en \$PATH. $INSTALL_HINT"
    return 0
  fi

  local location="disponible (fixture --dry-run)"
  if (( DRY_RUN == 0 )); then
    location="$(command -v "$name" 2>/dev/null || printf 'en PATH')"
  fi
  add_result "$name" "ok" "$location" "binary"
}

check_pactl_info() {
  if is_missing pactl; then
    add_result "pactl info" "missing" "pactl no disponible" "functional"
    return 0
  fi

  local ok=0
  if (( DRY_RUN )); then
    local fixture
    fixture="$(dry_pactl_info)"
    if [[ -n "$fixture" ]]; then
      ok=1
    fi
  else
    if pactl info >/dev/null 2>&1; then
      ok=1
    fi
  fi

  if (( ok )); then
    add_result "pactl info" "ok" "servidor PipeWire responde" "functional"
  else
    add_result "pactl info" "missing" "pactl info no responde" "functional"
    add_error "'pactl info' no responde; PipeWire puede estar caído. $INSTALL_HINT"
  fi
}

check_pwcli_info() {
  if is_missing pw-cli; then
    add_result "pw-cli info" "missing" "pw-cli no disponible" "functional"
    return 0
  fi

  local ok=0
  if (( DRY_RUN )); then
    local fixture
    fixture="$(dry_pwcli_info)"
    if [[ -n "$fixture" ]]; then
      ok=1
    fi
  else
    if pw-cli info 0 >/dev/null 2>&1; then
      ok=1
    elif pw-cli info >/dev/null 2>&1; then
      ok=1
    fi
  fi

  if (( ok )); then
    add_result "pw-cli info" "ok" "core de PipeWire responde" "functional"
  else
    add_result "pw-cli info" "missing" "pw-cli info no responde" "functional"
    add_error "'pw-cli info' no responde; revisa el daemon PipeWire. $INSTALL_HINT"
  fi
}

check_pwdump_json() {
  if is_missing pw-dump; then
    add_result "pw-dump JSON" "missing" "pw-dump no disponible" "functional"
    return 0
  fi

  local payload="" ok=0
  if (( DRY_RUN )); then
    payload="$(dry_pwdump)"
  else
    payload="$(pw-dump 2>/dev/null || printf '')"
  fi

  if [[ -n "$payload" ]] && is_valid_json "$payload"; then
    ok=1
  fi

  if (( ok )); then
    add_result "pw-dump JSON" "ok" "grafo serializado como JSON válido" "functional"
  else
    add_result "pw-dump JSON" "missing" "pw-dump no produjo JSON válido" "functional"
    add_error "'pw-dump' no produjo JSON válido; actualiza libpipewire. $INSTALL_HINT"
  fi
}

check_pipewire_process() {
  local active=0
  if (( DRY_RUN )); then
    if [[ -z "${SIM_MISSING[pipewire]:-}" ]] && [[ -z "$INACTIVE_OVERRIDE" ]]; then
      active=1
    fi
  else
    if process_active pipewire; then
      active=1
    fi
  fi

  if (( active )); then
    add_result "pipewire (proceso)" "ok" "activo" "process"
  else
    add_result "pipewire (proceso)" "inactive" "proceso no encontrado" "process"
    add_error "El proceso 'pipewire' no está activo. Arranca el servicio: systemctl --user restart pipewire. Si falta: $INSTALL_HINT"
  fi
}

compute_exit_code() {
  local status
  for status in "${STATUSES[@]}"; do
    case "$status" in
      missing)
        exit_code="$EXIT_MISSING"
        ;;
      inactive)
        if (( exit_code == EXIT_OK )); then
          exit_code="$EXIT_INACTIVE"
        fi
        ;;
    esac
  done
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

emit_json() {
  local ok_word="false"
  if (( exit_code == EXIT_OK )); then
    ok_word="true"
  fi

  local output="{\"ok\": $ok_word, \"components\": {"
  local i first=1
  for i in "${!NAMES[@]}"; do
    if (( first == 0 )); then
      output+=","
    fi
    first=0
    output+="\"$(json_escape "${NAMES[$i]}")\": {"
    output+="\"status\": \"$(json_escape "${STATUSES[$i]}")\", "
    output+="\"category\": \"$(json_escape "${CATEGORIES[$i]}")\", "
    output+="\"detail\": \"$(json_escape "${DETAILS[$i]}")\"}"
  done
  output+="}, \"errors\": ["

  first=1
  local error
  for error in "${ERRORS[@]}"; do
    if (( first == 0 )); then
      output+=","
    fi
    first=0
    output+="\"$(json_escape "$error")\""
  done
  output+="]}"

  printf '%s\n' "$output"
}

emit_human() {
  local mode="real"
  if (( DRY_RUN )); then
    mode="dry-run"
  fi

  printf 'BabelPipe — verificación de entorno PipeWire [%s]\n\n' "$mode"
  printf '%-22s %-10s %s\n' 'COMPONENTE' 'ESTADO' 'DETALLE'
  printf '%s\n' '--------------------------------------------------------------------------------'

  local i
  for i in "${!NAMES[@]}"; do
    printf '%-22s %-10s %s\n' "${NAMES[$i]}" "${STATUSES[$i]}" "${DETAILS[$i]}"
  done

  if (( ${#ERRORS[@]} > 0 )); then
    printf '\nERRORES Y ACCIÓN REQUERIDA:\n'
    local error
    for error in "${ERRORS[@]}"; do
      printf '  - %s\n' "$error"
      # Se replica en stderr para que el diagnóstico esté presente en ambos flujos.
      printf '  - %s\n' "$error" >&2
    done
  fi

  printf '\n'
  if (( exit_code == EXIT_OK )); then
    printf 'Resultado: ENTORNO OK — PipeWire listo para BabelPipe.\n'
  elif (( exit_code == EXIT_MISSING )); then
    printf 'Resultado: FALLÓ — componente faltante o no operativo (código %d).\n' "$exit_code"
  else
    printf 'Resultado: FALLÓ — PipeWire no activo (código %d).\n' "$exit_code"
  fi
}

main() {
  parse_args "$@"
  parse_sim_overrides

  local binary
  for binary in "${REQUIRED_BINARIES[@]}"; do
    check_binary "$binary"
  done
  check_pactl_info
  check_pwcli_info
  check_pwdump_json
  check_pipewire_process

  compute_exit_code

  if (( JSON_OUTPUT )); then
    emit_json
  else
    emit_human
  fi

  return "$exit_code"
}

main "$@"
