#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# BabelPipe — creación/limpieza idempotente de nodos PipeWire virtuales.
#
# Subcomandos:
#   --setup      Crea Translator_Rx_Sink y Translator_Tx_Mic si no existen.
#   --teardown   Descarga SOLO los módulos creados por BabelPipe.
#   --status     Informa del estado de ambos nodos (0 = ambos presentes).
#
# Principios: sin sudo, sin shell anidado, sin duplicados. Los traps de
# INT/TERM/ERR descargan únicamente los IDs de módulo creados en esta sesión;
# un --setup correcto deja los nodos activos al terminar el script.
# ---------------------------------------------------------------------------
set -euo pipefail

readonly RX_SINK="Translator_Rx_Sink"
readonly TX_SOURCE="Translator_Tx_Mic"
readonly TX_MASTER="Translator_Tx_Master"
readonly RATE=48000
readonly CHANNELS=2
CREATED_MODULES=()

usage() {
  cat <<'EOF'
Uso: bash scripts/setup-nodes.sh {--setup|--teardown|--status}

  --setup      Crea Translator_Rx_Sink y Translator_Tx_Mic (idempotente).
  --teardown   Descarga solo los módulos de BabelPipe (idempotente).
  --status     Muestra el estado de los nodos; 0 si ambos existen, 1 si no.
  -h, --help   Muestra esta ayuda.

Requiere pactl y un servicio PipeWire de usuario activo. Nunca ejecuta sudo.
EOF
}

require_pactl() {
  if ! command -v pactl >/dev/null 2>&1; then
    printf 'Error: no se encontró pactl. Instala PipeWire en espacio de usuario: sudo apt install pipewire wireplumber libpipewire-0.3-utils\n' >&2
    return 1
  fi
  if ! pactl info >/dev/null 2>&1; then
    printf 'Error: pactl info no responde. Arranca PipeWire: systemctl --user restart pipewire\n' >&2
    return 1
  fi
}

regex_escape() {
  local value="$1"
  value="${value//./\\.}"
  printf '%s' "$value"
}

# Coincidencia exacta por token: "Translator_Rx_Sink_Other" NO es
# "Translator_Rx_Sink". Se grepea una variable para no sufrir SIGPIPE con
# pipefail al usar grep -q directamente sobre la salida de pactl.
listing_has_name() {
  local listing="$1" escaped
  escaped="$(regex_escape "$2")"
  printf '%s\n' "$listing" | grep -E "(^|[[:space:]])${escaped}([[:space:]]|\$)" >/dev/null
}

sink_exists() {
  local listing
  listing="$(pactl list short sinks 2>/dev/null)" || return 1
  listing_has_name "$listing" "$1"
}

source_exists() {
  local listing
  listing="$(pactl list short sources 2>/dev/null)" || return 1
  listing_has_name "$listing" "$1"
}

# Devuelve los IDs de los módulos que declaran exactamente option=value.
# El anclaje por espacios impide confundir Translator_Rx_Sink con
# Translator_Rx_Sink_OtraApp.
module_ids_for() {
  local option="$1" value="$2" escaped listing
  escaped="$(regex_escape "$value")"
  listing="$(pactl list short modules 2>/dev/null)" || return 0
  printf '%s\n' "$listing" \
    | grep -E "(^|[[:space:]])${option}=${escaped}([[:space:]]|\$)" \
    | cut -f1 \
    || true
}

load_module() {
  local label="$1"
  shift
  local module_id
  if ! module_id="$(pactl load-module "$@" 2>/dev/null)"; then
    printf 'Error: no se pudo crear %s con pactl load-module %s\n' "$label" "$*" >&2
    return 1
  fi
  if [[ ! "$module_id" =~ ^[0-9]+$ ]]; then
    printf 'Error: ID de módulo inesperado para %s: %s\n' "$label" "$module_id" >&2
    return 1
  fi
  CREATED_MODULES+=("$module_id")
  printf '  creado %s (módulo %s)\n' "$label" "$module_id"
}

unload_created_modules() {
  local module_id
  for module_id in "${CREATED_MODULES[@]}"; do
    pactl unload-module "$module_id" >/dev/null 2>&1 || true
  done
  CREATED_MODULES=()
}

on_error() {
  local status=$?
  printf '\nBabelPipe: fallo durante la operación (código %s); limpiando módulos creados.\n' "$status" >&2
  unload_created_modules
  exit "$status"
}

on_signal() {
  local signal_name="$1" status=143
  if [[ "$signal_name" == "INT" ]]; then
    status=130
  fi
  printf '\nBabelPipe: señal %s recibida; limpiando módulos creados.\n' "$signal_name" >&2
  unload_created_modules
  exit "$status"
}

cmd_setup() {
  require_pactl
  printf 'BabelPipe — configurando nodos PipeWire...\n'

  if sink_exists "$RX_SINK"; then
    printf '  %s ya existe (sin cambios)\n' "$RX_SINK"
  else
    load_module "$RX_SINK" module-null-sink \
      "sink_name=${RX_SINK}" \
      "sink_properties=device.description=${RX_SINK}" \
      "rate=${RATE}" \
      "channels=${CHANNELS}"
  fi

  if source_exists "$TX_SOURCE"; then
    printf '  %s ya existe (sin cambios)\n' "$TX_SOURCE"
  else
    if ! sink_exists "$TX_MASTER"; then
      load_module "$TX_MASTER" module-null-sink \
        "sink_name=${TX_MASTER}" \
        "sink_properties=device.description=${TX_MASTER}" \
        "rate=${RATE}" \
        "channels=${CHANNELS}"
    else
      printf '  %s ya existe (sin cambios)\n' "$TX_MASTER"
    fi
    load_module "$TX_SOURCE" module-remap-source \
      "master=${TX_MASTER}.monitor" \
      "source_name=${TX_SOURCE}" \
      "source_properties=device.description=${TX_SOURCE}" \
      "rate=${RATE}" \
      "channels=${CHANNELS}"
  fi

  # El setup ha terminado: los nodos deben persistir tras la salida del script.
  CREATED_MODULES=()
  printf 'Resultado: nodos BabelPipe listos.\n'
}

cmd_teardown() {
  require_pactl
  local raw id
  local -a ids=()

  raw="$(
    {
      module_ids_for sink_name "$RX_SINK"
      module_ids_for sink_name "$TX_MASTER"
      module_ids_for source_name "$TX_SOURCE"
    } | sort -u
  )"
  if [[ -n "$raw" ]]; then
    mapfile -t ids <<<"$raw"
  fi

  if (( ${#ids[@]} == 0 )); then
    printf 'No hay nodos BabelPipe que descargar.\n'
    return 0
  fi

  printf 'BabelPipe — descargando módulos propios...\n'
  for id in "${ids[@]}"; do
    if pactl unload-module "$id" >/dev/null 2>&1; then
      printf '  descargado módulo %s\n' "$id"
    else
      printf '  aviso: no se pudo descargar el módulo %s\n' "$id" >&2
    fi
  done
  printf 'Resultado: limpieza completada.\n'
}

cmd_status() {
  require_pactl
  local rx_present=1 tx_present=1

  if sink_exists "$RX_SINK"; then
    rx_present=0
  fi
  if source_exists "$TX_SOURCE"; then
    tx_present=0
  fi

  printf 'BabelPipe — estado de nodos PipeWire\n'
  printf '  %-22s %s\n' "$RX_SINK" "$([[ $rx_present -eq 0 ]] && printf 'presente' || printf 'ausente')"
  printf '  %-22s %s\n' "$TX_SOURCE" "$([[ $tx_present -eq 0 ]] && printf 'presente' || printf 'ausente')"

  if (( rx_present == 0 && tx_present == 0 )); then
    printf 'Resultado: OK — ambos nodos disponibles.\n'
    return 0
  fi
  printf 'Resultado: faltan nodos; ejecuta --setup.\n'
  return 1
}

main() {
  if (( $# == 0 )); then
    usage >&2
    exit 2
  fi
  case "$1" in
    --setup)
      cmd_setup
      ;;
    --teardown)
      cmd_teardown
      ;;
    --status)
      cmd_status
      ;;
    -h | --help)
      usage
      ;;
    *)
      printf 'Error: subcomando desconocido "%s".\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
}

trap on_error ERR
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

main "$@"
