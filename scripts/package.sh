#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# BabelPipe — Empaquetado y verificación rápida de la distribución.
#
# Modos:
#   --check      Verificación RÁPIDA (no construye nada, no requiere git).
#   --appimage   Construye un AppImage en dist/ mediante appimagetool.
#   --flatpak    Construye un bundle Flatpak en dist/ mediante flatpak-builder.
#
# Garantías:
#   - No ejecuta sudo ni instala paquetes por sí mismo.
#   - No usa eval ni interpola entradas externas dentro de comandos.
#   - Todas las rutas se derivan de la ubicación del script y se normalizan.
#   - Trap de limpieza para cualquier directorio temporal creado.
# ---------------------------------------------------------------------------
set -euo pipefail

readonly EXIT_OK=0
readonly EXIT_ERROR=1
readonly EXIT_USAGE=2

APP_ID="com.babelpipe.BabelPipe"
SCRIPT_PATH=""
PROJECT_ROOT=""
OUT_DIR=""
BUILD_TMP=""

# Paquetes prohibidos por SPEC.md §6 / ADR-003 / ADR-004.
readonly -a VETOED_PACKAGES=(
  "nllb" "nllb-200" "xtts" "xtts-v2" "f5-tts" "piper"
  "espeak" "espeak-ng" "phonemizer" "phonemizer-fork"
)

# Secciones obligatorias de README.md (AC-13).
readonly -a REQUIRED_SECTIONS=(
  "## Requisitos"
  "## Setup"
  "## Variables de entorno"
  "## Ejecución"
  "## Arquitectura"
  "## Modos de uso"
  "## Tests"
  "## Licencias"
  "## Empaquetado"
  "## Troubleshooting"
)

log()  { printf '[package] %s\n' "$*"; }
warn() { printf '[package] AVISO: %s\n' "$*" >&2; }
die()  { printf '[package] ERROR: %s\n' "$*" >&2; exit "$EXIT_ERROR"; }

usage() {
  cat <<'EOF'
Uso: bash scripts/package.sh [MODO]

Modos (exactamente uno):
  --check      Verificación rápida: README, plantillas, entry points, uv.lock,
               resolución de uv y ausencia de dependencias vetadas. Sin build.
  --appimage   Construye dist/BabelPipe-x86_64.AppImage (requiere appimagetool).
  --flatpak    Construye dist/babelpipe.flatpak (requiere flatpak-builder y flatpak).
  -h, --help   Muestra esta ayuda.

Códigos de salida:
  0  Correcto
  1  Error de verificación o construcción
  2  Uso incorrecto

El script no ejecuta sudo ni instala herramientas automáticamente. Si falta una
herramienta de build, se muestra el comando de instalación para el operador.
EOF
}

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -n "${BUILD_TMP}" && -d "${BUILD_TMP}" ]]; then
    rm -rf -- "${BUILD_TMP}" || true
  fi
  if (( rc != 0 )); then
    exit "$rc"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

init_paths() {
  local script_dir
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
  SCRIPT_PATH="${script_dir}/$(basename -- "${BASH_SOURCE[0]}")"
  PROJECT_ROOT="$(cd -- "${script_dir}/.." && pwd -P)"
  case "${PROJECT_ROOT}" in
    *$'\n'* | *$'\r'* | *$'\t'* | *'"'* | *'\'*)
      die "La ruta del proyecto contiene caracteres no seguros: ${PROJECT_ROOT}"
      ;;
  esac
  OUT_DIR="${PROJECT_ROOT}/dist"
  cd -- "${PROJECT_ROOT}"
}

# --------------------------------------------------------------------------
# Modo --check
# --------------------------------------------------------------------------

check_readme() {
  [[ -f README.md ]] || die "Falta README.md en la raíz del proyecto."
  local section
  for section in "${REQUIRED_SECTIONS[@]}"; do
    grep -Fq -- "${section}" README.md \
      || die "README.md no contiene la sección obligatoria '${section}'."
  done
  grep -Fq -- 'BABELPIPE_' README.md \
    || die "README.md no documenta las variables BABELPIPE_*."
  grep -Fq -- 'uv sync' README.md \
    || die "README.md no documenta el comando 'uv sync'."
  log "README.md: secciones obligatorias presentes"
}

check_templates() {
  local file
  for file in .env.example config.toml.example NOTICE; do
    [[ -f "${file}" ]] || die "Falta el archivo obligatorio '${file}'."
  done
  log "Plantillas y NOTICE: presentes"
}

check_uv() {
  command -v uv >/dev/null 2>&1 \
    || die "No se encontró 'uv'. Instálalo con: curl -LsSf https://astral.sh/uv/install.sh | sh"
  uv lock --check --offline >/dev/null 2>&1 \
    || die "uv.lock no está sincronizado con pyproject.toml. Ejecuta: uv lock"
  if ! uv sync --dry-run --frozen --offline --quiet >/dev/null 2>&1; then
    uv sync --dry-run --frozen --quiet >/dev/null 2>&1 \
      || die "uv sync no pudo resolver el entorno. Revisa uv.lock y ejecuta: uv sync"
  fi
  log "uv: lockfile consistente y dependencias resueltas"
}

check_entry_points() {
  [[ -f pyproject.toml ]] || die "Falta pyproject.toml."
  grep -Eq '^[[:space:]]*babelpipe-client[[:space:]]*=' pyproject.toml \
    || die "El entry point 'babelpipe-client' no está declarado en pyproject.toml."
  grep -Eq '^[[:space:]]*babelpipe-server[[:space:]]*=' pyproject.toml \
    || die "El entry point 'babelpipe-server' no está declarado en pyproject.toml."
  log "Entry points: babelpipe-client / babelpipe-server declarados"
}

check_vetoed_packages() {
  [[ -f uv.lock ]] || die "Falta uv.lock."
  local pkg_name lowered veto
  # Se inspeccionan SOLO los bloques [[package]]: la sección [manifest].excludes
  # de uv.lock contiene intencionadamente algunos nombres vetados como excluidos.
  while IFS= read -r pkg_name; do
    [[ -n "${pkg_name}" ]] || continue
    lowered="${pkg_name,,}"
    for veto in "${VETOED_PACKAGES[@]}"; do
      if [[ "${lowered}" == "${veto}" ]]; then
        die "'${pkg_name}' figura como paquete en uv.lock: licencia prohibida (SPEC.md §6)."
      fi
    done
  done < <(
    awk '
      /^\[\[package\]\][[:space:]]*$/ { in_pkg = 1; next }
      in_pkg && /^name[[:space:]]*=/ {
        line = $0
        sub(/^name[[:space:]]*=[[:space:]]*"/, "", line)
        sub(/"[[:space:]]*$/, "", line)
        print line
        in_pkg = 0
      }
    ' uv.lock
  )
  log "uv.lock: 0 dependencias vetadas (NLLB/XTTS/F5/Piper/espeak/phonemizer)"
}

check_git_not_required() {
  # Autos auditoría: el modo --check no debe contener invocaciones a git.
  if grep -Eq '^[[:space:]]*git[[:space:]]' "${SCRIPT_PATH}"; then
    die "El script invoca git en la ruta de verificación; git no debe ser necesario."
  fi
  log "git: no requerido (verificación y build no dependen de un repositorio)"
}

run_check() {
  log "Iniciando verificación rápida (sin construcción)"
  check_readme
  check_templates
  check_uv
  check_entry_points
  check_vetoed_packages
  check_git_not_required
  log "VERIFICACIÓN CORRECTA — AC-13 cubierto"
}

# --------------------------------------------------------------------------
# Modo --appimage
# --------------------------------------------------------------------------

write_appdir_files() {
  local appdir="$1"
  mkdir -p -- "${appdir}/usr/bin" \
              "${appdir}/usr/lib/babelpipe" \
              "${appdir}/usr/share/applications" \
              "${appdir}/usr/share/icons/hicolor/256x256/apps"

  cp -a -- "${PROJECT_ROOT}/client" "${appdir}/usr/lib/babelpipe/client"
  cp -a -- "${PROJECT_ROOT}/server" "${appdir}/usr/lib/babelpipe/server"
  cp -a -- "${PROJECT_ROOT}/shared" "${appdir}/usr/lib/babelpipe/shared"
  cp -a -- "${PROJECT_ROOT}/.venv" "${appdir}/usr/lib/python-env"

  cat > "${appdir}/usr/bin/babelpipe" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PYTHONPATH="${APP_ROOT}/usr/lib/babelpipe:${APP_ROOT}/usr/lib/python-env/lib/python3.12/site-packages${PYTHONPATH:+:${PYTHONPATH}}"
export PATH="${APP_ROOT}/usr/bin:${PATH}"
exec "${APP_ROOT}/usr/lib/python-env/bin/python" -m client.main "$@"
EOF
  chmod 755 -- "${appdir}/usr/bin/babelpipe"

  cat > "${appdir}/AppRun" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
exec "${HERE}/usr/bin/babelpipe" "$@"
EOF
  chmod 755 -- "${appdir}/AppRun"

  cat > "${appdir}/babelpipe.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Name=BabelPipe
GenericName=Traducción simultánea sobre PipeWire
Exec=babelpipe --run
Icon=babelpipe
Categories=AudioVideo;Audio;
Terminal=false
EOF
  cp -- "${appdir}/babelpipe.desktop" "${appdir}/usr/share/applications/babelpipe.desktop"

  # Icono PNG generado localmente: 256x256 con ImageMagick, 1x1 válido como mínimo.
  local icon="${appdir}/usr/share/icons/hicolor/256x256/apps/babelpipe.png"
  if command -v convert >/dev/null 2>&1; then
    convert -size 256x256 xc:'#0B0F14' -fill '#22D3EE' \
      -draw 'circle 128,128 128,40' "${icon}"
  elif command -v magick >/dev/null 2>&1; then
    magick -size 256x256 xc:'#0B0F14' -fill '#22D3EE' \
      -draw 'circle 128,128 128,40' "${icon}"
  else
    # PNG 1x1 válido como mínimo funcional cuando no hay ImageMagick.
    printf '%s' \
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==' \
      | base64 --decode > "${icon}"
  fi
}

build_appimage() {
  if ! command -v appimagetool >/dev/null 2>&1; then
    cat >&2 <<'EOF'
[package] ERROR: no se encontró 'appimagetool' para construir el AppImage.
Instalación sugerida (NO se ejecuta automáticamente y no requiere sudo):

  mkdir -p "$HOME/.local/bin"
  curl -fsSL -o "$HOME/.local/bin/appimagetool" \
    https://github.com/AppImage/appimagetool/releases/download/continuous/appimagetool-x86_64.AppImage
  chmod +x "$HOME/.local/bin/appimagetool"
  export PATH="$HOME/.local/bin:$PATH"
  bash scripts/package.sh --appimage

También puedes instalar 'appimage-builder' desde https://appimage-builder.readthedocs.io/
EOF
    return "$EXIT_ERROR"
  fi

  log "Construyendo AppImage con appimagetool"
  mkdir -p -- "${OUT_DIR}"
  BUILD_TMP="$(mktemp -d "${TMPDIR:-/tmp}/babelpipe-appimage.XXXXXX")"

  uv sync --frozen --quiet
  local appdir="${BUILD_TMP}/AppDir"
  write_appdir_files "${appdir}"

  local artifact="${OUT_DIR}/BabelPipe-x86_64.AppImage"
  (
    cd -- "${BUILD_TMP}"
    APPIMAGE_EXTRACT_AND_RUN=1 appimagetool "${appdir}" "${artifact}"
  )
  log "AppImage generado: ${artifact}"
}

# --------------------------------------------------------------------------
# Modo --flatpak
# --------------------------------------------------------------------------

build_flatpak() {
  if ! command -v flatpak-builder >/dev/null 2>&1 || ! command -v flatpak >/dev/null 2>&1; then
    cat >&2 <<'EOF'
[package] ERROR: faltan 'flatpak-builder' y/o 'flatpak' para construir el bundle.
Instalación sugerida (NO se ejecuta automáticamente; usa el gestor de tu distro):

  apt install flatpak flatpak-builder
  flatpak remote-add --if-not-exists flathub https://flathub.org/repo/flathub.flatpakrepo
  flatpak install flathub org.freedesktop.Platform//24.08 org.freedesktop.Sdk//24.08
  bash scripts/package.sh --flatpak

En Fedora: dnf install flatpak flatpak-builder
En Arch:   pacman -S flatpak flatpak-builder
EOF
    return "$EXIT_ERROR"
  fi

  log "Construyendo Flatpak con flatpak-builder"
  mkdir -p -- "${OUT_DIR}"
  BUILD_TMP="$(mktemp -d "${TMPDIR:-/tmp}/babelpipe-flatpak.XXXXXX")"

  local manifest="${BUILD_TMP}/${APP_ID}.json"
  local repo="${BUILD_TMP}/repo"
  local builddir="${BUILD_TMP}/builddir"
  local artifact="${OUT_DIR}/babelpipe.flatpak"

  cat > "${manifest}" <<EOF
{
  "app-id": "${APP_ID}",
  "runtime": "org.freedesktop.Platform",
  "runtime-version": "24.08",
  "sdk": "org.freedesktop.Sdk",
  "command": "babelpipe-client",
  "finish-args": [
    "--share=ipc",
    "--socket=wayland",
    "--socket=x11",
    "--socket=pulseaudio",
    "--filesystem=xdg-run/pipewire-0",
    "--device=all"
  ],
  "modules": [
    {
      "name": "babelpipe",
      "buildsystem": "simple",
      "build-commands": [
        "python3 -m pip install --no-cache-dir --prefix=/app ."
      ],
      "sources": [
        {
          "type": "dir",
          "path": "${PROJECT_ROOT}"
        }
      ]
    }
  ]
}
EOF

  flatpak-builder --user --force-clean --repo="${repo}" "${builddir}" "${manifest}"
  flatpak build-bundle "${repo}" "${artifact}" "${APP_ID}"
  log "Bundle Flatpak generado: ${artifact}"
}

# --------------------------------------------------------------------------
# Punto de entrada
# --------------------------------------------------------------------------

main() {
  local mode=""
  while (( $# > 0 )); do
    case "$1" in
      --check)
        [[ -z "${mode}" ]] || die "Especifica un único modo."
        mode="check"
        ;;
      --appimage)
        [[ -z "${mode}" ]] || die "Especifica un único modo."
        mode="appimage"
        ;;
      --flatpak)
        [[ -z "${mode}" ]] || die "Especifica un único modo."
        mode="flatpak"
        ;;
      -h | --help)
        usage
        exit "$EXIT_OK"
        ;;
      *)
        printf '[package] ERROR: opción desconocida "%s".\n' "$1" >&2
        usage >&2
        exit "$EXIT_USAGE"
        ;;
    esac
    shift
  done

  if [[ -z "${mode}" ]]; then
    usage >&2
    exit "$EXIT_USAGE"
  fi

  init_paths
  case "${mode}" in
    check) run_check ;;
    appimage) build_appimage ;;
    flatpak) build_flatpak ;;
  esac
}

main "$@"
