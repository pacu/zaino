#!/usr/bin/env bash

# Entrypoint for running Zaino in Docker.
#
# This script handles privilege dropping and directory setup for default paths.
# Configuration is managed by config-rs using defaults, optional TOML, and
# environment variables prefixed with ZAINO_.
#
# NOTE: This script only handles directories specified via environment variables
# or the defaults below. If you configure custom paths in a TOML config file,
# you are responsible for ensuring those directories exist with appropriate
# permissions before starting the container.

set -eo pipefail

# Default writable paths matching zaino's XDG defaults.
# Users can override via env vars; the entrypoint will create and chown them.
#
# XDG_CACHE_HOME defaults: storage.database.path
: "${ZAINO_STORAGE__DATABASE__PATH:=${XDG_CACHE_HOME:-${HOME}/.cache}/zaino}"
#
# XDG_RUNTIME_DIR defaults: json_server_settings.cookie_dir (ephemeral)
: "${ZAINO_JSON_SERVER_SETTINGS__COOKIE_DIR:=${XDG_RUNTIME_DIR:-/tmp}/zaino}"
#
# XDG_CONFIG_HOME defaults: config file location
ZAINO_CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/zaino"

# Drop privileges and execute command as non-root user
exec_as_user() {
  user=$(id -u)
  if [[ ${user} == '0' ]]; then
    exec setpriv --reuid="${UID}" --regid="${GID}" --init-groups "$@"
  else
    exec "$@"
  fi
}

exit_error() {
  echo "ERROR: $1" >&2
  exit 1
}

# Creates a directory if it doesn't exist and sets ownership to UID:GID.
create_owned_directory() {
  local dir="$1"
  [[ -z ${dir} ]] && return

  mkdir -p "${dir}" || exit_error "Failed to create directory: ${dir}"
  chown -R "${UID}:${GID}" "${dir}" || exit_error "Failed to set ownership on: ${dir}"

  # Set ownership on parent if it's not root or home
  local parent_dir
  parent_dir="$(dirname "${dir}")"
  if [[ "${parent_dir}" != "/" && "${parent_dir}" != "${HOME}" ]]; then
    chown "${UID}:${GID}" "${parent_dir}" 2>/dev/null || true
  fi
}

# Create and set ownership on writable directories
create_owned_directory "${ZAINO_STORAGE__DATABASE__PATH}"
create_owned_directory "${ZAINO_JSON_SERVER_SETTINGS__COOKIE_DIR}"
create_owned_directory "${ZAINO_CONFIG_DIR}"

# Execute zainod with dropped privileges
exec_as_user zainod "$@"
