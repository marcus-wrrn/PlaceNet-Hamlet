#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 --target <build-target> --server <user@host> --path <remote-path>"
    echo
    echo "  --target   Rust target triple (e.g. aarch64-unknown-linux-gnu)"
    echo "  --server   SSH destination (e.g. user@192.168.1.10)"
    echo "  --path     Remote deployment path (e.g. /opt/placenet-home)"
    exit 1
}

BUILD_TARGET=""
SERVER=""
DEPLOY_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target) BUILD_TARGET="$2"; shift 2 ;;
        --server) SERVER="$2"; shift 2 ;;
        --path)   DEPLOY_PATH="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

[[ -z "$BUILD_TARGET" ]] && { echo "Error: --target is required"; usage; }
[[ -z "$SERVER" ]]       && { echo "Error: --server is required"; usage; }
[[ -z "$DEPLOY_PATH" ]]  && { echo "Error: --path is required"; usage; }

BINARY_NAME="placenet-home"
BINARY_PATH="target/${BUILD_TARGET}/release/${BINARY_NAME}"

read -rsp "==> Remote sudo password for ${SERVER}: " SUDO_PASS
echo

echo "==> Building ${BINARY_NAME} for ${BUILD_TARGET}..."
cargo build --release --target "$BUILD_TARGET"

# echo "==> Ensuring remote directory exists..."
# ssh "$SERVER" "sudo mkdir -p '${DEPLOY_PATH}'"

echo "==> Copying binary to ${SERVER}:${DEPLOY_PATH}/${BINARY_NAME}..."
scp "$BINARY_PATH" "${SERVER}:/tmp/${BINARY_NAME}"
echo "$SUDO_PASS" | ssh "$SERVER" "sudo -S bash -c 'mkdir -p ${DEPLOY_PATH} && mv /tmp/${BINARY_NAME} ${DEPLOY_PATH}/${BINARY_NAME} && chmod +x ${DEPLOY_PATH}/${BINARY_NAME}'"

echo "==> Restarting ${BINARY_NAME}.service on ${SERVER}..."
echo "$SUDO_PASS" | ssh "$SERVER" "sudo -S bash -c 'systemctl stop ${BINARY_NAME}.service && systemctl start ${BINARY_NAME}.service'"

echo "==> Done. Binary deployed to ${SERVER}:${DEPLOY_PATH}/${BINARY_NAME}"
