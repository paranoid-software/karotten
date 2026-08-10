#!/usr/bin/env bash
# Standalone runner for local dev — loads `.env` and execs the binary.
# Picks the right binary for the current OS/arch automatically.
#
# Usage: ./run-local.sh

set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "no .env in $(pwd) — copy .env.example and fill it in" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

case "$(uname -s)/$(uname -m)" in
  Darwin/arm64)  bin=./karotten-darwin-arm64 ;;
  Linux/aarch64) bin=./karotten-linux-arm64 ;;
  Linux/arm64)   bin=./karotten-linux-arm64 ;;
  *)
    echo "no prebuilt binary for $(uname -s)/$(uname -m) — build with 'go build -o karotten .'" >&2
    exit 1
    ;;
esac

if [ ! -x "$bin" ]; then
  echo "binary $bin not found or not executable" >&2
  exit 1
fi

exec "$bin"
