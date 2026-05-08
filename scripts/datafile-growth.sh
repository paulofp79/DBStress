#!/bin/sh

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TARGET_SCRIPT="$SCRIPT_DIR/datafile-growth.bash"

if command -v bash >/dev/null 2>&1; then
  exec bash "$TARGET_SCRIPT" "$@"
fi

echo "This script requires bash." >&2
exit 1
