#!/usr/bin/env bash
set -a
source "$(dirname "$0")/.env"
set +a
echo "curl -X POST http://127.0.0.1:8000/ -H 'Content-Type: application/json' -d '{\"prompt\":\"create a simple dashboard\"}' --no-buffer"
exec /Users/paul/Sources/wasmCloud/target/debug/wash dev "$@"
