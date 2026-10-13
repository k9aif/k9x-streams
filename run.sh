#!/usr/bin/env bash
# K9X Streams — run locally.
#
#   ./run.sh                 start K9X Streams on http://localhost:8780
#                            (starts a local Redpanda in podman/docker unless KAFKA_BROKER is set)
#   ./run.sh stop-kafka      stop the local Redpanda this script started
#
# Settings come from .env (copy .env.sample). Uses the shared k9-aif-framework venv when it
# exists (sibling checkout), otherwise a local .venv with requirements.txt.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
FRAMEWORK="$(cd "$DIR/../.." && pwd)/k9-aif-framework"
KAFKA_CONTAINER="k9x-streams-redpanda"
LOCAL_KAFKA_PORT=19092

engine() { command -v podman >/dev/null 2>&1 && echo podman || { command -v docker >/dev/null 2>&1 && echo docker; } || true; }

if [[ "${1:-}" == "stop-kafka" ]]; then
  E="$(engine)"; [[ -n "$E" ]] && "$E" rm -f "$KAFKA_CONTAINER" >/dev/null 2>&1 && echo "Stopped $KAFKA_CONTAINER." || echo "Nothing to stop."
  exit 0
fi

# ── settings ──
ENV_FILE="${STREAMS_ENV_FILE:-.env}"     # another file: STREAMS_ENV_FILE=.env.confluent ./run.sh
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
PORT="${STREAMS_PORT:-8780}"

# ── Python ──
if [[ -x "$FRAMEWORK/.venv/bin/python" ]]; then
  PY="$FRAMEWORK/.venv/bin/python"
  export PYTHONPATH="$DIR:$FRAMEWORK${PYTHONPATH:+:$PYTHONPATH}"
else
  [[ -d .venv ]] || python3 -m venv .venv
  PY="$DIR/.venv/bin/python"
  export PYTHONPATH="$DIR${PYTHONPATH:+:$PYTHONPATH}"
fi
"$PY" -c "import kafka, mcp, fastapi, uvicorn, k9_aif_abb.k9_streams.context_ingest" 2>/dev/null || "$PY" -m pip install -q -r requirements.txt

# ── Kafka: yours (KAFKA_BROKER), or a local Redpanda ──
if [[ -z "${KAFKA_BROKER:-}" ]]; then
  E="$(engine)"
  if [[ -z "$E" ]]; then
    echo "Set KAFKA_BROKER in .env, or install podman/docker for a local Redpanda." >&2; exit 1
  fi
  if ! "$E" ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
    echo "Starting a local Redpanda ($E) on localhost:$LOCAL_KAFKA_PORT ..."
    "$E" run -d --rm --name "$KAFKA_CONTAINER" -p "127.0.0.1:$LOCAL_KAFKA_PORT:$LOCAL_KAFKA_PORT" \
      docker.io/redpandadata/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 1G \
      --reserve-memory 0M --node-id 0 --check=false \
      --kafka-addr "PLAINTEXT://0.0.0.0:$LOCAL_KAFKA_PORT" \
      --advertise-kafka-addr "PLAINTEXT://localhost:$LOCAL_KAFKA_PORT" >/dev/null
    for _ in $(seq 1 40); do
      "$E" exec "$KAFKA_CONTAINER" rpk cluster health -X "brokers=localhost:$LOCAL_KAFKA_PORT" 2>/dev/null \
        | grep -q "Healthy:.*true" && break
      sleep 2
    done
  fi
  export KAFKA_BROKER="localhost:$LOCAL_KAFKA_PORT"
fi

echo ""
echo "  K9X Streams    http://localhost:$PORT        (landing)"
echo "                 http://localhost:$PORT/demo   (live demo)"
echo "                 http://localhost:$PORT/mcp    (MCP endpoint)"
echo "  Kafka          $KAFKA_BROKER"
echo ""
exec "$PY" -m uvicorn streams.app:app --host 127.0.0.1 --port "$PORT"
