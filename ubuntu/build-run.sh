#!/usr/bin/env bash
# k9x_streams — build and run helper (Podman pod: K9X Streams + its own Redpanda)
# Run from any directory on the Podman host (no sudo needed — script handles it).
#
# streams.k9x.ai → Cloudflare Tunnel → http://localhost:8016 (localhost-only).
#
# Kafka: by default the pod runs its own Redpanda, so the public demo never publishes to
# a shared broker. To use your own Kafka instead, set in .env:
#   STREAMS_KAFKA=external
#   KAFKA_BROKER=host:9092   (plus KAFKA_SECURITY_PROTOCOL / KAFKA_SASL_* if required)
#
# Commands:
#   build   — build the k9x-streams image
#   start   — start the pod (app on 127.0.0.1:8016, Redpanda inside the pod)
#   stop    — stop and remove the pod
#   logs    — tail the app's logs
#   status  — pod, containers and health
#   all     — build + start

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGE="k9x-streams:latest"
POD="k9x-streams"
APP="k9x_streams"
KAFKA="k9x_streams_redpanda"
PORT=8016
REDPANDA_IMAGE="docker.io/redpandadata/redpanda:latest"

env_value() {  # value of KEY in .env, or empty
  { grep -E "^$1=" "$PROJECT_DIR/.env" 2>/dev/null || true; } | tail -1 | cut -d= -f2- | tr -d '"'"'"
}

cmd="${1:-help}"

case "$cmd" in

  build)
    echo "Building $IMAGE ..."
    cd "$PROJECT_DIR"
    # K9AIF_CACHE_VERSION busts the framework git-clone layer so every build has framework main.
    sudo podman build \
      --build-arg K9AIF_CACHE_VERSION="$(date +%s)" \
      -f ubuntu/Containerfile -t "$IMAGE" .
    echo "Build complete: $IMAGE"
    ;;

  start)
    MODE="$(env_value STREAMS_KAFKA)"; MODE="${MODE:-pod}"
    echo "Starting pod $POD on 127.0.0.1:$PORT (Kafka: $MODE) ..."
    sudo podman pod rm -f "$POD" 2>/dev/null || true
    sudo podman pod create --name "$POD" -p "127.0.0.1:$PORT:$PORT"

    ENV_ARGS=()
    [ -f "$PROJECT_DIR/.env" ] && ENV_ARGS=(--env-file "$PROJECT_DIR/.env")
    if [ "$MODE" = "external" ]; then
      BROKER="$(env_value KAFKA_BROKER)"
      [ -n "$BROKER" ] || { echo "STREAMS_KAFKA=external needs KAFKA_BROKER in .env" >&2; exit 1; }
      KAFKA_ARGS=()
    else
      BROKER="localhost:9092"
      sudo podman run -d --name "$KAFKA" --pod "$POD" --restart=always --memory=2g \
        "$REDPANDA_IMAGE" redpanda start --overprovisioned --smp 1 --memory 1G \
        --reserve-memory 0M --node-id 0 --check=false \
        --kafka-addr "PLAINTEXT://0.0.0.0:9092" --advertise-kafka-addr "PLAINTEXT://localhost:9092" >/dev/null
      echo -n "Waiting for Redpanda "
      for _ in $(seq 1 40); do
        sudo podman exec "$KAFKA" rpk cluster health 2>/dev/null | grep -q "Healthy:.*true" && break
        echo -n "."; sleep 2
      done
      echo ""
      # Inside the pod the broker is localhost; .env's broker and security settings don't apply.
      KAFKA_ARGS=(-e KAFKA_BROKER=localhost:9092 -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092
                  -e KAFKA_SECURITY_PROTOCOL=PLAINTEXT)
    fi

    sudo podman run -d --name "$APP" --pod "$POD" --restart=always \
      --memory=2g --cpus=2 \
      "${ENV_ARGS[@]}" \
      "${KAFKA_ARGS[@]}" \
      -e STREAMS_PORT="$PORT" \
      -e STREAMS_MCP_URL="http://localhost:$PORT/mcp" \
      -e K9_ENV=production \
      "$IMAGE" >/dev/null

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  k9x_streams"
    echo "  Landing:    http://127.0.0.1:$PORT/        (public: https://streams.k9x.ai/)"
    echo "  Demo:       http://127.0.0.1:$PORT/demo"
    echo "  Implement:  http://127.0.0.1:$PORT/implement"
    echo "  MCP:        http://127.0.0.1:$PORT/mcp"
    echo "  Health:     http://127.0.0.1:$PORT/api/health"
    echo "  Kafka:      $BROKER ($MODE)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    ;;

  stop)
    echo "Stopping pod $POD ..."
    sudo podman pod rm -f "$POD" 2>/dev/null || true
    echo "Stopped."
    ;;

  logs)
    sudo podman logs -f "$APP"
    ;;

  status)
    sudo podman pod ps --filter "name=$POD"
    sudo podman ps --filter "pod=$POD" --format "table {{.Names}}\t{{.Status}}"
    curl -s "http://127.0.0.1:$PORT/api/health" && echo "" || echo "health: not answering"
    ;;

  all)
    "$0" build
    "$0" start
    ;;

  help|*)
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  build   — build the Podman image ($IMAGE)"
    echo "  start   — start the pod (app on 127.0.0.1:$PORT, its own Redpanda unless STREAMS_KAFKA=external)"
    echo "  stop    — stop and remove the pod"
    echo "  logs    — tail the app's logs"
    echo "  status  — pod, containers and health"
    echo "  all     — build + start"
    ;;

esac
