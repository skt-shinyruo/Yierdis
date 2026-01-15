#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Build control
MVN_ARGS="${MVN_ARGS:--q -DskipTests package}"
SKIP_BUILD="${SKIP_BUILD:-0}"

# Smoke config
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16379}"
SERVER_LOG="${SERVER_LOG:-$ROOT_DIR/.tmp-smoke-server.log}"

# Bench smoke (keep tiny; correctness only)
KEYSPACE="${KEYSPACE:-10}"
DATA_SIZE="${DATA_SIZE:-8}"
REQUESTS="${REQUESTS:-50}"
CLIENTS="${CLIENTS:-1}"
PIPELINE="${PIPELINE:-1}"

server_pid=""

die() { printf "[smoke][ERROR] %s\n" "$*" >&2; exit 1; }

build_if_needed() {
  if [[ "$SKIP_BUILD" == "1" ]]; then
    return 0
  fi
  (cd "$ROOT_DIR" && mvn $MVN_ARGS)
}

pick_jar() {
  local pattern="$1"
  local exclude="${2:-}"
  local jar

  jar="$(ls -1t $pattern 2>/dev/null | head -n 1 || true)"
  if [[ -n "$exclude" ]]; then
    jar="$(ls -1t $pattern 2>/dev/null | grep -v "$exclude" | head -n 1 || true)"
  fi

  [[ -n "$jar" ]] || die "未找到 jar：pattern=$pattern"
  printf "%s" "$jar"
}

wait_ready() {
  local client_jar="$1"
  local deadline_sec=30
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if timeout 7s java -jar "$client_jar" --host "$HOST" --port "$PORT" PING >/dev/null 2>&1; then
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= deadline_sec )); then
      return 1
    fi
    sleep 0.1
  done
}

main() {
  build_if_needed

  local server_jar bench_jar client_jar
  server_jar="$(pick_jar "$ROOT_DIR/yierdis-server/target/yierdis-*.jar" "original-")"
  bench_jar="$(pick_jar "$ROOT_DIR/yierdis-bench/target/yierdis-bench-*.jar" "original-")"
  client_jar="$(pick_jar "$ROOT_DIR/yierdis-client/target/yierdis-client-*.jar" "original-")"

  printf "[smoke] serverJar: %s\n" "$server_jar"
  printf "[smoke] benchJar : %s\n" "$bench_jar"
  printf "[smoke] clientJar: %s\n" "$client_jar"
  printf "[smoke] log      : %s\n" "$SERVER_LOG"

  cleanup() {
    local pid="${server_pid:-}"
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup EXIT

  printf "[smoke] 启动 server: %s:%s\n" "$HOST" "$PORT"
  java -jar "$server_jar" --port "$PORT" >"$SERVER_LOG" 2>&1 &
  server_pid="$!"

  if ! wait_ready "$client_jar"; then
    die "server 未在 10s 内就绪，请检查日志：$SERVER_LOG"
  fi
  printf "[smoke] server 就绪\n"

  printf "[smoke] CLI: PING\n"
  timeout 10s java -jar "$client_jar" --host "$HOST" --port "$PORT" PING

  printf "[smoke] bench（connect-only + strictReplies）\n"
  java -jar "$bench_jar" \
    --noStartServer \
    --backends none \
    --host "$HOST" \
    --portBase "$PORT" \
    --keyspace "$KEYSPACE" \
    --dataSize "$DATA_SIZE" \
    --requests "$REQUESTS" \
    --clients "$CLIENTS" \
    --pipeline "$PIPELINE" \
    --skipLatency \
    --strictReplies

  printf "[smoke] done\n"
}

main "$@"
