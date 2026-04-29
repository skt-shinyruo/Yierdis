#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Build control
MVN_ARGS="${MVN_ARGS:--q -DskipTests package}"
SKIP_BUILD="${SKIP_BUILD:-0}"

# Bench config overrides (keep empty to use yierdis-bench built-in defaults)
HOST="${HOST:-}"
PORT_BASE="${PORT_BASE:-}"
BACKENDS="${BACKENDS:-}"

KEYSPACE="${KEYSPACE:-}"
DATA_SIZE="${DATA_SIZE:-}"

REQUESTS="${REQUESTS:-}"
CLIENTS="${CLIENTS:-}"
PIPELINE="${PIPELINE:-}"

LATENCY_REQUESTS="${LATENCY_REQUESTS:-}"
LATENCY_CLIENTS="${LATENCY_CLIENTS:-}"

# Server (child process) JVM overrides
XMS="${XMS:-}"
XMX="${XMX:-}"
MAX_DIRECT_MEMORY="${MAX_DIRECT_MEMORY:-}"

# Server args overrides (keep empty to use yierdis-server-app defaults in yierdis-args)
MAXMEMORY_BYTES="${MAXMEMORY_BYTES:-}"
OFFHEAP_MAX_BYTES="${OFFHEAP_MAX_BYTES:-}"
MAXMEMORY_POLICY="${MAXMEMORY_POLICY:-}"
MAXMEMORY_SAMPLES="${MAXMEMORY_SAMPLES:-}"

# Optional flags
SKIP_PREFILL="${SKIP_PREFILL:-0}"
SKIP_LATENCY="${SKIP_LATENCY:-0}"

# Extra args
JAVA_CMD="${JAVA_CMD:-java}"                       # used to start server child process
SERVER_ARGS_EXTRA="${SERVER_ARGS_EXTRA:-}"         # appended to server args (split by shell)
BENCH_ARGS_EXTRA="${BENCH_ARGS_EXTRA:-}"           # appended to bench args (as-is)
BENCH_JVM_OPTS="${BENCH_JVM_OPTS:-}"               # JVM opts for the bench tool itself

die() { printf "[bench][ERROR] %s\n" "$*" >&2; exit 1; }

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

main() {
  build_if_needed

  local server_jar bench_jar
  server_jar="$(pick_jar "$ROOT_DIR/yierdis-app/yierdis-server-app/target/yierdis-server-app-*.jar" "original-")"
  bench_jar="$(pick_jar "$ROOT_DIR/yierdis-bench/target/yierdis-bench-*.jar" "original-")"

  local args=()
  args+=(--serverJar "$server_jar")
  [[ -n "$HOST" ]] && args+=(--host "$HOST")
  [[ -n "$PORT_BASE" ]] && args+=(--portBase "$PORT_BASE")
  [[ -n "$BACKENDS" ]] && args+=(--backends "$BACKENDS")

  [[ -n "$KEYSPACE" ]] && args+=(--keyspace "$KEYSPACE")
  [[ -n "$DATA_SIZE" ]] && args+=(--dataSize "$DATA_SIZE")

  [[ -n "$REQUESTS" ]] && args+=(--requests "$REQUESTS")
  [[ -n "$CLIENTS" ]] && args+=(--clients "$CLIENTS")
  [[ -n "$PIPELINE" ]] && args+=(--pipeline "$PIPELINE")

  [[ -n "$LATENCY_REQUESTS" ]] && args+=(--latencyRequests "$LATENCY_REQUESTS")
  [[ -n "$LATENCY_CLIENTS" ]] && args+=(--latencyClients "$LATENCY_CLIENTS")

  [[ -n "$JAVA_CMD" ]] && args+=(--javaCmd "$JAVA_CMD")
  [[ -n "$XMS" ]] && args+=(--xms "$XMS")
  [[ -n "$XMX" ]] && args+=(--xmx "$XMX")
  [[ -n "$MAX_DIRECT_MEMORY" ]] && args+=(--maxDirectMemory "$MAX_DIRECT_MEMORY")

  if [[ "$SKIP_PREFILL" == "1" ]]; then
    args+=(--skipPrefill)
  fi
  if [[ "$SKIP_LATENCY" == "1" ]]; then
    args+=(--skipLatency)
  fi

  # Server args: pass through (parsed by yierdis-args on bench side, then forwarded to server).
  local server_args=()
  local extra_server_args=()
  [[ -n "$MAXMEMORY_BYTES" ]] && server_args+=(--maxmemoryBytes "$MAXMEMORY_BYTES")
  [[ -n "$MAXMEMORY_POLICY" ]] && server_args+=(--maxmemoryPolicy "$MAXMEMORY_POLICY")
  [[ -n "$MAXMEMORY_SAMPLES" ]] && server_args+=(--maxmemorySamples "$MAXMEMORY_SAMPLES")
  [[ -n "$OFFHEAP_MAX_BYTES" ]] && server_args+=(--offheapMaxBytes "$OFFHEAP_MAX_BYTES")

  # SERVER_ARGS_EXTRA is appended as-is (split by shell)
  # shellcheck disable=SC2206
  extra_server_args=($SERVER_ARGS_EXTRA)
  if [[ ${#extra_server_args[@]} -gt 0 ]]; then
    server_args+=("${extra_server_args[@]}")
  fi

  if [[ ${#server_args[@]} -gt 0 ]]; then
    args+=(--)
    args+=("${server_args[@]}")
  fi

  # BENCH_ARGS_EXTRA is appended as-is (split by shell)
  # shellcheck disable=SC2086
  exec java $BENCH_JVM_OPTS -jar "$bench_jar" "${args[@]}" $BENCH_ARGS_EXTRA
}

main "$@"
