#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Build control
MVN_ARGS="${MVN_ARGS:--q -DskipTests package}"
SKIP_BUILD="${SKIP_BUILD:-0}"

# Bench config (defaults are aligned with yierdis-bench built-in defaults)
HOST="${HOST:-127.0.0.1}"
PORT_BASE="${PORT_BASE:-16378}"
BACKENDS="${BACKENDS:-none,netty,unsafe}"

KEYSPACE="${KEYSPACE:-1000000}"
DATA_SIZE="${DATA_SIZE:-256}"

REQUESTS="${REQUESTS:-1000000}"
CLIENTS="${CLIENTS:-200}"
PIPELINE="${PIPELINE:-16}"

LATENCY_REQUESTS="${LATENCY_REQUESTS:-200000}"
LATENCY_CLIENTS="${LATENCY_CLIENTS:-50}"

# Server (child process) JVM
XMS="${XMS:-4g}"
XMX="${XMX:-4g}"
MAX_DIRECT_MEMORY="${MAX_DIRECT_MEMORY:-6g}"

# Server memory budgets (container-friendly conservative defaults for memory limit=16G)
MAXMEMORY_BYTES="${MAXMEMORY_BYTES:-7516192768}"      # 7GiB
OFFHEAP_MAX_BYTES="${OFFHEAP_MAX_BYTES:-4294967296}"  # 4GiB
MAXMEMORY_POLICY="${MAXMEMORY_POLICY:-allkeys-lru}"
MAXMEMORY_SAMPLES="${MAXMEMORY_SAMPLES:-5}"

# Optional flags
SKIP_PREFILL="${SKIP_PREFILL:-0}"
SKIP_LATENCY="${SKIP_LATENCY:-0}"

# Extra args
JAVA_CMD="${JAVA_CMD:-java}"                       # used to start server child process
SERVER_ARGS_EXTRA="${SERVER_ARGS_EXTRA:-}"         # appended to server args (best-effort split)
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
  server_jar="$(pick_jar "$ROOT_DIR/yierdis-server/target/yierdis-*.jar" "original-")"
  bench_jar="$(pick_jar "$ROOT_DIR/yierdis-bench/target/yierdis-bench-*.jar")"

  local args=()
  args+=(--serverJar "$server_jar")
  args+=(--host "$HOST")
  args+=(--portBase "$PORT_BASE")
  args+=(--backends "$BACKENDS")

  args+=(--keyspace "$KEYSPACE")
  args+=(--dataSize "$DATA_SIZE")

  args+=(--requests "$REQUESTS")
  args+=(--clients "$CLIENTS")
  args+=(--pipeline "$PIPELINE")

  args+=(--latencyRequests "$LATENCY_REQUESTS")
  args+=(--latencyClients "$LATENCY_CLIENTS")

  args+=(--javaCmd "$JAVA_CMD")
  args+=(--xms "$XMS")
  args+=(--xmx "$XMX")
  args+=(--maxDirectMemory "$MAX_DIRECT_MEMORY")

  args+=(--maxmemoryBytes "$MAXMEMORY_BYTES")
  args+=(--maxmemoryPolicy "$MAXMEMORY_POLICY")
  args+=(--maxmemorySamples "$MAXMEMORY_SAMPLES")
  args+=(--offheapMaxBytes "$OFFHEAP_MAX_BYTES")

  if [[ "$SKIP_PREFILL" == "1" ]]; then
    args+=(--skipPrefill)
  fi
  if [[ "$SKIP_LATENCY" == "1" ]]; then
    args+=(--skipLatency)
  fi
  if [[ -n "$SERVER_ARGS_EXTRA" ]]; then
    args+=(--serverArg "$SERVER_ARGS_EXTRA")
  fi

  # BENCH_ARGS_EXTRA is appended as-is (split by shell)
  # shellcheck disable=SC2086
  exec java $BENCH_JVM_OPTS -jar "$bench_jar" "${args[@]}" $BENCH_ARGS_EXTRA
}

main "$@"

