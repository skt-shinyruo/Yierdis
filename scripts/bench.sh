#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SKIP_BUILD="${SKIP_BUILD:-0}"
BENCH_JVM_OPTS="${BENCH_JVM_OPTS:-}"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16378}"
REQUESTS="${REQUESTS:-100000}"
CLIENTS="${CLIENTS:-50}"
DATA_SIZE="${DATA_SIZE:-3}"
PIPELINE="${PIPELINE:-1}"
FORMAT="${FORMAT:-human}"

KEYSPACE="${KEYSPACE:-}"
TESTS="${TESTS:-}"
KEEP_ALIVE="${KEEP_ALIVE:-}"
PRECISION="${PRECISION:-}"
SEED="${SEED:-}"
USERNAME="${USERNAME:-}"
PASSWORD="${PASSWORD:-}"
DATABASE="${DATABASE:-}"

die() {
  printf '[bench][ERROR] %s\n' "$*" >&2
  exit 1
}

build_if_needed() {
  if [[ "$SKIP_BUILD" == "1" ]]; then
    return 0
  fi
  (cd "$ROOT_DIR" && mvn -pl yierdis-benchmark -am -q -DskipTests package)
}

pick_bench_jar() {
  local bench_jar
  bench_jar="$(ls -1t "$ROOT_DIR"/yierdis-benchmark/target/yierdis-benchmark-*.jar 2>/dev/null \
    | grep -v '/original-' \
    | head -n 1 || true)"
  [[ -n "$bench_jar" ]] || die 'shaded yierdis-benchmark jar not found'
  printf '%s' "$bench_jar"
}

main() {
  build_if_needed

  local bench_jar
  bench_jar="$(pick_bench_jar)"

  local optional_args=()
  [[ -n "$KEYSPACE" ]] && optional_args+=(--keyspace "$KEYSPACE")
  [[ -n "$TESTS" ]] && optional_args+=(--tests "$TESTS")
  [[ -n "$KEEP_ALIVE" ]] && optional_args+=("--keep-alive=$KEEP_ALIVE")
  [[ -n "$PRECISION" ]] && optional_args+=(--precision "$PRECISION")
  [[ -n "$SEED" ]] && optional_args+=(--seed "$SEED")
  [[ -n "$USERNAME" ]] && optional_args+=(--username "$USERNAME")
  [[ -n "$PASSWORD" ]] && optional_args+=(--password "$PASSWORD")
  [[ -n "$DATABASE" ]] && optional_args+=(--database "$DATABASE")

  # shellcheck disable=SC2086
  exec java $BENCH_JVM_OPTS -jar "$bench_jar" \
    --host "$HOST" --port "$PORT" \
    --requests "$REQUESTS" --clients "$CLIENTS" \
    --data-size "$DATA_SIZE" --pipeline "$PIPELINE" \
    --format "$FORMAT" "${optional_args[@]}"
}

main "$@"
