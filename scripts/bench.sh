#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_SCRIPT_TAG="bench"
# shellcheck source=scripts/lib.sh
source "$ROOT_DIR/scripts/lib.sh"

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
DATABASE="${DATABASE:-}"

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
  [[ -n "$DATABASE" ]] && optional_args+=(--database "$DATABASE")

  # shellcheck disable=SC2086
  exec java $BENCH_JVM_OPTS -jar "$bench_jar" \
    --host "$HOST" --port "$PORT" \
    --requests "$REQUESTS" --clients "$CLIENTS" \
    --data-size "$DATA_SIZE" --pipeline "$PIPELINE" \
    --format "$FORMAT" "${optional_args[@]}"
}

main "$@"
