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
BENCH_USERNAME="${BENCH_USERNAME:-${USERNAME:-}}"
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
  local target_dir="$ROOT_DIR/yierdis-benchmark/target"
  local original_jar original_name bench_jar
  while IFS= read -r original_jar; do
    original_name="${original_jar##*/}"
    bench_jar="$target_dir/${original_name#original-}"
    if [[ -f "$bench_jar" ]]; then
      printf '%s' "$bench_jar"
      return 0
    fi
  done < <(ls -1t "$target_dir"/original-yierdis-benchmark-*.jar 2>/dev/null || true)
  die 'shaded yierdis-benchmark jar not found'
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
  [[ -n "$BENCH_USERNAME" ]] && optional_args+=(--username "$BENCH_USERNAME")
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
