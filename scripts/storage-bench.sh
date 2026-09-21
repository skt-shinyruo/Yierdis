#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_SCRIPT_TAG="storage-bench"
# shellcheck source=scripts/lib.sh
source "$ROOT_DIR/scripts/lib.sh"

SKIP_BUILD="${SKIP_BUILD:-0}"
BENCH_JVM_OPTS="${BENCH_JVM_OPTS:-}"

STORAGE_KEYS="${STORAGE_KEYS:-1000000}"
STORAGE_KEY_SIZE="${STORAGE_KEY_SIZE:-16}"
STORAGE_VALUE_SIZE="${STORAGE_VALUE_SIZE:-16}"
STORAGE_WARMUP_OPERATIONS="${STORAGE_WARMUP_OPERATIONS:-50000}"
STORAGE_PRECISION="${STORAGE_PRECISION:-3}"
FORMAT="${FORMAT:-human}"

main() {
  build_if_needed

  local bench_jar
  bench_jar="$(pick_bench_jar)"

  # shellcheck disable=SC2086
  exec java $BENCH_JVM_OPTS -jar "$bench_jar" storage \
    --keys "$STORAGE_KEYS" \
    --key-size "$STORAGE_KEY_SIZE" \
    --value-size "$STORAGE_VALUE_SIZE" \
    --warmup-operations "$STORAGE_WARMUP_OPERATIONS" \
    --precision "$STORAGE_PRECISION" \
    --format "$FORMAT"
}

main "$@"
