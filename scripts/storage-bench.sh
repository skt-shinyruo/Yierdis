#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SKIP_BUILD="${SKIP_BUILD:-0}"
BENCH_JVM_OPTS="${BENCH_JVM_OPTS:-}"

STORAGE_KEYS="${STORAGE_KEYS:-1000000}"
STORAGE_KEY_SIZE="${STORAGE_KEY_SIZE:-16}"
STORAGE_VALUE_SIZE="${STORAGE_VALUE_SIZE:-16}"
STORAGE_WARMUP_OPERATIONS="${STORAGE_WARMUP_OPERATIONS:-50000}"
STORAGE_PRECISION="${STORAGE_PRECISION:-3}"
FORMAT="${FORMAT:-human}"

die() {
  printf '[storage-bench][ERROR] %s\n' "$*" >&2
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
