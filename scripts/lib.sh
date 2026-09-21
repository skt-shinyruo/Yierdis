# shellcheck shell=bash
# Shared helpers for bench.sh and storage-bench.sh.
# Requires ROOT_DIR, SKIP_BUILD and BENCH_SCRIPT_TAG to be set by the caller.

die() {
  printf '[%s][ERROR] %s\n' "$BENCH_SCRIPT_TAG" "$*" >&2
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
