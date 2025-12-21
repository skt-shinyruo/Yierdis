#!/usr/bin/env bash
set -euo pipefail

scenario="${1:-strings}"

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="bench-out/${scenario}_${ts}"
mkdir -p "$out_dir"

echo "Output: $out_dir"

echo "Building shaded jar..."
mvn -q -DskipTests package

jar_path="$(ls -1 target/yierdis-*.jar | head -n 1)"
if [[ -z "${jar_path}" ]]; then
  echo "Could not find target/yierdis-*.jar" >&2
  exit 1
fi

ready_file="${out_dir}/ready.txt"
gc_log="${out_dir}/gc.log"

java_opts=(
  -Xms2g
  -Xmx2g
  "-Xlog:gc*:file=${gc_log}:time,level,tags"
)

echo "Running scenario=${scenario} (holdMillis=600000)..."
echo "Jar: ${jar_path}"
java "${java_opts[@]}" -cp "${jar_path}" yier.bubu.redis.bench.YierdisBench \
  --scenario "${scenario}" \
  --readyFile "${ready_file}" \
  --holdMillis 600000 &
pid="$!"

echo "pid=${pid}"
echo "Waiting for ready file: ${ready_file}"
for _ in $(seq 1 600); do
  if [[ -f "${ready_file}" ]]; then
    break
  fi
  sleep 1
done

if [[ ! -f "${ready_file}" ]]; then
  echo "Timed out waiting for ready file. Check process output / gc log." >&2
  exit 1
fi

echo "Collecting histograms..."
jcmd "${pid}" GC.class_histogram > "${out_dir}/class_histogram.txt"
jcmd "${pid}" GC.heap_info > "${out_dir}/heap_info.txt"
jcmd "${pid}" VM.native_memory summary > "${out_dir}/native_memory.txt" || true

echo "Done."
echo "  - ${out_dir}/class_histogram.txt"
echo "  - ${out_dir}/heap_info.txt"
echo "  - ${out_dir}/gc.log"
echo
echo "Stop the benchmark process when finished:"
echo "  kill ${pid}"

