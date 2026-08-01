#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JDK25_HOME="/usr/lib/jvm/java-25-openjdk-amd64"
DURATION_SECONDS=600
SEED=20260710
SKIP_PACKAGE=0

usage() {
  cat <<'EOF'
Usage: scripts/production-hardening-soak.sh [--duration-seconds <seconds>] [--seed <seed>] [--skip-package]

Packages the current candidate, then runs ProductionHardeningSoakTest against a real loopback TCP server.
Use --skip-package only when the candidate artifacts were already packaged and must remain frozen.
The default is the required 600-second acceptance workload. Use a shorter positive duration while developing.
EOF
}

die() {
  printf '[production-hardening-soak][ERROR] %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration-seconds)
      [[ $# -ge 2 ]] || die '--duration-seconds requires a value'
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --seed)
      [[ $# -ge 2 ]] || die '--seed requires a value'
      SEED="$2"
      shift 2
      ;;
    --skip-package)
      SKIP_PACKAGE=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ "$DURATION_SECONDS" =~ ^[1-9][0-9]*$ ]] || die '--duration-seconds must be a positive integer'
[[ "$SEED" =~ ^-?[0-9]+$ ]] || die '--seed must be an integer'
[[ -x "$JDK25_HOME/bin/java" ]] || die "JDK 25 is unavailable: $JDK25_HOME"

export JAVA_HOME="$JDK25_HOME"
export PATH="$JDK25_HOME/bin:$PATH"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
report_dir="$ROOT_DIR/target/production-hardening-soak/${timestamp}-seed-${SEED}"
mkdir -p "$report_dir"

candidate_commit="$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf unknown)"
server_jar="$ROOT_DIR/yierdis-server/yierdis-server-main/target/yierdis-server-main-0.1.0-SNAPSHOT.jar"

if [[ "$SKIP_PACKAGE" == "0" ]]; then
  printf '[production-hardening-soak] packaging current candidate\n'
  (
    cd "$ROOT_DIR"
    mvn -q \
      -pl yierdis-server/yierdis-server-main,yierdis-tests \
      -am \
      -DskipTests package
  ) 2>&1 | tee "$report_dir/package.log"
else
  printf '[production-hardening-soak] using pre-packaged frozen candidate\n'
fi

[[ -f "$server_jar" ]] || die "server candidate artifact is missing: $server_jar"

server_sha256="$(sha256sum "$server_jar" | awk '{print $1}')"
{
  printf 'candidate_commit=%s\n' "$candidate_commit"
  printf 'duration_seconds=%s\n' "$DURATION_SECONDS"
  printf 'seed=%s\n' "$SEED"
  printf 'skip_package=%s\n' "$SKIP_PACKAGE"
  printf 'java_home=%s\n' "$JAVA_HOME"
  printf 'java_version='; java -version 2>&1 | head -n 1
  printf 'maven_version='; mvn -version 2>&1 | head -n 1
  printf 'os='; uname -a
  printf 'server_jar=%s\n' "$server_jar"
  printf 'server_jar_sha256=%s\n' "$server_sha256"
} > "$report_dir/environment.txt"

printf '[production-hardening-soak] report directory: %s\n' "$report_dir"
printf '[production-hardening-soak] running %s-second deterministic workload (seed=%s)\n' "$DURATION_SECONDS" "$SEED"
(
  cd "$ROOT_DIR"
  mvn -q \
    -pl yierdis-tests \
    -am \
    -Dtest=ProductionHardeningSoakTest \
    -Dyierdis.soak.durationSeconds="$DURATION_SECONDS" \
    -Dyierdis.soak.seed="$SEED" \
    -Dyierdis.soak.commit="$candidate_commit" \
    -Dyierdis.soak.reportDir="$report_dir" \
    -Dsurefire.failIfNoSpecifiedTests=false \
    test
) 2>&1 | tee "$report_dir/soak.log"

printf '[production-hardening-soak] passed; logs and JSONL samples: %s\n' "$report_dir"
