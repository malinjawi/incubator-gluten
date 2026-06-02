#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

SPARK_PROFILE="${SPARK_PROFILE:-spark-3.5}"
ROWS="${ROWS:-}"
FILES="${FILES:-}"
ITERATIONS="${ITERATIONS:-}"
DELETE_MODE="${DELETE_MODE:-}"
EXECUTION_MODE="${EXECUTION_MODE:-}"
DELETE_SHAPE="${DELETE_SHAPE:-}"
TIER="${TIER:-tiny}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$REPO_ROOT/target/delta-dv-evidence}"
NATIVE_LIBPATH="${NATIVE_LIBPATH:-$REPO_ROOT/cpp/build/releases/libvelox.so}"
ARROW_C_DATA_SHIM="${ARROW_C_DATA_SHIM:-}"
SKIP_COMPILE="${SKIP_COMPILE:-0}"
DRY_RUN="${DRY_RUN:-0}"
JAVA_HOME_OVERRIDE="${JAVA_HOME_OVERRIDE:-}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Runs org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark and stores
per-command logs plus a parsed CSV summary.

Options:
  --tier tiny|developer|perf
      Defaults: tiny=10k rows/4 files/1 iter/mod10/all modes/create;
      developer=100k rows/8 files/1 iter/allshapes/all modes/all deletes;
      perf=1M rows/32 files/3 iters/allshapes/all modes/all deletes.
  --rows N
  --files N
  --iterations N
  --delete-mode create|update|all
  --execution-mode spark|gluten-jvm-bitmap|gluten-native-bitmap|gluten|all
  --delete-shape sparse1|mod10|dense50|uniformhot|fileskewhot|allshapes
  --spark-profile PROFILE
      Maven Spark profile, default spark-3.5.
  --evidence-dir DIR
      Default target/delta-dv-evidence.
  --native-libpath PATH
      Passed as spark.gluten.sql.columnar.libpath for Gluten modes.
  --arrow-c-data-shim JAR
      Optional local ARM evidence shim. This is a harness workaround only.
  --java-home DIR
      Java home for Maven/Spark. Defaults to JAVA_HOME, or Homebrew OpenJDK 17
      on macOS when available.
  --skip-compile
      Skip backends-velox test-compile before running.
  --dry-run
      Print commands without executing them.
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier)
      TIER="$2"
      shift 2
      ;;
    --rows)
      ROWS="$2"
      shift 2
      ;;
    --files)
      FILES="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --delete-mode)
      DELETE_MODE="$2"
      shift 2
      ;;
    --execution-mode)
      EXECUTION_MODE="$2"
      shift 2
      ;;
    --delete-shape)
      DELETE_SHAPE="$2"
      shift 2
      ;;
    --spark-profile)
      SPARK_PROFILE="$2"
      shift 2
      ;;
    --evidence-dir)
      EVIDENCE_DIR="$2"
      shift 2
      ;;
    --native-libpath)
      NATIVE_LIBPATH="$2"
      shift 2
      ;;
    --arrow-c-data-shim)
      ARROW_C_DATA_SHIM="$2"
      shift 2
      ;;
    --java-home)
      JAVA_HOME_OVERRIDE="$2"
      shift 2
      ;;
    --skip-compile)
      SKIP_COMPILE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$TIER" in
  tiny)
    : "${ROWS:=10000}"
    : "${FILES:=4}"
    : "${ITERATIONS:=1}"
    : "${DELETE_MODE:=create}"
    : "${EXECUTION_MODE:=all}"
    : "${DELETE_SHAPE:=mod10}"
    ;;
  developer)
    : "${ROWS:=100000}"
    : "${FILES:=8}"
    : "${ITERATIONS:=1}"
    : "${DELETE_MODE:=all}"
    : "${EXECUTION_MODE:=all}"
    : "${DELETE_SHAPE:=allshapes}"
    ;;
  perf)
    : "${ROWS:=1000000}"
    : "${FILES:=32}"
    : "${ITERATIONS:=3}"
    : "${DELETE_MODE:=all}"
    : "${EXECUTION_MODE:=all}"
    : "${DELETE_SHAPE:=allshapes}"
    ;;
  *)
    echo "Unknown tier '$TIER'. Expected tiny, developer, or perf." >&2
    exit 2
    ;;
esac

mkdir -p "$EVIDENCE_DIR"

if [[ -n "$JAVA_HOME_OVERRIDE" ]]; then
  export JAVA_HOME="$JAVA_HOME_OVERRIDE"
elif [[ -z "${JAVA_HOME:-}" &&
    -d "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]]; then
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
elif [[ -z "${JAVA_HOME:-}" && -d "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]]; then
  export JAVA_HOME="/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-delete-dv-${SPARK_PROFILE}-${TIER}-${ROWS}r-${FILES}f-${ITERATIONS}i-${DELETE_MODE}-${EXECUTION_MODE}-${DELETE_SHAPE}"
LOG_FILE="$EVIDENCE_DIR/$RUN_ID.log"
CSV_FILE="$EVIDENCE_DIR/$RUN_ID.csv"

needs_gluten_runtime() {
  case "$EXECUTION_MODE" in
    spark)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

prepare_no_space_native_libpath() {
  local libpath="$1"
  if [[ ! -f "$libpath" ]]; then
    echo "$libpath"
    return 0
  fi

  local libdir
  libdir="$(cd "$(dirname "$libpath")" && pwd)"
  local tmpdir="/tmp/gluten-delta-dv-native-libs-${USER:-unknown}-$RUN_ID"
  mkdir -p "$tmpdir"
  ln -sf "$libdir/libgluten.so" "$tmpdir/libgluten.so"
  ln -sf "$libdir/libvelox.so" "$tmpdir/libvelox.so"
  echo "$tmpdir/$(basename "$libpath")"
}

prepare_no_space_file() {
  local source_path="$1"
  local prefix="$2"
  if [[ -z "$source_path" || ! -f "$source_path" ]]; then
    echo "$source_path"
    return 0
  fi

  local tmpdir="/tmp/gluten-delta-dv-${prefix}-${USER:-unknown}-$RUN_ID"
  mkdir -p "$tmpdir"
  ln -sf "$source_path" "$tmpdir/$(basename "$source_path")"
  echo "$tmpdir/$(basename "$source_path")"
}

EFFECTIVE_NATIVE_LIBPATH="$NATIVE_LIBPATH"
if needs_gluten_runtime; then
  EFFECTIVE_NATIVE_LIBPATH="$(prepare_no_space_native_libpath "$NATIVE_LIBPATH")"
fi

EFFECTIVE_ARROW_C_DATA_SHIM=""
if [[ -n "$ARROW_C_DATA_SHIM" ]]; then
  EFFECTIVE_ARROW_C_DATA_SHIM="$(prepare_no_space_file "$ARROW_C_DATA_SHIM" "arrow-c-data")"
fi

JAVA_EXPORTS=(
  "-XX:+IgnoreUnrecognizedVMOptions"
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-opens=java.base/java.net=ALL-UNNAMED"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/java.util=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  "-Djdk.reflect.useDirectMethodHandle=false"
  "-Dio.netty.tryReflectionSetAccessible=true"
  "-Dfile.encoding=UTF-8"
)

if java -Djava.security.manager=allow -version >/dev/null 2>&1; then
  JAVA_EXPORTS+=("-Djava.security.manager=allow")
fi

BASE_MAVEN_OPTS="${MAVEN_OPTS:-} -Xss128m -Xmx4g -XX:ReservedCodeCacheSize=2g ${JAVA_EXPORTS[*]}"
if needs_gluten_runtime; then
  BASE_MAVEN_OPTS="$BASE_MAVEN_OPTS -Dspark.gluten.sql.columnar.libpath=$EFFECTIVE_NATIVE_LIBPATH"
fi
if [[ -n "$EFFECTIVE_ARROW_C_DATA_SHIM" ]]; then
  BASE_MAVEN_OPTS="$BASE_MAVEN_OPTS -Dspark.driver.extraClassPath=$EFFECTIVE_ARROW_C_DATA_SHIM"
fi
export MAVEN_OPTS="$BASE_MAVEN_OPTS"

MVN=("$REPO_ROOT/build/mvn" "-Pbackends-velox" "-Pdelta" "-P$SPARK_PROFILE")
COMPILE_CMD=(
  "${MVN[@]}"
  "-pl" "backends-velox"
  "-am"
  "-DskipTests"
  "test-compile"
)
BENCH_CMD=(
  "${MVN[@]}"
  "-pl" "backends-velox"
  "-DskipTests"
  "-Dexec.classpathScope=test"
  "-Dexec.mainClass=org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark"
  "-Dexec.args=$ROWS $FILES $ITERATIONS $DELETE_MODE $EXECUTION_MODE $DELETE_SHAPE"
  "exec:java"
)

print_command() {
  printf '%q ' "$@"
  printf '\n'
}

{
  echo "run_id=$RUN_ID"
  echo "repo_root=$REPO_ROOT"
  echo "spark_profile=$SPARK_PROFILE"
  echo "rows=$ROWS"
  echo "files=$FILES"
  echo "iterations=$ITERATIONS"
  echo "delete_mode=$DELETE_MODE"
  echo "execution_mode=$EXECUTION_MODE"
  echo "delete_shape=$DELETE_SHAPE"
  echo "native_libpath=$NATIVE_LIBPATH"
  echo "effective_native_libpath=$EFFECTIVE_NATIVE_LIBPATH"
  echo "arrow_c_data_shim=$ARROW_C_DATA_SHIM"
  echo "effective_arrow_c_data_shim=$EFFECTIVE_ARROW_C_DATA_SHIM"
  echo "java_home=${JAVA_HOME:-}"
  echo "java_version=$(java -version 2>&1 | head -1 || true)"
  echo "maven_opts=$MAVEN_OPTS"
  echo "compile_command=$(print_command "${COMPILE_CMD[@]}")"
  echo "benchmark_command=$(print_command "${BENCH_CMD[@]}")"
} > "$LOG_FILE"

if [[ "$DRY_RUN" == "1" ]]; then
  cat "$LOG_FILE"
  exit 0
fi

cd "$REPO_ROOT"

if [[ "$SKIP_COMPILE" != "1" ]]; then
  {
    echo
    echo "== test-compile =="
    "${COMPILE_CMD[@]}"
  } >> "$LOG_FILE" 2>&1
fi

set +e
{
  echo
  echo "== benchmark =="
  "${BENCH_CMD[@]}"
} >> "$LOG_FILE" 2>&1
BENCHMARK_EXIT_CODE=$?
set -e

echo "benchmark_exit_code=$BENCHMARK_EXIT_CODE" >> "$LOG_FILE"

awk -v run_id="$RUN_ID" '
  BEGIN {
    header = "runId,mode,activeFiles,deleteShape,deleteLayout,expectedDeletedRows,deleteDensityPct,touchedFiles,touchedFilePct,filesWithDvs,dvCardinality,dvCardinalityPct,dvPayloadBytes,payloadBytesPerDeletedRow,payloadBytesPerDvRow,finalRows,finalIdSum,deleteMs,validationMs,deletePlans,glutenDeleteCommands,deltaScanTransformers,nativeHashAggregateTransformers,bitmapAggregatorMentions,nativeBitmapAggregatePlans,sparkBitmapAggregatePlans,dmlRowIndexFallbackScans"
    print header
  }
  / result: / {
    mode = $1
    sub(/ result:.*/, "", mode)
    line = $0
    sub(/^.* result: /, "", line)
    split(line, parts, ", ")
    delete values
    for (i in parts) {
      split(parts[i], kv, "=")
      if (length(kv[1]) > 0) {
        values[kv[1]] = kv[2]
      }
    }
    printf "%s,%s", run_id, mode
    n = split("activeFiles deleteShape deleteLayout expectedDeletedRows deleteDensityPct touchedFiles touchedFilePct filesWithDvs dvCardinality dvCardinalityPct dvPayloadBytes payloadBytesPerDeletedRow payloadBytesPerDvRow finalRows finalIdSum deleteMs validationMs deletePlans glutenDeleteCommands deltaScanTransformers nativeHashAggregateTransformers bitmapAggregatorMentions nativeBitmapAggregatePlans sparkBitmapAggregatePlans dmlRowIndexFallbackScans", cols, " ")
    for (j = 1; j <= n; j++) {
      printf ",%s", values[cols[j]]
    }
    printf "\n"
  }
' "$LOG_FILE" > "$CSV_FILE"

echo "log=$LOG_FILE"
echo "csv=$CSV_FILE"
echo "benchmark_exit_code=$BENCHMARK_EXIT_CODE"

exit "$BENCHMARK_EXIT_CODE"
