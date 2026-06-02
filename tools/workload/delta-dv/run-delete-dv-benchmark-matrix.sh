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
CUSTOM_SETTINGS=0
EVIDENCE_DIR="${EVIDENCE_DIR:-$REPO_ROOT/target/delta-dv-evidence}"
NATIVE_LIBPATH="${NATIVE_LIBPATH:-$REPO_ROOT/cpp/build/releases/libvelox.so}"
ARROW_C_DATA_SHIM="${ARROW_C_DATA_SHIM:-}"
SKIP_COMPILE="${SKIP_COMPILE:-0}"
CLEAN_BUILD="${CLEAN_BUILD:-0}"
DRY_RUN="${DRY_RUN:-0}"
JAVA_HOME_OVERRIDE="${JAVA_HOME_OVERRIDE:-}"
LAUNCHER="${LAUNCHER:-maven-exec}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-}"

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
  --delete-shape sparse1|mod10|dense50|uniformhot|fileskewhot|partitionedmod10|allshapes
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
  --launcher maven-exec|direct-java
      Maven exec is the default. Use direct-java when a shim jar must be
      prepended to the actual benchmark JVM classpath.
  --skip-compile
      Skip backends-velox test-compile before running.
  --clean-build
      Remove benchmark module target directories before compiling. Useful when
      switching between Scala 2.12 and 2.13 Spark profiles in one worktree.
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
      CUSTOM_SETTINGS=1
      shift 2
      ;;
    --files)
      FILES="$2"
      CUSTOM_SETTINGS=1
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      CUSTOM_SETTINGS=1
      shift 2
      ;;
    --delete-mode)
      DELETE_MODE="$2"
      CUSTOM_SETTINGS=1
      shift 2
      ;;
    --execution-mode)
      EXECUTION_MODE="$2"
      CUSTOM_SETTINGS=1
      shift 2
      ;;
    --delete-shape)
      DELETE_SHAPE="$2"
      CUSTOM_SETTINGS=1
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
    --launcher)
      LAUNCHER="$2"
      shift 2
      ;;
    --skip-compile)
      SKIP_COMPILE=1
      shift
      ;;
    --clean-build)
      CLEAN_BUILD=1
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

case "$LAUNCHER" in
  maven-exec|direct-java)
    ;;
  *)
    echo "Unknown launcher '$LAUNCHER'. Expected maven-exec or direct-java." >&2
    exit 2
    ;;
esac
if [[ "$CLEAN_BUILD" == "1" && "$SKIP_COMPILE" == "1" ]]; then
  echo "--clean-build cannot be combined with --skip-compile." >&2
  exit 2
fi

if [[ -z "$SCALA_BINARY_VERSION" ]]; then
  case "$SPARK_PROFILE" in
    spark-4.*)
      SCALA_BINARY_VERSION="2.13"
      ;;
    *)
      SCALA_BINARY_VERSION="2.12"
      ;;
  esac
fi
SPARK_SHIM_DIR="spark${SPARK_PROFILE#spark-}"
SPARK_SHIM_DIR="${SPARK_SHIM_DIR//./}"
MAVEN_PROFILES=("-Pbackends-velox" "-Pdelta" "-P$SPARK_PROFILE")
if [[ "$SCALA_BINARY_VERSION" == "2.13" ]]; then
  MAVEN_PROFILES+=("-Pscala-2.13")
fi
BENCHMARK_MODULES=(
  backends-velox
  gluten-delta
  gluten-core
  gluten-substrait
  gluten-arrow
  gluten-ui
  shims/common
  "shims/$SPARK_SHIM_DIR"
)

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

RUN_TIER="$TIER"
if [[ "$CUSTOM_SETTINGS" == "1" ]]; then
  RUN_TIER="custom-${TIER}"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-delete-dv-${SPARK_PROFILE}-${RUN_TIER}-${ROWS}r-${FILES}f-${ITERATIONS}i-${DELETE_MODE}-${EXECUTION_MODE}-${DELETE_SHAPE}"
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

  local source_abs
  source_abs="$(cd "$(dirname "$source_path")" && pwd)/$(basename "$source_path")"
  local tmpdir="/tmp/gluten-delta-dv-${prefix}-${USER:-unknown}-$RUN_ID"
  mkdir -p "$tmpdir"
  ln -sf "$source_abs" "$tmpdir/$(basename "$source_path")"
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

MVN=("$REPO_ROOT/build/mvn" "${MAVEN_PROFILES[@]}")
CLASSPATH_FILE="$EVIDENCE_DIR/$RUN_ID.classpath"
COMPILE_CMD=(
  "${MVN[@]}"
  "-pl" "backends-velox"
  "-am"
  "-DskipTests"
  "test-compile"
)
if [[ "$SKIP_COMPILE" == "1" ]]; then
  CLASSPATH_CMD=(
    "${MVN[@]}"
    "-pl" "backends-velox"
    "-am"
    "-DskipTests"
    "-DincludeScope=test"
    "-Dmdep.outputFile=$CLASSPATH_FILE"
    "dependency:build-classpath"
  )
else
  CLASSPATH_CMD=(
    "${MVN[@]}"
    "-pl" "backends-velox"
    "-am"
    "-DskipTests"
    "-DincludeScope=test"
    "-Dmdep.outputFile=$CLASSPATH_FILE"
    "test-compile"
    "dependency:build-classpath"
  )
fi
BENCH_CMD=(
  "${MVN[@]}"
  "-pl" "backends-velox"
  "-DskipTests"
  "-Dexec.classpathScope=test"
  "-Dexec.mainClass=org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark"
  "-Dexec.args=$ROWS $FILES $ITERATIONS $DELETE_MODE $EXECUTION_MODE $DELETE_SHAPE"
  "exec:java"
)

append_classpath_entry() {
  local entry="$1"
  [[ -z "$entry" ]] && return 0
  if [[ -z "${DIRECT_CLASSPATH:-}" ]]; then
    DIRECT_CLASSPATH="$entry"
  else
    DIRECT_CLASSPATH="$DIRECT_CLASSPATH:$entry"
  fi
}

append_target_class_dirs() {
  local module
  for module in "${BENCHMARK_MODULES[@]}"; do
    append_classpath_entry "$REPO_ROOT/$module/target/scala-$SCALA_BINARY_VERSION/test-classes"
    append_classpath_entry "$REPO_ROOT/$module/target/scala-$SCALA_BINARY_VERSION/classes"
    append_classpath_entry "$REPO_ROOT/$module/target/test-classes"
    append_classpath_entry "$REPO_ROOT/$module/target/classes"
  done
}

print_command() {
  printf '%q ' "$@"
  printf '\n'
}

{
  echo "run_id=$RUN_ID"
  echo "repo_root=$REPO_ROOT"
  echo "spark_profile=$SPARK_PROFILE"
  echo "scala_binary_version=$SCALA_BINARY_VERSION"
  echo "spark_shim_dir=$SPARK_SHIM_DIR"
  echo "rows=$ROWS"
  echo "files=$FILES"
  echo "iterations=$ITERATIONS"
  echo "delete_mode=$DELETE_MODE"
  echo "execution_mode=$EXECUTION_MODE"
  echo "delete_shape=$DELETE_SHAPE"
  echo "launcher=$LAUNCHER"
  echo "clean_build=$CLEAN_BUILD"
  echo "native_libpath=$NATIVE_LIBPATH"
  echo "effective_native_libpath=$EFFECTIVE_NATIVE_LIBPATH"
  echo "arrow_c_data_shim=$ARROW_C_DATA_SHIM"
  echo "effective_arrow_c_data_shim=$EFFECTIVE_ARROW_C_DATA_SHIM"
  echo "java_home=${JAVA_HOME:-}"
  echo "java_version=$(java -version 2>&1 | head -1 || true)"
  echo "maven_opts=$MAVEN_OPTS"
  echo "compile_command=$(print_command "${COMPILE_CMD[@]}")"
  echo "classpath_command=$(print_command "${CLASSPATH_CMD[@]}")"
  echo "classpath_file=$CLASSPATH_FILE"
  echo "benchmark_command=$(print_command "${BENCH_CMD[@]}")"
} > "$LOG_FILE"

if [[ "$DRY_RUN" == "1" ]]; then
  cat "$LOG_FILE"
  exit 0
fi

cd "$REPO_ROOT"

if [[ "$CLEAN_BUILD" == "1" ]]; then
  {
    echo
    echo "== clean benchmark targets =="
    for module in "${BENCHMARK_MODULES[@]}"; do
      echo "rm -rf $REPO_ROOT/$module/target"
      rm -rf "$REPO_ROOT/$module/target"
    done
  } >> "$LOG_FILE" 2>&1
fi

if [[ "$LAUNCHER" == "direct-java" ]]; then
  {
    echo
    echo "== classpath =="
    "${CLASSPATH_CMD[@]}"
  } >> "$LOG_FILE" 2>&1
elif [[ "$SKIP_COMPILE" != "1" ]]; then
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
  if [[ "$LAUNCHER" == "direct-java" ]]; then
    if [[ ! -f "$CLASSPATH_FILE" ]]; then
      echo "Missing classpath file: $CLASSPATH_FILE" >&2
      exit 1
    fi
    DIRECT_CLASSPATH=""
    append_classpath_entry "$EFFECTIVE_ARROW_C_DATA_SHIM"
    append_target_class_dirs
    append_classpath_entry "$(cat "$CLASSPATH_FILE")"
    echo "direct_classpath_length=${#DIRECT_CLASSPATH}"
    JAVA_BENCH_CMD=(
      java
      -Xss128m
      -Xmx4g
      -XX:ReservedCodeCacheSize=2g
      "${JAVA_EXPORTS[@]}"
    )
    if needs_gluten_runtime; then
      JAVA_BENCH_CMD+=("-Dspark.gluten.sql.columnar.libpath=$EFFECTIVE_NATIVE_LIBPATH")
    fi
    JAVA_BENCH_CMD+=(
      -cp "$DIRECT_CLASSPATH"
      org.apache.spark.sql.delta.DeltaDeleteDeletionVectorBenchmark
      "$ROWS" "$FILES" "$ITERATIONS" "$DELETE_MODE" "$EXECUTION_MODE" "$DELETE_SHAPE"
    )
    print_command "${JAVA_BENCH_CMD[@]}"
    "${JAVA_BENCH_CMD[@]}"
  else
    "${BENCH_CMD[@]}"
  fi
} >> "$LOG_FILE" 2>&1
BENCHMARK_EXIT_CODE=$?
set -e

echo "benchmark_exit_code=$BENCHMARK_EXIT_CODE" >> "$LOG_FILE"

awk -v run_id="$RUN_ID" '
  BEGIN {
    header = "runId,mode,iteration,deleteMode,activeFiles,deleteShape,deleteLayout,deletePredicate,expectedDeletedRows,deleteDensityPct,touchedFiles,touchedFilePct,filesWithDvs,dvCardinality,dvCardinalityPct,dvPayloadBytes,payloadBytesPerDeletedRow,payloadBytesPerDvRow,finalRows,finalIdSum,deleteMs,validationMs,deletePlans,glutenDeleteCommands,deltaScanTransformers,nativeHashAggregateTransformers,bitmapAggregatorMentions,nativeBitmapAggregatePlans,sparkBitmapAggregatePlans,finalNativeBitmapAggregatePlans,finalSparkBitmapAggregatePlans,initialSparkBitmapAggregatePlans,dmlRowIndexFallbackScans,nativeBitmapAggMetricNodes,rowToVeloxColumnarMetricNodes,veloxColumnarToRowMetricNodes,columnarShuffleMetricNodes,nativeBitmapAggWallMs,nativeBitmapAggOutputRows,nativeBitmapAggOutputBytes,nativeBitmapAggSpilledBytes,rowToVeloxColumnarConvertMs,rowToVeloxColumnarInputRows,rowToVeloxColumnarOutputBatches,veloxColumnarToRowConvertMs,veloxColumnarToRowOutputRows,veloxColumnarToRowInputBatches,columnarShuffleBytesWritten,columnarShuffleRecordsWritten,columnarShuffleWriteTimeNs,columnarShuffleBytesRead,columnarShuffleRecordsRead"
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
      sep = index(parts[i], "=")
      if (sep > 0) {
        key = substr(parts[i], 1, sep - 1)
        value = substr(parts[i], sep + 1)
        if (length(key) > 0) {
          values[key] = value
        }
      }
    }
    printf "%s,%s", run_id, mode
    n = split("iteration deleteMode activeFiles deleteShape deleteLayout deletePredicate expectedDeletedRows deleteDensityPct touchedFiles touchedFilePct filesWithDvs dvCardinality dvCardinalityPct dvPayloadBytes payloadBytesPerDeletedRow payloadBytesPerDvRow finalRows finalIdSum deleteMs validationMs deletePlans glutenDeleteCommands deltaScanTransformers nativeHashAggregateTransformers bitmapAggregatorMentions nativeBitmapAggregatePlans sparkBitmapAggregatePlans finalNativeBitmapAggregatePlans finalSparkBitmapAggregatePlans initialSparkBitmapAggregatePlans dmlRowIndexFallbackScans nativeBitmapAggMetricNodes rowToVeloxColumnarMetricNodes veloxColumnarToRowMetricNodes columnarShuffleMetricNodes nativeBitmapAggWallMs nativeBitmapAggOutputRows nativeBitmapAggOutputBytes nativeBitmapAggSpilledBytes rowToVeloxColumnarConvertMs rowToVeloxColumnarInputRows rowToVeloxColumnarOutputBatches veloxColumnarToRowConvertMs veloxColumnarToRowOutputRows veloxColumnarToRowInputBatches columnarShuffleBytesWritten columnarShuffleRecordsWritten columnarShuffleWriteTimeNs columnarShuffleBytesRead columnarShuffleRecordsRead", cols, " ")
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
