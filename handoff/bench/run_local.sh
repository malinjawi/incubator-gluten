#!/usr/bin/env bash
# Runs the Lance write benchmark on one machine in local mode: vanilla Spark, Gluten with
# lance-spark's row writer, and Gluten with the native writer, each in its own spark-submit.
#
# Required: GLUTEN_JAR (the Velox bundle, built with -Plance) and LANCE_JAR (lance-spark-bundle at
# 0.9.0-beta.2 or later). Optional: SPARK_HOME, DATA_DIR, CORES, ROWS, VECTOR_ROWS, DIM, REPS,
# DATASETS, SETUPS, RESULTS.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
: "${SPARK_HOME:?set SPARK_HOME}"
: "${GLUTEN_JAR:?set GLUTEN_JAR}"
: "${LANCE_JAR:?set LANCE_JAR}"
DATA_DIR="${DATA_DIR:-$HERE/data}"
CORES="${CORES:-8}"
ROWS="${ROWS:-60000000}"
VECTOR_ROWS="${VECTOR_ROWS:-1000000}"
DIM="${DIM:-768}"
REPS="${REPS:-3}"
FILES="${FILES:-$((CORES * 4))}"
DATASETS="${DATASETS:-lineitem vectors vectors-null}"
SETUPS="${SETUPS:-vanilla gluten-row gluten-native}"
RESULTS="${RESULTS:-$HERE/results.csv}"
# The same memory for every setup: vanilla Spark gets it as heap, Gluten mostly off-heap.
HEAP_VANILLA="${HEAP_VANILLA:-16g}"
HEAP_GLUTEN="${HEAP_GLUTEN:-6g}"
OFFHEAP_GLUTEN="${OFFHEAP_GLUTEN:-10g}"

submit() {
  local setup=$1
  shift
  local conf=(--master "local[$CORES]"
    --conf spark.driver.extraClassPath="$LANCE_JAR"
    --conf spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true
    --conf spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true
    --conf spark.sql.files.maxPartitionBytes=256m
    --conf spark.ui.showConsoleProgress=false)
  case "$setup" in
    vanilla) conf+=(--driver-memory "$HEAP_VANILLA") ;;
    gluten-*)
      conf+=(--driver-memory "$HEAP_GLUTEN"
        --conf spark.driver.extraClassPath="$GLUTEN_JAR:$LANCE_JAR"
        --conf spark.plugins=org.apache.gluten.GlutenPlugin
        --conf spark.memory.offHeap.enabled=true
        --conf spark.memory.offHeap.size="$OFFHEAP_GLUTEN"
        --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager)
      if [[ "$setup" == gluten-native ]]; then
        conf+=(--conf spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite=true)
      else
        conf+=(--conf spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite=false)
      fi
      ;;
    *) echo "unknown setup $setup" >&2; exit 1 ;;
  esac
  "$SPARK_HOME/bin/spark-submit" "${conf[@]}" "$HERE/lance_write_bench.py" "$@"
}

gen() {
  local name=$1
  shift
  if [[ ! -f "$DATA_DIR/$name/_SUCCESS" ]]; then
    echo "== generating $name"
    submit vanilla gen --files "$FILES" --out "$DATA_DIR/$name" "$@"
  fi
}

mkdir -p "$DATA_DIR/out"
for dataset in $DATASETS; do
  case "$dataset" in
    lineitem) gen lineitem --dataset lineitem --rows "$ROWS"; extra=() ;;
    vectors) gen vectors --dataset vectors --rows "$VECTOR_ROWS" --dim "$DIM"
      extra=(--vector-col v --dim "$DIM") ;;
    vectors-null) gen vectors-null --dataset vectors --rows "$VECTOR_ROWS" --dim "$DIM" --null-every 100
      extra=(--vector-col v --dim "$DIM") ;;
    *) echo "unknown dataset $dataset" >&2; exit 1 ;;
  esac
  for setup in $SETUPS; do
    echo "== $dataset / $setup"
    submit "$setup" write --input "$DATA_DIR/$dataset" --target "$DATA_DIR/out" --label "$setup" \
      --reps "$REPS" --results "$RESULTS" ${extra[@]+"${extra[@]}"}
  done
done
python3 "$HERE/summarize.py" "$RESULTS"
