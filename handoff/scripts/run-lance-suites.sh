#!/opt/homebrew/bin/bash
# Runs the Lance suites in the writer worktree against lance-spark from Maven Central, one Spark
# version at a time, and prints each run's test summary, failures and Arrow/Velox leak count.
#
# Usage: run-lance-suites.sh <log dir> [35 40 41 42 34]   (default: 35 40)
# Needs bash 4+ (run-scala-test.sh uses declare -A), hence /opt/homebrew/bin/bash.
set -uo pipefail

LOGS=${1:?log dir}
shift
VERSIONS=${*:-35 40}
WORKTREE=${WORKTREE:-$HOME/Documents/GitHub/gluten-lance-writer}
J21=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home
J17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
SUITES="-s org.apache.gluten.execution.VeloxLanceWriteSuite -s org.apache.gluten.execution.VeloxLanceSuite"
# LanceFixedSizeListsSuite exists from the vectors commit on.
if [[ -f $WORKTREE/backends-velox/src-lance/test/scala/org/apache/gluten/connector/write/LanceFixedSizeListsSuite.scala ]]; then
  SUITES="$SUITES -s org.apache.gluten.connector.write.LanceFixedSizeListsSuite"
fi

mkdir -p "$LOGS"
cd "$WORKTREE"
for v in $VERSIONS; do
  case $v in
    35) jdk=$J21 profiles=spark-3.5,scala-2.12 ;;
    34) jdk=$J17 profiles=java-17,spark-3.4,scala-2.12 ;;
    40|41|42) jdk=$J17 profiles=java-17,spark-${v:0:1}.${v:1:1},scala-2.13 ;;
    *) echo "unknown Spark version $v" >&2; exit 1 ;;
  esac
  log=$LOGS/lance-$v.log
  JAVA_HOME=$jdk /opt/homebrew/bin/bash ./dev/run-scala-test.sh \
    -P$profiles,backends-velox,lance,lance-test -pl backends-velox $SUITES > "$log" 2>&1
  echo "-- Spark $v: leaks=$(grep -c 'Memory leak' "$log")"
  sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -E '\*\*\* FAILED|^Tests: |TESTS? FAILED|BUILD FAILURE' | head -8
done
