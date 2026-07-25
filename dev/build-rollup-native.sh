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

readonly VELOX_BRANCH="dft-2026_06_05"
readonly VELOX_COMMIT="053db35254cda24af0b69b8a6693d1b295b4fba5"
readonly CMAKE_VERSION="3.31.1"
readonly ARROW_VERSION="15.0.0"
readonly DEFAULT_JOBS="2"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./dev/build-rollup-native.sh <command>

Commands:
  doctor         Show the isolated toolchain and validate completed build state.
  setup          Fetch the pinned Velox tree and build its pinned dependencies.
  build          Incrementally build Velox, Gluten native, and the rollup test.
  test           Rebuild Gluten native + test, then run enabled tests.
  test-pressure  Rebuild native + test, then run pressure tests (except benchmark).
  test-scala     Install Spark 4 / Scala 2.13 reactor deps and run the planner suite.
  rerun          Alias for test.
  all            Run setup, build, and enabled tests.
  env            Print the persistent paths and exported build environment.

Environment overrides:
  ROLLUP_NATIVE_JOBS       Parallel build jobs (default: 2).
  ROLLUP_NATIVE_STATE_DIR  Persistent state directory. It must not contain spaces.

The helper does not alter the caller's shell. It removes Conda and other ambient
compiler/package search variables inside its own process. The first setup can install
missing Homebrew formulae globally; build artifacts and downloaded sources are kept in
the persistent state directory.
EOF
}

# Re-enter once with an explicit environment so Conda, CMake toolchains,
# package-root variables, ccache, and deployment settings cannot leak into a
# fresh cache. The working directory and command arguments are preserved.
if [[ "${GLUTEN_ROLLUP_ENV_ISOLATED:-}" != "1" ]]; then
  self_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
  self_path="$self_dir/$(basename "${BASH_SOURCE[0]}")"
  [[ -n "${HOME:-}" ]] || die "HOME is required"

  clean_env=(
    /usr/bin/env -i
    GLUTEN_ROLLUP_ENV_ISOLATED=1
    HOME="$HOME"
    USER="${USER:-}"
    LOGNAME="${LOGNAME:-${USER:-}}"
    TMPDIR="${TMPDIR:-/tmp}"
    TERM="${TERM:-dumb}"
    LANG="${LANG:-C}"
    PATH=/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin
  )
  if [[ -n "${ROLLUP_NATIVE_JOBS:-}" ]]; then
    clean_env+=(ROLLUP_NATIVE_JOBS="$ROLLUP_NATIVE_JOBS")
  fi
  if [[ -n "${ROLLUP_NATIVE_STATE_DIR:-}" ]]; then
    clean_env+=(ROLLUP_NATIVE_STATE_DIR="$ROLLUP_NATIVE_STATE_DIR")
  fi
  exec "${clean_env[@]}" /bin/bash "$self_path" "$@"
fi

command -v shasum >/dev/null 2>&1 || die "shasum is required"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
REPO_HASH="$(printf '%s' "$REPO_ROOT" | shasum -a 256 | awk '{print substr($1, 1, 12)}')"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
ALIAS_PARENT="$TMP_BASE/gluten-rollup-native-$REPO_HASH"
SOURCE_ALIAS="$ALIAS_PARENT/repo"

mkdir -p "$ALIAS_PARENT"
if [[ -L "$SOURCE_ALIAS" ]]; then
  ALIAS_TARGET="$(cd "$SOURCE_ALIAS" && pwd -P)"
  [[ "$ALIAS_TARGET" == "$REPO_ROOT" ]] ||
    die "$SOURCE_ALIAS points to $ALIAS_TARGET instead of $REPO_ROOT"
elif [[ -e "$SOURCE_ALIAS" ]]; then
  die "$SOURCE_ALIAS exists and is not a symlink; move it aside and retry"
else
  ln -s "$REPO_ROOT" "$SOURCE_ALIAS"
fi

DEFAULT_STATE_DIR="$SOURCE_ALIAS/ep/build-velox/build/rollup-native/$VELOX_BRANCH-cmake-$CMAKE_VERSION"
STATE_DIR="${ROLLUP_NATIVE_STATE_DIR:-$DEFAULT_STATE_DIR}"
[[ "$STATE_DIR" != *" "* ]] || die "ROLLUP_NATIVE_STATE_DIR must not contain spaces: $STATE_DIR"

JOBS="${ROLLUP_NATIVE_JOBS:-$DEFAULT_JOBS}"
[[ "$JOBS" =~ ^[1-9][0-9]*$ ]] || die "ROLLUP_NATIVE_JOBS must be a positive integer"

TOOLS_VENV="$STATE_DIR/tools/cmake-$CMAKE_VERSION"
VELOX_HOME="$STATE_DIR/velox_ep"
INSTALL_PREFIX="$VELOX_HOME/deps-install"
DEPENDENCY_DIR="$STATE_DIR/deps-src"
ARROW_PREFIX="$STATE_DIR/arrow_ep"
VELOX_BUILD_PATH="$VELOX_HOME/_build/release"
GLUTEN_BUILD_PATH="$STATE_DIR/gluten-build"
LOG_DIR="$STATE_DIR/logs"
SETUP_STAMP="$STATE_DIR/.dependencies-complete"
ARROW_STAMP="$STATE_DIR/.arrow-cpp-complete"
PATCH_FILE="$SOURCE_ALIAS/ep/build-velox/src/modify_hash_aggregation_input_buffer.patch"

mkdir -p "$STATE_DIR" "$LOG_DIR"

# This process intentionally ignores the caller's Conda/compiler/package state.
unset CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M
unset VIRTUAL_ENV PYTHONHOME PYTHONPATH
unset CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH LIBRARY_PATH
unset DYLD_LIBRARY_PATH DYLD_FALLBACK_LIBRARY_PATH LD_LIBRARY_PATH
unset CMAKE_PREFIX_PATH PKG_CONFIG_PATH BOOST_ROOT
unset CMAKE_TOOLCHAIN_FILE CMAKE_GENERATOR CMAKE_GENERATOR_PLATFORM
unset CMAKE_GENERATOR_TOOLSET CMAKE_OSX_ARCHITECTURES
unset CMAKE_OSX_DEPLOYMENT_TARGET CMAKE_OSX_SYSROOT
unset MACOSX_DEPLOYMENT_TARGET DEVELOPER_DIR VELOX_DEPENDENCY_SOURCE
unset CCACHE_DIR CCACHE_BASEDIR CCACHE_PREFIX CCACHE_COMPILERCHECK
unset MAKEFLAGS
unset CPPFLAGS CFLAGS CXXFLAGS LDFLAGS
unset SDKROOT CC CXX

JAVA_HOME="$(/usr/libexec/java_home -v 1.8 2>/dev/null)" ||
  die "A Java 8 JDK is required (used by CMake FindJNI)"
export JAVA_HOME

export PATH="$TOOLS_VENV/bin:$INSTALL_PREFIX/bin:$JAVA_HOME/bin:/opt/homebrew/opt/llvm@15/bin:/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:/opt/homebrew/opt/m4/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
export CC=/usr/bin/clang
export CXX=/usr/bin/clang++
export CPU_TARGET=arm64
export BUILD_THREADS="$JOBS"
export NUM_THREADS="$JOBS"
export NPROC="$JOBS"
export CMAKE_BUILD_PARALLEL_LEVEL="$JOBS"
export CMAKE_POLICY_VERSION_MINIMUM=3.5
export INSTALL_PREFIX
export DEPENDENCY_DIR
export BOOST_ROOT="$INSTALL_PREFIX"
export CMAKE_PREFIX_PATH="$INSTALL_PREFIX"
export PKG_CONFIG_PATH="$INSTALL_PREFIX/lib/pkgconfig:$INSTALL_PREFIX/lib64/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig:/opt/homebrew/opt/icu4c/lib/pkgconfig:/opt/homebrew/lib/pkgconfig:/opt/homebrew/share/pkgconfig"
export INSTALL_PREREQUISITES=N
export PROMPT_ALWAYS_RESPOND=y
export BUILD_GEOS=true
export BUILD_S2GEOMETRY=false
export BUILD_FAISS=false
export BUILD_DUCKDB=true

timestamp() {
  date "+%Y%m%d-%H%M%S"
}

run_logged() {
  local name="$1"
  shift
  local log_file="$LOG_DIR/$(timestamp)-$name.log"
  echo "Logging $name to $log_file"
  "$@" 2>&1 | tee "$log_file"
}

builddeps() {
  "$SOURCE_ALIAS/dev/builddeps-veloxbe.sh" \
    --velox_home="$VELOX_HOME" \
    --build_type=Release \
    --build_tests=ON \
    --build_velox_tests=OFF \
    --build_velox_benchmarks=OFF \
    --build_benchmarks=OFF \
    --build_examples=OFF \
    --build_arrow=OFF \
    --run_setup_script=OFF \
    --enable_vcpkg=OFF \
    --enable_s3=OFF \
    --enable_gcs=OFF \
    --enable_hdfs=OFF \
    --enable_abfs=OFF \
    --enable_gpu=OFF \
    --num_threads="$JOBS" \
    "$@"
}

install_gluten_velox_build_patches() {
  local source_dir="$SOURCE_ALIAS/ep/build-velox/src"
  local destination_dir="$VELOX_HOME/CMake/resolve_dependency_modules/arrow"
  local patch

  for patch in modify_arrow.patch modify_arrow_dataset_scan_option.patch; do
    [[ -f "$source_dir/$patch" ]] ||
      die "Gluten's Velox build patch is missing: $source_dir/$patch"
    cp "$source_dir/$patch" "$destination_dir/$patch"
    git -C "$VELOX_HOME" add "CMake/resolve_dependency_modules/arrow/$patch"
  done
}

bootstrap_tools() {
  local python_bin="/opt/homebrew/opt/python@3.11/bin/python3.11"
  [[ -x "$python_bin" ]] || die "Homebrew python@3.11 is required at $python_bin"

  if [[ ! -x "$TOOLS_VENV/bin/cmake" ]]; then
    echo "Installing private CMake $CMAKE_VERSION in $TOOLS_VENV"
    "$python_bin" -m venv "$TOOLS_VENV"
    "$TOOLS_VENV/bin/python" -m pip install --disable-pip-version-check "cmake==$CMAKE_VERSION"
    hash -r
  fi

  local actual
  actual="$("$TOOLS_VENV/bin/cmake" --version | awk 'NR == 1 {print $3}')"
  [[ "$actual" == "$CMAKE_VERSION" ]] ||
    die "Expected private CMake $CMAKE_VERSION, found $actual"
}

ensure_velox_source() {
  if [[ ! -d "$VELOX_HOME/.git" ]]; then
    [[ ! -e "$VELOX_HOME" ]] ||
      die "$VELOX_HOME exists but is not a Git checkout; move it aside and retry"
    mkdir -p "$VELOX_HOME"
    git -C "$VELOX_HOME" init
    git -C "$VELOX_HOME" remote add origin https://github.com/IBM/velox.git
    run_logged fetch-velox git -C "$VELOX_HOME" fetch --depth 1 origin "$VELOX_COMMIT"
    git -C "$VELOX_HOME" checkout --detach FETCH_HEAD
    run_logged submodules git -C "$VELOX_HOME" submodule update --init --recursive
    install_gluten_velox_build_patches
  fi

  local actual
  actual="$(git -C "$VELOX_HOME" rev-parse HEAD)"
  [[ "$actual" == "$VELOX_COMMIT" ]] ||
    die "Velox is at $actual; expected pinned commit $VELOX_COMMIT"
}

fix_velox_macos_setup_path_quoting() {
  local setup_common="$VELOX_HOME/scripts/setup-common.sh"
  local unquoted='git apply $ABSOLUTE_SCRIPTDIR/../CMake/resolve_dependency_modules/absl/absl-macos.patch'
  local quoted='git apply "$ABSOLUTE_SCRIPTDIR/../CMake/resolve_dependency_modules/absl/absl-macos.patch"'

  # The pinned script resolves SOURCE_ALIAS back to the real checkout path,
  # which may contain spaces, and then passes that path to git without quotes.
  if grep -Fq "$unquoted" "$setup_common"; then
    sed -i '' "s|$unquoted|$quoted|" "$setup_common"
  fi
  grep -Fq "$quoted" "$setup_common" ||
    die "Could not install the pinned Velox macOS path-quoting fix"
}

apply_hash_aggregation_patch() {
  [[ -f "$PATCH_FILE" ]] || die "Required Velox patch is missing: $PATCH_FILE"

  if git -C "$VELOX_HOME" apply --reverse --check "$PATCH_FILE" >/dev/null 2>&1; then
    echo "Required HashAggregation patch is already applied."
  elif git -C "$VELOX_HOME" apply --check "$PATCH_FILE" >/dev/null 2>&1; then
    git -C "$VELOX_HOME" apply "$PATCH_FILE"
    echo "Applied required HashAggregation patch to the isolated Velox checkout."
  else
    die "HashAggregation patch neither applies nor reverse-applies; inspect $VELOX_HOME"
  fi
}

verify_hash_aggregation_patch() {
  [[ -f "$PATCH_FILE" ]] || die "Required Velox patch is missing: $PATCH_FILE"
  git -C "$VELOX_HOME" apply --reverse --check "$PATCH_FILE" >/dev/null 2>&1 ||
    die "Required HashAggregation patch is not applied; run setup or rerun"
}

validate_dependencies() {
  [[ -f "$INSTALL_PREFIX/include/boost/version.hpp" ]] || return 1
  grep -q 'BOOST_LIB_VERSION "1_84"' "$INSTALL_PREFIX/include/boost/version.hpp" || return 1
  [[ -x "$INSTALL_PREFIX/bin/protoc" ]] || return 1
  [[ "$("$INSTALL_PREFIX/bin/protoc" --version)" == "libprotoc 3.21.8" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/libprotobuf.a" ]] || return 1
  [[ -f "$INSTALL_PREFIX/include/fmt/format.h" ]] || return 1
  [[ -f "$INSTALL_PREFIX/include/gflags/gflags.h" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/libgflags.dylib" ]] || return 1
  [[ -f "$INSTALL_PREFIX/include/glog/logging.h" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/libglog.dylib" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/folly/folly-config.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/fizz/fizz-config.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/wangle/wangle-config.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/mvfst/mvfst-config.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/fbthrift/FBThriftConfig.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/xsimd/xsimdConfig.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/libstemmer.a" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/DuckDB/DuckDBConfig.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/GEOS/geos-config.cmake" ]] || return 1
}

setup_dependencies() {
  if validate_dependencies; then
    touch "$SETUP_STAMP"
    echo "Pinned Velox dependencies are already installed."
    return
  fi

  # Source the pinned setup script and call its dependency function directly.
  # Executing setup-macos.sh normally performs an unconditional global
  # `brew update`; dependency formulae are still installed when missing.
  sed -i '' '/run_and_time install_arrow/d' "$VELOX_HOME/scripts/setup-macos.sh"
  run_logged setup-dependencies env HOMEBREW_NO_AUTO_UPDATE=1 \
    VELOX_SETUP_HOME="$VELOX_HOME" /bin/bash -c '
      cd "$VELOX_SETUP_HOME"
      source scripts/setup-macos.sh
      install_velox_deps
    '
  validate_dependencies ||
    die "Dependency setup finished but the pinned Boost/Protobuf/fmt/gflags/glog/Folly/DuckDB/GEOS files are incomplete"
  touch "$SETUP_STAMP"
}

validate_arrow() {
  [[ -f "$INSTALL_PREFIX/include/arrow/util/config.h" ]] || return 1
  grep -q '#define ARROW_VERSION_MAJOR 15' "$INSTALL_PREFIX/include/arrow/util/config.h" || return 1
  grep -q '#define ARROW_VERSION_MINOR 0' "$INSTALL_PREFIX/include/arrow/util/config.h" || return 1
  [[ -f "$INSTALL_PREFIX/lib/libarrow.a" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/libarrow_bundled_dependencies.a" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/libthrift.a" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/Arrow/ArrowConfig.cmake" ]] || return 1
  [[ -f "$INSTALL_PREFIX/lib/cmake/thrift/ThriftConfig.cmake" ]] || return 1
  [[ -d "$ARROW_PREFIX/cpp/_build/thrift_ep-install" ]] || return 1
}

setup_arrow() {
  if [[ -f "$ARROW_STAMP" ]] && validate_arrow; then
    echo "Patched Arrow $ARROW_VERSION C++ libraries are already installed."
    return
  fi

  run_logged arrow-cpp env \
    BUILD_ARROW_JAVA=OFF \
    ARROW_PREFIX="$ARROW_PREFIX" \
    "$SOURCE_ALIAS/dev/build-arrow.sh"
  validate_arrow || die "Arrow C++ build completed but Arrow $ARROW_VERSION was not found in $INSTALL_PREFIX"
  touch "$ARROW_STAMP"
}

require_setup() {
  [[ -f "$SETUP_STAMP" ]] && validate_dependencies ||
    die "Dependencies are not ready; run ./dev/build-rollup-native.sh setup"
  [[ -f "$ARROW_STAMP" ]] && validate_arrow ||
    die "Arrow is not ready; run ./dev/build-rollup-native.sh setup"
  [[ -d "$VELOX_HOME/.git" ]] ||
    die "Velox source is not ready; run ./dev/build-rollup-native.sh setup"
}

validate_velox_build() {
  local required=(
    "$VELOX_BUILD_PATH/lib/libvelox.a"
    "$VELOX_BUILD_PATH/velox/tpch/gen/dbgen/libdbgen.a"
    "$VELOX_BUILD_PATH/velox/vector/tests/utils/libvelox_vector_test_lib.a"
    "$VELOX_BUILD_PATH/velox/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a"
    "$VELOX_BUILD_PATH/velox/common/file/tests/libvelox_file_test_utils.a"
    "$VELOX_BUILD_PATH/velox/exec/tests/utils/libvelox_exec_test_lib.a"
  )
  local file
  for file in "${required[@]}"; do
    [[ -f "$file" ]] || return 1
  done
  [[ "$VELOX_BUILD_PATH/lib/libvelox.a" -nt "$VELOX_HOME/velox/exec/HashAggregation.cpp" ]] ||
    return 1
  [[ "$VELOX_BUILD_PATH/lib/libvelox.a" -nt "$VELOX_HOME/velox/exec/HashAggregation.h" ]] ||
    return 1
}

build_velox() {
  apply_hash_aggregation_patch
  run_logged build-velox builddeps build_velox
  validate_velox_build ||
    die "Velox build is incomplete or older than the required HashAggregation patch"
}

configure_gluten() {
  mkdir -p "$GLUTEN_BUILD_PATH"
  run_logged configure-gluten cmake \
    -S "$SOURCE_ALIAS/cpp" \
    -B "$GLUTEN_BUILD_PATH" \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=/usr/bin/clang \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=FALSE \
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=FALSE \
    -DCMAKE_NO_SYSTEM_FROM_IMPORTED=ON \
    "-DCMAKE_IGNORE_PREFIX_PATH=/usr/local;/opt/anaconda3" \
    "-DCMAKE_IGNORE_PATH=/usr/local;/usr/local/include;/usr/local/lib;/usr/local/lib/cmake;/opt/anaconda3" \
    "-DCMAKE_SYSTEM_IGNORE_PATH=/usr/local;/usr/local/include;/usr/local/lib;/usr/local/lib/cmake;/opt/anaconda3" \
    -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX" \
    "-DCMAKE_CXX_FLAGS=-Wno-inconsistent-missing-override -Wno-macro-redefined" \
    -DProtobuf_ROOT="$INSTALL_PREFIX" \
    -DProtobuf_INCLUDE_DIR="$INSTALL_PREFIX/include" \
    -DProtobuf_LIBRARY="$INSTALL_PREFIX/lib/libprotobuf.a" \
    -DProtobuf_PROTOC_EXECUTABLE="$INSTALL_PREFIX/bin/protoc" \
    -DVELOX_HOME="$VELOX_HOME" \
    -DVELOX_BUILD_PATH="$VELOX_BUILD_PATH" \
    -DBUILD_VELOX_BACKEND=ON \
    -DBUILD_TESTS=ON \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_BENCHMARKS=OFF \
    -DENABLE_JEMALLOC_STATS=OFF \
    -DENABLE_QAT=OFF \
    -DENABLE_GCS=OFF \
    -DENABLE_S3=OFF \
    -DENABLE_HDFS=OFF \
    -DENABLE_ABFS=OFF \
    -DENABLE_GPU=OFF
}

reject_contaminated_cache() {
  local cache="$1"
  [[ -f "$cache" ]] || return 0
  if grep '/opt/anaconda3' "$cache" |
    grep -Ev '^CMAKE_(SYSTEM_)?IGNORE_(PATH|PREFIX_PATH)' ||
    grep -E '^(Arrow|Boost|boost|DuckDB|duckdb|FOLLY|Folly|fmt|gflags|glog|Protobuf|PROTOBUF|re2|RE2|Thrift|THRIFT|xsimd|simdjson|stemmer|GEOS|geos)[^=]*(:FILEPATH|:PATH)=/usr/local' \
      "$cache"; then
    die "Contaminated dependency path found in $cache"
  fi
}

build_gluten_targets() {
  configure_gluten
  reject_contaminated_cache "$VELOX_BUILD_PATH/CMakeCache.txt"
  reject_contaminated_cache "$GLUTEN_BUILD_PATH/CMakeCache.txt"
  run_logged build-gluten-targets cmake --build "$GLUTEN_BUILD_PATH" \
    --target velox velox_rollup_aggregation_test -j "$JOBS"
  [[ -f "$GLUTEN_BUILD_PATH/releases/libvelox.dylib" ]] ||
    die "Gluten native library was not produced"
  [[ -x "$GLUTEN_BUILD_PATH/velox/tests/velox_rollup_aggregation_test" ]] ||
    die "Rollup test executable was not produced"
}

prepare_gluten_targets() {
  bootstrap_tools
  require_setup
  ensure_velox_source
  apply_hash_aggregation_patch
  validate_velox_build ||
    die "Velox is missing or stale; run ./dev/build-rollup-native.sh build"
  build_gluten_targets
}

run_enabled_tests() {
  local binary="$GLUTEN_BUILD_PATH/velox/tests/velox_rollup_aggregation_test"
  local discovered_tests
  [[ -x "$binary" ]] || die "Test binary is missing; run ./dev/build-rollup-native.sh build"
  discovered_tests="$("$binary" --gtest_list_tests)"
  grep -q '^MultiGroupingSetAggregationTest\.' <<<"$discovered_tests" ||
    die "The rollup binary did not discover MultiGroupingSetAggregationTest"
  run_logged test-enabled "$binary" --gtest_color=yes
}

run_pressure_tests() {
  local binary="$GLUTEN_BUILD_PATH/velox/tests/velox_rollup_aggregation_test"
  [[ -x "$binary" ]] || die "Test binary is missing; run ./dev/build-rollup-native.sh build"
  # Five differential pressure cases are disabled because the upstream
  # Expand-based reference plan fails at these budgets. Run their fused side
  # to stress this operator; the two budget-sweep tests are fused-only already.
  run_logged test-pressure env PLAN_ONLY=fused "$binary" \
    --gtest_color=yes \
    --gtest_also_run_disabled_tests \
    '--gtest_filter=*-*.DISABLED_q67ProfileBenchmark'
}

run_scala_planner_tests() {
  local java17_home="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
  local java17_path="$java17_home/bin:$PATH"
  local common_library="$GLUTEN_BUILD_PATH/releases/libgluten.dylib"
  local native_library="$GLUTEN_BUILD_PATH/releases/libvelox.dylib"
  local profiles="java-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3"

  [[ -x "$java17_home/bin/java" ]] ||
    die "JDK 17 is required at $java17_home"
  [[ -f "$common_library" ]] ||
    die "Common JNI library is missing; run build first"
  [[ -f "$native_library" ]] ||
    die "Native backend library is missing; run build first"

  (
    cd "$SOURCE_ALIAS"
    # Clean first because Maven target directories are shared across Spark/
    # Scala profiles and stale Scala signatures are not cross-compatible.
    run_logged scala-install env \
      JAVA_HOME="$java17_home" \
      PATH="$java17_path" \
      "$SOURCE_ALIAS/build/mvn" clean install \
      -pl backends-velox -am \
      -P"$profiles" \
      -DskipTests

    run_logged scala-planner-test env \
      JAVA_HOME="$java17_home" \
      PATH="$java17_path" \
      "$SOURCE_ALIAS/build/mvn" \
      -pl backends-velox \
      -P"$profiles" \
      org.scalatest:scalatest-maven-plugin:2.2.0:test \
      -Dsuites=org.apache.gluten.execution.FusedGroupingSetAggregateSuite \
      "-DargLine=-Dgluten.rollup.test.commonLibPath=$common_library -Dspark.gluten.sql.columnar.libpath=$native_library"
  )
}

write_manifest() {
  local manifest="$STATE_DIR/manifest.txt"
  {
    echo "source=$REPO_ROOT"
    echo "source_alias=$SOURCE_ALIAS"
    echo "state=$STATE_DIR"
    echo "velox_home=$VELOX_HOME"
    echo "velox_commit=$(git -C "$VELOX_HOME" rev-parse HEAD 2>/dev/null || echo missing)"
    echo "install_prefix=$INSTALL_PREFIX"
    echo "velox_build=$VELOX_BUILD_PATH"
    echo "gluten_build=$GLUTEN_BUILD_PATH"
    echo "jobs=$JOBS"
    echo "cmake=$("$TOOLS_VENV/bin/cmake" --version 2>/dev/null | head -n 1 || echo missing)"
    echo "ninja=$(ninja --version 2>/dev/null || echo missing)"
    echo "clang=$(/usr/bin/clang --version | head -n 1)"
    echo "clang_format=$(clang-format --version 2>/dev/null || echo missing)"
    echo "java=$("$JAVA_HOME/bin/java" -version 2>&1 | head -n 1)"
    echo "protoc=$("$INSTALL_PREFIX/bin/protoc" --version 2>/dev/null || echo missing)"
  } >"$manifest"
  echo "Manifest: $manifest"
}

print_environment() {
  printf 'export JAVA_HOME=%q\n' "$JAVA_HOME"
  printf 'export PATH=%q\n' "$PATH"
  printf 'export CC=%q\n' "$CC"
  printf 'export CXX=%q\n' "$CXX"
  printf 'export CPU_TARGET=%q\n' "$CPU_TARGET"
  printf 'export BUILD_THREADS=%q\n' "$BUILD_THREADS"
  printf 'export NUM_THREADS=%q\n' "$NUM_THREADS"
  printf 'export NPROC=%q\n' "$NPROC"
  printf 'export CMAKE_BUILD_PARALLEL_LEVEL=%q\n' "$CMAKE_BUILD_PARALLEL_LEVEL"
  printf 'export INSTALL_PREFIX=%q\n' "$INSTALL_PREFIX"
  printf 'export DEPENDENCY_DIR=%q\n' "$DEPENDENCY_DIR"
  printf 'export BOOST_ROOT=%q\n' "$BOOST_ROOT"
  printf 'export CMAKE_PREFIX_PATH=%q\n' "$CMAKE_PREFIX_PATH"
  printf 'export PKG_CONFIG_PATH=%q\n' "$PKG_CONFIG_PATH"
  printf 'export SOURCE_ALIAS=%q\n' "$SOURCE_ALIAS"
  printf 'export VELOX_HOME=%q\n' "$VELOX_HOME"
  printf 'export VELOX_BUILD_PATH=%q\n' "$VELOX_BUILD_PATH"
  printf 'export GLUTEN_BUILD_PATH=%q\n' "$GLUTEN_BUILD_PATH"
}

doctor() {
  echo "Source:        $REPO_ROOT"
  echo "No-space root: $SOURCE_ALIAS"
  echo "State:         $STATE_DIR"
  echo "Velox:         $VELOX_HOME"
  echo "Prefix:        $INSTALL_PREFIX"
  echo "Velox build:   $VELOX_BUILD_PATH"
  echo "Gluten build:  $GLUTEN_BUILD_PATH"
  echo "Jobs:          $JOBS"
  echo "JAVA_HOME:     $JAVA_HOME"
  echo "PATH cmake:    $(command -v cmake || echo missing)"
  echo "PATH ninja:    $(command -v ninja || echo missing)"
  echo "PATH protoc:   $(command -v protoc || echo missing)"

  [[ "$(uname -s)" == "Darwin" ]] || die "This helper is for native macOS validation"
  [[ "$(uname -m)" == "arm64" ]] || die "This helper currently targets Apple Silicon"
  [[ "$PATH" != *anaconda* ]] || die "Conda remains in PATH"

  if [[ -x "$TOOLS_VENV/bin/cmake" ]]; then
    "$TOOLS_VENV/bin/cmake" --version | head -n 1
  else
    echo "Private CMake: not installed (run setup)"
  fi
  if command -v clang-format >/dev/null 2>&1; then
    clang-format --version | grep -q 'version 15\.' ||
      die "clang-format must be version 15"
    clang-format --version | head -n 1
  else
    echo "clang-format 15: missing (/opt/homebrew/opt/llvm@15/bin)"
  fi

  if [[ -d "$VELOX_HOME/.git" ]]; then
    ensure_velox_source
    verify_hash_aggregation_patch
    echo "Velox pin and required patch: OK"
  else
    echo "Velox source: not installed (run setup)"
  fi

  if validate_dependencies; then
    echo "Pinned Boost/Protobuf/fmt/gflags/glog/Folly/DuckDB/GEOS: OK"
  else
    echo "Pinned dependencies: incomplete (run setup)"
  fi

  if validate_arrow; then
    echo "Patched Arrow $ARROW_VERSION C++: OK"
  else
    echo "Patched Arrow C++: incomplete (run setup)"
  fi

  reject_contaminated_cache "$VELOX_BUILD_PATH/CMakeCache.txt"
  reject_contaminated_cache "$GLUTEN_BUILD_PATH/CMakeCache.txt"

  if validate_velox_build; then
    echo "Velox library and test utilities: OK"
  else
    echo "Velox library/test utilities: incomplete or stale (run build)"
  fi

  local native_library="$GLUTEN_BUILD_PATH/releases/libvelox.dylib"
  if [[ -f "$native_library" ]]; then
    local native_linkage
    native_linkage="$(otool -L "$native_library")" ||
      die "otool could not inspect Gluten's native library"
    if grep -q '/opt/anaconda3' <<<"$native_linkage"; then
      die "Gluten's native library links an Anaconda library"
    fi
    grep -q 'libgflags.*\.dylib' <<<"$native_linkage" ||
      die "Gluten's native library does not load shared gflags"
    grep -q 'libglog.*\.dylib' <<<"$native_linkage" ||
      die "Gluten's native library does not load shared glog"
    if nm -gU "$native_library" | grep -q 'FLAGS_flagfile'; then
      die "Gluten's native library also contains a static gflags registry"
    fi
    echo "Gluten native library linkage: OK"
  else
    echo "Gluten native library: not built"
  fi

  if [[ -x "$GLUTEN_BUILD_PATH/velox/tests/velox_rollup_aggregation_test" ]]; then
    local binary="$GLUTEN_BUILD_PATH/velox/tests/velox_rollup_aggregation_test"
    local linkage
    local discovered_tests
    linkage="$(otool -L "$binary")" ||
      die "otool could not inspect the rollup test binary"
    if grep -q '/opt/anaconda3' <<<"$linkage"; then
      die "Test binary links an Anaconda library"
    fi
    if grep -q 'libvelox\.dylib' <<<"$linkage"; then
      die "Rollup test loads libvelox.dylib as well as the monolithic archive"
    fi
    grep -q 'libgflags.*\.dylib' <<<"$linkage" ||
      die "Rollup test does not load shared gflags"
    grep -q 'libglog.*\.dylib' <<<"$linkage" ||
      die "Rollup test does not load shared glog"
    discovered_tests="$("$binary" --gtest_list_tests)" ||
      die "Rollup test binary failed during test discovery"
    grep -q '^MultiGroupingSetAggregationTest\.' <<<"$discovered_tests" ||
      die "Rollup test binary did not discover its aggregation tests"
    echo "Rollup test runtime/linkage/discovery: OK"
  else
    echo "Rollup test binary: not built"
  fi
}

command_name="${1:-}"
case "$command_name" in
doctor)
  doctor
  ;;
setup)
  bootstrap_tools
  ensure_velox_source
  fix_velox_macos_setup_path_quoting
  apply_hash_aggregation_patch
  setup_dependencies
  setup_arrow
  write_manifest
  doctor
  ;;
build)
  bootstrap_tools
  require_setup
  ensure_velox_source
  build_velox
  build_gluten_targets
  write_manifest
  ;;
test)
  prepare_gluten_targets
  run_enabled_tests
  write_manifest
  ;;
test-pressure)
  prepare_gluten_targets
  run_pressure_tests
  write_manifest
  ;;
test-scala)
  prepare_gluten_targets
  run_scala_planner_tests
  write_manifest
  ;;
rerun)
  prepare_gluten_targets
  run_enabled_tests
  write_manifest
  ;;
all)
  bootstrap_tools
  ensure_velox_source
  fix_velox_macos_setup_path_quoting
  apply_hash_aggregation_patch
  setup_dependencies
  setup_arrow
  build_velox
  build_gluten_targets
  run_enabled_tests
  write_manifest
  ;;
env)
  print_environment
  ;;
"")
  usage
  exit 2
  ;;
*)
  usage
  die "Unknown command: $command_name"
  ;;
esac
