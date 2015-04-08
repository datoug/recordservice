#!/bin/sh
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts up a recordserviced

set -e
set -u

BUILD_TYPE=debug
RECORDSERVICED_ARGS=""
BINARY_BASE_DIR=${IMPALA_HOME}/be/build
GDB_PREFIX=""
RECORDSERVICED_BINARY=service/recordserviced
JVM_DEBUG_PORT=""
JVM_SUSPEND="n"
JVM_ARGS=""

for ARG in $*
do
  case "$ARG" in
    -build_type=debug)
      BUILD_TYPE=debug
      ;;
    -build_type=release)
      BUILD_TYPE=release
      ;;
    -build_type=*)
      echo "Invalid build type. Valid values are: debug, release"
      exit 1
      ;;
    -gdb)
      echo "Starting Impala under gdb..."
      GDB_PREFIX="gdb --args"
      ;;
    -jvm_debug_port=*)
      JVM_DEBUG_PORT="${ARG#*=}"
      ;;
    -jvm_suspend)
      JVM_SUSPEND="y"
      ;;
    -jvm_args=*)
      JVM_ARGS="${ARG#*=}"
      ;;
    # Pass all other options as an Impalad argument
    *)
      RECORDSERVICED_ARGS="${RECORDSERVICED_ARGS} ${ARG}"
  esac
done

CMD=${BINARY_BASE_DIR}/${BUILD_TYPE}/${RECORDSERVICED_BINARY}

# Temporarily disable unbound variable checking in case JAVA_TOOL_OPTIONS is not set.
set +u
# Optionally enable Java debugging.
if [ -n "$JVM_DEBUG_PORT" ]; then
  export JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=localhost:${JVM_DEBUG_PORT},server=y,suspend=${JVM_SUSPEND} ${JAVA_TOOL_OPTIONS}"
fi
# Optionally add additional JVM args.
if [ -n "$JVM_ARGS" ]; then
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} ${JVM_ARGS}"
fi

# If Kerberized, source appropriate vars and set startup options
if ${CLUSTER_DIR}/admin is_kerberized; then
  . ${MINIKDC_ENV}
  RECORDSERVICED_ARGS="${RECORDSERVICED_ARGS} -principal=${MINIKDC_PRINC_IMPALA}"
  RECORDSERVICED_ARGS="${RECORDSERVICED_ARGS} -be_principal=${MINIKDC_PRINC_IMPALA_BE}"
  RECORDSERVICED_ARGS="${RECORDSERVICED_ARGS} -keytab_file=${KRB5_KTNAME}"
  RECORDSERVICED_ARGS="${RECORDSERVICED_ARGS} -krb5_conf=${KRB5_CONFIG}"
  if [ "${MINIKDC_DEBUG}" = "true" ]; then
      RECORDSERVICED_ARGS="${RECORDSERVICED_ARGS} -krb5_debug_file=/tmp/impalad.krb5_debug"
  fi
fi

. ${IMPALA_HOME}/bin/set-classpath.sh
exec ${GDB_PREFIX} ${CMD} ${RECORDSERVICED_ARGS}
