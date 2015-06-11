#!/usr/bin/env bash
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

# Generates the symbols file for impala components.
# TODO: we should also generate these files for all .so's that impala loads.
# This would include the ones in the other CDH projects (e.g. hadoop native code),
# things we don't ship (e.g. libjvm.so) and .so's from the system that we don't
# ship (e.g. libpthread.so) We'd store the symbol files per platform and per release.

# Exit on reference to uninitialized variable
set -u

# Exit on non-zero return value
set -e

cd $IMPALA_HOME/be/build/debug
$IMPALA_HOME/tools/breakpad_gen_symbols.py /tmp/symbols service/impalad codegen/libCodeGen.so runtime/libRuntime.so transport/libThriftSaslTransport.so rpc/libRpc.so exec/libExec.so thrift/libImpalaThrift.so exprs/libExprs.so common/libGlobalFlags.so common/libCommon.so resourcebroker/libResourceBroker.so catalog/libCatalog.so service/libfesupport.so service/libService.so statestore/libStatestore.so udf/libUdf.so util/libUtil.so util/libloggingsupport.so
