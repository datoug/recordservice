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
#
# Runs the example RecordService MR job that reads the given table columns
# using JDBC.

. ${IMPALA_HOME}/bin/set-classpath.sh

set -e
set -u

# Output location
OUTPUT_DIR=/tmp/output-`date +%s`

echo "Running job against db=$1,tbl=$2,cols=$3. Output to (HDFS): $OUTPUT_DIR"

# Job to run
JAR_FILE=${IMPALA_HOME}/fe/target/impala-frontend-0.1-SNAPSHOT-jar-with-dependencies.jar

HADOOP_CLASSPATH=$CLASSPATH:${HIVE_HOME}/lib/hive-jdbc-0.13.1-cdh5.4.0-SNAPSHOT.jar \
    hadoop jar ${JAR_FILE} com.cloudera.recordservice.lib.RecordServiceMRExample \
    $1 $2 $3 ${OUTPUT_DIR}
