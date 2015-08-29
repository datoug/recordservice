#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

CMD=$1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"
}

# RECORDSERVICE_HOME is provided in the recordservice parcel's env script.
RECORD_SERVICE_BIN_HOME=$RECORDSERVICE_HOME/../../bin
log "RECORD_SERVICE_BIN_HOME: $RECORD_SERVICE_BIN_HOME"

log "CMD: $CMD"

export HADOOP_CONF_DIR=$CONF_DIR/hadoop-conf
export HIVE_CONF_DIR=$CONF_DIR/hive-conf
export USER=recordservice

log "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
log "HIVE_CONF_DIR: $HIVE_CONF_DIR"
log "USER: $USER"

KEYTAB_FILE=$HADOOP_CONF_DIR/../record_service.keytab

case $CMD in
  (start)
    # The following parameters are provided in descriptor/service.sdl.
    log "Starting RecordService"
    log "recordservice_planner_port: $PLANNER_PORT"
    log "recordservice_worker_port: $WORKER_PORT"
    log "state_store_subscriber_port: $STATESTORE_SUB_PORT"
    log "statestore_subscriber_timeout_seconds: $STATESTORE_SUB_TIMEOUT_SEC"
    log "log_filename: $LOG_FILENAME"
    log "hostname: $HOSTNAME"
    log "state_store_host: $STATESTORE_HOST"
    log "state_store_port: $STATESTORE_PORT"
    log "catalog_service_host: $CATALOG_SERVICE_HOST"
    log "catalog_service_port: $CATALOG_SERVICE_PORT"
    log "recordservice_webserver_port: $WEBSERVICE_PORT"
    log "webserver_doc_root: $RECORDSERVICE_HOME"
    log "log_dir: $LOG_DIR"
    log "principal: $PRINCIPAL"
    log "keytab_file: $KEYTAB_FILE"
    log "v: $V"
    log "kerberos_reinit_interval: $KERBEROS_REINIT_INTERVAL"

    exec $RECORD_SERVICE_BIN_HOME/recordserviced \
      -recordservice_planner_port=$PLANNER_PORT \
      -recordservice_worker_port=$WORKER_PORT \
      -state_store_subscriber_port=$STATESTORE_SUB_PORT \
      -statestore_subscriber_timeout_seconds=$STATESTORE_SUB_TIMEOUT_SEC \
      -log_filename=$LOG_FILENAME \
      -hostname=$HOSTNAME \
      -state_store_host=$STATESTORE_HOST \
      -state_store_port=$STATESTORE_PORT \
      -catalog_service_host=$CATALOG_SERVICE_HOST \
      -catalog_service_port=$CATALOG_SERVICE_PORT \
      -recordservice_webserver_port=$WEBSERVICE_PORT \
      -webserver_doc_root=$RECORDSERVICE_HOME \
      -log_dir=$LOG_DIR \
      -abort_on_config_error=false \
      -principal=$PRINCIPAL \
      -keytab_file=$KEYTAB_FILE \
      -lineage_event_log_dir=$LOG_DIR/lineage \
      -audit_event_log_dir=$LOG_DIR/audit \
      -profile_log_dir=$LOG_DIR/profiles/ \
      -v=$V \
      -kerberos_reinit_interval=$KERBEROS_REINIT_INTERVAL
    ;;

  (stopAll)
    # MAIN_PROCESS is provided in descriptor/service.sdl.
    log "Stop mainprocess [$MAIN_PROCESS]"
    exec killall -w $MAIN_PROCESS
    ;;

  (*)
    log "Don't understand [$CMD]"
    ;;
esac
