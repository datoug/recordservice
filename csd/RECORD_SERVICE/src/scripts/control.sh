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

# Adds a xml config to hdfs-site.xml
add_to_hdfs_site() {
  FILE=`find $CONF_DIR/hadoop-conf -name hdfs-site.xml`
  CONF_END="</configuration>"
  NEW_PROPERTY="<property><name>$1</name><value>$2</value></property>"
  TMP_FILE=$CONF_DIR/tmp-hdfs-site
  cat $FILE | sed "s#$CONF_END#$NEW_PROPERTY#g" > $TMP_FILE
  cp $TMP_FILE $FILE
  rm -f $TMP_FILE
  echo $CONF_END >> $FILE
}

# RECORDSERVICE_HOME is provided in the recordservice parcel's env script.
RECORD_SERVICE_BIN_HOME=$RECORDSERVICE_HOME/../../bin
log "RECORD_SERVICE_BIN_HOME: $RECORD_SERVICE_BIN_HOME"

log "CMD: $CMD"

export HADOOP_CONF_DIR=$CONF_DIR/hadoop-conf
export HIVE_CONF_DIR=$CONF_DIR/hive-conf
export USER=recordservice

env

log "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
log "HIVE_CONF_DIR: $HIVE_CONF_DIR"
log "USER: $USER"

KEYTAB_FILE=$HADOOP_CONF_DIR/../record_service.keytab

# The following parameters are provided in descriptor/service.sdl.
log "Starting RecordService"
log "recordservice_planner_port: $PLANNER_PORT"
log "recordservice_worker_port: $WORKER_PORT"
log "log_filename: $LOG_FILENAME"
log "hostname: $HOSTNAME"
log "recordservice_webserver_port: $WEBSERVICE_PORT"
log "webserver_doc_root: $RECORDSERVICE_HOME"
log "log_dir: $LOG_DIR"
log "principal: $RECORD_SERVICE_PRINCIPAL"
log "keytab_file: $KEYTAB_FILE"
log "v: $V"
log "kerberos_reinit_interval: $KERBEROS_REINIT_INTERVAL"
log "mem_limit: $MEM_LIMIT%"
log "sentry_config: $SENTRY_CONFIG"
log "advanced_config: $ADVANCED_CONFIG"

# The HDFS default has the wrong units so configure this.
add_to_hdfs_site dfs.client.file-block-storage-locations.timeout.millis 5000000
# Add zk quorum to hdfs-site.xml
add_to_hdfs_site recordservice.zookeeper.connectString $ZK_QUORUM
# FIXME this is not secure.
add_to_hdfs_site recordservice.zookeeper.acl world:anyone:cdrwa

ARGS="\
  -log_filename=$LOG_FILENAME \
  -hostname=$HOSTNAME \
  -recordservice_webserver_port=$WEBSERVICE_PORT \
  -webserver_doc_root=$RECORDSERVICE_HOME \
  -log_dir=$LOG_DIR \
  -abort_on_config_error=false \
  -lineage_event_log_dir=$LOG_DIR/lineage \
  -audit_event_log_dir=$LOG_DIR/audit \
  -profile_log_dir=$LOG_DIR/profiles/ \
  -v=$V \
  -mem_limit=$MEM_LIMIT% \
  -sentry_config=$SENTRY_CONFIG \
  "
if env | grep -q ^RECORD_SERVICE_PRINCIPAL=
then
  log "Starting kerberized cluster"
  ARGS=$ARGS"\
    -principal=$RECORD_SERVICE_PRINCIPAL \
    -keytab_file=$KEYTAB_FILE \
    -kerberos_reinit_interval=$KERBEROS_REINIT_INTERVAL\
    "
fi

if [[ -n $ADVANCED_CONFIG ]];
then
  log "Add advanced config:$ADVANCED_CONFIG"
  ARGS="$ARGS $ADVANCED_CONFIG"
fi

case $CMD in
  (start_planner_worker)
    log "Starting recordserviced running planner and worker services"
    exec $RECORD_SERVICE_BIN_HOME/recordserviced $ARGS \
      -recordservice_planner_port=$PLANNER_PORT \
      -recordservice_worker_port=$WORKER_PORT
  ;;

  (start_planner)
    log "Starting recordserviced running planner service"
    exec $RECORD_SERVICE_BIN_HOME/recordserviced $ARGS \
      -recordservice_planner_port=$PLANNER_PORT \
      -recordservice_worker_port=0
  ;;

  (start_worker)
    log "Starting recordserviced running worker service"
    exec $RECORD_SERVICE_BIN_HOME/recordserviced $ARGS \
      -recordservice_planner_port=0 \
      -recordservice_worker_port=$WORKER_PORT
  ;;

  (stopAll)
    log "Stopping recordserviced"
    exec killall -w recordserviced
    ;;

  (*)
    log "Don't understand [$CMD]"
    ;;
esac
