// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaInternalService.

#include <unistd.h>
#include <jni.h>

#include "common/logging.h"
#include "common/init.h"
#include "exec/hbase-table-scanner.h"
#include "exec/hbase-table-writer.h"
#include "runtime/hbase-table-factory.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "util/jni-util.h"
#include "util/network-util.h"
#include "util/recordservice-metrics.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "rpc/rpc-trace.h"
#include "service/impala-server.h"
#include "service/fe-support.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "util/minidump.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(classpath);
DECLARE_bool(use_statestore);
DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_int32(be_port);
DECLARE_string(principal);

DECLARE_int32(recordservice_planner_port);
DECLARE_int32(recordservice_worker_port);

// FIXME: remove this flag when recordserviced is ready.
DEFINE_bool(start_recordservice, true, "Start RecordService services");

// Start a test thrift service. This service is used to test authentication and should
// not be on for production.
DEFINE_int32(test_service_port, 0, "Port to start test service on. 0 to not start.");

DEFINE_string(minidump_path, "/tmp/minidumps",
    "Directory to output minidumps on crash. If empty, minidumps is disabled.");

int main(int argc, char** argv) {
  if (FLAGS_minidump_path.size() > 0) RegisterMinidump(FLAGS_minidump_path.c_str());
  InitCommonRuntime(argc, argv, true);

  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();
  EXIT_IF_ERROR(HBaseTableScanner::Init());
  EXIT_IF_ERROR(HBaseTableFactory::Init());
  EXIT_IF_ERROR(HBaseTableWriter::InitJNI());
  InitFeSupport();

  // Create a service ID that will unique across the cluster.
  string service_id = ImpalaServer::CreateServerId(
      "impalad", FLAGS_hostname, FLAGS_be_port);

  ExecEnv exec_env(service_id, false, FLAGS_start_recordservice);
  StartThreadInstrumentation(exec_env.metrics(), exec_env.webserver());
  InitRpcEventTracing(exec_env.webserver());

  ThriftServer* beeswax_server = NULL;
  ThriftServer* hs2_server = NULL;
  ThriftServer* be_server = NULL;

  ThriftServer* recordservice_planner = NULL;
  ThriftServer* recordservice_worker = NULL;

  ThriftServer* test_server = NULL;

  shared_ptr<ImpalaServer> server;
  EXIT_IF_ERROR(CreateImpalaServer(&exec_env, FLAGS_beeswax_port, FLAGS_hs2_port,
      FLAGS_be_port, &beeswax_server, &hs2_server, &be_server,
      &server));

  if (FLAGS_start_recordservice) {
    EXIT_IF_ERROR(ImpalaServer::StartRecordServiceServices(&exec_env, server,
        FLAGS_recordservice_planner_port, FLAGS_recordservice_worker_port,
        &recordservice_planner, &recordservice_worker));
  }
  if (FLAGS_test_service_port != 0) {
    EXIT_IF_ERROR(ImpalaServer::StartTestService(
        server, FLAGS_test_service_port, &test_server));
  }

  EXIT_IF_ERROR(be_server->Start());

  Status status = exec_env.StartServices();
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting.  Error: "
               << status.GetDetail();
    ShutdownLogging();
    exit(1);
  }

  // this blocks until the beeswax and hs2 servers terminate
  EXIT_IF_ERROR(beeswax_server->Start());
  EXIT_IF_ERROR(hs2_server->Start());
  if (recordservice_planner != NULL) {
    EXIT_IF_ERROR(recordservice_planner->Start());
    RecordServiceMetrics::RUNNING_PLANNER->set_value(true);
  }
  if (recordservice_worker != NULL) {
    EXIT_IF_ERROR(recordservice_worker->Start());
    RecordServiceMetrics::RUNNING_WORKER->set_value(true);
  }
  if (test_server != NULL) EXIT_IF_ERROR(test_server->Start());

  ImpaladMetrics::IMPALA_SERVER_READY->set_value(true);
  LOG(INFO) << "Impala has started.";

  beeswax_server->Join();
  hs2_server->Join();
  if (recordservice_planner != NULL) recordservice_planner->Join();
  if (recordservice_worker != NULL) recordservice_worker->Join();
  if (test_server != NULL) test_server->Join();

  delete be_server;
  delete beeswax_server;
  delete hs2_server;
}
