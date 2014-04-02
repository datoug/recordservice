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

#include "util/locks.h"

#include "util/lock-tracker.h"

using namespace impala;

Lock::Lock(const char* name, LockTracker* tracker)
  : is_locked_(false), desc_(name) {
  if (tracker != NULL) tracker->RegisterLock(this);
  ClearCounters();
}

SpinLock::SpinLock(const char* name, LockTracker* tracker)
    : locked_(false), desc_(name) {
  if (tracker != NULL) tracker->RegisterLock(this);
  ClearCounters();
}
