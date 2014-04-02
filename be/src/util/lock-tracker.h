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


#ifndef IMPALA_UTIL_LOCK_TRACKER_H
#define IMPALA_UTIL_LOCK_TRACKER_H

#include <map>
#include <string>
#include <vector>

#include "util/locks.h"
#include "util/stopwatch.h"

namespace impala {

class RuntimeProfile;

// This class collects groups of locks and can generate aggregate contention
// metrics.
class LockTracker {
 public:
  // Returns the lock tracker for process wide locks.
  static LockTracker* global() { return global_; }

  LockTracker(bool is_global = false);

  // Adds a lock for tracking. The runtime state will aggregate the times in
  // ComputeLockTimes()
  // The registered locks must have lifetime at least as long as this object.
  void RegisterLock(Lock* lock);
  void RegisterLock(SpinLock* lock);

  // Converts the registered locks to counters that are added to 'profile'.
  void ToRuntimeProfile(RuntimeProfile* profile);

  std::string ToString() const;

 private:
  mutable SpinLock lock_;

  // Mapping of lock name to the list of locks with that name.
  // Use a map so these are sorted by name.
  std::map<std::string, std::vector<Lock*> > locks_;
  std::map<std::string, std::vector<SpinLock*> > spin_locks_;

  static LockTracker* global_;
};

}

#endif
