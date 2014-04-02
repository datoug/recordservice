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

#include "util/lock-tracker.h"

#include <gutil/strings/substitute.h>

#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile.h"

using namespace boost;
using namespace impala;
using namespace std;
using namespace strings;

LockTracker* LockTracker::global_;

LockTracker::LockTracker(bool global) {
  if (global) {
    DCHECK(global_ == NULL);
    global_ = this;
  }
}

void LockTracker::RegisterLock(Lock* lock) {
  DCHECK(lock != NULL);
  DCHECK(strcmp(lock->name(), "Lock") != 0) << "Cannot add unnamed lock.";
  lock_guard<SpinLock> l(lock_);
  locks_[lock->name()].push_back(lock);
}

void LockTracker::RegisterLock(SpinLock* lock) {
  DCHECK(lock != NULL);
  DCHECK(strcmp(lock->name(), "SpinLock") != 0) << "Cannot add unnamed lock.";
  lock_guard<SpinLock> l(lock_);
  spin_locks_[lock->name()].push_back(lock);
}

template<typename T>
static void ToRuntimeProfileHelper(RuntimeProfile* profile,
    const map<string, vector<T*> >& locks) {
  typename map<string, vector<T*> >::const_iterator it;
  int64_t total_lock_time_ns = 0;
  for (it = locks.begin(); it != locks.end(); ++it) {
    int64_t lock_time_ns = 0;
    int64_t num_contended = 0;
    // Collect all stats for locks with the same name.
    for (typename vector<T*>::const_iterator i = it->second.begin();
        i != it->second.end(); ++i) {
      const T* l = *i;
      lock_time_ns += l->contended_time_ns();
      num_contended += l->num_contended();
    }
    total_lock_time_ns += lock_time_ns;

    profile->AddCounter(Substitute("$0_ContendedTime", it->first),
        TUnit::TIME_NS)->Add(lock_time_ns);
    profile->AddCounter(Substitute("$0_ContendedCount", it->first),
        TUnit::UNIT)->Add(num_contended);
  }
  profile->total_time_counter()->Add(total_lock_time_ns);
}

void LockTracker::ToRuntimeProfile(RuntimeProfile* profile) {
  lock_guard<SpinLock> l(lock_);
  ToRuntimeProfileHelper<Lock>(profile, locks_);
  ToRuntimeProfileHelper<SpinLock>(profile, spin_locks_);
}

template <typename T>
void ToStringHelper(const map<string, vector<T*> >& locks, stringstream* ss) {
  typename map<string, vector<T*> >::const_iterator it;
  for (it = locks.begin(); it != locks.end(); ++it) {
    int64_t lock_time_ns = 0;
    int64_t num_contended = 0;
    // Collect all stats for locks with the same name.
    for (typename vector<T*>::const_iterator i = it->second.begin();
        i != it->second.end(); ++i) {
      const T* l = *i;
      lock_time_ns += l->contended_time_ns();
      num_contended += l->num_contended();
    }
    (*ss) << it->first <<  "(" << num_contended << "): "
          << PrettyPrinter::Print(lock_time_ns, TUnit::TIME_NS) << endl;
  }
}

string LockTracker::ToString() const {
  lock_guard<SpinLock> l(lock_);
  stringstream ss;
  ToStringHelper<Lock>(locks_, &ss);
  ToStringHelper<SpinLock>(spin_locks_, &ss);
  return ss.str();
}
