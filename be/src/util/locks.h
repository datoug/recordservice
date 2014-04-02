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


#ifndef IMPALA_UTIL_LOCKS_H
#define IMPALA_UTIL_LOCKS_H

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include "common/atomic.h"
#include "common/logging.h"
#include "util/stopwatch.h"

// Can be disabled if to measure if contention tracking is expensive.
#define ENABLE_LOCK_CONTENTION_TRACKING 1

namespace impala {

class LockTracker;

// Additional diagnostic information about a lock.
struct LockDesc {
  LockDesc(const char* name) : name(name) {
    ClearCounters();
  }

  void ClearCounters() {
    num_contended = contended_time_ns = 0;
  }

  // Updates contention counters. Not thread safe.
  void UpdateContentedCounters(const timespec& start, const timespec& end) {
    ++num_contended;
    contended_time_ns += MonotonicStopWatch::ComputeDeltaNs(start, end);
  }

  const char* name;
  int64_t num_contended;
  int64_t contended_time_ns;
};

// Wrapper class around boost::mutex to provide additional diagnostic functionality.
// To work with the other boost synchronization classes (e.g. lock guard), this
// needs to implement the same API as boost::mutex.
class Lock {
 public:
  Lock(const char* name = "Lock", LockTracker* tracker = NULL);

  void lock() {
#if ENABLE_LOCK_CONTENTION_TRACKING
    if (!lock_.try_lock()) {
      timespec start, end;
      clock_gettime(CLOCK_MONOTONIC, &start);
      lock_.lock();
      clock_gettime(CLOCK_MONOTONIC, &end);
      desc_.UpdateContentedCounters(start, end);
    }
#else
    lock_.lock();
#endif
    DCHECK(!is_locked());
    is_locked_ = true;
  }

  void unlock() {
    DCHECK(is_locked());
    is_locked_ = false;
    lock_.unlock();
  }

  bool try_lock() {
    if (lock_.try_lock()) {
      DCHECK(!is_locked());
      is_locked_ = true;
      return true;
    }
    return false;
  }

  void ClearCounters() {
    desc_.ClearCounters();
  }

  bool is_locked() const { return is_locked_; }
  const char* name() const { return desc_.name; }
  int64_t num_contended() const { return desc_. num_contended; }
  int64_t contended_time_ns() const { return desc_.contended_time_ns; }

 private:
  boost::mutex lock_;
  bool is_locked_;
  LockDesc desc_;
};

// Lightweight spinlock.
// Also implements the boost lock interface. Although there is some code duplication
// with Lock, we want to avoid virtual calls and to make sure that the core member
// variables come at the beginning of the object.
class SpinLock {
 public:
  SpinLock(const char* name = "SpinLock", LockTracker* tracker = NULL);

  void lock() {
    if (try_lock()) return;
    timespec start, end;
#if ENABLE_LOCK_CONTENTION_TRACKING
    clock_gettime(CLOCK_MONOTONIC, &start);
#endif

    while (true) {
      if (try_lock()) break;
      for (int i = 0; i < NUM_SPIN_CYCLES + 1; ++i) {
        AtomicUtil::CpuWait();
      }
      if (try_lock()) break;
      sched_yield();
    }

#if ENABLE_LOCK_CONTENTION_TRACKING
    clock_gettime(CLOCK_MONOTONIC, &end);
    desc_.UpdateContentedCounters(start, end);
#endif
  }

  void unlock() {
    // Memory barrier here. All updates before the unlock need to be made visible.
    __sync_synchronize();
    DCHECK(locked_);
    locked_ = false;
  }

  bool try_lock() {
    return __sync_bool_compare_and_swap(&locked_, false, true);
  }

  void ClearCounters() {
    desc_.ClearCounters();
  }

  bool is_locked() const { return locked_; }
  const char* name() const { return desc_.name; }
  int64_t num_contended() const { return desc_. num_contended; }
  int64_t contended_time_ns() const { return desc_.contended_time_ns; }

 private:
  // In typical spin lock implementations, we want to spin (and keep the core fully
  // busy) for some number of cycles before yielding. Consider these three
  // cases:
  //  1) lock is un-contended - spinning doesn't kick in and has no effect.
  //  2) lock is taken by another thread and that thread finishes quickly
  //  3) lock is taken by another thread and that thread is slow (e.g. scheduled
  //     away).
  // In case 3), we'd want to yield so another thread can do work. This thread
  // won't be able to do anything useful until the thread with the lock runs again.
  // In case 2), we don't want to yield (and give up our scheduling time slice)
  // since we will get to run soon after.
  // To try to get the best of everything, we will busy spin for a while before
  // yielding to another thread.
  // TODO: how do we set this.
  static const int NUM_SPIN_CYCLES = 70;
  // TODO: pad this to be a cache line?
  bool locked_;
  LockDesc desc_;
};

}

#endif
