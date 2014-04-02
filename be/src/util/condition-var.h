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


#ifndef IMPALA_UTIL_CONDITION_VAR_H
#define IMPALA_UTIL_CONDITION_VAR_H

#include <pthread.h>
#include "util/locks.h"

namespace impala {

// Wrapper class around pthread condition variables.
// This is based on the boost condition variable class but modified to work with
// Lock.
class ConditionVariable {
 public:
  ConditionVariable() {
    int res = pthread_mutex_init(&mutex_, NULL);
    DCHECK_EQ(res, 0);
    res = pthread_cond_init(&cond_, NULL);
    DCHECK_EQ(res, 0);
  }

  ~ConditionVariable() {
    int res = pthread_mutex_destroy(&mutex_);
    DCHECK_EQ(res, 0);
    res = pthread_cond_destroy(&cond_);
    DCHECK_EQ(res, 0);
  }

  // Waits until this condition variable is notified. lock must be taken
  // before calling this and is held when this function returns.
  template <typename T>
  void Wait(boost::unique_lock<T>* lock) {
    pthread_mutex_lock(&mutex_);
    lock->unlock();
    int res = pthread_cond_wait(&cond_, &mutex_);
    DCHECK_EQ(res, 0) << "Condition variable has likely been deleted";
    pthread_mutex_unlock(&mutex_);
    lock->lock();
  }

  // Notifies one thread in Wait().
  void NotifyOne() {
    pthread_mutex_lock(&mutex_);
    int res = pthread_cond_signal(&cond_);
    DCHECK_EQ(res, 0) << "Condition variable has likely been deleted";
    pthread_mutex_unlock(&mutex_);
  }

  // Notifies all threads in Wait().
  void NotifyAll() {
    pthread_mutex_lock(&mutex_);
    int res = pthread_cond_broadcast(&cond_);
    DCHECK_EQ(res, 0) << "Condition variable has likely been deleted";
    pthread_mutex_unlock(&mutex_);
  }

 private:
  // pthread mutex object that is used with cond_. This is internal
  // and not the mutex the caller passes in.
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;
};

}

#endif
