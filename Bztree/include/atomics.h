// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <semaphore.h>

#include <atomic>
#include "macros.h"

namespace pmwcas {
const static uintptr_t kAtomicAlignment = 4;

// The minimum alignment to guarantee atomic operations on a platform for both
// value types and pointer types
// The maximum size to guarantee atomic operations with the primitives below
#if defined (_M_I386)
#define AtomicAlignment  4
#define PointerAlignment 4
#define AtomicMaxSize    4
#else
#define AtomicAlignment  4
#define PointerAlignment 8
#define AtomicMaxSize    8
#endif

// Identical for WIN32 and Linux
template <typename T> T LdImm(T const* const source) {
  static_assert(sizeof(T) <= AtomicMaxSize,
      "Type must be aligned to native pointer alignment.");
  DCHECK((((uintptr_t)source) & (kAtomicAlignment - 1)) == 0);

  // The volatile qualifier has the side effect of being a load-aquire on IA64.
  // That's a result of overloading the volatile keyword.
  return *((volatile T*)source);
}

/// A load with acquire semantics for a type T
template <typename T> inline T LdAq(T const* const source) {
  static_assert(sizeof(T) <= AtomicMaxSize,
      "Type must be aligned to native pointer alignment.");
  assert((((uintptr_t)source) & (AtomicAlignment - 1)) == 0);
  _mm_lfence();
  return *((volatile T*)source);
}

template <typename T> void StRel(T* destination, T value) {
  static_assert(sizeof(T) <= AtomicMaxSize,
      "Type must be aligned to native pointer alignment.");
  DCHECK((((uintptr_t)destination) & (AtomicAlignment - 1)) == 0);

  // COMPILER-WISE:
  // A volatile write is not compiler reorderable w/r to writes
  // PROCESSOR-WISE:
  // X86 and X64 do not reorder stores. IA64 volatile emits st.rel,
  //  which ensures all stores complete before this one
  *((volatile T*)destination) = value;
}


template <typename T> T CompareExchange64(T* destination, T new_value,
    T comparand) {
  static_assert(sizeof(T) == 8,
      "CompareExchange64 only works on 64 bit values");
  ::__atomic_compare_exchange_n(destination, &comparand, new_value, false,
                                       __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  return comparand;
}

template <typename T>
T* CompareExchange64Ptr(T** destination, T* new_value, T* comparand) {
  ::__atomic_compare_exchange_n(destination, &comparand, new_value, false,
                                       __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  return comparand;
}

template <typename T> T CompareExchange32(T* destination, T new_value,
    T comparand) {
  static_assert(sizeof(T) == 4,
      "CompareExchange32 only works on 32 bit values");
  ::__atomic_compare_exchange_n(destination, &comparand, new_value, false,
                                       __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  return comparand;
}

template <typename T> T FetchAdd64(T* destination, T add_value) {
  static_assert(sizeof(T) <= AtomicMaxSize,
      "Type must be aligned to native pointer alignment.");
  return ::__atomic_fetch_add(destination, add_value, __ATOMIC_SEQ_CST);
}

template <typename T> T Decrement64(T* destination) {
  static_assert(sizeof(T) <= AtomicMaxSize,
      "Type must be aligned to native pointer alignment.");
  return ::__atomic_sub_fetch(destination, 1, __ATOMIC_SEQ_CST);
}

template <typename T> T Decrement32(T* destination) {
  static_assert(sizeof(T) <= AtomicMaxSize,
      "Type must be aligned to native pointer alignment.");
  return ::__atomic_sub_fetch(destination, 1);
}

class Barrier {
 public:
  Barrier(uint64_t thread_count)
    : wait_count_{ thread_count } {
  }

  ~Barrier() {}

  void CountAndWait() {
    uint64_t c = --wait_count_;
    while(wait_count_ != 0) {}
  }

 private:
  std::atomic<uint64_t> wait_count_;
};

} // namespace pmwcas
