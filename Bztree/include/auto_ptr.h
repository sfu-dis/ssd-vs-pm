// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <functional>
#include <memory>
#include "allocator_internal.h"

namespace pmwcas {

template<typename T>
using unique_ptr_t = std::unique_ptr<T, std::function<void(T*)>>;

template <typename T>
unique_ptr_t<T> make_unique_ptr_t(T* p) {
  return unique_ptr_t<T> (p,
  [](T* t) {
    t->~T();
    free(t);
  });
}

template <typename T>
unique_ptr_t<T> make_unique_ptr_aligned_t(T* p) {
  return unique_ptr_t<T> (p,
  [](T* t) {
    t->~T();
    free(t);
  });
}

/// Allocate memory without concern for alignment.
template <typename T>
unique_ptr_t<T> alloc_unique(size_t size) {
  T *ptr = nullptr;
  Allocator::Get()->Allocate((void **)&ptr, size);
  return make_unique_ptr_t<T>(ptr);
}

/// Allocate memory, aligned at the specified alignment.
template <typename T>
unique_ptr_t<T> alloc_unique_aligned(size_t size, size_t alignment) {
  T* ptr=nullptr;
  Allocator::Get()->AllocateAligned((void **)&ptr, size, alignment);
  return make_unique_ptr_aligned_t<T>(ptr);
}

template<typename T>
using shared_ptr_t = std::shared_ptr<T>;

}
