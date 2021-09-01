#pragma once

#include "pmwcas.h"

#include <functional>
#include <type_traits>

namespace pmwcas {

#ifdef PMEM
template <typename T>
class nv_ptr {
  // An nv_ptr acts like a regular pointer, except that it only
  // keeps a 8-byte offset within a PM pool. The nv_ptr is swizzled
  // at runtime with a call to the PM allocator.
  using element_type = T;
  using pointer = std::add_pointer_t<T>;
  using reference = std::add_lvalue_reference_t<T>;
  using offset_type = uint64_t;

  offset_type offset_;

 public:
  constexpr nv_ptr() noexcept : offset_(0ull) {}
  constexpr nv_ptr(std::nullptr_t) noexcept : offset_(0ull) {}
  constexpr nv_ptr(offset_type offset) noexcept : offset_(offset) {}
  constexpr nv_ptr(pointer ptr) noexcept : offset_(fromPointer(ptr)) {}

  template <typename W, typename = std::enable_if_t<std::is_convertible_v<
                            std::add_pointer_t<W>, pointer>>>
  constexpr nv_ptr(nv_ptr<W> other) noexcept : offset_(other.get()) {}

  constexpr pointer get() const noexcept { return toPointer(offset_); }

  constexpr reference operator*() const { return *toPointer(offset_); }

  constexpr pointer operator->() const noexcept { return toPointer(offset_); }

  constexpr explicit operator bool() const noexcept { return offset_ != 0ull; }

  constexpr explicit operator offset_type() const noexcept { return offset_; }

  constexpr operator pointer() const noexcept { return toPointer(offset_); }

 private:
  static offset_type fromPointer(pointer ptr) {
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    return allocator->GetOffset<element_type>(ptr);
  }

  static pointer toPointer(offset_type offset) {
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    return allocator->GetDirect<element_type>(offset);
  }

  template <typename U>
  friend nv_ptr<U> CompareExchange64(nv_ptr<U> *destination,
                                     nv_ptr<U> new_value, nv_ptr<U> comparand);
};

template <typename T1, typename T2>
bool operator==(nv_ptr<T1> a, nv_ptr<T2> b) {
  return a.get() == b.get();
}

template <typename T1, typename T2>
bool operator!=(nv_ptr<T1> a, nv_ptr<T2> b) {
  return !(a == b);
}

template <typename T>
bool operator==(nv_ptr<T> p, std::nullptr_t) {
  return !p;
}

template <typename T>
bool operator==(std::nullptr_t, nv_ptr<T> p) {
  return !p;
}

template <typename T>
bool operator!=(nv_ptr<T> p, std::nullptr_t) {
  return (bool)p;
}

template <typename T>
bool operator!=(std::nullptr_t, nv_ptr<T> p) {
  return (bool)p;
}

template <typename T1, typename T2>
bool operator<(nv_ptr<T1> a, nv_ptr<T2> b) {
  return std::less<
      std::common_type_t<std::add_pointer_t<T1>, std::add_pointer_t<T2>>>()(
      a.get(), b.get());
}

template <typename T1, typename T2>
bool operator>(nv_ptr<T1> a, nv_ptr<T2> b) {
  return b < a;
}

template <typename T1, typename T2>
bool operator<=(nv_ptr<T1> a, nv_ptr<T2> b) {
  return !(a > b);
}

template <typename T1, typename T2>
bool operator>=(nv_ptr<T1> a, nv_ptr<T2> b) {
  return !(a < b);
}

template <typename T>
nv_ptr<T> CompareExchange64(nv_ptr<T> *destination, nv_ptr<T> new_value,
                            nv_ptr<T> comparand) {
  static_assert(sizeof(nv_ptr<T>) == 8,
                "CompareExchange64 only works on 64 bit values");
  ::__atomic_compare_exchange_n(&destination->offset_, &comparand.offset_,
                                new_value.offset_, false, __ATOMIC_SEQ_CST,
                                __ATOMIC_SEQ_CST);
  return comparand;
}

#else
template <typename T>
using nv_ptr = T *;

#endif
}  // namespace pmwcas
#ifdef PMEM
namespace std {
template <typename T>
struct hash<pmwcas::nv_ptr<T>> {
  size_t operator()(const pmwcas::nv_ptr<T> &ptr) const noexcept {
    return hash<add_pointer_t<T>>()(ptr.get());
  }
};
}  // namespace std
#endif