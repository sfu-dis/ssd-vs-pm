// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
//
// Implements variants of the multi-word compare-and-swap (MwCAS) primitive
// that can work for volatile DRAM and persistent memory. The operation is
// lock-free and non-blocking. It requires flag bits on each word. Currently
// Intel and AMD implements 48 out of the 64 bits for addresses, so these
// bits reside in the most significant 16 bits.
//
// The basic MwCAS algorithm is based on ideas in:
//    Harris, Timothy L., Fraser, Keir, Pratt, Ian A.,
//    A Practical Multi-word Compare-and-Swap Operation", DISC 2002, 265-279
//
// It requires using a conditional CAS (RDCSS) to install MwCAS descriptors,
// RDCSS itself in turn needs descriptors. This requires two bits in the MSBs.
//
// The persistence support employes an extra bit to indicate if the word is
// possbily dirty (not reflected in NVRAM yet).
//
// |--63---|----62---|---61--|--rest bits--|
// |-MwCAS-|-CondCAS-|-Dirty-|-------------|
//
// Any application that uses this MwCAS primitive thus cannot use the 3 MSBs
// (e.g., the deletion marker of a linked list has to be at bit 60 or lower).
//
// Interactions with user data memory to prevent persistent memory leaks:
// Assume a persistent allocator is already in place, and it provides the
// following semantics:
// 1. Allocation is done through a posix_memalign-like interface which accepts
//    a reference to the location which stores the address of the allocated
//    memory. This provided 'reference' must be part of the allocation's
//    persistent data structure, or any place that the application can read
//    during recovery.
// 2. Upon recovery, the allocator will examine (by actively remembering all
//    memory it allocated through e.g., a hashtab or upon request from the
//    application) each reference provided. If it contains null, then the memory
//    should be freed internally (i.e., ownership didn't transfer to
//    application); otherwise the application should decide what to do with the
//    memory. Ownership is transferred.
//
// With the above interface/guarantees, the application can pass a reference of
// the 'new value' fields in each word to the allocator. The allocator will
// examine these places upon recovery. In general, PMwCAS's recovery routine
// will deallocate the 'old values' for successful mwcas ops, and deallocate the
// 'new values' for failed mwcas ops. See kRecycle* for all possible policies.
// After freeing memory, the recovery routine resets the descriptor fields to
// null.
//
// Note: this requires the allocator to do double-free detection in case there
// is repeated failure.
//
// (If an persistent allocator can provide a drop-in replacement to malloc, then
// it's up to the application to decide what to do.)
//
// During forward processing, the application can choose to piggy back on
// PMwCAS's epoch manager for pointer stability. Depending on whether the mwcas
// succeeded, the descriptor free routine will deallocate memory addresses
// stored in 'old value' or 'new value'. This means the application also need
// not handle pointer stability itself, the MwCAS's epoch manager does it
// transparently.
//
// The application can specify the callbacks for allocating and deallocating
// memory when allocating MwCAS descriptors. Each word can be specified to
// whether use mwcas's mechanism for memory deallocation.
#pragma once

#ifndef DESC_CAP
#define DESC_CAP 4
#warning "DESC_CAP not defined - setting to 4"
#endif

#include <assert.h>
#include <stdio.h>

#include <cstdint>
#include <mutex>

#include "environment.h"
#include "metrics.h"
#include "allocator_internal.h"
#include "environment_internal.h"
#include "epoch.h"
#include "garbage_list_unsafe.h"
#include "garbage_list_unsafe_persistent.h"
#include "nv_ptr.h"
#include "nvram.h"

namespace pmwcas {

// Forward references
struct DescriptorPartition;
class Descriptor;
class DescriptorPool;

#define PMWCAS_THREAD_HELP 1
#define PMWCAS_SAFE_MEMORY 1

class FreeCallbackArray {
 public:
  /// FreeCallbacks are invoked on a pointer that *points* to another pointer
  /// that points to the memory region to be freed (thus the double pointer).
  /// We expect that they clear the void* pointer (i.e. set it to nullptr) to
  /// denote a succeeded reclamation.

  /// A FreeCallbackArray is an array of FreeCallback function pointers. It is
  /// never persisted (i.e. it resides in volatile memory), but it should have
  /// the same layout after restarting the process so the persisted offset in
  /// each descriptor can reliably refer to the same callback function even if
  /// it is loaded into a different virtual address.

  /// Signaure for garbage free callback
  using Type = uint64_t;
  using FreeCallback = void (*)(Type* mem);
  using Idx = size_t;

  /// Maximum number of FreeCallbacks that can be registered
  static constexpr Idx kFreeCallbackCapacity = 16;

  /// The default free callback used if no callback is specified by the user
  static void DefaultFreeCallback(Type* mem) {
#ifdef PMEM
#ifdef PMDK
    auto allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
    allocator->FreeOffset(mem);
#else
    static_assert(false, "not implemented");
#endif
#else
    Allocator::Get()->Free((void**)mem);
#endif
  }

  FreeCallbackArray() { RegisterFreeCallback(DefaultFreeCallback); }

  ~FreeCallbackArray() = default;

  FreeCallbackArray(const FreeCallbackArray&) = delete;
  FreeCallbackArray& operator=(const FreeCallbackArray&) = delete;

  /// Register a FreeCallback in the array
  Idx RegisterFreeCallback(FreeCallback fc) {
    RAW_CHECK(next_ < kFreeCallbackCapacity, "too many free callbacks");
    RAW_CHECK(fc != nullptr, "free callbacks cannot be nullptr");
    array_[next_] = fc;
    return next_++;
  }

  /// Look up a FreeCallback in the array
  FreeCallback GetFreeCallback(Idx index) const {
    RAW_CHECK(index < next_, "invalid free callback");
    return array_[index];
  }

 private:
  FreeCallback array_[kFreeCallbackCapacity];
  size_t next_{0};
};

class alignas(kCacheLineSize) Descriptor {
  template <typename T>
  friend class MwcTargetField;

 public:
  /// Signifies a dirty word requiring cache line write back
  static const uint64_t kDirtyFlag = (uint64_t)1 << 61;

  /// Flag signifying a conditional CAS is underway for the target word.
  static const uint64_t kCondCASFlag = (uint64_t)1 << 62;

  /// Flag signifying an multi-word CAS is underway for the target word.
  static const uint64_t kMwCASFlag = (uint64_t)1 << 63;

  /// Garbage list recycle policy: only free [new_value] upon restart
  static const uint32_t kRecycleOnRecovery = 0x1;

  /// Garbage list recycle policy: leave the memory alone
  static const uint32_t kRecycleNever = 0x2;

  /// Garbage list recycle policy: free [old/new_value] if succeeded/failed
  static const uint32_t kRecycleAlways = 0x3;

  /// Garbage list recycle policy: free only [old value] if succeeded
  static const uint32_t kRecycleOldOnSuccess = 0x4;

  /// Garbage list recycle policy: free only [new value] if succeeded
  static const uint32_t kRecycleNewOnFailure = 0x5;

  /// Recycle and installation policy: neither install nor recycle
  /// only used for allocation purpose
  static constexpr nv_ptr<uint64_t> kAllocNullAddress = nullptr;

  /// Value signifying an internal reserved value for a new entry
  static const uint64_t kNewValueReserved = 0ull;

  /// Returns whether the value given is an MwCAS descriptor or not.
  inline static bool IsMwCASDescriptorPtr(uint64_t value) {
    return value & kMwCASFlag;
  }

  /// Returns whether the value given is a CondCAS descriptor or not.
  inline static bool IsCondCASDescriptorPtr(uint64_t value) {
    return value & kCondCASFlag;
  }

  /// Returns whether the underlying word is dirty (not surely persisted).
  inline static bool IsDirtyPtr(uint64_t value) { return value & kDirtyFlag; }

  /// Returns true if the target word has no pmwcas management flags set.
  inline static bool IsCleanPtr(uint64_t value) {
    return (value & (kCondCASFlag | kMwCASFlag | kDirtyFlag)) == 0;
  }

  /// Clear the descriptor flag for the provided /a ptr
  static inline uint64_t CleanPtr(uint64_t ptr) {
    return ptr & ~(kMwCASFlag | kCondCASFlag | kDirtyFlag);
  }

  /// Specifies what word to update in the mwcas, storing before/after images so
  /// others may help along. This also servers as the descriptor for conditional
  /// CAS(RDCSS in the Harris paper). status_address_ points to the parent
  /// Descriptor's status_ field which determines whether a CAS that wishes to
  /// make address_ point to WordDescriptor can happen.
  struct WordDescriptor {
    static const uint64_t kRecycleFlag = (uint64_t)1 << 63;

    /// The target address
    nv_ptr<uint64_t> address_;

    /// The original old value stored at /a Address
    uint64_t old_value_;

    /// The new value to be stored at /a Address
    uint64_t new_value_;

    /// The parent Descriptor's status
    nv_ptr<uint32_t> status_address_;

    /// Returns the parent descriptor for this particular word
    inline nv_ptr<Descriptor> GetDescriptor() {
      return nv_ptr<Descriptor>((uint64_t)status_address_ -
                                offsetof(Descriptor, status_));
    }

    inline uint64_t GetOldValue() const {
#if PMWCAS_SAFE_MEMORY == 1
      return old_value_ & ~kRecycleFlag;
#else
      DCHECK((old_value_ & kRecycleFlag) == 0);
      return old_value_;
#endif
    }

    inline uint64_t GetNewValue() const {
#if PMWCAS_SAFE_MEMORY == 1
      return new_value_ & ~kRecycleFlag;
#else
      DCHECK((new_value_ & kRecycleFlag) == 0);
      return new_value_;
#endif
    }

    inline uint64_t* GetOldValuePtr() { return &old_value_; }

    inline uint64_t* GetNewValuePtr() { return &new_value_; }

#if PMWCAS_SAFE_MEMORY == 1
    inline static uint64_t SetRecycleFlag(uint64_t value) {
      return value | kRecycleFlag;
    }

    inline bool ShouldRecycleOldValue() { return old_value_ & kRecycleFlag; }

    inline bool ShouldRecycleNewValue() { return new_value_ & kRecycleFlag; }
#endif

#ifdef PMEM
    /// Persist the data pointed to by address_
    inline void PersistAddress() {
      uint64_t* addr = address_;
      NVRAM::Flush(sizeof(uint64_t), addr);
    }
#endif
  };
  static_assert(sizeof(WordDescriptor) == 32,
                "WordDescriptor must occupy half a cacheline");

  /// Default constructor
  Descriptor() = delete;

  /// Function for initializing a Descriptor.
  /// Called only during system initialization/recovery.
  Descriptor(DescriptorPartition* partition, FreeCallbackArray* callbacks);

  /// Function for reinitializing a finalized Descriptor.
  /// Called only on newly allocated Descriptors at runtime.
  void Initialize();

  /// Function for finalizing a concluded Descriptor.
  void Finalize();

  /// Executes the multi-word compare and swap operation.
  bool MwCAS() {
    RAW_CHECK(status_ == kStatusUndecided,
              "status of descriptor is not kStatusUndecided");
#ifdef PMEM
    return PersistentMwCAS(0);
#else
    return VolatileMwCAS(0);
#endif
  }

  /// Retrieves the new value for the given word index in the PMwCAS
  inline uint64_t GetNewValue(uint32_t index) {
    return words_[index].GetNewValue();
  }

  /// Retrieves the pointer to the new value slot for a given word in the PMwCAS
  inline uint64_t* GetNewValuePtr(uint32_t index) {
    return &words_[index].new_value_;
  }

  /// Adds information about a new word to be modifiec by the MwCAS operator.
  /// Word descriptors are stored sorted on the word address to prevent
  /// livelocks. Return value is negative if the descriptor is full.
  int32_t AddEntry(nv_ptr<uint64_t> addr, uint64_t oldval, uint64_t newval,
                   uint32_t recycle_policy = kRecycleNever);

  /// Reserve a slot in the words array, but don't know what the new value is
  /// yet. The application should use GetNewValue[Ptr] to fill in later.
  inline uint32_t ReserveAndAddEntry(nv_ptr<uint64_t> addr, uint64_t oldval,
                                     uint32_t recycle_policy) {
    RAW_CHECK(recycle_policy == kRecycleAlways ||
                  recycle_policy == kRecycleNewOnFailure,
              "wrong policy specified for ReserveAndAddEntry()");
    return AddEntry(addr, oldval, kNewValueReserved, recycle_policy);
  }

  /// Abort the MwCAS operation, can be used only before the operation starts.
  Status Abort();

 private:
  /// Allow tests to access privates for failure injection purposes.
  FRIEND_TEST(PMwCASTest, SingleThreadedRecovery);

  friend class DescriptorPool;

  /// Internal helper function to conduct a double-compare, single-swap
  /// operation on an target field depending on the value of the status_ field
  /// in Descriptor. The conditional CAS tries to install a pointer to the MwCAS
  /// descriptor derived from one of words_, expecting the status_ field
  /// indicates Undecided.
  inline uint64_t CondCAS(uint32_t word_index, WordDescriptor desc[],
                          uint64_t dirty_flag = 0);

  /// Internal helper function to finish an RDCSS operation.
  static void CompleteCondCAS(WordDescriptor* wd) {
#ifdef PMEM
    return PersistentCompleteCondCAS(wd);
#else
    return VolatileCompleteCondCAS(wd);
#endif
  }
#ifndef PMEM
#endif

#ifdef RTM
  bool RTMInstallDescriptors(WordDescriptor all_desc[],
                             uint64_t dirty_flag = 0);
#endif

  /// Retrieve the index position in the descriptor of the given address.
  int32_t GetInsertPosition(nv_ptr<uint64_t> addr);

#ifndef PMEM
  /// Complete the RDCSS operation.
  static void VolatileCompleteCondCAS(WordDescriptor* wd);

  /// Execute the multi-word compare and swap operation.
  bool VolatileMwCAS(uint32_t calldepth = 0);
#endif

#ifdef PMEM
  /// Complete the RDCSS operation on persistent memory.
  static void PersistentCompleteCondCAS(WordDescriptor* wd);

  /// Execute the multi-word compare and swap operation on persistent memory.
  bool PersistentMwCAS(uint32_t calldepth = 0);

  /// Flush only the Status field to persistent memory.
  inline void PersistStatus() { NVRAM::Flush(sizeof(status_), &status_); }

  // Read and persist the status field (if its dirty bit is set).
  // The caller must ensure that the descriptor is already persistent.
  // The returned value is guaranteed to be persistent in PM.
  uint32_t ReadPersistStatus();
#endif

  /// Bitwise-or the given flags to the given value
  inline static uint64_t SetFlags(uint64_t value, uint64_t flags) {
    RAW_CHECK((flags & ~(kMwCASFlag | kCondCASFlag | kDirtyFlag)) == 0,
              "invalid flags");
    return value | flags;
  }

  /// Mask to indicate the status field is dirty, any reader should first flush
  /// it before use.
  static const uint32_t kStatusDirtyFlag = 1ULL << 31;

  /// Cleanup steps of MWCAS common to both persistent and volatile versions.
  bool Cleanup();

#if PMWCAS_SAFE_MEMORY == 1
  /// Deallocate the memory associated with the MwCAS if needed.
  void DeallocateMemory();
#endif

  /// Places a descriptor back on the descriptor free pool (partitioned). This
  /// can be used as the callback function for the epoch manager/garbage list to
  /// reclaim this descriptor for reuse after we are sure no one is using or
  /// could possibly access this descriptor.
  static void FreeDescriptor(void* context, void* desc);

  /// Descriptor states. Valid transitions are as follows:
  /// kStatusUndecided->kStatusSucceeded->kStatusFinished->kStatusUndecided
  ///               \-->kStatusFailed-->kStatusFinished->kStatusUndecided
  static const uint32_t kStatusFinished = 0U;
  static const uint32_t kStatusSucceeded = 1U;
  static const uint32_t kStatusFailed = 2U;
  static const uint32_t kStatusUndecided = 3U;

  inline void assert_valid_status() {
    auto s = status_ & ~kStatusDirtyFlag;
    RAW_CHECK(s == kStatusFinished || s == kStatusFailed ||
                  s == kStatusSucceeded || s == kStatusUndecided,
              "invalid status");
  }

  /// Tracks the current status of the descriptor.
  uint32_t status_;

  /// Count of actual descriptors held in #WordDesc
  uint32_t count_;

  /// Free list pointer for managing free pre-allocated descriptor pools
  Descriptor* next_ptr_;

  /// Back pointer to owning partition so the descriptor can be returned to its
  /// home partition when it is freed.
  DescriptorPartition* owner_partition_;

  /// A callback for freeing the words listed in [words_] when recycling the
  /// descriptor. Optional: only for applications that use it.
  FreeCallbackArray::Idx callback_idx_;

  /// A reference to the array of callbacks. It should only be dereferenced at
  /// runtime and should never be dereferenced directly upon recovery; instead
  /// the recovery protocol will fix it to point to the new array.
  FreeCallbackArray* free_callbacks_;

  /// Array of word descriptors bounded DESC_CAP
  alignas(kCacheLineSize) WordDescriptor words_[DESC_CAP];

#ifdef PMEM
  /// Array of (sorted) offsets into words_ to avoid in-place sorting
  alignas(kCacheLineSize) uint8_t indexes_[DESC_CAP];
#endif
};

class DescriptorGuard {
 public:
  explicit DescriptorGuard(Descriptor* desc) : desc_{desc}, finished_{false} {}

  ~DescriptorGuard() {
    if (!finished_) {
      desc_->Abort();
    }
  }

  DescriptorGuard(const DescriptorGuard& guard) = delete;
  DescriptorGuard& operator=(const DescriptorGuard& guard) = delete;

  /// Getter, but you should not use it unless you know the consequences
  Descriptor* GetRaw() { return desc_; }

  /// Retrieves the new value for the given word index in the PMwCAS
  inline uint64_t GetNewValue(uint32_t index) {
    return desc_->GetNewValue(index);
  }

  /// Retrieves the pointer to the new value slot for a given word in the PMwCAS
  inline uint64_t* GetNewValuePtr(uint32_t index) {
    return desc_->GetNewValuePtr(index);
  }

  /// Adds information about a new word to be modifiec by the MwCAS operator.
  /// Word descriptors are stored sorted on the word address to prevent
  /// livelocks. Return value is negative if the descriptor is full.
  int32_t AddEntry(nv_ptr<uint64_t> addr, uint64_t oldval, uint64_t newval,
                   uint32_t recycle_policy = Descriptor::kRecycleNever) {
    return desc_->AddEntry(addr, oldval, newval, recycle_policy);
  }

  /// Reserve a slot in the words array, but don't know what the new value is
  /// yet. The application should use GetNewValue[Ptr] to fill in later.
  inline uint32_t ReserveAndAddEntry(nv_ptr<uint64_t> addr, uint64_t oldval,
                                     uint32_t recycle_policy) {
    return desc_->ReserveAndAddEntry(addr, oldval, recycle_policy);
  }

  /// Executes the multi-word compare and swap operation.
  bool MwCAS() {
    finished_ = true;
    return desc_->MwCAS();
  }

  /// Abort the MwCAS operation, can be used only before the operation starts.
  Status Abort() {
    finished_ = true;
    return desc_->Abort();
  }

 private:
  /// The descriptor behind it
  Descriptor* desc_;

  bool finished_;
};

/// A partitioned pool of Descriptors used for fast allocation of descriptors.
/// The pool of descriptors will be bounded by the number of threads actively
/// performing an mwcas operation.
struct alignas(kCacheLineSize) DescriptorPartition {
  DescriptorPartition() = delete;
  DescriptorPartition(EpochManager* epoch, DescriptorPool* pool);

  ~DescriptorPartition();

  /// Pointer to the free list head (currently managed by lock-free list)
  Descriptor* free_list;

  /// Back pointer to the owner pool
  DescriptorPool* desc_pool;

  /// Garbage list holding freed pointers/words waiting to clear epoch
  /// protection before being truly recycled.
  GarbageListUnsafe* garbage_list;

  /// Number of allocated descriptors
  uint32_t allocated_desc;
};

class DescriptorPool {
 private:
  /// Total number of descriptors in the pool
  uint32_t pool_size_;

  /// Number of descriptors per partition
  uint32_t desc_per_partition_;

  /// Points to all descriptors
  nv_ptr<Descriptor> descriptors_;

  /// Number of partitions in the partition_table_
  uint32_t partition_count_;

  /// Descriptor partitions (per thread)
  DescriptorPartition* partition_table_;

  /// The next partition to assign (round-robin) for a new thread joining the
  /// pmwcas library.
  std::atomic<uint32_t> next_partition_;

  /// Epoch manager controling garbage/access to descriptors.
  EpochManager epoch_;

  /// Array of callbacks to be invoked during memory deallocation
  std::unique_ptr<FreeCallbackArray> free_callbacks_;

  void InitDescriptors();

 public:
  DescriptorPool(uint32_t pool_size, uint32_t partition_count,
                 bool enable_stats = false);

  Descriptor* GetDescriptor() { return descriptors_; }

#ifdef PMEM
  inline void ClearFreeCallbackArray() {
    // Initialize free callback array before scanning desc pool
    // Release first: whatever was there is not interpretable
    free_callbacks_.release();
    free_callbacks_ = std::make_unique<FreeCallbackArray>();
  }

  /// Run recovery protocol on the descriptor pool.
  /// If the provided partition_count is zero, keep partition_count_ in
  /// the pool as is. Otherwise, repartition the pool (useful when the
  /// number of worker threads changes).
  void Recovery(uint32_t partition_count, bool enable_stats,
                bool clear_free_callbacks = true);
#endif

  ~DescriptorPool();

  inline uint32_t GetDescPerPartition() { return desc_per_partition_; }

  /// Returns a pointer to the epoch manager associated with this pool.
  /// MwcTargetField::GetValue() needs it.
  EpochManager* GetEpoch() { return &epoch_; }

  // Get a free descriptor from the pool.
  DescriptorGuard AllocateDescriptor(FreeCallbackArray::Idx fc);

  // Allocate a free descriptor from the pool using default allocate and
  // free callbacks.
  inline DescriptorGuard AllocateDescriptor() { return AllocateDescriptor(0); }

  // Register a FreeCallback in the array
  size_t RegisterFreeCallback(FreeCallbackArray::FreeCallback fc) {
    return free_callbacks_->RegisterFreeCallback(fc);
  }
};

/// Represents an 8-byte word that is a target for a compare-and-swap. Used to
/// abstract away and hide all the low-level bit manipulation to track internal
/// status of the word. By default use the 2 LSBs as flags, assuming the values
/// point to word-aligned addresses.
template <class T>
class MwcTargetField {
  static_assert(sizeof(T) == 8, "MwCTargetField type is not of size 8 bytes");

 public:
  static const uint64_t kMwCASFlag = Descriptor::kMwCASFlag;
  static const uint64_t kCondCASFlag = Descriptor::kCondCASFlag;
  static const uint64_t kDirtyFlag = Descriptor::kDirtyFlag;

  MwcTargetField(void* desc = nullptr) { value_ = T(desc); }

  /// Enter epoch protection and then return the value.
  inline T GetValue(EpochManager* epoch) {
#ifdef PMEM
    return GetValuePersistent(epoch);
#else
    return GetValueVolatile(epoch);
#endif
  }

  /// Get value assuming epoch protection.
  inline T GetValueProtected() {
#ifdef PMEM
    return GetValueProtectedPersistent();
#else
    return GetValueProtectedVolatile();
#endif
  }

  /// Returns true if the given word does not have any internal management
  /// flags set, false otherwise.
  static inline bool IsCleanPtr(uint64_t ptr) {
    return (ptr & (kCondCASFlag | kMwCASFlag | kDirtyFlag)) == 0;
  }

  /// Returns true if the value does not have any internal management flags set,
  /// false otherwise.
  inline bool IsCleanPtr() {
    return (value_ & (kCondCASFlag | kMwCASFlag | kDirtyFlag)) == 0;
  }

#ifdef PMEM
  /// Persist the value_ to be read
  inline void PersistValue() {
    NVRAM::Flush(sizeof(uint64_t), (const void*)&value_);
  }
#endif

  /// Return an integer representation of the target word
  operator uint64_t() { return uint64_t(value_); }

  /// Copy operator
  MwcTargetField<T>& operator=(MwcTargetField<T>& rhval) {
    value_ = rhval.value_;
    return *this;
  }

  /// Address-of operator
  T* operator&() { return const_cast<T*>(&value_); }

  /// Assignment operator
  MwcTargetField<T>& operator=(T rhval) {
    value_ = rhval;
    return *this;
  }

  /// Content-of operator
  T& operator*() { return *value_; }

  /// Dereference operator
  T* operator->() { return value_; }

 private:
#ifndef PMEM
  /// Return the value in this word. If the value is a descriptor there is a CAS
  /// in progress, so help along completing the CAS before returning the value
  /// in the target word.
  inline T GetValueVolatile(EpochManager* epoch) {
    EpochGuard guard(epoch, !epoch->IsProtected());
    return GetValueProtectedVolatile();
  }

  /// Same as GetValue, but guaranteed to be Protect()'ed already.
  inline T GetValueProtectedVolatile() {
    MwCASMetrics::AddRead();

  retry:
    uint64_t val = (uint64_t)value_;

    if (val & kCondCASFlag) {
#if PMWCAS_THREAD_HELP == 1
      Descriptor::WordDescriptor* wd =
          (Descriptor::WordDescriptor*)Descriptor::CleanPtr(val);
      Descriptor::VolatileCompleteCondCAS(wd);
#endif
      goto retry;
    }

    if (val & kMwCASFlag) {
#if PMWCAS_THREAD_HELP == 1
      // While the address contains a descriptor, help along completing the CAS
      Descriptor* desc = (Descriptor*)Descriptor::CleanPtr(val);
      RAW_CHECK(desc, "invalid descriptor pointer");
      desc->VolatileMwCAS(1);
#endif
      goto retry;
    }
    RAW_CHECK(IsCleanPtr(val), "flags set on return value");

    return val;
  }
#endif

#ifdef PMEM
  // The persistent variant of GetValue().
  inline T GetValuePersistent(EpochManager* epoch) {
    EpochGuard guard(epoch, !epoch->IsProtected());
    return GetValueProtectedPersistent();
  }

  // The "protected" variant of GetPersistValue().
  inline T GetValueProtectedPersistent() {
    MwCASMetrics::AddRead();

  retry:
    uint64_t val = (uint64_t)value_;

    if (val & kCondCASFlag) {
#if PMWCAS_THREAD_HELP == 1
      RAW_CHECK((val & kDirtyFlag) == 0,
                "dirty flag set on CondCAS descriptor");

      Descriptor::WordDescriptor* wd =
          nv_ptr<Descriptor::WordDescriptor>(Descriptor::CleanPtr(val));
      Descriptor::PersistentCompleteCondCAS(wd);
#endif
      goto retry;
    }

    if (val & kDirtyFlag) {
#if PMWCAS_THREAD_HELP == 1
      PersistValue();
      CompareExchange64((uint64_t*)&value_, val & ~kDirtyFlag, val);
#endif
      goto retry;
    }
    RAW_CHECK((val & kDirtyFlag) == 0, "dirty flag set on return value");

    if (val & kMwCASFlag) {
#if PMWCAS_THREAD_HELP == 1
      // While the address contains a descriptor, help along completing the CAS
      Descriptor* desc = nv_ptr<Descriptor>(Descriptor::CleanPtr(val));
      RAW_CHECK(desc, "invalid descriptor pointer");
      desc->PersistentMwCAS(1);
#endif
      goto retry;
    }
    RAW_CHECK(IsCleanPtr(val), "flags set on return value");

    return val;
  }
#endif

  /// The 8-byte target word
  volatile T value_;
};

}  // namespace pmwcas
