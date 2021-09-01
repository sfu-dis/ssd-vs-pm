// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <mutex>
#include <atomic>
#include "allocator_internal.h"
#include "epoch.h"
#include "garbage_list.h"
#include "environment_linux.h"
#include "macros.h"
#include "atomics.h"
#include "nv_ptr.h"
#include "nvram.h"
#include "allocator.h"

namespace pmwcas {

#ifdef PMEM
/// XXX(shiges): This file is duplicated from garbage_list_unsafe.h. It
/// is intended to mimic a (MT-unsafe) garbage list residing on persistent
/// memory, but we didn't implement a full-fledged one here. Rather, this
/// implementation merely serves as an upper bound "guess" of the cost of
/// a real persistent garbage list.
///
/// Exactly the same as GarbageList, except that this one doesn't use any
/// synchronization mechanism for getting a slot etc. It is only intended
/// to be used as a thread-local list. The only user so far (20160811) is
/// the MwCAS library (for descriptor allocation/reuse).
class GarbageListUnsafePersistent : public IGarbageList {
 public:
  /// Holds a pointer to an object in the garbage list along with the Epoch
  /// in which it was removed and a chain field so that it can be linked into
  /// a queue.
  struct Item {
    /// Epoch in which the #m_removedItem was removed from the data
    /// structure. In practice, due to delay between the actual removal
    /// operation and the push onto the garbage list, #m_removalEpoch may
    /// be later than when the actual remove happened, but that is safe
    /// since the invariant is that the epoch stored here needs to be
    /// greater than or equal to the current global epoch in which the
    /// item was actually removed.
    Epoch removal_epoch;

    /// Function provided by user on Push() called when an object
    /// that was pushed to the list is safe for reclamation. When invoked the
    /// function is passed a pointer to an object that is safe to destroy and
    /// free along with #m_pbDestroyCallbackContext. The function must
    /// perform all needed destruction and release any resources associated
    /// with the object.
    DestroyCallback destroy_callback;

    /// Passed along with a pointer to the object to destroy to
    /// #m_destroyCallback; it threads state to destroyCallback calls so they
    /// can access, for example, the allocator from which the object was
    /// allocated.
    void* destroy_callback_context;

    /// Point to the object that is enqueued for destruction. Concurrent
    /// accesses may still be ongoing to the object, so absolutely no
    /// changes should be made to the value it refers to until
    /// #m_removalEpoch is deemed safe for reclamation by the
    /// EpochManager.
    void* removed_item;
  };
  static_assert(std::is_pod<Item>::value, "Item should be POD");

  /// Construct a GarbageList in an uninitialized state.
  GarbageListUnsafePersistent()
    : epoch_manager_{}
    , tail_{}
    , item_count_{}
    , items_{} {
  }

  /// Uninitialize the GarbageList (if still initialized) and destroy it.
  virtual ~GarbageListUnsafePersistent() {
    Uninitialize();
  }

  /// Initialize the GarbageList and associate it with an EpochManager.
  /// This must be called on a newly constructed instance before it
  /// is safe to call other methods. If the GarbageList is already
  /// initialized then it will have no effect.
  ///
  /// \param pEpochManager
  ///      EpochManager that is used to determine when it is safe to reclaim
  ///      items pushed onto the list. Must not be nullptr.
  /// \param nItems
  ///      Number of addresses that can be held aside for pointer stability.
  ///      If this number is too small the system runs the risk of deadlock.
  ///      Must be a power of two.
  ///
  /// \retval S_OK
  ///      The instance is now initialized and ready for use.
  /// \retval S_FALSE
  ///      The instance was already initialized; no effect.
  /// \retval E_INVALIDARG
  ///      \a nItems wasn't a power of two.
  virtual Status Initialize(EpochManager* epoch_manager,
      size_t item_count = 128 * 1024) {
    if(epoch_manager_) return Status::OK();

    if(!epoch_manager) return Status::InvalidArgument("Null pointer");

    if(!item_count || !IS_POWER_OF_TWO(item_count)) {
      return Status::InvalidArgument("items not a power of two");
    }

    size_t nItemArraySize = sizeof(*items_) * item_count;
    // XXX(shiges):

    auto pop = reinterpret_cast<PMDKAllocator*>(Allocator::Get())->GetPool();
    pmemobj_zalloc(pop, &oid_, nItemArraySize, TOID_TYPE_NUM(char));
    items_ = oid_.off;
    if (!items_)
      return Status::Corruption("Out of memory");

    for(size_t i = 0; i < item_count; ++i) new(&items_[i]) Item{};
    NVRAM::Flush(nItemArraySize, items_);

    item_count_ = item_count;
    tail_ = 0;
    epoch_manager_ = epoch_manager;

    return Status::OK();
  }

  /// Uninitialize the GarbageList and disassociate from its EpochManager;
  /// for each item still on the list call its destructor and free it.
  /// Careful: objects freed by this call will NOT obey the epoch protocol,
  /// so it is important that this thread is only called when it is clear
  /// that no other threads may still be concurrently accessing items
  /// on the list.
  ///
  /// \retval S_OK
  ///      The instance is now uninitialized; resources were released.
  /// \retval S_FALSE
  ///      The instance was already uninitialized; no effect.
  virtual Status Uninitialize() {
    if(!epoch_manager_) return Status::OK();

    for(size_t i = 0; i < item_count_; ++i) {
      Item& item = items_[i];
      if(item.removed_item) {
        item.destroy_callback(
          item.destroy_callback_context,
          item.removed_item);
        item.removed_item = nullptr;
        item.removal_epoch = 0;
      }
    }

    pmemobj_free(&oid_);

    items_ = nullptr;
    tail_ = 0;
    item_count_ = 0;
    epoch_manager_ = nullptr;

    return Status::OK();
  }

  /// Append an item to the reclamation queue; the item will be stamped
  /// with an epoch and will not be reclaimed until the EpochManager confirms
  /// that no threads can ever access the item again. Once an item is ready
  /// for removal the destruction callback passed to Initialize() will be
  /// called which must free all resources associated with the object
  /// INCLUDING the memory backing the object.
  ///
  /// \param removed_item
  ///      Item to place on the list; it will remain live until
  ///      the EpochManager indicates that no threads will ever access it
  ///      again, after which the destruction callback will be invoked on it.
  /// \param callback
  ///      Function to call when the object that was pushed to the list is safe
  ///      for reclamation. When invoked the, function is passed a pointer to
  ///      an object that is safe to destroy and free along with
  ///      \a pvDestroyCallbackContext. The function must perform
  ///      all needed destruction and release any resources associated with
  ///      the object. Must not be nullptr.
  /// \param context
  ///      Passed along with a pointer to the object to destroy to
  ///      \a destroyCallback; it threads state to destroyCallback calls so
  ///      they can access, for example, the allocator from which the object
  ///      was allocated. Left uninterpreted, so may be nullptr.
  virtual Status Push(void* removed_item, DestroyCallback callback,
      void* context) {
    Epoch removal_epoch = epoch_manager_->GetCurrentEpoch();
    const uint64_t invalid_epoch = ~0llu;

    for(;;) {
      // Consistent from my own point of view, no other observers
      int64_t slot = (tail_++) & (item_count_ - 1);

      // Everytime we work through 25% of the capacity of the list roll
      // the epoch over.
      if(((slot << 2) & (item_count_ - 1)) == 0)
        epoch_manager_->BumpCurrentEpoch();

      Item& item = items_[slot];

      Epoch priorItemEpoch = item.removal_epoch;
      RAW_CHECK(priorItemEpoch != invalid_epoch, "invalid priorItemEpoch");

      // No synchronization for a single thread; guaranteed to succeed
      item.removal_epoch = invalid_epoch;

      // Ensure it is safe to free the old entry.
      if(priorItemEpoch) {
        if(!epoch_manager_->IsSafeToReclaim(priorItemEpoch)) {
          // Uh-oh, we couldn't free the old entry. Things aren't looking
          // good, but maybe it was just the result of a race. Replace the
          // epoch number we mangled and try elsewhere.
          *((volatile Epoch*) &item.removal_epoch) = priorItemEpoch;
          continue;
        }
        item.destroy_callback(item.destroy_callback_context,
                              item.removed_item);
      }

      // Now populate the entry with the new item.
      item.destroy_callback = callback;
      item.destroy_callback_context = context;
      item.removed_item = removed_item;
      item.removal_epoch = removal_epoch;

      // Persist this change
      // XXX(shiges): This simple approach is not enough for a real
      // garbage list.
      NVRAM::Flush(sizeof(item), &item);

      return Status::OK();
    }
  }

  /// Scavenge items that are safe to be reused - useful when the user cannot
  /// wait until the garbage list is full. Currently (May 2016) the only user is
  /// MwCAS' descriptor pool which we'd like to keep small. Tedious to tune the
  /// descriptor pool size vs. garbage list size, so there is this function.
  int32_t Scavenge() {
    const uint64_t invalid_epoch = ~0llu;
    auto max_slot = tail_;
    int32_t scavenged = 0;

    for(int64_t slot = 0; slot < item_count_; ++slot) {
      auto& item = items_[slot];
      Epoch priorItemEpoch = item.removal_epoch;
      if(priorItemEpoch == 0 || priorItemEpoch == invalid_epoch) {
        // Someone is modifying this slot. Try elsewhere.
        continue;
      }

      // No synchronization for a single thread; guaranteed to succeed
      item.removal_epoch = invalid_epoch;

      if(priorItemEpoch) {
        if(!epoch_manager_->IsSafeToReclaim(priorItemEpoch)) {
          // Uh-oh, we couldn't free the old entry. Things aren't looking
          // good, but maybe it was just the result of a race. Replace the
          // epoch number we mangled and try elsewhere.
          *((volatile Epoch*) &item.removal_epoch) = priorItemEpoch;
          continue;
        }
        item.destroy_callback(item.destroy_callback_context,
                              item.removed_item);
      }

      // Now reset the entry
      item.destroy_callback = nullptr;
      item.destroy_callback_context = nullptr;
      item.removed_item = nullptr;
      *((volatile Epoch*) &item.removal_epoch) = 0;
      ++scavenged;
    }

    /// This does not look good.
    LOG_IF(WARNING, scavenged == 0)
        << "No safe garbage scavenged!" << std::endl;

    return scavenged;
  }

  /// Returns (a pointer to) the epoch manager associated with this garbage list.
  EpochManager* GetEpoch() {
    return epoch_manager_;
  }

 private:
  /// EpochManager instance that is used to determine when it is safe to
  /// free up items. Specifically, it is used to stamp items during Push()
  /// with the current epoch, and it is used in to ensure
  /// that deletion of each item on the list is safe.
  EpochManager* epoch_manager_;

  /// Point in the #m_items ring where the next pushed address will be placed.
  /// Also indicates the next address that will be freed on the next push.
  /// Atomically incremented within Push().
  int64_t tail_;

  /// Size of the #m_items array. Must be a power of two.
  size_t item_count_;

  /// Ring of addresses the addresses pushed to the list and metadata about
  /// them needed to determine when it is safe to free them and how they
  /// should be freed. This is filled as a ring; when a new Push() comes that
  /// would replace an already occupied slot the entry in the slot is freed,
  /// if possible.
  nv_ptr<Item> items_;

  /// The OID for PMDK to manage the array. The offset part of this OID is
  // identical to [items_].
  PMEMoid oid_;
};

#else
// FIXME(shiges): hack
using GarbageListUnsafePersistent = GarbageListUnsafe;
#endif
} // namespace pmwcas
