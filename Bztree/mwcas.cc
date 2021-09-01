// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "mwcas.h"

#include "pmwcas.h"
#include "atomics.h"

namespace pmwcas {

using WordDescriptor = Descriptor::WordDescriptor;

bool MwCASMetrics::enabled = false;
CoreLocal<MwCASMetrics*> MwCASMetrics::instance;
uint64_t RecoveryMetrics::stats_[RecoveryStats::MAX_RECOVERY_ITEM] = {0};

DescriptorPartition::DescriptorPartition(EpochManager* epoch,
                                         DescriptorPool* pool)
    : desc_pool(pool), allocated_desc(0) {
  free_list = nullptr;
  garbage_list = new GarbageListUnsafe;
  auto s = garbage_list->Initialize(epoch, pool->GetDescPerPartition());
  RAW_CHECK(s.ok(), "garbage list initialization failure");
}

DescriptorPartition::~DescriptorPartition() {
  garbage_list->Uninitialize();
  delete garbage_list;
}

DescriptorPool::DescriptorPool(uint32_t requested_pool_size,
                               uint32_t requested_partition_count,
                               bool enable_stats)
    : pool_size_(0),
      desc_per_partition_(0),
      partition_count_(0),
      partition_table_(nullptr),
      next_partition_(0) {
  MwCASMetrics::enabled = enable_stats;
  if (enable_stats) {
    auto s = MwCASMetrics::Initialize();
    RAW_CHECK(s.ok(), "failed initializing metric objects");
  }

  auto s = epoch_.Initialize();
  RAW_CHECK(s.ok(), "epoch initialization failure");

  // Round up pool size to the nearest power of 2
  pool_size_ = 1;
  while (requested_pool_size > pool_size_) {
    pool_size_ *= 2;
  }

  // Round partitions to a power of two but no higher than 1024
  partition_count_ = 1;
  for (uint32_t exp = 1; exp < 10; exp++) {
    if (requested_partition_count <= partition_count_) {
      break;
    }
    partition_count_ *= 2;
  }

  desc_per_partition_ = pool_size_ / partition_count_;
  RAW_CHECK(desc_per_partition_ > 0, "descriptor per partition is 0");

  partition_table_ = (DescriptorPartition*)malloc(sizeof(DescriptorPartition) *
                                                  partition_count_);
  RAW_CHECK(nullptr != partition_table_, "out of memory");

  for (uint32_t i = 0; i < partition_count_; ++i) {
    new (&partition_table_[i]) DescriptorPartition(&epoch_, this);
  }

  // A new descriptor pool area always comes zeroed.
  RAW_CHECK(pool_size_ > 0, "invalid pool size");

  // Create a new pool
#ifdef PMDK
  reinterpret_cast<PMDKAllocator*>(Allocator::Get())
      ->AllocateOffset((uint64_t*)&descriptors_,
                       sizeof(Descriptor) * pool_size_, false);
#else
  Allocator::Get()->AllocateAligned(
      (void**)&descriptors_, sizeof(Descriptor) * pool_size_, kCacheLineSize);
#endif
  RAW_CHECK(descriptors_, "out of memory");

  free_callbacks_ = std::make_unique<FreeCallbackArray>();

  InitDescriptors();
}

#ifdef PMEM
void DescriptorPool::Recovery(uint32_t requested_partition_count,
                              bool enable_stats, bool clear_free_callbacks) {
  MwCASMetrics::enabled = enable_stats;
  RecoveryMetrics::Reset();

  if (enable_stats) {
    auto s = MwCASMetrics::Initialize();
    RAW_CHECK(s.ok(), "failed initializing metric objects");
  }

  new (&epoch_) EpochManager;
  auto s = epoch_.Initialize();
  RAW_CHECK(s.ok(), "epoch initialization failure");

  // Round partitions to a power of two but no higher than 1024
  if (requested_partition_count) {
    uint32_t new_partition_count = 1;
    for (uint32_t exp = 1; exp < 10; exp++) {
      if (requested_partition_count <= new_partition_count) {
        break;
      }
      new_partition_count *= 2;
    }
    LOG(INFO) << "Descriptor pool redistributed into " << new_partition_count
              << " partitions (was " << partition_count_ << ")";
    partition_count_ = new_partition_count;
  }

  desc_per_partition_ = pool_size_ / partition_count_;
  RAW_CHECK(desc_per_partition_ > 0, "descriptor per partition is 0");

  partition_table_ = (DescriptorPartition*)malloc(sizeof(DescriptorPartition) *
                                                  partition_count_);
  RAW_CHECK(nullptr != partition_table_, "out of memory");

  for (uint32_t i = 0; i < partition_count_; ++i) {
    new (&partition_table_[i]) DescriptorPartition(&epoch_, this);
  }

  Descriptor* descriptors = descriptors_;
  RAW_CHECK(descriptors, "invalid descriptor array pointer");
  RAW_CHECK(pool_size_ > 0, "invalid pool size");

  if (clear_free_callbacks) {
    ClearFreeCallbackArray();
  }

#ifdef PMDK
  auto new_pmdk_pool =
      reinterpret_cast<PMDKAllocator*>(Allocator::Get())->GetPool();
#else
  static_assert(false, "Only recovery with PMDK is supported");
#endif

  // begin recovery process
  // Iterate over the whole desc pool, see if there's anything we can do
  for (uint32_t i = 0; i < pool_size_; ++i) {
    auto& desc = descriptors[i];

    desc.assert_valid_status();

    uint32_t status = desc.status_ & ~Descriptor::kStatusDirtyFlag;
    if (status == Descriptor::kStatusFinished) {
      RecoveryMetrics::IncValue(finished_desc);
      continue;
    } else if (status == Descriptor::kStatusUndecided ||
               status == Descriptor::kStatusFailed) {
      RecoveryMetrics::IncValue(roll_back_desc);

      for (uint32_t i = 0; i < DESC_CAP; ++i) {
        auto& word = desc.words_[i];
        if (word.address_ == Descriptor::kAllocNullAddress) {
          continue;
        }
        uint64_t* addr = word.address_;
        uint64_t val = *addr;
        if (Descriptor::IsDirtyPtr(val)) {
          *addr = val & ~Descriptor::kDirtyFlag;
          word.PersistAddress();
        }
        bool roll_back = false;
        if (Descriptor::IsCondCASDescriptorPtr(val)) {
          if (nv_ptr<WordDescriptor>(Descriptor::CleanPtr(val)) == &word) {
            roll_back = true;
          }
        } else if (Descriptor::IsMwCASDescriptorPtr(val)) {
          if (nv_ptr<Descriptor>(Descriptor::CleanPtr(val)) == &desc) {
            roll_back = true;
          }
        }
        if (roll_back) {
          // If it's a CondCAS descriptor, then MwCAS descriptor wasn't
          // installed/persisted, i.e., new value (succeeded) or old value
          // (failed) wasn't installed on the field. If it's an MwCAS
          // descriptor, then the final value didn't make it to the field
          // (status is Undecided). In both cases we should roll back to old
          // value.
          *addr = word.GetOldValue();
          word.PersistAddress();
          RecoveryMetrics::IncValue(roll_back_words);
          LOG(INFO) << "Applied old value 0x" << std::hex << word.GetOldValue()
                    << " at 0x" << static_cast<uint64_t>(word.address_)
                    << std::endl;
        }
      }

#if PMWCAS_SAFE_MEMORY == 1
      auto free_callback = free_callbacks_->GetFreeCallback(desc.callback_idx_);
      for (uint32_t i = 0; i < DESC_CAP; ++i) {
        auto& word = desc.words_[i];
        if (word.ShouldRecycleNewValue()) {
          free_callback(word.GetNewValuePtr());
        }
      }
#endif
    } else {
      RAW_CHECK(status == Descriptor::kStatusSucceeded, "invalid status");
      RecoveryMetrics::IncValue(roll_forward_desc);

      for (uint32_t i = 0; i < DESC_CAP; ++i) {
        auto& word = desc.words_[i];
        if (word.address_ == Descriptor::kAllocNullAddress) {
          continue;
        }
        uint64_t* addr = word.address_;
        uint64_t val = *addr;
        if (Descriptor::IsDirtyPtr(val)) {
          *addr = val & ~Descriptor::kDirtyFlag;
          word.PersistAddress();
        }
        bool roll_back = false;
        bool roll_forward = false;
        if (Descriptor::IsCondCASDescriptorPtr(val)) {
          if (nv_ptr<WordDescriptor>(Descriptor::CleanPtr(val)) == &word) {
            roll_back = true;
          }
        } else if (Descriptor::IsMwCASDescriptorPtr(val)) {
          if (nv_ptr<Descriptor>(Descriptor::CleanPtr(val)) == &desc) {
            roll_forward = true;
          }
        }
        RAW_CHECK(not(roll_back and roll_forward),
                  "Cannot roll back and forward at the same time");
        /// For a successful PMwCAS, we roll forward a target word if and only
        /// if it contains a pointer to the MwCAS descriptor.
        if (roll_forward) {
          *addr = word.GetNewValue();
          word.PersistAddress();
          RecoveryMetrics::IncValue(roll_forward_words);
          LOG(INFO) << "Applied new value 0x" << std::hex << word.GetNewValue()
                    << " at 0x" << addr << std::endl;
        } else if (roll_back) {
          *addr = word.GetOldValue();
          word.PersistAddress();
          RecoveryMetrics::IncValue(roll_back_words);
          LOG(INFO) << "Applied old value 0x" << std::hex << word.GetOldValue()
                    << " at 0x" << addr << std::endl;
        }
      }

#if PMWCAS_SAFE_MEMORY == 1
      auto free_callback = free_callbacks_->GetFreeCallback(desc.callback_idx_);
      for (uint32_t i = 0; i < DESC_CAP; ++i) {
        auto& word = desc.words_[i];
        if (word.ShouldRecycleOldValue()) {
          free_callback(word.GetOldValuePtr());
        }
      }
#endif
    }

    for (uint32_t i = 0; i < DESC_CAP; ++i) {
      auto& word = desc.words_[i];
      if (word.address_ == Descriptor::kAllocNullAddress) {
        continue;
      }
      int64_t val = *word.address_;

      RAW_CHECK(
          (val & ~Descriptor::kDirtyFlag) !=
              ((uint64_t)(nv_ptr<Descriptor>(&desc)) | Descriptor::kMwCASFlag),
          "invalid word value");
      RAW_CHECK((val & ~Descriptor::kDirtyFlag) !=
                    ((uint64_t)(nv_ptr<Descriptor::WordDescriptor>(&word)) |
                     Descriptor::kCondCASFlag),
                "invalid word value");
    }
  }
  RecoveryMetrics::PrintStats();

  InitDescriptors();
}
#endif

void DescriptorPool::InitDescriptors() {
  // (Re-)initialize descriptors. Any recovery business should be done by now,
  // start as a clean slate.
  Descriptor* descriptors = descriptors_;
  RAW_CHECK(descriptors, "null descriptor pool");
  memset(descriptors, 0, sizeof(Descriptor) * pool_size_);

  // Distribute this many descriptors per partition
  RAW_CHECK(pool_size_ > partition_count_,
            "provided pool size is less than partition count");

  for (uint32_t i = 0; i < partition_count_; ++i) {
    DescriptorPartition* p = partition_table_ + i;
    for (uint32_t d = 0; d < desc_per_partition_; ++d) {
      Descriptor* desc = descriptors + i * desc_per_partition_ + d;
      new (desc) Descriptor(p, free_callbacks_.get());
      desc->next_ptr_ = p->free_list;
      p->free_list = desc;
    }
  }

#ifdef PMEM
  // Flush the entire pool at (re-)start. Then it would be ready to
  // conduct PMwCAS operations.
  NVRAM::Flush(sizeof(Descriptor) * pool_size_, descriptors);
#endif
}

DescriptorPool::~DescriptorPool() {
  for (uint32_t i = 0; i < partition_count_; ++i) {
    DescriptorPartition* p = partition_table_ + i;
    p->~DescriptorPartition();
  }
  MwCASMetrics::Uninitialize();
}

DescriptorGuard DescriptorPool::AllocateDescriptor(FreeCallbackArray::Idx fc) {
  thread_local DescriptorPartition* tls_part = nullptr;
  if (!tls_part) {
    // Sometimes e.g., benchmark data loading will create new threads when
    // starting real work. So % partition_count_ here is safe. This is so far
    // the only safe case allowed.
    auto index = next_partition_.fetch_add(1, std::memory_order_seq_cst) %
                 partition_count_;

    // TODO: Currently we actually require strictly TLS partitions - there is no
    // CC whatsoever for partitions.
    tls_part = &partition_table_[index];

    // Track the partition pointer handed out to this thread.
    Thread::RegisterTls((uint64_t*)&tls_part, (uint64_t) nullptr);
  }

  Descriptor* desc = tls_part->free_list;
  while (!desc) {
    // See if we can scavenge some descriptors from the garbage list
    tls_part->garbage_list->GetEpoch()->BumpCurrentEpoch();
    auto scavenged = tls_part->garbage_list->Scavenge();
    tls_part->allocated_desc -= scavenged;
    desc = tls_part->free_list;
    RAW_CHECK(scavenged > 0 || !desc, "scavenged but still not descriptor");
    MwCASMetrics::AddDescriptorScavenge();
  }
  tls_part->free_list = desc->next_ptr_;

  MwCASMetrics::AddDescriptorAlloc();
  RAW_CHECK(desc, "null descriptor pointer");
  desc->callback_idx_ = fc;

  desc->Initialize();

  return DescriptorGuard(desc);
}

Descriptor::Descriptor(DescriptorPartition* partition,
                       FreeCallbackArray* callbacks) {
  count_ = 0;
  next_ptr_ = nullptr;
  owner_partition_ = partition;
  free_callbacks_ = callbacks;
  for (uint32_t i = 0; i < DESC_CAP; ++i) {
    DCHECK(!words_[i].address_);
    DCHECK(words_[i].old_value_ == 0x0);
    DCHECK(words_[i].new_value_ == 0x0);
    words_[i].status_address_ = &status_;
  }
  status_ = kStatusFinished;
}

void Descriptor::Initialize() {
  RAW_CHECK(status_ == kStatusFinished, "invalid status");
#if defined(PMEM) && not(defined(NDEBUG))
  // For PMwCAS we want to always start with a clean slate
  for (uint32_t i = 0; i < DESC_CAP; ++i) {
    DCHECK(!words_[i].address_);
    DCHECK(words_[i].old_value_ == 0x0);
    DCHECK(words_[i].new_value_ == 0x0);
  }
#endif

  count_ = 0;
  next_ptr_ = nullptr;

  status_ = kStatusUndecided;
#ifdef PMEM
  // Persisting the Undecided status before adding entries allows
  // the recovery to undo the changes if a crash happens during
  // preparing entries.
  PersistStatus();
#endif
}

void Descriptor::Finalize() {
  RAW_CHECK(status_ == kStatusSucceeded || status_ == kStatusFailed,
            "invalid status");

  // TODO(shiges): discuss persist or not
  status_ = kStatusFinished;
#ifdef PMEM
  for (uint32_t i = 0; i < count_; ++i) {
    words_[i].address_ = nullptr;
    words_[i].old_value_ = 0x0;
    words_[i].new_value_ = 0x0;
  }
  NVRAM::Flush(sizeof(WordDescriptor) * count_, &words_);
#endif
}

int32_t Descriptor::AddEntry(nv_ptr<uint64_t> addr, uint64_t oldval,
                             uint64_t newval, uint32_t recycle_policy) {
  // IsProtected() checks are quite expensive, use DCHECK instead of RAW_CHECK.
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());
  DCHECK(IsCleanPtr(oldval));
  DCHECK(IsCleanPtr(newval) || newval == kNewValueReserved);
  RAW_CHECK(status_ == kStatusUndecided, "invalid status");
#if PMWCAS_SAFE_MEMORY == 1
  if (recycle_policy == kRecycleAlways ||
      recycle_policy == kRecycleOldOnSuccess) {
    oldval = WordDescriptor::SetRecycleFlag(oldval);
  }
  if (recycle_policy == kRecycleAlways ||
      recycle_policy == kRecycleNewOnFailure) {
    newval = WordDescriptor::SetRecycleFlag(newval);
  }
#else
  RAW_CHECK(recycle_policy == kRecycleNever,
            "Safe memory ownership transfer is disabled");
#endif
  int insertpos = GetInsertPosition(addr);
  if (insertpos >= 0) {
    words_[insertpos].address_ = addr;
    words_[insertpos].old_value_ = oldval;
    words_[insertpos].new_value_ = newval;
    ++count_;
  }
  return insertpos;
}

int32_t Descriptor::GetInsertPosition(nv_ptr<uint64_t> addr) {
  DCHECK(uint64_t(addr) % sizeof(uint64_t) == 0);
  RAW_CHECK(count_ < DESC_CAP, "too many words");

  int32_t insertpos = count_;
  for (int32_t i = count_ - 1; i >= 0; i--) {
    if (addr != Descriptor::kAllocNullAddress && words_[i].address_ == addr) {
      // Can't allow duplicate addresses because it makes the desired result of
      // the operation ambigous. If two different new values are specified for
      // the same address, what is the correct result? Also, if the operation
      // fails we can't guarantee that the old values will be correctly
      // restored.
      return -2;
    }
  }
  return insertpos;
}

#ifdef PMEM
uint32_t Descriptor::ReadPersistStatus() {
  auto curr_status = *&status_;
  uint32_t stable_status = curr_status & ~kStatusDirtyFlag;
  if (curr_status & kStatusDirtyFlag) {
    // We have a persistent descriptor that includes all the old and new values
    // needed for recovery, so only persist the new status.
    PersistStatus();
    // Now we can clear the dirty bit; this has to be a CAS, somebody else might
    // be doing the same and already proceeded to further phases.
    CompareExchange32(&status_, stable_status, curr_status);
  }
  return stable_status;
}
#endif

/// Installing mwcas descriptor must be a conditional CAS (double-compare
/// single-swap, RDCSS): a thread can only CAS in a pointer to the mwcas
/// descriptor if the mwcas operation is still in Undecided status. Otherwise
/// if a thread delays the CAS until another thread T1 (as a helper) has
/// finished the mwcas operation, and another thread T2 conducted yet another
/// mwcas to change the value back to the original value, T1 when resumes
/// would produce incorrect result. An example:
///
/// T1 mwcas(A1 [1 - 2], A2 [3 - 4])
/// Suppose T1 went to sleep after installed descriptor on A1.
/// T2 comes to help the mwcas operation and finished.
/// Now A1=2, A2=4.
/// Suppose T3 now conducted an mwcas that reversed the previous mwcas:
/// Now A1=1, A2=3 again.
/// Suppose T1 now resumes execution, it will continue to install
/// descriptor on A2, which will succeed because it has value 3.
/// T1 then continues to CAS in new values, which fails for A1 but the
/// algo will think it's ok (assuming some other thread did it). The
/// installation for A2, however, will succeeded because it contains
/// a descriptor. Now A1=1, A2=4, an inconsistent state.
uint64_t Descriptor::CondCAS(uint32_t word_index, WordDescriptor desc[],
                             uint64_t dirty_flag) {
  auto* w = &desc[word_index];
  uint64_t cond_descptr =
      SetFlags((uint64_t)(nv_ptr<WordDescriptor>(w)), kCondCASFlag);
  uint64_t* addr = w->address_;
  uint64_t old_value = w->GetOldValue();

retry:
  uint64_t ret = CompareExchange64(addr, cond_descptr, old_value);
#ifdef PMEM
  if (ret & dirty_flag) {
#if PMWCAS_THREAD_HELP == 1
    w->PersistAddress();
    CompareExchange64(addr, ret & ~dirty_flag, ret);
#endif
    goto retry;
  }
#else
  RAW_CHECK((ret & kDirtyFlag) == 0, "dirty flag set on return value");
#endif
  if (IsCondCASDescriptorPtr(ret)) {
    // Already a CondCAS descriptor (ie a WordDescriptor pointer)
#if PMWCAS_THREAD_HELP == 1
    WordDescriptor* wd = nv_ptr<WordDescriptor>(CleanPtr(ret));
    RAW_CHECK(wd->address_ == w->address_, "wrong address");
    CompleteCondCAS(wd);
#endif
    // Retry this operation
    goto retry;
  } else if (ret == old_value) {
    CompleteCondCAS(w);
  }

  // ret could be a normal value or a pointer to a MwCAS descriptor
  return ret;
}

#ifndef PMEM
void Descriptor::VolatileCompleteCondCAS(WordDescriptor* wd) {
  Descriptor* mdesc = wd->GetDescriptor();
  uint64_t ptr = SetFlags((uint64_t)mdesc, kMwCASFlag);
  uint64_t expected = (uint64_t)wd | kCondCASFlag;
  uint64_t desired =
      *wd->status_address_ == kStatusUndecided ? ptr : wd->GetOldValue();
  uint64_t rval = CompareExchange64(wd->address_, desired, expected);
}
#endif

#ifdef PMEM
/// Complete the RDCSS operation on persistent memory.
void Descriptor::PersistentCompleteCondCAS(WordDescriptor* wd) {
  Descriptor* mdesc = wd->GetDescriptor();
  uint64_t ptr = SetFlags((uint64_t)(nv_ptr<Descriptor>(mdesc)), kMwCASFlag);
  uint64_t expected =
      SetFlags((uint64_t)(nv_ptr<WordDescriptor>(wd)), kCondCASFlag);
  uint64_t desired =
      mdesc->ReadPersistStatus() == kStatusUndecided ? ptr : wd->GetOldValue();
  desired = SetFlags(desired, kDirtyFlag);
  uint64_t* addr = wd->address_;
  uint64_t rval = CompareExchange64(addr, desired, expected);
  if (rval == expected || rval == desired) {
    wd->PersistAddress();
    CompareExchange64(addr, desired & ~kDirtyFlag, desired);
  }
}
#endif

#ifdef RTM
bool Descriptor::RTMInstallDescriptors(WordDescriptor all_desc[],
                                       uint64_t dirty_flag) {
  nv_ptr<Descriptor> self = this;
  uint64_t mwcas_descptr = SetFlags((uint64_t)self, kMwCASFlag | dirty_flag);
  uint64_t tries = 0;
  static const uint64_t kMaxTries = 4;

  while (tries < kMaxTries) {
    auto status = _xbegin();
    if (status == _XBEGIN_STARTED) {
      for (uint32_t i = 0; i < count_; ++i) {
        WordDescriptor* wd = &all_desc[i];
        // Skip entries added purely for allocating memory
        if (wd->address_ == Descriptor::kAllocNullAddress) {
          continue;
        }
        if (*wd->address_ != wd->GetOldValue()) {
          _xabort(0);
        }
        *wd->address_ = mwcas_descptr;
      }
      _xend();
#ifdef PMEM
      // Persist these pointers to MwCAS descriptors
      for (uint32_t i = 0; i < count_; ++i) {
        WordDescriptor* wd = &all_desc[i];
        // Skip entries added purely for allocating memory
        if (wd->address_ == Descriptor::kAllocNullAddress) {
          continue;
        }
        uint64_t* addr = wd->address_;
        uint64_t val = *addr;
        if (val == mwcas_descptr) {
          wd->PersistAddress();
          CompareExchange64(addr, mwcas_descptr & ~kDirtyFlag, mwcas_descptr);
        }
      }
#endif
      return true;
    }
    if ((status & _XABORT_EXPLICIT)) {
      // HTM aborted due to address != old_value
      break;
    }
    // else retry
    tries++;
  }
  return false;
}
#endif

#ifndef PMEM
bool Descriptor::VolatileMwCAS(uint32_t calldepth) {
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());

  RAW_CHECK(status_ != kStatusFinished, "invalid status");

#if not(PMWCAS_THREAD_HELP == 1)
  RAW_CHECK(calldepth == 0, "recursive helping is not enabled");
#endif

  if (calldepth == 0) {
    std::sort(words_, words_ + count_,
              [this](WordDescriptor& a, WordDescriptor& b) -> bool {
                return a.address_ < b.address_;
              });
  }

  uint32_t my_status = kStatusSucceeded;
  bool rtm_install_success = false;

  if (status_ != kStatusUndecided) {
    // Skip phase 1 if already concluded
    goto phase_2;
  }

#if 0
  // Go to phase 2 directly if helping along.
  // FIXME(shiges): why?
  if (calldepth > 0) {
    my_status = kStatusFailed;
  }
#endif

#ifdef RTM
  // Try RTM install first, if failed go to fallback solution.
  rtm_install_success = RTMInstallDescriptors(words_);
#endif

  if (!rtm_install_success) {
    for (uint32_t i = 0; i < count_ && my_status == kStatusSucceeded; ++i) {
      WordDescriptor* wd = &words_[i];
      if (wd->address_ == Descriptor::kAllocNullAddress) {
        continue;
      }
    retry_entry:
      auto rval = CondCAS(i, words_);

      // Ok if a) we succeeded to swap in a pointer to this descriptor or b)
      // some other thread has already done so.
      if (rval == wd->GetOldValue() || CleanPtr(rval) == (uint64_t)this) {
        continue;
      }

      // Do we need to help another MWCAS operation?
      if (IsMwCASDescriptorPtr(rval)) {
#if PMWCAS_THREAD_HELP == 1
        // Clashed with another MWCAS; help complete the other MWCAS if it is
        // still being worked on.
        Descriptor* otherMWCAS = (Descriptor*)CleanPtr(rval);
        otherMWCAS->VolatileMwCAS(calldepth + 1);
        MwCASMetrics::AddHelpAttempt();
#endif
        goto retry_entry;
      } else {
        // rval must be another value, we failed
        my_status = kStatusFailed;
      }
    }
  }

  CompareExchange32(&status_, my_status, kStatusUndecided);

phase_2:
  bool succeeded = (status_ == kStatusSucceeded);
  uint64_t descptr = SetFlags((uint64_t)this, kMwCASFlag);
  for (uint32_t i = 0; i < count_; i += 1) {
    WordDescriptor* wd = &words_[i];
    if (wd->address_ == Descriptor::kAllocNullAddress) {
      continue;
    }
    CompareExchange64(wd->address_,
                      succeeded ? wd->GetNewValue() : wd->GetOldValue(),
                      descptr);
  }

  if (calldepth == 0) {
    return Cleanup();
  } else {
    return succeeded;
  }
}
#endif

#ifdef PMEM
bool Descriptor::PersistentMwCAS(uint32_t calldepth) {
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());
  // FIXME(shiges): the in-cache mirror for word descriptors cannot
  // be nested/recursively used. Replace this with a stack.
  // thread_local WordDescriptor tls_desc[DESC_CAP];

  RAW_CHECK(status_ != kStatusFinished, "invalid status");

#if not(PMWCAS_THREAD_HELP == 1)
  RAW_CHECK(calldepth == 0, "recursive helping is not enabled");
#endif

  // Not visible to anyone else, persist before making the descriptor visible
  if (calldepth == 0) {
    // Sort all words in address order to avoid livelock.
    for (uint32_t i = 0; i < count_; ++i) {
      indexes_[i] = i;
    }
    std::sort(indexes_, indexes_ + count_, [this](auto a, auto b) -> bool {
      return words_[a].address_ < words_[b].address_;
    });
    for (uint32_t i = 1; i < count_; ++i) {
      if (words_[indexes_[i - 1]].address_ && words_[indexes_[i]].address_) {
        DCHECK(words_[indexes_[i - 1]].address_ < words_[indexes_[i]].address_);
      }
    }
    RAW_CHECK(status_ == kStatusUndecided, "invalid status");

    NVRAM::Flush(sizeof(WordDescriptor) * count_, &words_);
  }

  uint32_t my_status = kStatusSucceeded;
  bool rtm_install_success = false;
  nv_ptr<Descriptor> self = this;

  if (ReadPersistStatus() != kStatusUndecided) {
    // Skip phase 1 if already concluded
    goto phase_2;
  }

#if 0
  // Go to phase 2 directly if helping along.
  // FIXME(shiges): why?
  if (calldepth > 0) {
    my_status = kStatusFailed;
  }
#endif

#ifdef RTM
  // Try RTM install first, if failed go to fallback solution.
  rtm_install_success = RTMInstallDescriptors(words_, kDirtyFlag);
#endif

  if (!rtm_install_success) {
    for (uint32_t i = 0; i < count_ && my_status == kStatusSucceeded; ++i) {
      WordDescriptor* wd = &words_[indexes_[i]];
      // Skip entries added purely for allocating memory
      if (wd->address_ == Descriptor::kAllocNullAddress) {
        continue;
      }
    retry_entry:
      auto rval = CondCAS(indexes_[i], words_, kDirtyFlag);
      RAW_CHECK((rval & kDirtyFlag) == 0, "dirty flag set on return value");

      // Ok if a) we succeeded to swap in a pointer to this descriptor or b)
      // some other thread has already done so. Need to persist all fields
      // (which point to descriptors) before switching to final status, so
      // that recovery will know reliably whether to roll forward or back for
      // this descriptor.
      if (rval == wd->GetOldValue() || CleanPtr(rval) == (uint64_t)self) {
        continue;
      }

      // Do we need to help another MWCAS operation?
      if (IsMwCASDescriptorPtr(rval)) {
#if PMWCAS_THREAD_HELP == 1
        // Clashed with another MWCAS; help complete the other MWCAS if it is
        // still in flight.
        Descriptor* otherMWCAS = nv_ptr<Descriptor>(CleanPtr(rval));
        otherMWCAS->PersistentMwCAS(calldepth + 1);
        MwCASMetrics::AddHelpAttempt();
#endif
        goto retry_entry;
      } else {
        // rval must be another value, we failed
        my_status = kStatusFailed;
      }
    }
  }
  // Switch to the final state, the MwCAS concludes after this point
  CompareExchange32(&status_, my_status | kStatusDirtyFlag, kStatusUndecided);

  // Now the MwCAS is concluded - status is either succeeded or failed, and
  // no observers will try to help finish it, so do a blind flush and reset
  // the dirty bit.
  RAW_CHECK((status_ & ~kStatusDirtyFlag) != kStatusUndecided,
            "invalid status");
  PersistStatus();
  status_ &= ~kStatusDirtyFlag;
  // No need to flush again, recovery does not care about the dirty bit

phase_2:
  bool succeeded = (status_ == kStatusSucceeded);
  uint64_t descptr = SetFlags((uint64_t)self, kMwCASFlag);
  for (uint32_t i = 0; i < count_; i += 1) {
    WordDescriptor* wd = &words_[indexes_[i]];
    if (wd->address_ == Descriptor::kAllocNullAddress) {
      continue;
    }
    uint64_t val = succeeded ? wd->GetNewValue() : wd->GetOldValue();
    val = SetFlags(val, kDirtyFlag);
    uint64_t* addr = wd->address_;

    uint64_t rval = CompareExchange64(addr, val, descptr);
    if (rval == descptr || rval == val) {
      wd->PersistAddress();
      CompareExchange64(addr, val & ~kDirtyFlag, val);
    }
  }

  if (calldepth == 0) {
    return Cleanup();
  } else {
    return succeeded;
  }
}

#endif

bool Descriptor::Cleanup() {
  // Remeber outcome so we can return it.
  // We are sure here Status doesn't have dirty flag set
  RAW_CHECK((status_ & kStatusDirtyFlag) == 0, "invalid status");
  RAW_CHECK(status_ == kStatusFailed || status_ == kStatusSucceeded,
            "invalid status");

  bool success = (status_ == kStatusSucceeded);

  if (success) {
    MwCASMetrics::AddSucceededUpdate();
  } else {
    MwCASMetrics::AddFailedUpdate();
  }

  // There will be no new accessors once we have none of the target fields
  // contain a pointer to this descriptor; this is the point we can put this
  // descriptor in the garbage list. Note that multiple threads might be
  // working on the same descriptor at the same time, and only one thread can
  // push to the garbage list, so we can only change to kStatusFinished state
  // after no one is using the descriptor, i.e., in FreeDescriptor(), and let
  // the original owner (calldepth=0, i.e., the op that calls Cleanup()) push
  // the descriptor to the garbage list.
  //
  // Note: It turns out frequently Protect() and Unprotect() is expensive, so
  // let the user determine when to do it (e.g., exit/re-enter every X mwcas
  // operations). Inside any mwcas-related operation we assume it's already
  // protected.
  auto s = owner_partition_->garbage_list->Push(
      this, Descriptor::FreeDescriptor, nullptr);
  RAW_CHECK(s.ok(), "garbage list push() failed");
  DCHECK(owner_partition_->garbage_list->GetEpoch()->IsProtected());
  return success;
}

Status Descriptor::Abort() {
  RAW_CHECK(status_ == kStatusUndecided, "cannot abort under current status");
  status_ = kStatusFailed;
  auto s = owner_partition_->garbage_list->Push(
      this, Descriptor::FreeDescriptor, nullptr);
  RAW_CHECK(s.ok(), "garbage list push() failed");
  return s;
}

#if PMWCAS_SAFE_MEMORY == 1
void Descriptor::DeallocateMemory() {
  // Free the memory associated with the descriptor if needed
  auto free_callback = free_callbacks_->GetFreeCallback(callback_idx_);

  for (uint32_t i = 0; i < count_; ++i) {
    auto& word = words_[i];
    auto status = status_;
    if (status == kStatusSucceeded) {
      if (word.ShouldRecycleOldValue()) {
        free_callback(word.GetOldValuePtr());
      }
    } else if (status == kStatusFailed) {
      if (word.ShouldRecycleNewValue()) {
        free_callback(word.GetNewValuePtr());
      }
    }
  }
}
#endif

void Descriptor::FreeDescriptor(void* context, void* desc) {
  MARK_UNREFERENCED(context);

  Descriptor* desc_to_free = reinterpret_cast<Descriptor*>(desc);

#if PMWCAS_SAFE_MEMORY == 1
  desc_to_free->DeallocateMemory();
#endif
  desc_to_free->Finalize();

  RAW_CHECK(desc_to_free->status_ == kStatusFinished, "invalid status");

  desc_to_free->next_ptr_ = desc_to_free->owner_partition_->free_list;
  desc_to_free->owner_partition_->free_list = desc_to_free;
}

}  // namespace pmwcas
