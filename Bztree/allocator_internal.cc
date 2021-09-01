// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "allocator_internal.h"
#include "auto_ptr.h"
#include "macros.h"

#pragma warning(disable: 4172)

namespace pmwcas {

unique_ptr_t<IAllocator> Allocator::allocator_;

Status Allocator::Initialize(std::function<Status(IAllocator*&)> create,
    std::function<void(IAllocator*)> destroy) {
  if(allocator_.get()) {
    return Status::Corruption("Allocator has already been initialized.");
  }
  IAllocator* allocator;
  RETURN_NOT_OK(create(allocator));
  allocator_ = unique_ptr_t<IAllocator>(allocator, destroy);
  return Status::OK();
}

} // namespace pmwcas
