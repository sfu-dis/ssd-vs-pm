// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "allocator.h"
#include "environment.h"
#include "status.h"
#include "allocator_internal.h"
#include "environment_internal.h"
#include "mwcas.h"

namespace pmwcas {

/// Initialize the pmwcas library, creating a library-wide allocator.
Status InitLibrary(std::function<Status(IAllocator*&)> create_allocator,
    std::function<void(IAllocator*)> destroy_allocator);

/// Initialize the pmwcas library, creating library-wide allocator and environment.
Status InitLibrary(std::function<Status(IAllocator*&)> create_allocator,
    std::function<void(IAllocator*)> destroy_allocator,
    std::function<Status(IEnvironment*&)> create_environment,
    std::function<void(IEnvironment*)> destroy_environment);

/// Explicitly uninitializes the pmwcas library, destroying the library-wide allocator and
/// environment.
void UninitLibrary();

}  // namespace pmwcas
