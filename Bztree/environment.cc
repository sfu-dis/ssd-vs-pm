// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "environment.h"
#include "allocator_internal.h"
#include "auto_ptr.h"

namespace pmwcas {

unique_ptr_t<RandomReadWriteAsyncFile>
RandomReadWriteAsyncFile::make_unique_ptr_t(RandomReadWriteAsyncFile* p) {
  return unique_ptr_t<RandomReadWriteAsyncFile>(p,
  [](RandomReadWriteAsyncFile* p) {
    Status s = p->Close();
    ALWAYS_ASSERT(s.ok());
    free(p);
  });
}

}
