#include "buffer_manager.h"
#define MAX_BUFFER_PAGES (0xFFFFFFu)

// Initialize a new buffer manager
BufferManager::BufferManager(pagenum_t page_count) {
  CHECK(page_count <= MAX_BUFFER_PAGES) << "Max allowed number of buffer pages is " << MAX_BUFFER_PAGES << ", requested " << page_count;
  // Initialize the page_count member variable.
  this->page_count = page_count;

  // Malloc the desired amount of memory specified by page_count and store the address in member variable page_frames.
  this->page_frames = std::launder(reinterpret_cast<Page *>(std::aligned_alloc(ALIGNMENT, sizeof(Page) * page_count)));

  // Clear the allocated memory to 0.
  std::memset(page_frames, 0, sizeof(Page) * page_count);

  // Use placement new to initialize each frame
  // and add the page frames to LRU queue which will later be used by PinPage.
  for (uint32_t i = 0; i < page_count; ++i) {
    new (&page_frames[i]) Page();
  }

  this->page_cur = 0;
}

void BufferManager::Finalize() {
  // Flush all dirty pages.
  for (uint32_t i = 0; i < page_count; ++i) {
    if (page_frames[i].IsDirty()) {
      PageId pid = page_frames[i].GetPageId();
      bool success = file->flush(pid.GetPageID(), page_frames[i]);
      LOG_IF(ERROR, !success) << "Can't flush the page while destructing the buffer manager.";
    }
    page_frames[i].~Page();
  }

  // Free page frames.
  std::free(page_frames);
}

Page* BufferManager::PinPage(PageId page_id, uint16_t page_mode) {
  if (!page_id.IsValid()) {
    LOG(ERROR) << "Invalid PageId";
    return nullptr;
  }

  Page* page_frame = nullptr;
  auto it = page_table.find(page_id.GetValue());
  
  // Check if the page already exists in the buffer pool (using the PageID - Page* mapping). 
  // If so, return it.
  if (it != page_table.end()) {
    page_frame = it->second;
    page_frame->SetUsed(page_mode);
    return page_frame;
  }

  // If not, find a free page frame [p] using CLOCK algorithm and
  // setup the PageID - [p] mapping
next_page:
  page_frame = &page_frames[page_cur];

  // If the page frame is recently used, set it unused.
  if (page_frame->IsUsed()) {
    page_frame->SetIdle();
    page_cur = (++page_cur >= page_count) ? 0 : page_cur;
    goto next_page;
  }

  // If the page frame is dirty, flush the page inside before loading a new page.
  if (page_frame->IsDirty()) {
    PageId pid = page_frame->GetPageId();
    if (pid.GetFileID() != fid) {
      LOG(ERROR) << "The file is not registered.";
      return nullptr;
    }
    bool success = file->flush(pid.GetPageID(), *page_frame);
    LOG_IF(ERROR, !success) << "Can't flush the dirty page evicted from the LRU queue.";
  }

  // Update the PageId - page frame mapping.
  page_table.erase(page_frame->GetPageId().GetValue());

  // Load the page into the page frame from storage
  if (page_id.GetFileID() != fid) {
    LOG(ERROR) << "The file is not registered.";
    return nullptr;
  }

  if (!file->load(page_id.GetPageID(), *page_frame)) {
    LOG(ERROR) << "Can't load the page into the page frame.";
    return nullptr;
  }

  // Update the page frame metadata.
  page_frame->page_id = page_id;
  page_frame->SetUsed(page_mode);
  page_frame->SetDirty(false);
  page_table.try_emplace(page_id.GetValue(), page_frame);

  return page_frame;
}

void BufferManager::UnpinPage(Page *page) {

}

void BufferManager::RegisterFile(File *file) {
  assert(file);
  this->file = file;
  this->fid = file->GetId();
}

