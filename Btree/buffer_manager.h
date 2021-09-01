#pragma once
#ifndef B_TREE_BUFFER_H
#define B_TREE_BUFFER_H

#include <algorithm>
#include <list>
#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>

#include "file.h"
#include "../include/types.hpp"
#include "absl/container/flat_hash_map.h"

struct LRUNode {
	pagenum_t page_frame_id;
	LRUNode * next;
	LRUNode * pre;
};

struct LRUQueue {
	LRUNode * front;
	LRUNode * back;
};

class BufferManager {
 public:
  // Buffer manager constructor
  // @page_count: number of pages in the buffer pool
  explicit BufferManager(pagenum_t page_count);
  ~BufferManager() = default;

  void Finalize();

  // Default constructor: construct a dummy BM
  // Used for raw device mode
  BufferManager() = default;

  // Pin a page
  // @page_id: ID of the page to pin
  // Returns a page frame containing the pinned page; nullptr if the page cannot
  // be pinned
  [[nodiscard]] Page *PinPage(PageId page_id, uint16_t page_mode);

  // Unpin a page
  // @page: Page to unpin
  void UnpinPage(Page *page);

  // Add the file ID - BaseFile* mappings to support multiple tables
  // @file: pointer to the File object
  void RegisterFile(File *file);

 private:
  // File ID - BaseFile* mapping
  // std::map<uint16_t, File*> file_map;

  // The file that this buffer manager works on.
  File *file;

  // FID of the file that this buffer manager works on.
  fileid_t fid;

  // Page ID (file-local) - Page frame mapping
  absl::flat_hash_map<pageid_t, Page*> page_table;

  // Number of page frames
  uint32_t page_count;

  // An array of buffer pages
  Page *page_frames;

  // Current page frame position
  uint32_t page_cur;

  // Prevent unintentional copies
  BufferManager(const BufferManager&) = delete;
  BufferManager(BufferManager&&) = delete;
  BufferManager &operator=(const BufferManager&) = delete;
  BufferManager &operator=(BufferManager&&) = delete;
};

#endif  // B_TREE_BUFFER_H
