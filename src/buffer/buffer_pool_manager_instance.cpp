//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "buffer/clock_replacer.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  // replacer_ = new LRUReplacer(pool_size);
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // cannot add latch here
  // std::scoped_lock<std::mutex> lock(latch_);

  // Make sure you call DiskManager::WritePage!
  // frame_id_t frame_id = page_table_[page_id];
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    char *page_data = pages_[it->second].GetData();
    disk_manager_->WritePage(page_id, page_data);
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  // You can do it!
  for (const auto &[page_id, frame_id] : page_table_) {
    char *page_data = pages_[frame_id].GetData();
    disk_manager_->WritePage(page_id, page_data);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool flag = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() <= 0) {
      flag = false;
      break;
    }
  }
  if (flag) {
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  if (frame_id == -1) {
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].is_dirty_) {
      FlushPgImp(pages_[frame_id].page_id_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // BUSTUB_ASSERT(sizeof(pages_[frame_id].data_) == PAGE_SIZE, "I hope error never happens here");
  // memset(pages_[frame_id].data_, 0, PAGE_SIZE);
  page_id_t next_page_id = AllocatePage();
  *page_id = pages_[frame_id].page_id_ = next_page_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = true;
  page_table_[next_page_id] = frame_id;
  pages_[frame_id].pin_count_ = 1;
  replacer_->Pin(frame_id);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1.     Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    if (pages_[frame_id].pin_count_ > 0) {
      return nullptr;
    }
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  if (frame_id == -1) {
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
  }
  // 2.     If R is dirty, write it back to the disk.
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(pages_[frame_id].page_id_);
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // memset(pages_[frame_id].data_, 0, PAGE_SIZE);
  pages_[frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);  // IMPORTANT HERE!

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  replacer_->Pin(frame_id);
  pages_[frame_id].is_dirty_ = false;
  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (it == page_table_.end()) {
    DeallocatePage(page_id);
    return true;
  }
  frame_id_t frame_id = it->second;
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  // memset(pages_[frame_id].data_, 0, PAGE_SIZE);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  free_list_.push_front(frame_id);
  page_table_.erase(it);
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  if (pages_[frame_id].pin_count_ > 0) {
    if (--pages_[frame_id].pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
    if (!pages_[frame_id].is_dirty_) {
      pages_[frame_id].is_dirty_ = is_dirty;
    }
    return true;
  }
  return false;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
