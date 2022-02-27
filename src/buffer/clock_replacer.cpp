//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : frame_ids_(num_pages, -1),
      pinned_(num_pages, false),
      ref_bits_(num_pages, false),
      max_size_(num_pages),
      now_(0),
      size_(0) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  const std::scoped_lock<std::mutex> lock(mutex_);
  if (size_ == 0) {
    return false;
  }
  size_t pinned_cnt = 0;
  for (size_t i = 0; i < size_; i++) {
    if (frame_ids_[i] != -1 && pinned_[i]) {
      pinned_cnt++;
    }
  }
  if (pinned_cnt == size_) {
    return false;
  }
  // clock hand moves only when result value is true
  for (;; now_ = (now_ + 1) % size_) {
    if (pinned_[now_] || frame_ids_[now_] == -1) {
      continue;
    }
    if (ref_bits_[now_]) {
      ref_bits_[now_] = false;
    } else {
      *frame_id = frame_ids_[now_];
      frame_ids_[now_] = -1;
      now_ = (now_ + 1) % size_;
      return true;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  const std::scoped_lock<std::mutex> lock(mutex_);
  for (size_t i = 0; i < max_size_; i++) {
    if (frame_ids_[i] == frame_id) {
      pinned_[i] = true;
      ref_bits_[i] = true;
      return;
    }
  }
  for (size_t i = 0; i < max_size_; i++) {
    if (frame_ids_[i] == -1) {
      frame_ids_[i] = frame_id;
      pinned_[i] = true;

      //      ref_bits_[i] = true;
      ref_bits_[i] = false;

      size_ = std::min(size_ + 1, max_size_);
      return;
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  const std::scoped_lock<std::mutex> lock(mutex_);
  // try accessing frame from buffer pool
  for (size_t i = 0; i < max_size_; i++) {
    if (frame_ids_[i] == frame_id) {
      // previous value existed

      if (pinned_[i]) {
        ref_bits_[i] = true;
        pinned_[i] = false;
      }
      return;
    }
  }
  // no existed value
  for (size_t i = 0; i < max_size_; i++) {
    if (frame_ids_[i] == -1) {
      // null, can add here
      frame_ids_[i] = frame_id;
      pinned_[i] = false;
      // ref_bits_[i] = true;
      ref_bits_[i] = false;
      size_ = std::min(size_ + 1, max_size_);
      return;
    }
  }
  // slots are enough, so no need to worry
}

size_t ClockReplacer::Size() {
  const std::scoped_lock<std::mutex> lock(mutex_);
  size_t cnt = 0;
  for (size_t i = 0; i < max_size_; i++) {
    if (frame_ids_[i] != -1 && !pinned_[i]) {
      cnt++;
    }
  }
  return cnt;
}

}  // namespace bustub
