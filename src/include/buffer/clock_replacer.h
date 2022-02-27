//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  std::mutex mutex_;
  std::vector<frame_id_t> frame_ids_;
  std::vector<bool> pinned_;
  std::vector<bool> ref_bits_;
  size_t max_size_;
  size_t now_;
  size_t size_;
};

}  // namespace bustub
