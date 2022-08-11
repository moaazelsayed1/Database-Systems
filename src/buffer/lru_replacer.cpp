//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : size_{0} {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (replace_frames_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }

  *frame_id = replace_frames_.back();
  replace_frames_.pop_back();
  replace_map_.erase(*frame_id);
  size_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (replace_map_.find(frame_id) == replace_map_.end()) {
    return;
  }

  replace_frames_.erase(replace_map_[frame_id]);
  replace_map_.erase(frame_id);
  size_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (replace_map_.find(frame_id) != replace_map_.end()) {
    return;
  }

  replace_frames_.push_front(frame_id);
  size_++;
  replace_map_[frame_id] = replace_frames_.begin();
}

auto LRUReplacer::Size() -> size_t { return this->size_; }

}  // namespace bustub
