//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);
  dir_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  return index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t {
  page_id_t page_id = dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
  return page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool full = bucket_page->IsFull();
  bool done = false;
  if (!full) {
    done = bucket_page->Insert(key, value, comparator_);
  }
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.RUnlock();
  // all latches unlocked before SplitInsert
  if (full) {
    return SplitInsert(transaction, key, value);
  }
  return done;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);

  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  // what if another thread changed full in Insert? check again :)
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  if (!bucket_page->IsFull()) {
    bool done = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.WUnlock();
    return done;
  }

  if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  page_id_t new_bucket_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  assert(new_page != nullptr);
  new_page->WLatch();
  auto new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());

  dir_page->IncrLocalDepth(bucket_idx);
  auto new_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  auto new_local_depth = dir_page->GetLocalDepth(bucket_idx);
  auto new_local_mask = dir_page->GetLocalDepthMask(bucket_idx);
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t page_id = dir_page->GetBucketPageId(i);
    if (page_id == bucket_page_id) {
      dir_page->SetLocalDepth(i, new_local_depth);
      if ((i & new_local_mask) != (bucket_idx & new_local_mask)) {
        dir_page->SetBucketPageId(i, new_bucket_page_id);
      }
    }
  }
  KeyType bucket_key;
  ValueType bucket_value;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!bucket_page->IsReadable(i)) {
      continue;
    }
    bucket_key = bucket_page->KeyAt(i);
    bucket_value = bucket_page->ValueAt(i);
    auto new_idx = KeyToDirectoryIndex(bucket_key, dir_page);
    if (new_idx == new_bucket_idx) {
      bucket_page->RemoveAt(i);
      new_bucket_page->Insert(bucket_key, bucket_value, comparator_);
    }
  }
  page_id_t new_idx = KeyToPageId(key, dir_page);
  bool done;
  if (new_idx == bucket_page_id) {
    done = bucket_page->Insert(key, value, comparator_);
  } else {
    done = new_bucket_page->Insert(key, value, comparator_);
  }

  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  reinterpret_cast<Page *>(new_bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);

  table_latch_.WUnlock();
  return done;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool done = bucket_page->Remove(key, value, comparator_);
  uint32_t bucket_size = bucket_page->NumReadable();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.RUnlock();
  if (bucket_size == 0) {
    Merge(transaction, key, value);
  }
  return done;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_local_depth = dir_page->GetLocalDepth(bucket_idx);

  // don't merge
  if (bucket_local_depth == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }
  auto split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  auto image_local_depth = dir_page->GetLocalDepth(split_idx);
  if (image_local_depth != bucket_local_depth) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  if (!bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return;
  }

  page_id_t image_page_id = dir_page->GetBucketPageId(split_idx);
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t page_id = dir_page->GetBucketPageId(i);
    if (page_id == bucket_page_id || page_id == image_page_id) {
      dir_page->SetBucketPageId(i, image_page_id);
      dir_page->DecrLocalDepth(i);
    }
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
