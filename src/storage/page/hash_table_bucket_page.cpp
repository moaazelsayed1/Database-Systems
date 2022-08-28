//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool done = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0) {
      result->push_back(array_[i].second);
      done = true;
    }
    if (!IsOccupied(i)) {
      break;
    }
  }
  return done;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  uint32_t idx = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(array_[i].first, key) == 0 && ValueAt(i) == value) {
        return false;
      }
    } else {
      if (idx == BUCKET_ARRAY_SIZE) {
        idx = i;
      }
      if (!IsOccupied(i)) {
        break;
      }
    }
  }
  if (idx == BUCKET_ARRAY_SIZE) {
    return false;
  }
  array_[idx] = {key, value};
  SetOccupied(idx);
  SetReadable(idx);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(KeyAt(i), key) == 0 && value == ValueAt(i)) {
      RemoveAt(i);
      return true;
    }
    if (!IsOccupied(i)) {
      break;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  size_t idx = bucket_idx >> 3;
  uint32_t offset = bucket_idx & 7;
  readable_[idx] &= (~(1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t idx = bucket_idx >> 3;
  uint32_t offset = bucket_idx & 7;
  return static_cast<bool>(occupied_[idx] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t idx = bucket_idx >> 3;
  uint32_t offset = bucket_idx & 7;
  occupied_[idx] |= 1 << offset;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t idx = bucket_idx >> 3;
  uint32_t offset = bucket_idx & 7;
  return static_cast<bool>(readable_[idx] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t idx = bucket_idx >> 3;
  uint32_t offset = bucket_idx & 7;
  readable_[idx] |= 1 << offset;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  uint32_t size = BUCKET_ARRAY_SIZE / 8;
  for (uint32_t i = 0; i < size; i++) {
    if (readable_[i] != static_cast<char>(0xff)) {
      return false;
    }
  }
  uint32_t remainder = BUCKET_ARRAY_SIZE - size * 8;
  return !(remainder != 0 && readable_[size] != static_cast<char>((1 << remainder) - 1));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t count = 0;
  uint32_t size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;

  uint8_t n;
  for (uint32_t i = 0; i < size; i++) {
    n = static_cast<uint8_t>(readable_[i]);
    while (n != 0) {
      n &= (n - 1);
      count++;
    }
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  uint32_t read_array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for (uint32_t i = 0; i < read_array_size; i++) {
    if (readable_[i] != static_cast<char>(0)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
