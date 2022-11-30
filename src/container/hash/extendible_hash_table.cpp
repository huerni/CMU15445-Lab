//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

#include "common/logger.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {

      dir_.push_back(std::shared_ptr<Bucket>(new Bucket(bucket_size_)));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;

  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //UNREACHABLE("not implemented");
  auto it = dir_[IndexOf(key)];
  if(it->Find(key, value)) { return true; }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  auto it = dir_[IndexOf(key)];
  if(it->Remove(key)) { return true; }

  return false;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) {
  bucket->IncrementDepth();
  ++num_buckets_;

  auto list = bucket->GetItems();
  int index = -1;
  for(auto& [k, v] : list) {
    if(index == -1) {
      dir_[IndexOf(k)] = std::shared_ptr<Bucket>(new Bucket(bucket_size_, bucket->GetDepth()));
      index = IndexOf(k);
    }
    if(index != -1 && (int)IndexOf(k) != index) {
      dir_[IndexOf(k)] = std::shared_ptr<Bucket>(new Bucket(bucket_size_, bucket->GetDepth()));
      break;
    }
      
  }

  for(auto& [k, v] : list) {
    dir_[IndexOf(k)]->Insert(k, v);
  }
}

// 插入失败：
// 1. 如果localdepth等于globaldepth，++globaldepth，dir长度加倍
// 2. localdepth++，分裂。
// 3. 继续尝试插入
// 怎么分裂？
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //UNREACHABLE("not implemented");
  
  std::scoped_lock<std::mutex> lock(latch_);
  while(true) {
    auto bucket = dir_[IndexOf(key)];

    if(bucket->Insert(key, value)) break;

    if(bucket->GetDepth() == global_depth_) {
      ++global_depth_;
      std::vector<std::shared_ptr<Bucket>> tmp(dir_.size()*2);

      for(int i = 0; i<num_buckets_; ++i) {
        for(auto& [k, v] : dir_[i]->GetItems()) {
          tmp[IndexOf(k)] = dir_[i];
        }
      }

      dir_.swap(tmp);
    }

    RedistributeBucket(bucket);

  }

}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  //UNREACHABLE("not implemented");
  for(auto& [k, v] : list_) {
    if(key == k) {
      value = v;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  for(auto& it : list_) {
    if(it.first == key) {
      list_.remove(it);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //UNREACHABLE("not implemented");
  if(IsFull()) { return false; }
  
  for(auto& [k, v] : list_) {
    if(key == k) {
      v = value;
      return true;
    }
  }

  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
