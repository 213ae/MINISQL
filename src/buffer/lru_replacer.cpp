#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages){
  for (size_t i = 0; i < num_pages; ++i) {
    lru_list.emplace_back(i);
  }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(!lru_list.empty()){
    *frame_id = lru_list.front();
    lru_list.pop_front();
    return true;
  }else{
    frame_id = nullptr;
    return false;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if ((size_t)frame_id < capacity) {
    auto iter = remove(lru_list.begin(), lru_list.end(), frame_id);
    lru_list.erase(iter, lru_list.end());
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if ((size_t)frame_id < capacity) {
    for (int &iter : lru_list) {//检查frame有没有被pin过，没有被pin过则unpin无效
      if (iter == frame_id) return;
    }
    lru_list.emplace_back(frame_id);
  }
}

size_t LRUReplacer::Size() {
  return lru_list.size();
}