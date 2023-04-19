
#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages){
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(!lru_queue.empty()){
    while(!lru_queue.front().second) {
      lru_queue.pop_front();
      if(lru_queue.empty()) {
        frame_id = nullptr;
        return false;
      }
    }
    *frame_id = lru_queue.front().first;
    lru_queue.pop_front();
    lru_map.erase(*frame_id);
    return true;
  }else{
    frame_id = nullptr;
    return false;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_map.find(frame_id) == lru_map.end()) return;
  lru_map[frame_id]->second = false;
  lru_map.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(lru_map.find(frame_id) != lru_map.end()) return;
  lru_queue.emplace_back(pair(frame_id, true));
  lru_map[frame_id] = lru_queue.end() - 1;
}

size_t LRUReplacer::Size() {
  size_t size = 0;
  for(auto itr : lru_queue){
    if(itr.second) size++;
  }
  return size;
}

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (!clock_list.empty()) {
    *frame_id = clock_list.back();
    clock_list.pop_back();
    clock_status[*frame_id] = INVALID_PAGE_ID;
    return true;
  } else {
    frame_id = nullptr;
    return false;
  }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if ((size_t)frame_id < capacity) {
    if (clock_status[frame_id] == INVALID_PAGE_ID) return;
    auto iter = remove(clock_list.begin(), clock_list.end(), clock_status[frame_id]);
    clock_list.erase(iter, clock_list.end());
    clock_status[frame_id] = INVALID_PAGE_ID;
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if ((size_t)frame_id < capacity) {
    if (clock_status[frame_id] != INVALID_PAGE_ID) return;
    clock_list.push_front(frame_id);
    auto iter = clock_list.begin();
    clock_status[frame_id] = *iter;
  }
}

size_t CLOCKReplacer::Size() { return clock_list.size(); }