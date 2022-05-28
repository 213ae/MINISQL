#include "buffer/clock_replacer.h"

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