#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include "common/macros.h"

const int NumOfObjects = 20;  // 每一个内存块中节点数量

class MemHeap {
 public:
  virtual ~MemHeap() = default;

  /**
   * @brief Allocate a contiguous block of memory of the given size
   * @param size The size (in bytes) of memory to allocate
   * @return A non-null pointer if allocation is successful. A null pointer if
   * allocation fails.
   */
  virtual void *Allocate(size_t size) = 0;

  /**
   * @brief Returns the provided chunk of memory back into the pool
   */
  virtual void Free(void *ptr) = 0;
};

class SimpleMemHeap : public MemHeap {
 public:
  ~SimpleMemHeap() {
    for (auto it : allocated_) {
      free(it);
    }
  }

  void *Allocate(size_t size) {
    void *buf = malloc(size);
    ASSERT(buf != nullptr, "Out of memory exception");
    allocated_.insert(buf);
    return buf;
  }

  void Free(void *ptr) {
    if (ptr == nullptr) {
      return;
    }
    auto iter = allocated_.find(ptr);
    if (iter != allocated_.end()) {
      allocated_.erase(iter);
    }
  }

 private:
  std::unordered_set<void *> allocated_;
};

class PoolMemHeap : public MemHeap {
 public:
  PoolMemHeap() {
    freeNodeHeader = NULL;
    memBlockHeader = NULL;
  }

  ~PoolMemHeap() {
    MemBlock *ptr;
    while (memBlockHeader) {
      ptr = memBlockHeader->pNext;
      delete memBlockHeader;
      memBlockHeader = ptr;
    }
  }

  void *Allocate(size_t size) {
    // 无空闲节点，申请新内存块
    if (freeNodeHeader == NULL) {
      MemBlock *newBlock = new MemBlock;
      ASSERT(newBlock != nullptr, "Out of memory exception");

      newBlock->pNext = NULL;

      freeNodeHeader = &newBlock->data[0];  // 设置内存块的第一个节点为空闲节点链表的头节点

      // 将内存块的其他节点串起来
      for (int i = 1; i < NumOfObjects; i++) {
        newBlock->data[i - 1].pNext = &newBlock->data[i];
      }
      newBlock->data[NumOfObjects - 1].pNext = NULL;

      if (memBlockHeader == NULL) {
        memBlockHeader = newBlock;
      } else {
        newBlock->pNext = memBlockHeader;
        memBlockHeader = newBlock;
      }
    }

    void *freeNode = freeNodeHeader;
    freeNodeHeader = freeNodeHeader->pNext;
    return freeNode;
  }

  void Free(void *ptr) {
    FreeNode *pNode = (FreeNode *)ptr;
    pNode->pNext = freeNodeHeader;  //将释放的节点放到空闲节点链表头
    freeNodeHeader = pNode;
  }

 private:
  //空闲节点结构体
  struct FreeNode {
    FreeNode *pNext;
    char data[sizeof(size_t)];
  };

  //内存块结构体
  struct MemBlock {
    MemBlock *pNext;
    FreeNode data[NumOfObjects];
  };

  FreeNode *freeNodeHeader;
  MemBlock *memBlockHeader;
};

#endif  // MINISQL_MEM_HEAP_H
