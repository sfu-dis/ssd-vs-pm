#ifndef B_TREE_NODE_H
#define B_TREE_NODE_H

#include <cmath>

#include "../include/types.hpp"

template <class T>
struct Node {
  alignas(T) T data[MAX_DATA]{};
  pagenum_t page_id{0};
  pagenum_t right{0};
  pagenum_t children[MAX_CHILDREN]{};
  uint16_t count{0};

  Node() = default;
  explicit constexpr Node(pagenum_t page_id) : page_id{page_id} {
    count = 0;
    for (int i = 0; i < MAX_CHILDREN; i++) {
      children[i] = 0;
    }
    for (size_t i = 0; i < MAX_DATA; i++) {
      data[i] = {};
    }
  }

  void insert_in_node(int pos, const T &value) {
    int j = count;
    while (j > pos) {
      data[j] = data[j - 1];
      children[j + 1] = children[j];
      j--;
    }
    data[j] = value;
    children[j + 1] = children[j];

    count++;
  }

  void delete_in_node(int pos) {
    for (int i = pos; i < count; i++) {
      data[i] = data[i + 1];
      children[i + 1] = children[i + 2];
    }
    count--;
  }

  bool is_overflow() { return count > BTREE_ORDER; }

  bool is_underflow() { return count < floor(BTREE_ORDER / 2.0); }
};

#endif  // B_TREE_NODE_H
