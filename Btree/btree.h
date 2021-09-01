#ifndef B_TREE_BTREE_H
#define B_TREE_BTREE_H

#include <memory>
#include <optional>

#include "file.h"
#include "node.h"
#include "buffer_manager.h"
#include "../include/types.hpp"

#ifdef NO_BUFFER
struct alignas(PAGE_SIZE) Metadata {
#else
struct Metadata {
#endif
 private:
  long count{1};  // The number of noded (pages) created. Starts at 1 as 0 is
                  // for header
  record_t record_count{0};  // Number of records inserted into the btree
  const pagenum_t root_id{ROOT_PAGE_NUM};
  uint8_t padding[
#ifdef NO_BUFFER
      PAGE_SIZE
#else
      sizeof(decltype(std::declval<Page>().page_data))
#endif
      - sizeof(root_id) - sizeof(record_count) - sizeof(count)];
 public:
  inline static constexpr pagenum_t get_root_page_num() {
    return ROOT_PAGE_NUM;
  };
  pagenum_t get_page_count() const noexcept { return count; };
  pagenum_t get_next_page_num() noexcept { return ++count; };
  auto get_record_count() const noexcept { return record_count; }
  void inc_record_count() noexcept { record_count++; }
  Metadata(){ for(auto &c : padding) c = 0xff; }
};

#ifdef NO_BUFFER
static_assert(sizeof(Metadata) <= PAGE_SIZE);
#else
static_assert(sizeof(Metadata) <=
              sizeof(decltype(std::declval<Page>().page_data)));
#endif

template <class T>
class Btree {
 public:
  Btree(std::shared_ptr<File> file, pagenum_t page_count);
  ~Btree(){ buf_mgr.Finalize(); }

  typedef Node<T> node;
  static_assert(sizeof(node) <= PAGE_SIZE, "sizeof(node) must not be greater than PAGE_SIZE");
  static_assert(sizeof(node) <= sizeof(decltype(std::declval<Page>().page_data)));
  // typedef Iterator<T> iterator;

  // iterator begin();
  std::optional<T> find(const T &object);
  // iterator end();

  void insert(const T &value);
  void remove(const T &value);
  void scan(const T &object, int scan_sz, char*& values_out);
  auto get_record_count() { return get_header()->get_record_count(); }
 private:
  // Variables
  static constexpr int MAX_LEVEL = 8;
  BufferManager buf_mgr;
  std::shared_ptr<File> file;
  std::vector<std::pair<PageId, Page *>> pagelist{};
  node *stack[MAX_LEVEL];
  uint16_t insert_pos[MAX_LEVEL] = {0};
  uint16_t stack_idx;

  // Functions
  void new_node(node *&n);
  void read_node(long page_id, node *&n);
  void write_node(long page_id);
  int insert(node &ptr, const T &value);
  int remove(node &ptr, const T &value);
  void split(node &parent, int pos);
  void split_root();
  void decrease_height(node &ptr, node &node_in_underflow, int pos);
  bool merge_with_parent(node &ptr, node &node_in_underflow, int pos);
  void merge_leaf(node &ptr, node &node_in_underflow, int pos);
  bool steal_sibling(node &node_in_underflow, node &ptr, int pos);
  T succesor(node &ptr);

  // Header Metadata methods
  void flush_header();
  auto get_header();
  void increment_record_and_flush();
  auto get_root_page_num(){ return Metadata::get_root_page_num(); }

  inline Page *BufMgrPinPage(PageId pid, uint16_t page_mode) {
    Page *page = buf_mgr.PinPage(pid, page_mode);
    DCHECK_NOTNULL(page);
    pagelist.emplace_back(pid, page);
    return page;
  }

  inline void UnpinAllPages() {
    for (auto &p : pagelist) {
      buf_mgr.UnpinPage(p.second);
    }
    pagelist.clear();
  }

  enum state {
    BT_OVERFLOW,
    BT_UNDERFLOW,
    NORMAL,
  };
public:
  Btree() = delete;
  Btree(const Btree&) = delete;
  Btree(Btree&&) = delete;
  Btree &operator= (const Btree&) = delete;
};

#include "btree.hpp"

#endif  // B_TREE_BTREE_H
