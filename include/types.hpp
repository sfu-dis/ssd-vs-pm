#ifndef TYPES_H
#define TYPES_H
#include <cstdint>
class BufferManager;
using pagenum_t = uint32_t;
using pageid_t = uint64_t;
using fileid_t = uint16_t;
using record_t = uint64_t;
namespace ycsbc{
struct Pair {
  uint64_t key{};
  record_t record_number{0};

  Pair() = default;
  explicit constexpr Pair(uint64_t key) : key{key} {}
  constexpr Pair(uint64_t key, record_t record_number)
      : key{key}, record_number{record_number} {}

  constexpr bool operator<(const Pair &p) const noexcept {
    return (this->key < p.key);
  }

  constexpr bool operator<=(const Pair &p) const noexcept {
    return (this->key <= p.key);
  }

  constexpr bool operator==(const Pair &p) const noexcept {
    return (this->key == p.key);
  }
};
}
#define FILE_ID_SHIFT (48ull)
#define FILE_ID_MASK (0xffffull << FILE_ID_SHIFT)
#define PAGE_NUM_SHIFT (24ull)
#define PAGE_NUM_MASK (0xffffffull << PAGE_NUM_SHIFT)
#define PAGE_ID_INVALID_MASK (0xffffffull)

inline constexpr pageid_t shift(pageid_t val, pageid_t shift) {
  return val << shift;
}

// Page ID - a 64-bit integer
struct PageId {
  // Represents an Invalid Page ID
  static constexpr pageid_t kInvalidValue = ~pageid_t{0};

  // Structure of the Page ID pageid_t:
  // ---16 bits---|---24 bits---|---24 bits---|
  //    File ID   |   Page Num  |    Unused   |
  //    fileid_t  |   pagenum_t |             |
  pageid_t value;

  // Constructors
  PageId() : value(kInvalidValue) {}
  PageId(uint16_t file_id, pagenum_t page_num) {
    value = shift(file_id, FILE_ID_SHIFT) | shift(page_num, PAGE_NUM_SHIFT);
  }
  explicit PageId(pageid_t value) : value(value) {
    LOG_IF(FATAL, (value & PAGE_ID_INVALID_MASK))
        << "invalid page_id: " << value;
  }

  inline bool IsValid() { return !(value & PAGE_ID_INVALID_MASK); }
  inline pageid_t GetValue() const { return value; }
  inline pagenum_t GetPageID() const {
    return (value & PAGE_NUM_MASK) >> PAGE_NUM_SHIFT;
  }
  inline fileid_t GetFileID() const {
    return (value & FILE_ID_MASK) >> FILE_ID_SHIFT;
  }
};

#define PAGE_IDLE 0
#define PAGE_WRITE 1
#define PAGE_READ 2
#define PAGE_DATA_SIZE \
  (PAGE_SIZE - sizeof(page_id) - sizeof(flag_bytes) - sizeof(pin_count) - sizeof(last_used))
// Representation of a page in memory. The buffer has an array of Pages to
// accommodate DataPages and DirectoryPages.
struct [[nodiscard]] alignas(ALIGNMENT) Page {
  enum flag_idx { is_dirty = 0 };

  // ID of the page held in page_data
  PageId page_id;

  // Pin count - the number of users of this page
  uint16_t pin_count{0};

  uint16_t last_used{0};

  bool flag_bytes[8 - sizeof(pin_count) - sizeof(last_used)]{false};  // Also used for padding

  // Space to hold a real page loaded from storage
  alignas(sizeof(uint64_t)) char page_data[PAGE_DATA_SIZE];

  Page() = default;
  ~Page() {}

  // Helper functions
  [[nodiscard]] inline char *GetRealPage() noexcept { return page_data; }
  inline void SetUsed(uint16_t access_type) { last_used = access_type; }
  inline void SetIdle() { --last_used; }
  inline void SetDirty(bool dirty) noexcept { flag_bytes[is_dirty] = dirty; }
  inline PageId GetPageId() const noexcept { return page_id; }
  inline uint16_t IsUsed() const noexcept { return (last_used > 0); }
  inline uint16_t IsDirty() const noexcept { return flag_bytes[is_dirty]; }
  inline void IncPinCount() noexcept {
    assert(pin_count < std::numeric_limits<decltype(pin_count)>::max());
    pin_count += 1;
  }
  inline void DecPinCount() noexcept {
    assert(pin_count);
    pin_count -= 1;
  }
  inline auto GetPinCount() const noexcept { return pin_count; }
  // Prevent unintentional copies
  Page(const Page &) = delete;
  Page(Page &&) = delete;
  Page &operator=(const Page &) = delete;
  Page &operator=(Page &&) = delete;
};

static_assert(sizeof(Page) == PAGE_SIZE,
              "Actual aligned size of Page does not match PAGE_SIZE");

#define BTREE_ORDER                                                \
  ((int)((sizeof(decltype(std::declval<Page>().page_data)) - 16) / \
             (sizeof(ycsbc::Pair) + 4) - 2))
// #define BTREE_ORDER 45 // Btree of order m
#define MAX_CHILDREN (BTREE_ORDER + 2)  // Every node has at most m children
#define MAX_DATA \
  (BTREE_ORDER + 1)  // Non-leaf nodes with k children contains k-1 keys

// Every non-leaf has at least ceil(m/2) children (except root)
constexpr int MIN_NUM_CHILDREN =
    (BTREE_ORDER % 2) ? ((BTREE_ORDER / 2) + 1) : (BTREE_ORDER / 2);

#define ROOT_PAGE_NUM (1)  // todo: maybe not needed

#endif
