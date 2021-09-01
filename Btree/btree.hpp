#if defined(NO_BUFFER)
static_assert(false, "Not implemented");
#endif

template <class T>
Btree<T>::Btree(std::shared_ptr<File> file, pagenum_t page_count)
    : buf_mgr{page_count}, file{file} {
#ifndef NO_BUFFER
  // Use BufferManager::RegisterFile to register this file's both BaseFiles with
  // the buffer manager.
  buf_mgr.RegisterFile(file.get());
#endif
  pagelist.reserve(page_count);
  if (file->is_empty()) {
    node *header_page = nullptr;
    read_node(0, header_page);
    new (header_page) Metadata();
    node *root = nullptr;
    read_node(get_root_page_num(), root);
    new (root) node(get_root_page_num());
    write_node(get_root_page_num());
    flush_header();
    UnpinAllPages();
  } else {
    get_header();
  }
}

template <class T>
void Btree<T>::flush_header() {
  write_node(0);
}
template <class T>
auto Btree<T>::get_header() {
  node *header_page = nullptr;
  read_node(0, header_page);
  return std::launder(reinterpret_cast<Metadata *>(header_page));
}
template <class T>
void Btree<T>::increment_record_and_flush() {
  get_header()->inc_record_count();
  flush_header();
}

/*
template <class T>
typename Btree<T>::iterator Btree<T>::begin() {
  node aux = read_node(header.get_root_page_num());
  while (aux.children[0] != 0) {
    pagenum_t page_num = aux.children[0];
    aux = read_node(page_num);
  }
  iterator it(file);
  it.currentPosition = aux;
  return it;
}

template <class T>
typename Btree<T>::iterator Btree<T>::end() {
  thread_local iterator it(file); // We can reuse this sentinel
  return it;
}
*/

template <class T>
void Btree<T>::new_node(node *&n) {
  auto page_num = get_header()->get_next_page_num();
  flush_header();
  auto pid = PageId(file->GetId(), page_num);
  Page *buf_page = BufMgrPinPage(pid, PAGE_WRITE);
  buf_page->SetDirty(true);
  n = std::launder(reinterpret_cast<node *>(buf_page->GetRealPage()));
  new (n) node(page_num);
}

template <class T>
void Btree<T>::read_node(long page_id, node *&n) {
#if defined(NO_BUFFER)
  file->load(page_id, *n);
#else
  auto pid = PageId(file->GetId(), page_id);
  Page *buf_page = BufMgrPinPage(pid, PAGE_READ);
  n = std::launder(reinterpret_cast<node *>(buf_page->GetRealPage()));
#endif
}

template <class T>
void Btree<T>::write_node(long page_id) {
#if defined(NO_BUFFER)
  file->flush(page_id, *n);
#else
  auto pid = PageId(file->GetId(), page_id);
  Page *buf_page = BufMgrPinPage(pid, PAGE_WRITE);
  buf_page->SetDirty(true);
#endif
}

template <class T>
std::optional<T> Btree<T>::find(
    const T &object) {
  node *root = nullptr;
  node *child = nullptr;
  node *node_cur = nullptr;
  std::optional<T> ret{};

  stack_idx = 0;
  read_node(get_root_page_num(), root);
  stack[stack_idx++] = root;
  while (stack_idx > 0) {
    node_cur = stack[--stack_idx];
    DCHECK(stack_idx < MAX_LEVEL);
    int pos = 0;
    if (node_cur->children[0] != 0) { // node_cur is an internal node.
      while (pos < node_cur->count && node_cur->data[pos] <= object) {
        pos++;
      }
      DCHECK(pos < MAX_CHILDREN);
      read_node(node_cur->children[pos], child);
      stack[stack_idx++] = child;
    } else { // node_cur is a leaf node.
      while (pos < node_cur->count && node_cur->data[pos] < object) {
        pos++;
      }
      DCHECK(pos < MAX_CHILDREN);
      ret.emplace(node_cur->data[pos]);
    }
  }
  
  UnpinAllPages();
  return ret;
}

/*
template <class T>
typename Btree<T>::iterator Btree<T>::find(
    const T &object, const node &other) {
  int pos = 0;
  if (other.children[0] != 0) {
    while (pos < other.count && other.data[pos] <= object) {
      pos++;
    }
    node *child = nullptr;
    read_node(other.children[pos], child);
    return find(object, *child);
  } else {
    while (pos < other.count && other.data[pos] < object) {
      pos++;
    }
    iterator it(file);
    it.currentPosition = other;
    it.index = pos;
    return it;
  }
}
*/

template <class T>
void Btree<T>::insert(const T &value) {
  node *root = nullptr;
  node *node_cur = nullptr;
  node *child = nullptr;
  node *parent = nullptr;
  uint16_t pos = 0;

  stack_idx = 0;
  read_node(get_root_page_num(), root);
  stack[stack_idx] = root;

  // Step 1, insert value to the leaf node.
  while (true) {
    DCHECK(stack_idx < MAX_LEVEL);
    node_cur = stack[stack_idx];

    pos = 0;
    while (pos < node_cur->count && node_cur->data[pos] < value) {
      pos++;
    }
    DCHECK(pos < MAX_CHILDREN);
    insert_pos[stack_idx] = pos;
    if (node_cur->children[pos] != 0) {
      long page_id = node_cur->children[pos];
      read_node(page_id, child);
      stack[++stack_idx] = child; 
    } else {
      node_cur->insert_in_node(pos, value);
      write_node(node_cur->page_id);
      break;
    }
  }

  DCHECK(stack_idx < MAX_LEVEL);
  // Step 2, check if there is any node that needs to be split.
  while (stack_idx > 0) {
    node_cur = stack[stack_idx];
    parent = stack[stack_idx - 1];
    pos = insert_pos[stack_idx - 1];

    if (node_cur->is_overflow()) {
      split(*parent, pos);
    }
    --stack_idx;
  }
  if (stack[0]->is_overflow()) {
    split_root();
  }
  increment_record_and_flush();
  UnpinAllPages();
}

/*
template <class T>
int Btree<T>::insert(node &ptr, const T &value) {
  int pos = 0;
  while (pos < ptr.count && ptr.data[pos] < value) {
    pos++;
  }
  if (ptr.children[pos] != 0) {
    long page_id = ptr.children[pos];
    node *child = nullptr;
    read_node(page_id, child);
    int state = insert(*child, value);
    if (state == BT_OVERFLOW) {
      split(ptr, pos);
    }
  } else {
    ptr.insert_in_node(pos, value);
    write_node(ptr.page_id);
  }
  return ptr.is_overflow() ? BT_OVERFLOW : NORMAL;
}
*/

template <class T>
void Btree<T>::split(node &parent, int pos) {
  node *ptr_child1 = nullptr;
  node *ptr_child2 = nullptr;
  this->read_node(parent.children[pos], ptr_child1);
  node &child1 = *ptr_child1;
  this->new_node(ptr_child2);
  node &child2 = *ptr_child2;

  bool is_leaf = (child1.children[0] == 0);

  int iter = 0;
  int i;
  for (i = 0; iter < MIN_NUM_CHILDREN; i++) {
    iter++;
  }
  child1.count = iter;

  parent.insert_in_node(pos, child1.data[iter]);

  if (!is_leaf) {
    iter++;
  } else {
    child2.right = child1.right;
    child1.right = child2.page_id;
    parent.children[pos + 1] = child2.page_id;
  }

  for (i = 0; iter < BTREE_ORDER + 1; i++) {
    child2.children[i] = child1.children[iter];
    child2.data[i] = child1.data[iter];
    child2.count++;
    iter++;
  }
  child2.children[i] = child1.children[iter];

  parent.children[pos] = child1.page_id;
  parent.children[pos + 1] = child2.page_id;

  write_node(parent.page_id);
  write_node(child1.page_id);
  write_node(child2.page_id);
}

template <class T>
void Btree<T>::split_root() {
  node *ptr_child1 = nullptr;
  node *ptr_child2 = nullptr;
  node *ptr_node_in_overflow = nullptr;

  this->read_node(get_root_page_num(), ptr_node_in_overflow);
  node &node_in_overflow = *ptr_node_in_overflow;
  this->new_node(ptr_child1);
  node &child1 = *ptr_child1;
  this->new_node(ptr_child2);
  node &child2 = *ptr_child2;

  int iter = 0;
  int i;
  for (i = 0; iter < MIN_NUM_CHILDREN; i++) {
    child1.children[i] = node_in_overflow.children[iter];
    child1.data[i] = node_in_overflow.data[iter];
    child1.count++;
    iter++;
  }
  child1.children[i] = node_in_overflow.children[iter];

  node_in_overflow.data[0] = node_in_overflow.data[iter];

  child1.right = child2.page_id;

  if (node_in_overflow.children[0] != 0) {
    iter++;  // the middle element
  }

  for (i = 0; iter < BTREE_ORDER + 1; i++) {
    child2.children[i] = node_in_overflow.children[iter];
    child2.data[i] = node_in_overflow.data[iter];
    child2.count++;
    iter++;
  }
  child2.children[i] = node_in_overflow.children[iter];

  node_in_overflow.children[0] = child1.page_id;
  node_in_overflow.children[1] = child2.page_id;
  node_in_overflow.count = 1;

  write_node(node_in_overflow.page_id);
  write_node(child1.page_id);
  write_node(child2.page_id);
}
/*
template <class T>
void Btree<T>::remove(const T &value) {
  node root = read_node(header.root_id);
  int state = remove(root, value);
  /// root = read_node(header.root_id);
  if (state == BT_UNDERFLOW && root.count == 0) {
    header.root_id = root.children[0];
    write_node(root.page_id, root);
    file->flush(0, *get_header_buf(header));
  }
}

template <class T>
int Btree<T>::remove(node &ptr, const T &value) {
  int pos = 0;
  while (pos < ptr.count && ptr.data[pos] < value) {
    pos++;
  }
  if (ptr.children[pos] != 0) {
    if (ptr.data[pos] == value && pos != ptr.count) {
      node next = read_node(ptr.children[pos + 1]);
      ptr.data[pos] = succesor(next);
      write_node(ptr.page_id, ptr);
      pos++;
    }
    node child = read_node(ptr.children[pos]);
    int state = remove(child, value);
    if (state == BT_UNDERFLOW) {
      node node_in_underflow = child;
      bool can_steal = steal_sibling(node_in_underflow, ptr, pos);
      if (!can_steal) {
        if (node_in_underflow.children[0] == 0) {  // si el underflow es leaf
          merge_leaf(ptr, node_in_underflow, pos);
        } else {
          bool can_merge = merge_with_parent(ptr, node_in_underflow, pos);
          if (!can_merge) decrease_height(ptr, node_in_underflow, pos);
        }
      }
    }
  } else if (ptr.data[pos] == value) {
    ptr.delete_in_node(pos);
    write_node(ptr.page_id, ptr);
  }

  return ptr.is_underflow() ? BT_UNDERFLOW : NORMAL;
}

template <class T>
void Btree<T>::decrease_height(node &ptr, node &node_in_underflow,
                                            int pos) {
  if (pos != ptr.count) {
    node child = read_node(ptr.children[pos]);
    if (child.count < floor(BTREE_ORDER / 2.0)) {
      node sibling = read_node(ptr.children[pos + 1]);
      sibling.insert_in_node(0, ptr.data[pos]);
      int last = node_in_underflow.count;
      sibling.children[0] = node_in_underflow.children[last];

      for (int i = last - 1; i >= 0; --i) {
        sibling.insert_in_node(0, node_in_underflow.data[i]);
        sibling.children[0] = node_in_underflow.children[i];
      }
      ptr.delete_in_node(pos);
      ptr.children[pos] = sibling.page_id;
      write_node(sibling.page_id, sibling);
      write_node(ptr.page_id, ptr);
      return;
    }
  }
  node sibling = read_node(ptr.children[pos - 1]);
  int last = sibling.count;
  sibling.insert_in_node(last, ptr.data[pos - 1]);
  sibling.children[last + 1] = node_in_underflow.children[0];
  for (int i = 0; i < node_in_underflow.count; i++) {
    last = sibling.count;
    sibling.insert_in_node(last, node_in_underflow.data[i]);
    sibling.children[last + 1] = node_in_underflow.children[i + 1];
  }
  ptr.delete_in_node(pos - 1);
  ptr.children[pos - 1] = sibling.page_id;

  write_node(sibling.page_id, sibling);
  write_node(ptr.page_id, ptr);
}

template <class T>
bool Btree<T>::merge_with_parent(node &ptr,
                                              node &node_in_underflow,
                                              int pos) {
  if (pos != 0) {
    node sibling = read_node(ptr.children[pos - 1]);
    if (sibling.count - 1 >= floor(BTREE_ORDER / 2.0)) {
      T jesus = ptr.data[pos - 1];
      node_in_underflow.insert_in_node(0, jesus);
      ptr.data[pos - 1] = sibling.data[sibling.count - 1];
      node_in_underflow.children[0] = sibling.children[sibling.count];
      sibling.delete_in_node(sibling.count - 1);
      write_node(sibling.page_id, sibling);
      write_node(ptr.page_id, ptr);
      write_node(node_in_underflow.page_id, node_in_underflow);
      return true;
    }
  } else if (pos != BTREE_ORDER) {
    node sibling = read_node(ptr.children[pos + 1]);
    if (sibling.count - 1 >= floor(BTREE_ORDER / 2.0)) {
      T jesus = ptr.data[pos];
      node_in_underflow.insert_in_node(0, jesus);
      ptr.data[pos] = sibling.data[0];
      node_in_underflow.children[1] = sibling.children[0];
      sibling.children[0] = sibling.children[1];
      sibling.delete_in_node(0);

      write_node(sibling.page_id, sibling);
      write_node(ptr.page_id, ptr);
      write_node(node_in_underflow.page_id, node_in_underflow);
      return true;
    }
  }
  return false;
}

template <class T>
void Btree<T>::merge_leaf(node &ptr, node &node_in_underflow,
                                       int pos) {
  if (pos - 1 >= 0) {  // right se une a left
    node sibling = read_node(ptr.children[pos - 1]);
    for (int i = 0; i < node_in_underflow.count; i++) {
      int pos_in = sibling.count;
      sibling.insert_in_node(pos_in, node_in_underflow.data[i]);
    }
    sibling.right = node_in_underflow.right;
    ptr.delete_in_node(pos - 1);

    write_node(sibling.page_id, sibling);
    write_node(ptr.page_id, ptr);
  } else {  // left se une a la right
    node sibling = read_node(ptr.children[1]);
    for (int i = 0; i < sibling.count; i++) {
      int pos_in = node_in_underflow.count;
      node_in_underflow.insert_in_node(pos_in, sibling.data[i]);
    }
    node_in_underflow.right = sibling.right;
    ptr.delete_in_node(0);
    write_node(node_in_underflow.page_id, node_in_underflow);
    write_node(ptr.page_id, ptr);
  }
}

template <class T>
bool Btree<T>::steal_sibling(node &node_in_underflow, node &ptr,
                                          int pos) {
  if (node_in_underflow.children[0] == 0) {  // verificar que es hoja
    if (pos != ptr.count) {
      node sibling = read_node(ptr.children[pos + 1]);  // hno de la derecha
      if (sibling.count - 1 >= floor(BTREE_ORDER / 2.0)) {
        T jesus = sibling.data[0];
        T jesus2 = sibling.data[1];
        sibling.delete_in_node(0);
        node_in_underflow.insert_in_node(sibling.count - 1, jesus);
        ptr.data[pos] = jesus2;
        write_node(sibling.page_id, sibling);
        write_node(node_in_underflow.page_id, node_in_underflow);
        write_node(ptr.page_id, ptr);
        return true;
      }
    }
    if (pos > 0) {
      node sibling = read_node(ptr.children[pos - 1]);  // hno de la izquierda
      if (sibling.count - 1 >= floor(BTREE_ORDER / 2.0)) {
        T jesus = sibling.data[sibling.count - 1];
        sibling.delete_in_node(sibling.count - 1);
        node_in_underflow.insert_in_node(0, jesus);
        ptr.data[pos - 1] = jesus;
        write_node(sibling.page_id, sibling);
        write_node(node_in_underflow.page_id, node_in_underflow);
        write_node(ptr.page_id, ptr);
        return true;
      }
    }
  }
  return false;
}

template <class T>
T Btree<T>::succesor(node &ptr) {
  while (ptr.children[0] != 0) {
    ptr = read_node(ptr.children[0]);
  }
  if (ptr.count == 1) {
    if (ptr.right == -1) return NULL;
    ptr = read_node(ptr.right);
    return ptr.data[0];
  } else {
    return ptr.data[1];
  }
}

template <class T>
void scan(const T &object, int scan_sz, char*& values_out) {
  if (!values_out) {
    return;
  }
  auto iter = find(object);
  for(int i = 0; i < scan_sz; ++i) {
      auto pair = *iter;
      memcpy(values_out + i * sizeof(pair), &pair, sizeof(pair));
      ++iter;
  }
}
*/