#include <cstring>

template <class Register>
bool File::flush(long n, Register &reg) {
  void *buf = static_cast<void *>(&reg);
  size_t size = access_size;
  off_t offset = n * size;
  ssize_t ret;

  ret = pwrite(fd, buf, size, offsetStart + offset);

  PCHECK(ret >= 0);

  ret = fsync(fd);
  PCHECK(ret == 0);

#if defined(IOSTAT)
  writecount++;
#endif

  return ret >= 0;
}

template <class Register>
bool File::load(long n, Register &reg) {
  void *buf = static_cast<void *>(&reg);
  size_t size = access_size;
  off_t offset = n * size;
  ssize_t ret;

  ret = pread(fd, buf, size, offsetStart + offset);
  PCHECK(ret >= 0);
  if(ret == 0){
    std::memset(buf, '\0', PAGE_SIZE); // TODO: maybe we dont need?
  }

#if defined(IOSTAT)
  readcount++;
#endif

  return ret >= 0;
}
