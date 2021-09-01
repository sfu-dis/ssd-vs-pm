#pragma once
#ifndef B_TREE_FILE_H
#define B_TREE_FILE_H

#include <assert.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>
#include <iostream>

extern uint64_t readcount;
extern uint64_t writecount;

class File {
 public:
  File(const std::string &file_name, off_t len, bool trunc, size_t access_size): access_size{access_size}{
    int openFlags = 0;
    openFlags |= O_CREAT | O_RDWR | O_DIRECT;
  
    fileName = file_name;
  
    if (trunc) {
      openFlags |= O_TRUNC;
  
      fd = open(file_name.data(), openFlags, S_IRUSR | S_IWUSR);
      PCHECK(fd > 0);

      posix_fallocate(fd, 0, len);

      empty = true;
    } else {
      fd = open(file_name.data(), openFlags, S_IRUSR | S_IWUSR);
      PCHECK(fd > 0);
  
      FILE *fp = fopen(file_name.data(), "r");
      auto start = fgetc(fp);
      PCHECK(!fclose(fp)); // don't forget to close this file
      if (start == EOF) {
        empty = true;
      } else {
        empty = false;
      }
    }
  }
  
  File(const std::string &file_name, bool trunc, long offset_start, size_t access_size): access_size{access_size} {
    int openFlags = O_CREAT | O_RDWR | O_DIRECT | O_TRUNC;

    fileName = file_name;
    offsetStart = offset_start;

    fd = open(file_name.data(), openFlags, 0666);
    PCHECK(fd > 0);

    empty = true;
  }

  ~File(){
    PCHECK(close(fd) == 0);
  }

  inline bool is_empty() { return empty; }
  inline int GetId() {return fd;}

  template <class Register>
  bool flush(long n, Register &reg);

  template <class Register>
  bool load(long n, Register &reg);

 private:
  std::string fileName;
  size_t access_size;
  long offsetStart = 0;
  int fd;
  bool empty;
};

#include "file.hpp"

#endif  // B_TREE_FILE_H
