#pragma once

// #define PAGE_SIZE 4096
#define EXPAND_SIZE 1024 * PAGE_SIZE
#define _FILE_OFFSET_BITS 64

#include <cstdio>
#include <cassert>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include <cstring>

alignas(512) const char dummy_page[PAGE_SIZE] = {0};

// Meta Level Page Structure
// First Page: first 8 bytes, first free page no, second 8 bytes, effect file size.
// Every Page if empty page first 8 bytes -> next free page.

class HtFile {
public:
    HtFile(const std::string& path, size_t init_size, bool trunc) {
        int flags = O_CREAT | O_RDWR | O_DIRECT;

        if (trunc) {
            flags |= O_TRUNC;
        }

        fd = open(path.c_str(), flags, S_IRUSR | S_IWUSR);
        assert(fd > 0);

        struct stat buf;
        fstat(fd, &buf);

        first_free_page = reinterpret_cast<size_t*>(first_page);
        fsize = reinterpret_cast<size_t*>(first_page + sizeof(size_t));
        if (buf.st_size < PAGE_SIZE) {
            posix_fallocate(fd, 0, PAGE_SIZE);
            int res = pwrite(fd, dummy_page, PAGE_SIZE, 0);
            assert(res == PAGE_SIZE);
            memset(first_page, 0, PAGE_SIZE);
        } else {
            int res = pread(fd, first_page, PAGE_SIZE, 0);
            assert(res == PAGE_SIZE);
        }

        if (init_size > 0) {
            //Preallocate so that it does not change meta later.
            assert(init_size % PAGE_SIZE == 0 && init_size > PAGE_SIZE);
            posix_fallocate(fd, 0, init_size);
        }
        fstat(fd, &buf);
        flen = std::max((size_t)buf.st_size, init_size);

    }

    size_t GetThirdField() {
        return *reinterpret_cast<size_t*>(first_page + sizeof(size_t) * 2);
    }
    
    void SetThirdField(size_t x) {
        *reinterpret_cast<size_t*>(first_page + sizeof(size_t) * 2) = x;
        int res = pwrite(fd, first_page, PAGE_SIZE, 0);
        assert(res == PAGE_SIZE);
        fsync(fd);
    }

    inline void ReadPage(size_t page_id, char* buf) {
        int res = pread(fd, buf, PAGE_SIZE, PAGE_SIZE * page_id);
        assert(res == PAGE_SIZE);
    }

    inline void WritePage(size_t page_id, const char* buf) {
        // XX: Force fsync here??
        int res = pwrite(fd, buf, PAGE_SIZE, PAGE_SIZE * page_id);
        assert(res == PAGE_SIZE);
#ifdef FORCE_FSYNC
        fsync(fd);
#endif
    }

    // Make sure the page number returned is available to write, doesn't need to guarantee
    // it is zeroed.
    size_t AllocatePage() {
        if (*first_free_page != 0) {
            char buf[PAGE_SIZE];
            int res = pread(fd, buf, PAGE_SIZE, PAGE_SIZE * (*first_free_page));
            assert(res == PAGE_SIZE);
            size_t next_free_page = *reinterpret_cast<size_t*>(buf);
            size_t ans = *first_free_page;
            *first_free_page = next_free_page;
#ifdef FORCE_FSYNC
            Flush();
#endif
            return ans;
        } else {
            if ((*fsize) + 1 >= flen / PAGE_SIZE) {
                int ret = posix_fallocate(fd, flen, EXPAND_SIZE);
                if (ret != 0) return 0;
                flen += EXPAND_SIZE;
            }
            ++(*fsize);
#ifdef FORCE_FSYNC
            Flush();
#endif
            return *fsize;
        }
    }

    void FreePage(size_t page_id) {
        char buf[PAGE_SIZE] = {0};
        *reinterpret_cast<size_t*>(buf) = *first_free_page;
        int res = pwrite(fd, buf, PAGE_SIZE, page_id * PAGE_SIZE);
        assert(res == PAGE_SIZE);
        *first_free_page = page_id;
#ifdef FORCE_FSYNC
        Flush();
#endif
    }

    void TruncPage(size_t page_id) {
        int res = pwrite(fd, dummy_page, PAGE_SIZE, page_id * PAGE_SIZE);
        assert(res == PAGE_SIZE);
#ifdef FORCE_FSYNC
        fsync(fd);
#endif
    }

    void Flush() {
#ifndef FORCE_FSYNC
        int res = pwrite(fd, first_page, PAGE_SIZE, 0);
        assert(res == PAGE_SIZE);
#endif
        fsync(fd);
    }

    ~HtFile() {
        Flush();
        fsync(fd);
        close(fd);
    }
private:
    //FILE* fp;
    int fd;
    alignas(512) char first_page[PAGE_SIZE];
    size_t flen;
    size_t* fsize;
    size_t* first_free_page;
};
