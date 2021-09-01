#pragma once

#include "File.h"
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <absl/container/flat_hash_map.h>

#define CACHELINE_SIZE 64

//Make sure alignment as 16
struct BufferMeta {
    size_t page_id;
    uint32_t pin_count;
    uint16_t clock_count;
    uint16_t dirty;
};

class HtBufferManager {
public:
    HtBufferManager(HtFile* hpf, size_t buffer_size): file(hpf), n(buffer_size) {
        assert(buffer_size % 4 == 0);
        metas = static_cast<BufferMeta*>(std::aligned_alloc(CACHELINE_SIZE, sizeof(BufferMeta) * n));
        memset(metas, 0, sizeof(BufferMeta) * n);
        buffer_frames = static_cast<char*>(std::aligned_alloc(PAGE_SIZE, n * PAGE_SIZE));
        memset(buffer_frames, 0, PAGE_SIZE * n);
        lookup_table.clear();
        clock_hand = 0;
    }

    ~HtBufferManager() {
        Flush();
        std::free(metas);
        std::free(buffer_frames);
    }

    HtFile* GetFile() const {
        return file;
    }

    // These two are for memory safety. Lock manager handles concurrency.
    // Brought in the page.
    size_t PinPage(size_t page_id, char** frame) {
        auto res = lookup_table.find(page_id);
        size_t frame_id;
        if (res == lookup_table.end()) {
            frame_id = GetFreeFrame();
            *frame = buffer_frames + (frame_id * PAGE_SIZE);
            file->ReadPage(page_id, *frame);
            lookup_table.emplace(page_id, frame_id);
            metas[frame_id].dirty = 0;
            metas[frame_id].page_id = page_id;
            metas[frame_id].pin_count = 1;
        } else {
            frame_id = res->second;
            *frame = buffer_frames + (frame_id * PAGE_SIZE);
            metas[frame_id].pin_count++;
        }
        metas[frame_id].clock_count = 1;
        return frame_id;
    }

    // Release the page.
    inline void UnpinPage(size_t frame_id) {
        --metas[frame_id].pin_count;
    }

    inline void MarkDirty(size_t frame_id) {
        metas[frame_id].dirty = 1;
    }

    void FreePage(size_t frame_id) {
        assert(metas[frame_id].pin_count == 1);
        lookup_table.erase(metas[frame_id].page_id);
        //if (metas[frame_id].dirty) {
        //    file->WritePage(metas[frame_id].page_id, buffer_frames + (PAGE_SIZE * frame_id));
        //}

        file->FreePage(metas[frame_id].page_id);

        metas[frame_id].pin_count = 0;
        metas[frame_id].page_id = 0;
        metas[frame_id].dirty = 0;
        metas[frame_id].clock_count = 0;
    }

    void Flush() {
        for (size_t i = 0; i < n; ++i) {
            if (metas[i].dirty) {
                file->WritePage(metas[i].page_id, buffer_frames + (PAGE_SIZE * i));
                metas[i].dirty = 0;
            }
        }
        file->Flush();
    }

private:
    size_t GetFreeFrame() {
        while (metas[clock_hand].pin_count > 0 || metas[clock_hand].clock_count > 0) {
            if (metas[clock_hand].pin_count == 0) --metas[clock_hand].clock_count;
            clock_hand = (++clock_hand >= n) ? 0 : clock_hand;
        }
        //Evict
        lookup_table.erase(metas[clock_hand].page_id);
        if (metas[clock_hand].dirty) {
            file->WritePage(metas[clock_hand].page_id, buffer_frames + (PAGE_SIZE * clock_hand));
        }
        return clock_hand;
    }

    absl::flat_hash_map<size_t, size_t> lookup_table;
    HtFile* file;
    size_t n;
    size_t clock_hand;
    BufferMeta* metas;
    char* buffer_frames;
};

#undef CACHELINE_SIZE