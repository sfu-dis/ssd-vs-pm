#include "BufferManager.h"
#include "File.h"

// Directory Layout: first 8 byte next, second 8 byte n_entries, then every 8 byte a pointer to bucket.
// Bucket Layout: first 8 byte next, 4096-8 byte left. 16 byte per entry. 
// 32 byte bitmap -> 8*32 = 256 entries -> actual 253 entries.

// | -- Next -- | -- N_ENTRY -- | -- 32 byte bitmap -- | -------- 253 * 16 byte entries ---------- |
// Exactly 4096 bytes.

// 4096 / 8 bytes = 512 buckets per DIRECTORY

// Key uint64_t Value uint64_t

#define N_BUCKETS_PER_DIR (PAGE_SIZE / 8)
#define ENTRY_OFFSET 48
#define ENTRIES_PER_BUCKET 253


class HashTable {
public:
    HashTable(std::string path, size_t buffer_cap): hpf(path, 0, false) {
        bmgr = new HtBufferManager(&hpf, buffer_cap);
        n_buckets = hpf.GetThirdField();
    }

    HashTable(std::string path, size_t n_buckets, size_t buffer_cap): hpf(path, EXPAND_SIZE, true), n_buckets(n_buckets) {
        bmgr = new HtBufferManager(&hpf, buffer_cap);
        hpf.SetThirdField(n_buckets);
        size_t res;
        for (size_t i = 0; i < n_buckets / N_BUCKETS_PER_DIR + 1; ++i) {
            res = hpf.AllocatePage(); 
            hpf.TruncPage(res);
        }
        assert(res == n_buckets / N_BUCKETS_PER_DIR + 1);
    }

    HashTable(std::string path, size_t n_buckets, size_t buffer_cap, bool trunc): hpf(path, EXPAND_SIZE, trunc), n_buckets(n_buckets) {
        bmgr = new HtBufferManager(&hpf, buffer_cap);
    if (trunc) {
        hpf.SetThirdField(n_buckets);
        size_t res;
        for (size_t i = 0; i < n_buckets / N_BUCKETS_PER_DIR + 1; ++i) {
            res = hpf.AllocatePage(); 
            hpf.TruncPage(res);
        }
    } else {
        assert(hpf.GetThirdField() == n_buckets);
    }
    }

    bool Insert(uint64_t key, uint64_t value) {
        EntrySlot slot;
        auto success = GetFreeSlotWithProbe(key, slot);
        if (success) {
            *reinterpret_cast<uint64_t*>(slot.entry) = key;
            *reinterpret_cast<uint64_t*>(slot.entry + 8) = value;
            (*slot.n_entry)++;
            *slot.bitmap |= slot.bitmask;
            bmgr->MarkDirty(slot.frame_id);
            bmgr->UnpinPage(slot.frame_id);
            return true;
        }
        return false;
    }

    bool Search(const uint64_t key, uint64_t& value) {
        EntrySlot slot;
        auto success = ProbeAndCompress(key, slot);
        if (success) {
            value = *reinterpret_cast<uint64_t*>(slot.entry + 8);
            bmgr->UnpinPage(slot.frame_id);
            return true;
        } else return false;
    }

    bool Erase(uint64_t key) {
        EntrySlot slot;
        auto success = ProbeAndCompress(key, slot);
        if (success) {
            memset(reinterpret_cast<char*>(slot.entry), 0, 16);
            (*slot.n_entry)--;
            *slot.bitmap &= ~slot.bitmask;
            bmgr->MarkDirty(slot.frame_id);
            bmgr->UnpinPage(slot.frame_id);
            return true;
        } else return false;
    }


    ~HashTable() {
        delete bmgr;
    }

private:
    struct EntrySlot {
        char* entry;
        uint8_t* bitmap;
        uint8_t bitmask;
        size_t* n_entry;
        size_t frame_id;
    };

    bool GetFreeSlotWithProbe(const uint64_t& key, EntrySlot& slot) {
        bool free_slot = false;
        size_t bucket = std::hash<uint64_t>()(key) % n_buckets;
        size_t dir_page_no = bucket / N_BUCKETS_PER_DIR + 1;
        char *dir_frame, *bucket_frame;
        size_t dir_frame_id = bmgr->PinPage(dir_page_no, &dir_frame);
        size_t* dir_ptr = reinterpret_cast<size_t*>(dir_frame + (bucket % N_BUCKETS_PER_DIR) * sizeof(size_t));

        if (*dir_ptr == 0) {
            //not found, new page allocated.
            auto page_no = hpf.AllocatePage();
            hpf.TruncPage(page_no);
            *dir_ptr = page_no;
            bmgr->MarkDirty(dir_frame_id);
            bmgr->UnpinPage(dir_frame_id);
            slot.frame_id = bmgr->PinPage(page_no, &bucket_frame);
            slot.entry = reinterpret_cast<char*>(bucket_frame + ENTRY_OFFSET);
            slot.bitmap = reinterpret_cast<uint8_t*>(bucket_frame + 16);
            slot.bitmask = 1;
            slot.n_entry = reinterpret_cast<size_t*>(bucket_frame + 8);
            return true;
        } else {
            size_t cur_frame_id = bmgr->PinPage(*dir_ptr, &bucket_frame);
            size_t* n_entry = reinterpret_cast<size_t*>(bucket_frame + 8);

        
            // Compaction
            while (*n_entry == 0) {
                auto next = *reinterpret_cast<size_t*>(bucket_frame);
                *dir_ptr = next;
                bmgr->MarkDirty(dir_frame_id);
                bmgr->FreePage(cur_frame_id);

                if (next == 0) { // not found, hit the end.
                    auto page_no = hpf.AllocatePage();
                    hpf.TruncPage(page_no);
                    *dir_ptr = page_no;
                    bmgr->MarkDirty(dir_frame_id);
                    bmgr->UnpinPage(dir_frame_id);

                    // Starting from here bucket_frame is the new frame.
                    slot.frame_id = bmgr->PinPage(page_no, &bucket_frame);
                    slot.entry = reinterpret_cast<char*>(bucket_frame + ENTRY_OFFSET);
                    slot.bitmap = reinterpret_cast<uint8_t*>(bucket_frame + 16);
                    slot.bitmask = 1;
                    slot.n_entry = reinterpret_cast<size_t*>(bucket_frame + 8);
                    return true; 
                }
                
                
                size_t next_frame_id = bmgr->PinPage(next, &bucket_frame);
                cur_frame_id = next_frame_id;
                n_entry = reinterpret_cast<size_t*>(bucket_frame + 8);
            }
        
            bmgr->UnpinPage(dir_frame_id);

            size_t* next_ptr = NULL;
            do {
                size_t scanned = 0;
                size_t offset = ENTRY_OFFSET;
                size_t bitmap_offset = 16;
                uint8_t* bitmap = reinterpret_cast<uint8_t*>(bucket_frame + bitmap_offset);
                uint8_t bitmask = 1;
                for (size_t i = 0; i < ENTRIES_PER_BUCKET; ++i) {
                    auto occupy = bitmask & *bitmap;

                    if (occupy) {
                        if (*reinterpret_cast<uint64_t*>(bucket_frame + offset) == key) {
                            // Already there. Unpin the page, return false.
                            bmgr->UnpinPage(cur_frame_id);
                            return false;
                        }
                        ++scanned;
                    } else if (!free_slot) {
                        free_slot = true;
                        slot.entry = reinterpret_cast<char*>(bucket_frame + offset);
                        slot.bitmap = bitmap;
                        slot.bitmask = bitmask;
                        slot.n_entry = n_entry;
                        slot.frame_id = cur_frame_id;
                    }

                    if (free_slot && scanned == *n_entry) break;
                    offset += 16;
                    if (bitmask == (1 << 7)) {
                        bitmask = 1;
                        bitmap_offset += 1;
                        bitmap = reinterpret_cast<uint8_t*>(bucket_frame + bitmap_offset);
                    } else bitmask <<= 1;
                }
                next_ptr = reinterpret_cast<size_t*>(bucket_frame);
                char *next_frame;
                if (*next_ptr != 0) {
                    size_t next_frame_id = bmgr->PinPage(*next_ptr, &next_frame);
                    n_entry = reinterpret_cast<size_t*>(next_frame + 8);

                    while (*n_entry == 0) {
                        auto tmp_next = *reinterpret_cast<size_t*>(next_frame);
                        *next_ptr = tmp_next;
                        bmgr->MarkDirty(cur_frame_id);
                        bmgr->FreePage(next_frame_id);
                        
                        if (tmp_next == 0 && !free_slot) { // not found, hit the end.
                            auto page_no = hpf.AllocatePage();
                            hpf.TruncPage(page_no);
                            *next_ptr = page_no;
                            bmgr->MarkDirty(cur_frame_id);
                            bmgr->UnpinPage(cur_frame_id);

                            // Starting from here next_frame is the new frame.
                            slot.frame_id = bmgr->PinPage(page_no, &next_frame);
                            slot.entry = reinterpret_cast<char*>(next_frame + ENTRY_OFFSET);
                            slot.bitmap = reinterpret_cast<uint8_t*>(next_frame + 16);
                            slot.bitmask = 1;
                            slot.n_entry = reinterpret_cast<size_t*>(next_frame + 8);
                            return true; 
                        } else if (free_slot) {
                            return true;
                        }
                        
                        size_t tmp_next_frame_id = bmgr->PinPage(tmp_next, &next_frame);
                        next_frame_id = tmp_next_frame_id;
                        n_entry = reinterpret_cast<size_t*>(next_frame + 8);
                    }
            if (!free_slot || (free_slot && cur_frame_id != slot.frame_id)) bmgr->UnpinPage(cur_frame_id);
                    bucket_frame = next_frame;
                    cur_frame_id = next_frame_id;
                } else break; // Hit the end....
            } while (true);

            if (!free_slot) { // If there was no found free slots, then allocate new page.
                auto page_no = hpf.AllocatePage();
                hpf.TruncPage(page_no);
                *next_ptr = page_no;
                bmgr->MarkDirty(cur_frame_id);
                bmgr->UnpinPage(cur_frame_id);
                char* frame;
                // Starting from here next_frame is the new frame.
                slot.frame_id = bmgr->PinPage(page_no, &frame);
                slot.entry = reinterpret_cast<char*>(frame + ENTRY_OFFSET);
                slot.bitmap = reinterpret_cast<uint8_t*>(frame + 16);
                slot.bitmask = 1;
                slot.n_entry = reinterpret_cast<size_t*>(frame + 8);
            }
            return true;
        }
    }


    inline bool ProbeAndCompress(const uint64_t& key, EntrySlot& slot) {
        size_t bucket = std::hash<uint64_t>()(key) % n_buckets;
        size_t dir_page_no = bucket / N_BUCKETS_PER_DIR + 1;
        char *dir_frame, *bucket_frame;
        size_t dir_frame_id = bmgr->PinPage(dir_page_no, &dir_frame);
        size_t* dir_ptr = reinterpret_cast<size_t*>(dir_frame + (bucket % N_BUCKETS_PER_DIR * sizeof(size_t)));

        if (*dir_ptr == 0) {
            bmgr->UnpinPage(dir_frame_id);
            return false;
        } else {
            size_t cur_frame_id = bmgr->PinPage(*dir_ptr, &bucket_frame);
            size_t* n_entry = reinterpret_cast<size_t*>(bucket_frame + 8);
            while (*n_entry == 0) {
                auto next = *reinterpret_cast<size_t*>(bucket_frame);
                *dir_ptr = next;
                bmgr->MarkDirty(dir_frame_id);
                bmgr->FreePage(cur_frame_id);
                if (next == 0) {
                    bmgr->UnpinPage(dir_frame_id);
                    return false; 
                }
                size_t next_frame_id = bmgr->PinPage(next, &bucket_frame);
                cur_frame_id = next_frame_id;
                n_entry = reinterpret_cast<size_t*>(bucket_frame + 8);
            }
            bmgr->UnpinPage(dir_frame_id);

            do {
                size_t scanned = 0;
                size_t offset = ENTRY_OFFSET;
                size_t bitmap_offset = 16;
                uint8_t* bitmap = reinterpret_cast<uint8_t*>(bucket_frame + bitmap_offset);
                uint8_t bitmask = 1;
                for (size_t i = 0; i < ENTRIES_PER_BUCKET; ++i) {
                    auto occupy = bitmask & *bitmap;
                    if (occupy) {
                        if (*reinterpret_cast<uint64_t*>(bucket_frame + offset) == key) {
                            slot.frame_id = cur_frame_id;
                            slot.entry = reinterpret_cast<char*>(bucket_frame + offset);
                            slot.bitmap = bitmap;
                            slot.bitmask = bitmask;
                            slot.n_entry = n_entry;
                            return true;
                        }
                        ++scanned;
                        if (scanned == *n_entry) break;
                    }
                    offset += 16;
                    if (bitmask == (1 << 7)) {
                        bitmask = 1;
                        bitmap_offset += 1;
                        bitmap = reinterpret_cast<uint8_t*>(bucket_frame + bitmap_offset);
                    } else bitmask <<= 1;
                }
                size_t* next_ptr = reinterpret_cast<size_t*>(bucket_frame);
                char *next_frame;
                if (*next_ptr != 0) {
                    size_t next_frame_id = bmgr->PinPage(*next_ptr, &next_frame);
                    n_entry = reinterpret_cast<size_t*>(next_frame + 8);
                    while (*n_entry == 0) {
                        auto tmp_next = *reinterpret_cast<size_t*>(next_frame);
                        *next_ptr = tmp_next;
                        bmgr->MarkDirty(cur_frame_id);
                        bmgr->FreePage(next_frame_id);
                        if (tmp_next == 0) {
                            bmgr->UnpinPage(cur_frame_id);
                            return false;
                        }
                        size_t tmp_next_frame_id = bmgr->PinPage(tmp_next, &next_frame);
                        next_frame_id = tmp_next_frame_id;
                        n_entry = reinterpret_cast<size_t*>(next_frame + 8);
                    }
                    bmgr->UnpinPage(cur_frame_id);
                    bucket_frame = next_frame;
                    cur_frame_id = next_frame_id;
                } else {
                    bmgr->UnpinPage(cur_frame_id);
                    return false;
                } 
            } while (true);
        }
        return false;
    }

    HtFile hpf;
    HtBufferManager* bmgr;
    size_t n_buckets;
};
