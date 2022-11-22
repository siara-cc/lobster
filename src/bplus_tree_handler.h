#ifndef BP_TREE_H
#define BP_TREE_H
#ifdef ARDUINO
#include <HardwareSerial.h>
#include <string.h>
#else
#include <cstdio>
#include <cstring>
#include <iostream>
#endif
#include <stdint.h>
#include "univix_util.h"
#include "lru_cache.h"

using namespace std;

#define BPT_IS_LEAF_BYTE current_block[0] & 0x01
#define BPT_FILLED_SIZE current_block + 1
#define BPT_LAST_DATA_PTR current_block + 3
#define BPT_MAX_KEY_LEN current_block[5]
#define BPT_TRIE_LEN_PTR current_block + 6
#define BPT_TRIE_LEN current_block[7]
#define BPT_MAX_PFX_LEN current_block[8]

#define INSERT_AFTER 1
#define INSERT_BEFORE 2
#define INSERT_LEAF 3
#define INSERT_EMPTY 4
#define INSERT_THREAD 5
#define INSERT_CONVERT 6

#ifdef ARDUINO
#define BPT_INT64MAP 0
#else
#define BPT_INT64MAP 1
#endif
#define BPT_9_BIT_PTR 0

#if (defined(__AVR_ATmega328P__))
#define DEFAULT_PARENT_BLOCK_SIZE 512
#define DEFAULT_LEAF_BLOCK_SIZE 512
#elif (defined(__AVR_ATmega2560__))
#define DEFAULT_PARENT_BLOCK_SIZE 2048
#define DEFAULT_LEAF_BLOCK_SIZE 2048
#else
#define DEFAULT_PARENT_BLOCK_SIZE 4096
#define DEFAULT_LEAF_BLOCK_SIZE 4096
#endif

template<class T> // CRTP
class bplus_tree_handler {
protected:
    long total_size;
    int numLevels;
    int maxKeyCountNode;
    int maxKeyCountLeaf;
    int blockCountNode;
    int blockCountLeaf;
    long count1, count2;
    int max_key_len;
    lru_cache *cache;
    int is_block_given;

public:
    uint8_t *root_block;
    uint8_t *current_block;
    const char *key;
    uint8_t key_len;
    uint8_t *key_at;
    uint8_t key_at_len;
    const char *value;
    int16_t value_len;
#if BPT_9_BIT_PTR == 1
#if BPT_INT64MAP == 1
    uint64_t *bitmap;
#else
    uint32_t *bitmap1;
    uint32_t *bitmap2;
#endif
#endif

    const uint16_t leaf_block_size, parent_block_size;
    const int cache_size;
    const char *filename;
    bplus_tree_handler(uint16_t leaf_block_sz = DEFAULT_LEAF_BLOCK_SIZE,
            uint16_t parent_block_sz = DEFAULT_PARENT_BLOCK_SIZE, int cache_sz = 0,
            const char *fname = NULL, uint8_t *block = NULL) :
            leaf_block_size (leaf_block_sz), parent_block_size (parent_block_sz),
            cache_size (cache_sz), filename (fname) {
        init_stats();
        if (cache_size > 0) {
            cache = new lru_cache(leaf_block_size, cache_size, filename, 0, util::alignedAlloc);
            root_block = current_block = cache->get_disk_page_in_cache(0);
            if (cache->is_empty()) {
                static_cast<T*>(this)->initCurrentBlock();
            }
        } else {
            root_block = current_block = (block == NULL ? (uint8_t *) util::alignedAlloc(leaf_block_size) : block);
            if (block != NULL)
                static_cast<T*>(this)->setCurrentBlock(block);
            static_cast<T*>(this)->initCurrentBlock();
        }
        is_block_given = block == NULL ? 0 : 1;
    }

    ~bplus_tree_handler() {
        if (cache_size > 0)
            delete cache;
        else if (!is_block_given)
            free(root_block);
    }

    void initCurrentBlock() {
        //memset(current_block, '\0', BFOS_NODE_SIZE);
        //cout << "Tree init block" << endl;
        setLeaf(1);
        setFilledSize(0);
        BPT_MAX_KEY_LEN = 1;
        setKVLastPos(leaf_block_size);
    }

    void init_stats() {
        total_size = maxKeyCountLeaf = maxKeyCountNode = blockCountNode = 0;
        numLevels = blockCountLeaf = 1;
        count1 = count2 = 0;
        max_key_len = 0;
    }

    inline char *getValueAt(int16_t *vlen) {
        key_at += key_at_len;
        *vlen = *key_at;
        return (char *) key_at + 1;
    }

    void setCurrentBlockRoot();
    void setCurrentBlock(uint8_t *m);
    uint8_t *getCurrentBlock() {
        return current_block;
    }
    char *get(const char *key, uint8_t key_len, int16_t *pValueLen) {
        static_cast<T*>(this)->setCurrentBlockRoot();
        this->key = key;
        this->key_len = key_len;
        if ((isLeaf() ? static_cast<T*>(this)->searchCurrentBlock() : traverseToLeaf()) < 0)
            return null;
        return getValueAt(pValueLen);
    }

    inline bool isLeaf() {
        return BPT_IS_LEAF_BYTE;
    }

    uint8_t *skipChildren(uint8_t *t, uint8_t count);
    int16_t searchCurrentBlock();
    void setPrefixLast(uint8_t key_char, uint8_t *t, uint8_t pfx_rem_len);

    inline uint8_t *getKey(int16_t pos, uint8_t *plen) {
        uint8_t *kvIdx = current_block + getPtr(pos);
        *plen = *kvIdx;
        return kvIdx + 1;
    }
    uint8_t *getKey(uint8_t *t, uint8_t *plen);

    inline int getPtr(int16_t pos) {
#if BPT_9_BIT_PTR == 1
        uint16_t ptr = *(static_cast<T*>(this)->getPtrPos() + pos);
#if BPT_INT64MAP == 1
        if (*bitmap & MASK64(pos))
        ptr |= 256;
#else
        if (pos & 0xFFE0) {
            if (*bitmap2 & MASK32(pos - 32))
            ptr |= 256;
        } else {
            if (*bitmap1 & MASK32(pos))
            ptr |= 256;
        }
#endif
        return ptr;
#else
        return util::getInt(static_cast<T*>(this)->getPtrPos() + (pos << 1));
#endif
    }
    uint8_t *getPtrPos();

    int16_t traverseToLeaf(int8_t *plevel_count = NULL, uint8_t *node_paths[] = NULL) {
        unsigned long child_page = 0;
        while (!isLeaf()) {
            if (node_paths) {
                *node_paths++ = cache_size > 0 ? (uint8_t *) child_page : current_block;
                (*plevel_count)++;
            }
            int16_t search_result = static_cast<T*>(this)->searchCurrentBlock();
            uint8_t *child_ptr_loc = static_cast<T*>(this)->getChildPtrPos(search_result);
            uint8_t *child_ptr;
            if (cache_size > 0) {
                child_page = getChildPage(child_ptr_loc);
                child_ptr = cache->get_disk_page_in_cache(child_page);
            } else
                child_ptr = getChildPtr(child_ptr_loc);
            static_cast<T*>(this)->setCurrentBlock(child_ptr);
        }
        return static_cast<T*>(this)->searchCurrentBlock();
    }

    uint8_t *getLastPtr();
    uint8_t *getChildPtrPos(int16_t search_result);
    inline uint8_t *getChildPtr(uint8_t *ptr) {
        ptr += (*ptr + 1);
        return (uint8_t *) util::bytesToPtr(ptr);
    }

    inline int getChildPage(uint8_t *ptr) {
        ptr += (*ptr + 1);
        return util::bytesToPtr(ptr);
    }

    inline int16_t filledSize() {
        return util::getInt(BPT_FILLED_SIZE);
    }

    inline uint16_t getKVLastPos() {
        return util::getInt(BPT_LAST_DATA_PTR);
    }

    uint8_t *allocateBlock(int size) {
        if (cache_size > 0) {
            uint8_t *new_page = cache->get_new_page(current_block);
            *new_page = 0x02; // Set changed so it gets written next time
            return new_page;
        }
        return (uint8_t *) util::alignedAlloc(size);
    }

    char *put(const char *key, uint8_t key_len, const char *value,
            int16_t value_len, int16_t *pValueLen = NULL) {
        static_cast<T*>(this)->setCurrentBlockRoot();
        this->key = key;
        this->key_len = key_len;
        if (max_key_len < key_len)
            max_key_len = key_len;
        this->value = value;
        this->value_len = value_len;
        if (filledSize() == 0) {
            static_cast<T*>(this)->addFirstData();
            setChanged(1);
        } else {
            uint8_t *node_paths[9];
            int8_t level_count = 1;
            int16_t search_result = isLeaf() ?
                    static_cast<T*>(this)->searchCurrentBlock() :
                    traverseToLeaf(&level_count, node_paths);
            numLevels = level_count;
            if (search_result >= 0 && pValueLen != NULL)
                return getValueAt(pValueLen);
            recursiveUpdate(search_result, node_paths, level_count - 1);
        }
        total_size++;
        return NULL;
    }

    void recursiveUpdate(int16_t search_result, uint8_t *node_paths[], uint8_t level) {
        //int16_t search_result = pos; // lastSearchPos[level];
        if (search_result < 0) {
            search_result = ~search_result;
            if (static_cast<T*>(this)->isFull(search_result)) {
                updateSplitStats();
                uint8_t first_key[BPT_MAX_KEY_LEN]; // is max_pfx_len sufficient?
                int16_t first_len;
                uint8_t *old_block = current_block;
                uint8_t *new_block = static_cast<T*>(this)->split(first_key, &first_len);
                setChanged(1);
                int new_page = 0;
                if (cache_size > 0)
                    new_page = cache->get_page_count() - 1;
                int16_t cmp = util::compare((char *) first_key, first_len,
                        key, key_len);
                if (cmp <= 0)
                    static_cast<T*>(this)->setCurrentBlock(new_block);
                search_result = ~static_cast<T*>(this)->searchCurrentBlock();
                static_cast<T*>(this)->addData(search_result);
                //cout << "FK:" << level << ":" << first_key << endl;
                if (root_block == old_block) {
                    blockCountNode++;
                    int old_page = 0;
                    if (cache_size > 0) {
                        old_block = cache->get_new_page(new_block);
                        old_page = cache->get_page_count() - 1;
                        memcpy(old_block, root_block, parent_block_size);
                        *old_block |= 0x02;
                    } else
                        root_block = (uint8_t *) util::alignedAlloc(parent_block_size);
                    static_cast<T*>(this)->setCurrentBlock(root_block);
                    static_cast<T*>(this)->initCurrentBlock();
                    setLeaf(0);
                    setChanged(1);
                    if (getKVLastPos() == leaf_block_size)
                        setKVLastPos(parent_block_size);
                    uint8_t addr[9];
                    key = "";
                    key_len = 1;
                    value = (char *) addr;
                    value_len = util::ptrToBytes(cache_size > 0 ? (unsigned long) old_page : (unsigned long) old_block, addr);
                    //printf("value: %d, value_len1:%d\n", old_page, value_len);
                    static_cast<T*>(this)->addFirstData();
                    key = (char *) first_key;
                    key_len = first_len;
                    value = (char *) addr;
                    value_len = util::ptrToBytes(cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block, addr);
                    //printf("value: %d, value_len2:%d\n", new_page, value_len);
                    search_result = ~static_cast<T*>(this)->searchCurrentBlock();
                    static_cast<T*>(this)->addData(search_result);
                    numLevels++;
                } else {
                    int16_t prev_level = level - 1;
                    uint8_t *parent_data = cache_size > 0 ? cache->get_disk_page_in_cache((unsigned long)node_paths[prev_level]) : node_paths[prev_level];
                    static_cast<T*>(this)->setCurrentBlock(parent_data);
                    uint8_t addr[9];
                    key = (char *) first_key;
                    key_len = first_len;
                    value = (char *) addr;
                    value_len = util::ptrToBytes(cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block, addr);
                    //printf("value: %d, value_len3:%d\n", new_page, value_len);
                    search_result = static_cast<T*>(this)->searchCurrentBlock();
                    recursiveUpdate(search_result, node_paths, prev_level);
                }
            } else {
                static_cast<T*>(this)->addData(search_result);
                setChanged(1);
            }
        } else {
            //if (isLeaf()) {
            //    this->key_at += this->key_at_len;
            //    if (*key_at == this->value_len) {
            //        memcpy((char *) key_at + 1, this->value, this->value_len);
            //}
        }
    }

    bool isFull(int16_t search_result);
    void addFirstData();
    void addData(int16_t search_result);
    void insertCurrent();

    inline void setFilledSize(int16_t filledSize) {
        util::setInt(BPT_FILLED_SIZE, filledSize);
    }

    inline void insPtr(int16_t pos, uint16_t kv_pos) {
        int16_t filledSz = filledSize();
#if BPT_9_BIT_PTR == 1
        uint8_t *kvIdx = static_cast<T*>(this)->getPtrPos() + pos;
        memmove(kvIdx + 1, kvIdx, filledSz - pos);
        *kvIdx = kv_pos;
#if BPT_INT64MAP == 1
        insBit(bitmap, pos, kv_pos);
#else
        if (pos & 0xFFE0) {
            insBit(bitmap2, pos - 32, kv_pos);
        } else {
            uint8_t last_bit = (*bitmap1 & 0x01);
            insBit(bitmap1, pos, kv_pos);
            *bitmap2 >>= 1;
            if (last_bit)
            *bitmap2 |= MASK32(0);
        }
#endif
#else
        uint8_t *kvIdx = static_cast<T*>(this)->getPtrPos() + (pos << 1);
        memmove(kvIdx + 2, kvIdx, (filledSz - pos) * 2);
        util::setInt(kvIdx, kv_pos);
#endif
        setFilledSize(filledSz + 1);

    }

    inline void setPtr(int16_t pos, uint16_t ptr) {
#if BPT_9_BIT_PTR == 1
        *(static_cast<T*>(this)->getPtrPos() + pos) = ptr;
#if BPT_INT64MAP == 1
        if (ptr >= 256)
        *bitmap |= MASK64(pos);
        else
        *bitmap &= ~MASK64(pos);
#else
        if (pos & 0xFFE0) {
            pos -= 32;
            if (ptr >= 256)
            *bitmap2 |= MASK32(pos);
            else
            *bitmap2 &= ~MASK32(pos);
        } else {
            if (ptr >= 256)
            *bitmap1 |= MASK32(pos);
            else
            *bitmap1 &= ~MASK32(pos);
        }
#endif
#else
        uint8_t *kvIdx = static_cast<T*>(this)->getPtrPos() + (pos << 1);
        return util::setInt(kvIdx, ptr);
#endif
    }

    inline void updateSplitStats() {
        if (isLeaf()) {
            maxKeyCountLeaf += filledSize();
            blockCountLeaf++;
        } else {
            maxKeyCountNode += filledSize();
            blockCountNode++;
        }
    }

    inline void setLeaf(char isLeaf) {
        if (isLeaf)
            current_block[0] |= 0x01;
        else
            current_block[0] &= 0xFE;
    }

    inline void setChanged(char isChanged) {
        if (isChanged)
            current_block[0] |= 0x02;
        else
            current_block[0] &= 0xFD;
    }

    inline uint8_t isChanged() {
        return current_block[0] & 0x02;
    }

    inline void setKVLastPos(uint16_t val) {
        util::setInt(BPT_LAST_DATA_PTR, val);
    }

    inline void insBit(uint32_t *ui32, int pos, uint16_t kv_pos) {
        uint32_t ryte_part = (*ui32) & RYTE_MASK32(pos);
        ryte_part >>= 1;
        if (kv_pos >= 256)
            ryte_part |= MASK32(pos);
        (*ui32) = (ryte_part | ((*ui32) & LEFT_MASK32(pos)));

    }

    uint8_t *split(uint8_t *first_key, int16_t *first_len_ptr);
#if BPT_INT64MAP == 1
    inline void insBit(uint64_t *ui64, int pos, uint16_t kv_pos) {
        uint64_t ryte_part = (*ui64) & RYTE_MASK64(pos);
        ryte_part >>= 1;
        if (kv_pos >= 256)
            ryte_part |= MASK64(pos);
        (*ui64) = (ryte_part | ((*ui64) & LEFT_MASK64(pos)));

    }
#endif

    int getNumLevels() {
        return numLevels;
    }

    void printStats(long num_entries) {
        util::print("Block Count:");
        util::print((long) blockCountNode);
        util::print(", ");
        util::print((long) blockCountLeaf);
        util::endl();
        util::print("Avg Block Count:");
        util::print((long) (num_entries / blockCountLeaf));
        util::endl();
        util::print("Avg Max Count:");
        util::print((long) (maxKeyCountNode / (blockCountNode ? blockCountNode : 1)));
        util::print(", ");
        util::print((long) (maxKeyCountLeaf / blockCountLeaf));
        util::print(", ");
        util::print((long) max_key_len);
        util::endl();
    }
    void printNumLevels() {
        util::print("Level Count:");
        util::print((long) numLevels);
        util::endl();
    }
    void printCounts() {
        util::print("Count1:");
        util::print(count1);
        util::print(", Count2:");
        util::print(count2);
        util::print("\n");
        util::endl();
    }
    long size() {
        return total_size;
    }
    int get_max_key_len() {
        return max_key_len;
    }
    cache_stats get_cache_stats() {
        return cache->get_cache_stats();
    }

};

template<class T>
class bpt_trie_handler: public bplus_tree_handler<T> {

protected:
    int maxTrieLenNode;
    int maxTrieLenLeaf;

    bpt_trie_handler<T>(uint16_t leaf_block_sz = DEFAULT_LEAF_BLOCK_SIZE,
            uint16_t parent_block_sz = DEFAULT_PARENT_BLOCK_SIZE, int cache_sz = 0,
            const char *fname = NULL, uint8_t *block = NULL) :
       bplus_tree_handler<T>(leaf_block_sz, parent_block_sz, cache_sz, fname, block) {
        init_stats();
    }

    void init_stats() {
        maxTrieLenLeaf = maxTrieLenNode = 0;
    }

    inline void updateSplitStats() {
        if (bplus_tree_handler<T>::isLeaf()) {
            bplus_tree_handler<T>::maxKeyCountLeaf += bplus_tree_handler<T>::filledSize();
            maxTrieLenLeaf += bplus_tree_handler<T>::BPT_TRIE_LEN + (*(bplus_tree_handler<T>::BPT_TRIE_LEN_PTR) << 8);
            bplus_tree_handler<T>::blockCountLeaf++;
        } else {
            bplus_tree_handler<T>::maxKeyCountNode += bplus_tree_handler<T>::filledSize();
            maxTrieLenNode += bplus_tree_handler<T>::BPT_TRIE_LEN + (*(bplus_tree_handler<T>::BPT_TRIE_LEN_PTR) << 8);
            bplus_tree_handler<T>::blockCountNode++;
        }
    }

    inline void delAt(uint8_t *ptr) {
        bplus_tree_handler<T>::BPT_TRIE_LEN--;
        memmove(ptr, ptr + 1, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
    }

    inline void delAt(uint8_t *ptr, int16_t count) {
        bplus_tree_handler<T>::BPT_TRIE_LEN -= count;
        memmove(ptr, ptr + count, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
    }

    inline uint8_t insAt(uint8_t *ptr, uint8_t b) {
        memmove(ptr + 1, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        *ptr = b;
        bplus_tree_handler<T>::BPT_TRIE_LEN++;
        return 1;
    }

    inline uint8_t insAt(uint8_t *ptr, uint8_t b1, uint8_t b2) {
        memmove(ptr + 2, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        *ptr++ = b1;
        *ptr = b2;
        bplus_tree_handler<T>::BPT_TRIE_LEN += 2;
        return 2;
    }

    inline uint8_t insAt(uint8_t *ptr, uint8_t b1, uint8_t b2, uint8_t b3) {
        memmove(ptr + 3, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        *ptr++ = b1;
        *ptr++ = b2;
        *ptr = b3;
        bplus_tree_handler<T>::BPT_TRIE_LEN += 3;
        return 3;
    }

    inline uint8_t insAt(uint8_t *ptr, uint8_t b1, uint8_t b2, uint8_t b3, uint8_t b4) {
        memmove(ptr + 4, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        *ptr++ = b1;
        *ptr++ = b2;
        *ptr++ = b3;
        *ptr = b4;
        bplus_tree_handler<T>::BPT_TRIE_LEN += 4;
        return 4;
    }

    inline void insAt(uint8_t *ptr, uint8_t b, const char *s, uint8_t len) {
        memmove(ptr + 1 + len, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        *ptr++ = b;
        memcpy(ptr, s, len);
        bplus_tree_handler<T>::BPT_TRIE_LEN += len;
        bplus_tree_handler<T>::BPT_TRIE_LEN++;
    }

    inline void insAt(uint8_t *ptr, const char *s, uint8_t len) {
        memmove(ptr + len, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        memcpy(ptr, s, len);
        bplus_tree_handler<T>::BPT_TRIE_LEN += len;
    }

    void insBytes(uint8_t *ptr, int16_t len) {
        memmove(ptr + len, ptr, trie + bplus_tree_handler<T>::BPT_TRIE_LEN - ptr);
        bplus_tree_handler<T>::BPT_TRIE_LEN += len;
    }

    inline void setAt(uint8_t pos, uint8_t b) {
        trie[pos] = b;
    }

    inline void append(uint8_t b) {
        trie[bplus_tree_handler<T>::BPT_TRIE_LEN++] = b;
    }

    inline void appendPtr(uint16_t p) {
        util::setInt(trie + bplus_tree_handler<T>::BPT_TRIE_LEN, p);
        bplus_tree_handler<T>::BPT_TRIE_LEN += 2;
    }

    void append(const char *s, int16_t need_count) {
        memcpy(trie + bplus_tree_handler<T>::BPT_TRIE_LEN, s, need_count);
        bplus_tree_handler<T>::BPT_TRIE_LEN += need_count;
    }

public:
    uint8_t *trie;
    uint8_t *triePos;
    uint8_t *origPos;
    uint8_t need_count;
    int16_t insertState;
    uint8_t keyPos;
    static const uint8_t x00 = 0;
    static const uint8_t x01 = 1;
    static const uint8_t x02 = 2;
    static const uint8_t x03 = 3;
    static const uint8_t x04 = 4;
    static const uint8_t x05 = 5;
    static const uint8_t x06 = 6;
    static const uint8_t x07 = 7;
    static const uint8_t x08 = 8;
    static const uint8_t x0F = 0x0F;
    static const uint8_t x10 = 0x10;
    static const uint8_t x11 = 0x11;
    static const uint8_t x3F = 0x3F;
    static const uint8_t x40 = 0x40;
    static const uint8_t x41 = 0x41;
    static const uint8_t x55 = 0x55;
    static const uint8_t x7F = 0x7F;
    static const uint8_t x80 = 0x80;
    static const uint8_t x81 = 0x81;
    static const uint8_t xAA = 0xAA;
    static const uint8_t xBF = 0xBF;
    static const uint8_t xC0 = 0xC0;
    static const uint8_t xF8 = 0xF8;
    static const uint8_t xFB = 0xFB;
    static const uint8_t xFC = 0xFC;
    static const uint8_t xFD = 0xFD;
    static const uint8_t xFE = 0xFE;
    static const uint8_t xFF = 0xFF;
    static const int16_t x100 = 0x100;
    ~bpt_trie_handler() {}
    void printStats(long num_entries) {
        bplus_tree_handler<T>::printStats(num_entries);
        util::print("Avg Trie Len:");
        util::print((long) (maxTrieLenNode
                / (bplus_tree_handler<T>::blockCountNode ? bplus_tree_handler<T>::blockCountNode : 1)));
        util::print(", ");
        util::print((long) (maxTrieLenLeaf / bplus_tree_handler<T>::blockCountLeaf));
        util::endl();
    }
    void initCurrentBlock() {
        //cout << "Trie init block" << endl;
        bplus_tree_handler<T>::initCurrentBlock();
        *(bplus_tree_handler<T>::BPT_TRIE_LEN_PTR) = 0;
        bplus_tree_handler<T>::BPT_TRIE_LEN = 0;
        bplus_tree_handler<T>::BPT_MAX_PFX_LEN = 1;
        keyPos = 1;
        insertState = INSERT_EMPTY;
    }

};

#endif
