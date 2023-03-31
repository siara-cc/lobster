#ifndef LOGGER_H
#define LOGGER_H

#include <sys/stat.h>
#include <iostream>
#include <set>

#include "bfos.h"
#include "basix.h"
#include "sqlite.h"
#include "bloom.hpp"

//#define STAGING_BLOCK_SIZE 524288
#define STAGING_BLOCK_SIZE 262144
//#define STAGING_BLOCK_SIZE 65536
//#define STAGING_BLOCK_SIZE 32768
#define BUCKET_BLOCK_SIZE 4096

#define BUCKET_COUNT 2

//typedef vector<basix *> cache_more;
typedef std::vector<sqlite *> cache_more;
typedef std::vector<bloom_filter *> cache_more_bf;

class logger {
    protected:
      basix *idx0;
      //basix *idx1;
      sqlite *idx1;
      bloom_filter *bf_idx1;
      cache_more idx1_more;
      cache_more_bf bf_idx1_more;
#if BUCKET_COUNT == 2
      //basix *idx2;
      sqlite *idx2;
      bloom_filter *bf_idx2;
#endif
      bool is_cache0_full;
      int cache0_size;
      int cache0_page_count;
      int cache1_size;
      int cache2_size;
      int cache_more_size;
      long idx1_count_limit_mil;
      std::string idx1_name;
      std::string bf_idx1_name;
      std::string bf_idx2_name;
      bool use_bloom;

      int *flush_counts;
      long no_of_inserts;
      int zero_count;
      long *idx_more_found_counts;
      long *idx_more_pve_counts;
      long *idx_more_lookup_counts;

    public:
        logger(const char *fname, size_t cache_size_mb) {
            use_bloom = true;
            char fname0[strlen(fname) + 5];
            char fname1[strlen(fname) + 5];
            strcpy(fname0, fname);
            strcpy(fname1, fname);
            strcat(fname0, ".ix0");
            strcat(fname1, ".ix1");
            idx1_name = fname1;
            bf_idx1_name = fname1;
            bf_idx1_name += ".blm";
            cache0_size = (cache_size_mb > 0xFF ? cache_size_mb & 0xFF : cache_size_mb) * 16;
            cache0_size *= 1024;
            idx1_count_limit_mil = (cache_size_mb > 0xFF ? (cache_size_mb >> 8) & 0xFF : 250);
            cache1_size = (cache_size_mb > 0xFFFF ? (cache_size_mb >> 16) & 0xFF : cache_size_mb & 0xFF) * 16;
            cache1_size *= 1024;
            cache_more_size = (cache_size_mb > 0xFFFFFF ? (cache_size_mb >> 24) & 0x0F : (cache_size_mb & 0xFF) / (cache_size_mb < 4 ? 2 : 4)) * 16;
            cache_more_size *= 1024;
            idx0 = new basix(STAGING_BLOCK_SIZE, STAGING_BLOCK_SIZE, cache0_size, fname0);
            //idx1 = new basix(BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache1_size, fname1);
            idx1 = new sqlite(2, 1, "key, value", "imain", BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache1_size, fname1);
            if (use_bloom) {
                bf_idx1 = new bloom_filter;
                if (file_exists(bf_idx1_name.c_str()))
                    bf_idx1->import(bf_idx1_name.c_str());
                else
                    bf_idx1->init(idx1_count_limit_mil * 1000000L, 0.005);
            }
            idx1->to_demote_blocks = false;
            bool more_files = true;
            size_t more_count = 0;
            while (more_files) {
                char new_name[idx1_name.length() + 10];
                sprintf(new_name, "%s.%lu", idx1_name.c_str(), more_count + 1);
                char bf_new_name[bf_idx1_name.length() + 10];
                sprintf(bf_new_name, "%s.%lu.blm", idx1_name.c_str(), idx1_more.size() + 1);
                if (file_exists(new_name)) {
                    //idx1_more.push_back(new basix(BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache_more_size, new_name));
                    idx1_more.push_back(new sqlite(2, 1, "key, value", "imain", BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache_more_size, new_name));
                    if (use_bloom) {
                        bloom_filter *new_bf = new bloom_filter;
                        new_bf->import(bf_new_name);
                        bf_idx1_more.push_back(new_bf);
                    }
                } else {
                    more_files = false;
                    break;
                }
                more_count++;
            }
            std::cout << "Stg buf: " << cache0_size << "mb, Idx1 buf: " << cache1_size << "mb, Idx1+ buf: " << cache_more_size << "mb" << std::endl;
            std::cout << "Idx1 entry count limit: " << idx1_count_limit_mil << " million";
#if BUCKET_COUNT == 2
            char fname2[strlen(fname) + 5];
            strcpy(fname2, fname);
            strcat(fname2, ".ix2");
            bf_idx2_name = fname2;
            bf_idx2_name += ".blm";
            if (use_bloom) {
                bf_idx2 = new bloom_filter;
                if (file_exists(bf_idx2_name.c_str()))
                    bf_idx2->import(bf_idx2_name.c_str());
                else
                    bf_idx2->init(30000000L, 0.005);
            }
            cache2_size = (cache_size_mb > 0xFFFFFFF ? (cache_size_mb >> 28) & 0x0F : (cache_size_mb & 0xFF)) * 16;
            cache2_size *= 1024;
            std::cout << ", Idx2 buf: " << cache2_size << "mb" << std::endl;
            //idx2 = new basix(BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache2_size, fname2);
            idx2 = new sqlite(2, 1, "key, value", "imain", BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache2_size, fname2);
#else
            cout << endl;
#endif
            is_cache0_full = false;
            cache0_page_count = cache0_size * 1024 * 1024 / STAGING_BLOCK_SIZE;
            flush_counts = new int[cache0_page_count];
            memset(flush_counts, '\0', sizeof(int) * cache0_page_count);
            idx_more_found_counts = new long[50];
            idx_more_pve_counts = new long[50];
            idx_more_lookup_counts = new long[50];
            memset(idx_more_found_counts, '\0', sizeof(long) * 50);
            memset(idx_more_pve_counts, '\0', sizeof(long) * 50);
            memset(idx_more_lookup_counts, '\0', sizeof(long) * 50);
            no_of_inserts = 0;
            zero_count = cache0_page_count;
        }

        ~logger() {
            delete idx0;
            delete idx1;
            if (use_bloom) {
                bf_idx1->bf_export(bf_idx1_name.c_str());
                bf_idx1->destroy();
                delete bf_idx1;
            }
#if BUCKET_COUNT == 2
            delete idx2;
            if (use_bloom) {
                bf_idx2->bf_export(bf_idx2_name.c_str());
                bf_idx2->destroy();
                delete bf_idx2;
            }
#endif
            for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++)
                delete *it;
            if (use_bloom) {
                for (cache_more_bf::iterator it = bf_idx1_more.begin(); it != bf_idx1_more.end(); it++) {
                    (*it)->destroy();
                    delete *it;
                }
            }
            delete flush_counts;
            delete idx_more_found_counts;
            delete idx_more_pve_counts;
            delete idx_more_lookup_counts;
        }

        bool file_exists (const char *filename) {
          struct stat buffer;   
          return (stat (filename, &buffer) == 0);
        }

        void spawn_more_idx1_if_full() {
            if (cache_more_size > 0 && idx1->size() >= idx1_count_limit_mil * 1000000L) {
                delete idx1;
                if (use_bloom) {
                    bf_idx1->bf_export(bf_idx1_name.c_str());
                }
                char new_name[idx1_name.length() + 10];
                sprintf(new_name, "%s.%lu", idx1_name.c_str(), idx1_more.size() + 1);
                char bf_new_name[bf_idx1_name.length() + 10];
                sprintf(bf_new_name, "%s.%lu.blm", idx1_name.c_str(), idx1_more.size() + 1);
                if (rename(idx1_name.c_str(), new_name))
                    std::cout << "Error renaming file from: " << idx1_name << " to: " << new_name << std::endl;
                else {
                    if (use_bloom && rename(bf_idx1_name.c_str(), bf_new_name))
                        std::cout << "Error renaming file from: " << bf_idx1_name << " to: " << bf_new_name << std::endl;
                    else {
                        //idx1_more.insert(idx1_more.begin(), new basix(BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache_more_size, new_name));
                        //idx1 = new basix(BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache1_size, idx1_name.c_str());
                        idx1_more.insert(idx1_more.begin(), new sqlite(2, 1, "key, value", "imain", BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache_more_size, new_name));
                        idx1 = new sqlite(2, 1, "key, value", "imain", BUCKET_BLOCK_SIZE, BUCKET_BLOCK_SIZE, cache1_size, idx1_name.c_str());
                        if (use_bloom) {
                            bf_idx1_more.insert(bf_idx1_more.begin(), bf_idx1);
                            bf_idx1 = new bloom_filter;
                            bf_idx1->init(idx1_count_limit_mil * 1000000L, 0.005);
                            int count = idx1_more.size();
                            while (count--) {
                                idx_more_found_counts[count + BUCKET_COUNT] = idx_more_found_counts[count + BUCKET_COUNT - 1];
                                idx_more_pve_counts[count + BUCKET_COUNT] = idx_more_pve_counts[count + BUCKET_COUNT - 1];
                                idx_more_lookup_counts[count + BUCKET_COUNT] = idx_more_lookup_counts[count + BUCKET_COUNT - 1];
                            }
                            idx_more_found_counts[BUCKET_COUNT - 1] = 0;
                            idx_more_pve_counts[BUCKET_COUNT - 1] = 0;
                            idx_more_lookup_counts[BUCKET_COUNT - 1] = 0;
                        }
                    }
                }
            }
        }

        void remove_entry_from_more_idxs(const uint8_t *key, uint8_t key_len) {
            uint8_t *val;
            int val_len;
            cache_more_bf::iterator it_bf;
            if (use_bloom)
                it_bf = bf_idx1_more.begin();
            for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++) {
                if (!use_bloom || (use_bloom && (*it_bf)->check_uint8_str(key, key_len) != BLOOM_FAILURE)) {
                    //cout << it-idx1_more.begin() << " ";
                    //cout << "Found in idx_more" << it-idx1_more.begin() << endl;
                    if ((*it)->get(key, key_len, &val_len)) {
                        (*it)->remove_found_entry();
                        return;
                    }
                }
                it_bf++;
            }
        }

        void remove_entry_from_idx1_more(const uint8_t *key, uint8_t key_len) {
            uint8_t *val;
            int val_len;
            if (!use_bloom || (use_bloom && bf_idx1->check_uint8_str(key, key_len) != BLOOM_FAILURE)) {
                if (idx1->get(key, key_len, &val_len)) {
                    idx1->remove_found_entry();
                    return;
                }
                //  if (val != NULL && idx1->is_changed()) {
                //     //cout << "Found in idx1 " << endl;
                //      return val;
                //  }
            }
            remove_entry_from_more_idxs(key, key_len);
        }

        bool put(const char *key, uint8_t key_len, const char *value, int value_len) {
            return put((const uint8_t *) key, key_len, (const uint8_t *) value, value_len);
        }

        bool put(const uint8_t *key, uint8_t key_len, const uint8_t *value, int value_len) {
            no_of_inserts++;
            if (no_of_inserts % 5000000 == 0) {
                //for (int i = 0; i < cache0_page_count; i++)
                //    printf("%2x", flush_counts[i]);
                //cout << endl;
                std::cout << "Idx1+ lookups: ";
                std::cout << idx_more_lookup_counts[0] << " ";
                if (BUCKET_COUNT == 2)
                    std::cout << idx_more_lookup_counts[1] << " ";
                for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++) {
                    std::cout << idx_more_lookup_counts[it - idx1_more.begin() + BUCKET_COUNT] << " ";
                }
                std::cout << std::endl;
                if (use_bloom) {
                    std::cout << "Idx1+ positiv: ";
                    std::cout << idx_more_pve_counts[0] << " ";
                    if (BUCKET_COUNT == 2)
                        std::cout << idx_more_pve_counts[1] << " ";
                    for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++) {
                        std::cout << idx_more_pve_counts[it - idx1_more.begin() + BUCKET_COUNT] << " ";
                    }
                    std::cout << std::endl;
                }
                std::cout << "Idx1+ found  : ";
                std::cout << idx_more_found_counts[0] << " ";
                if (BUCKET_COUNT == 2)
                    std::cout << idx_more_found_counts[1] << " ";
                for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++) {
                    std::cout << idx_more_found_counts[it - idx1_more.begin() + BUCKET_COUNT] << " ";
                }
                std::cout << std::endl;
                memset(flush_counts, '\0', sizeof(int) * cache0_page_count);
                zero_count = cache0_page_count;
            }
            if (idx0->cache->cache_size_in_pages <= idx0->cache->file_page_count) {
                is_cache0_full = true;
            }
            int out_val_len = value_len + 1;
            uint8_t val[value_len + 1];
            bool is_found_in_idx0 = idx0->get(key, key_len, &out_val_len, val);
            idx0->set_value(value, value_len + 1);
            bool is_full = idx0->is_full(0);
            if (is_full && is_cache0_full) {
                int target_size = idx0->filled_size() / 3;
                int cur_count = 1;
                int next_min = 255;
                while (idx0->filled_size() > target_size) {
                    for (int i = 0; i < idx0->filled_size(); i++) {
                        uint32_t src_idx = idx0->get_ptr(i);
                        int k_len = idx0->current_block[src_idx];
                        uint8_t *k = idx0->current_block + src_idx + 1;
                        int v_len = idx0->current_block[src_idx + k_len + 1];
                        uint8_t *v = idx0->current_block + src_idx + k_len + 2;
                        if (v_len != value_len + 1)
                            std::cout << "src_idx: " << src_idx << ", vlen: " << v_len << " ";
                        int entry_count = v[v_len - 1];
                        if (v_len != value_len + 1) {
                            std::cout << entry_count << std::endl;
                            printf("k: %.*s, len: %d\n", k_len, k, k_len);
                        }
                        if (entry_count <= cur_count) {
                            int v1_len;
                            uint8_t v1[value_len + 1];
#if BUCKET_COUNT == 2
                            if (entry_count <= 1) {
                                bool is_inserted = !idx1->put(k, k_len, v, v_len - 1);
                                if (use_bloom && is_inserted)
                                    bf_idx1->add_uint8_str(k, k_len);
                                //remove_entry_from_more_idxs(k, k_len);
                                // if (v1 != NULL)
                                //     memcpy(v1, v, v_len - 1);
                                spawn_more_idx1_if_full();
                            } else {
                                bool is_inserted = !idx2->put(k, k_len, v, v_len - 1);
                                if (use_bloom && is_inserted)
                                    bf_idx2->add_uint8_str(k, k_len);
                                //printf("Key: %.*s, %d,%d,%d,%d,%d\n", k_len, k, v[0], v[1], v[2], v[4], v[5]);
                                //remove_entry_from_idx1_more(k, k_len);
                            }
#else
                            v1 = idx1->put(k, k_len, v, v_len - 1, &v1_len);
                            if (use_bloom && v1 == NULL)
                                bf_idx1->add_uint8_str(k, k_len);
                            remove_entry_from_more_idxs(k, k_len);
                            // if (v1 != NULL)
                            //     memcpy(v1, v, v_len - 1);
                            spawn_more_idx1_if_full();
#endif
                            idx0->remove_entry(i);
                            i--;
                        } else {
                            if (entry_count < next_min)
                                next_min = entry_count;
                            if (entry_count > 2)
                                v[v_len - 1]--;
                        }
                        if (idx0->filled_size() <= target_size && cur_count > 1)
                            break;
                    }
                    cur_count = (cur_count == next_min ? 255 : next_min);
                    //cout << "next_min: " << next_min << endl;
                }
                //if (idx0->filled_size() > 0)
                //    cout << "Not emptied" << endl;
                idx0->make_space();
                // if (idx0->current_block != idx0->root_block) {
                //     int idx = (idx0->current_block - idx0->cache->page_cache)/STAGING_BLOCK_SIZE;
                //     if (idx < cache0_page_count && flush_counts[idx] == 0 && zero_count)
                //         zero_count--;
                //     if (flush_counts[idx] < 255)
                //         flush_counts[idx]++;
                // }
            }
            int new_val_len = value_len;
            uint8_t new_val[new_val_len + 1];
            memcpy(new_val, value, new_val_len);
            if (is_found_in_idx0) {
                uint8_t entry_count = val[new_val_len];
                new_val[new_val_len] = entry_count < 255 ? entry_count + 1 : 255;
            } else
                new_val[new_val_len] = 1;
            new_val_len++;
            //cout << "Found in idx0: " << entry_count << endl;
            return idx0->put(key, key_len, new_val, new_val_len);
        }

        bool get(const char *key, uint8_t key_len, int *out_value_len, char *val) {
            return get((const uint8_t *) key, key_len, out_value_len, (uint8_t *) val);
        }

        bool get(const uint8_t *key, uint8_t key_len, int *in_size_out_value_len, uint8_t *val) {
            bool is_found = idx0->get(key, key_len, in_size_out_value_len, val);
            if (is_found) {
                return true;
            }
#if BUCKET_COUNT == 2
            if (!is_found) {
                idx_more_lookup_counts[0]++;
                if (!use_bloom || (use_bloom && bf_idx2->check_uint8_str(key, key_len) != BLOOM_FAILURE)) {
                    idx_more_pve_counts[0]++;
                    is_found = idx2->get(key, key_len, in_size_out_value_len, val);
                    if (is_found)
                        idx_more_found_counts[0]++;
                }
            }
#endif
            if (!is_found) {
                idx_more_lookup_counts[BUCKET_COUNT - 1]++;
                if (!use_bloom || (use_bloom && bf_idx1->check_uint8_str(key, key_len) != BLOOM_FAILURE)) {
                    idx_more_pve_counts[BUCKET_COUNT - 1]++;
                    is_found = idx1->get(key, key_len, in_size_out_value_len, val);
                    if (is_found)
                        idx_more_found_counts[BUCKET_COUNT - 1]++;
                }
            }
            if (!is_found) {
                cache_more_bf::iterator it_bf;
                if (use_bloom)
                    it_bf = bf_idx1_more.begin();
                for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++) {
                    idx_more_lookup_counts[it - idx1_more.begin() + BUCKET_COUNT]++;
                    if (!use_bloom || (use_bloom && (*it_bf)->check_uint8_str(key, key_len) != BLOOM_FAILURE)) {
                       idx_more_pve_counts[it - idx1_more.begin() + BUCKET_COUNT]++;
                       is_found = (*it)->get(key, key_len, in_size_out_value_len, val);
                       if (is_found) {
                           idx_more_found_counts[it - idx1_more.begin() + BUCKET_COUNT]++;
                           break;
                       }
                    }
                    it_bf++;
                }
            }
            return is_found;
        }

        cache_stats get_cache_stats() {
            return idx1->get_cache_stats();
        }
        int get_max_key_len() {
#if BUCKET_COUNT == 2
            return std::max(std::max(idx0->get_max_key_len(), idx1->get_max_key_len()), idx2->get_max_key_len());
#else
            return max(idx0->get_max_key_len(), idx1->get_max_key_len());
#endif
        }
        int get_num_levels() {
            return idx1->get_num_levels();
        }
        void print_stats(long sz) {
            idx0->print_stats(idx0->size());
            idx1->print_stats(idx1->size());
            if (use_bloom)
                bf_idx1->stats();
            cache_more_bf::iterator it_bf = bf_idx1_more.begin();
            for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++) {
            	(*it)->print_stats((*it)->size());
                if (use_bloom)
                    (*it_bf)->stats();
                it_bf++;
            }
#if BUCKET_COUNT == 2
            idx2->print_stats(idx2->size());
            if (use_bloom)
                bf_idx2->stats();
#endif
        }
        void print_num_levels() {
            idx0->print_num_levels();
            idx1->print_num_levels();
            for (cache_more::iterator it = idx1_more.begin(); it != idx1_more.end(); it++)
            	(*it)->print_num_levels();
#if BUCKET_COUNT == 2
            idx2->print_num_levels();
#endif
        }
        long size() {
#if BUCKET_COUNT == 2
            return idx0->size() + idx1->size() + idx2->size();
#else
            return idx0->size() + idx1->size();
#endif
        }
        long filled_size() {
#if BUCKET_COUNT == 2
            return idx0->filled_size() + idx1->filled_size() + idx2->size();
#else
            return idx0->filled_size() + idx1->filled_size();
#endif
        }

};
#endif // LOGGER_H
