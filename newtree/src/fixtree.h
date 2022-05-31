#ifndef __MORPHTREE2_FIXTREE_H__
#define __MORPHTREE2_FIXTREE_H__

#include <cstdio>
#include <cstring>
#include <cmath>
#include <cstdlib>

#include "../include/util.h"

namespace newtree {

static const int PIVOT_CARD = (64 / sizeof(_key_t));
static const int INNER_CARD = PIVOT_CARD * PIVOT_CARD;
static const int LEAF_CARD  = 8;
static const int LEAF_REBUILD_CARD  = 3;
static const int MAX_HEIGHT = 10;

class Fixtree {
public:
    struct INode {
        _key_t keys[PIVOT_CARD + INNER_CARD];

        void Set(_key_t * key_in) {
            memcpy(keys + PIVOT_CARD, key_in, INNER_CARD * sizeof(_key_t));
            for(int i = 0; i < PIVOT_CARD; i++) {
                keys[i] = key_in[i * PIVOT_CARD];
            }
        }

        int Lookup(_key_t k) {
            // access the first cache line
            int p;
            for(p = 1; p < PIVOT_CARD; p++) {
                if(keys[p] > k) {
                    break;
                }
            }

            // access the second cache line
            int i;
            for(i = 1 + p * PIVOT_CARD; i < p * PIVOT_CARD + PIVOT_CARD; i++) {
                if(keys[i] > k) {
                    break;
                }
            }
            return i - 1 - PIVOT_CARD;
        }

        void Print() {
            printf("(");
            for(int i = 0; i < INNER_CARD; i++) {
                printf("%12.8lf, ", keys[i + PIVOT_CARD]);
            }
            printf(")\n");
        }
    };

    struct LNode {
        Record recs[LEAF_CARD];

        bool Store(_key_t k, void * p) {
            int i;
            for(i = 0; i < LEAF_CARD; i++) {
                if(recs[i].key > k) {
                    break;
                }
            }

            if(recs[LEAF_CARD - 1].key == MAX_KEY) {
                memmove(&recs[i + 1], &recs[i], sizeof(Record) * (LEAF_CARD - 1 - i));
                recs[i] = {k, p};
                return true;
            } else {
                return false;
            }
        }

        void * Lookup(_key_t k) {
            int i;
            for(i = 1; i < LEAF_CARD; i++) {
                if(recs[i].key > k) {
                    break;
                }
            }

            return (void *)(recs[i - 1].val);
        }

        void Print() {
            printf("(");
            for(int i = 0; i < LEAF_CARD; i++) {
                if(recs[i].key != MAX_KEY)
                    printf("[%12.8lf] ", recs[i].key);
            }
            printf(")\n");
        }
    };

    Fixtree(Record * recs, int num) {
        const int lfary = LEAF_REBUILD_CARD;
        
        leaf_cnt_ = std::ceil((float)num / lfary);
        leaf_nodes_ = (LNode *) malloc(leaf_cnt_ * sizeof(LNode));
        height_ = std::ceil(std::log(std::max(INNER_CARD, leaf_cnt_)) / std::log(INNER_CARD));

        uint32_t innode_cnt = (std::pow(INNER_CARD, height_) - 1) / (INNER_CARD - 1);
        inner_nodes_ = (INode *) malloc(innode_cnt * sizeof(INode));

        // fill leaf nodes
        for(int i = 0; i < leaf_cnt_; i++) {
            LNode &l = leaf_nodes_[i];
            for(int j = 0; j < lfary; j++) {
                auto idx = i * lfary + j;
                leaf_nodes_[i].recs[j].key = idx < num ? recs[idx].key : MAX_KEY;
                leaf_nodes_[i].recs[j].val = idx < num ? recs[idx].val : nullptr;  
            }
            for(int j = lfary; j < LEAF_CARD; j++) { // intialized key
                leaf_nodes_[i].recs[j].key = MAX_KEY;
            }
        }
        
        // fill parent innodes of leaf nodes
        int cur_level_cnt = leaf_cnt_;
        int cur_level_off = innode_cnt - std::pow(INNER_CARD, height_ - 1);
        int last_level_off = 0;
        for(int i = 0; i < cur_level_cnt; i += INNER_CARD) {
            _key_t key_tmp[INNER_CARD];
            for(int j = 0; j < INNER_CARD; j++) {
                auto idx = i + j;
                key_tmp[j] = idx < cur_level_cnt ? leaf_nodes_[idx].recs[0].key : MAX_KEY; 
            }
            inner_nodes_[cur_level_off + i / INNER_CARD].Set(key_tmp);
        }
        
        // fill other inner nodes
        cur_level_cnt = std::ceil((float)cur_level_cnt / INNER_CARD);
        last_level_off = cur_level_off;
        cur_level_off = cur_level_off - std::pow(INNER_CARD, height_ - 2);
        for(int l = height_ - 2; l >= 0; l--) { // level by level
            for(int i = 0; i < cur_level_cnt; i += INNER_CARD) {
                _key_t key_tmp[INNER_CARD];
                for(int j = 0; j < INNER_CARD; j++) {
                    auto idx = i + j;
                    key_tmp[j] = idx < cur_level_cnt ? inner_nodes_[last_level_off + idx].keys[0]: MAX_KEY;
                }
                inner_nodes_[cur_level_off + i / INNER_CARD].Set(key_tmp);
            }

            cur_level_cnt = std::ceil((float)cur_level_cnt / INNER_CARD);
            last_level_off = cur_level_off;
            cur_level_off = cur_level_off - std::pow(INNER_CARD, l - 1);
        }
        
        uint32_t tmp = 0;
        for(int l = 0; l < height_; l++) {
            level_offset_[l] = tmp;
            tmp += std::pow(INNER_CARD, l);
        }
        level_offset_[height_] = tmp;

        return ;
    }

    ~Fixtree() {
        delete inner_nodes_;
        delete leaf_nodes_;
    }

    void * Lookup(_key_t k) {
        int cur_idx = level_offset_[0];
        for(int l = 0; l < height_; l++) {
            int child_id = inner_nodes_[cur_idx].Lookup(k);
            cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + child_id;
        }
        cur_idx -= level_offset_[height_];

        return leaf_nodes_[cur_idx].Lookup(k);
    }

    bool Store(_key_t k, void * p) {
        int cur_idx = level_offset_[0];
        for(int l = 0; l < height_; l++) {
            int child_id = inner_nodes_[cur_idx].Lookup(k);
            cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + child_id;
        }
        cur_idx -= level_offset_[height_];

        return leaf_nodes_[cur_idx].Store(k, p);
    }

    void printAll() {
        for(int l = 0; l < height_; l++) {
            printf("level: %d %d =>", l, level_offset_[l + 1]);
            
            for(int i = level_offset_[l]; i < level_offset_[l + 1]; i++) {
                for(int k = 0; k < l; k++) 
                    printf("\t");
                inner_nodes_[i].Print();
            }
        }
        printf("leafs(%d)", leaf_cnt_);
        for(int i = 0; i < leaf_cnt_; i++) {
            for(int k = 0; k < height_; k++) 
                printf("\t");
            leaf_nodes_[i].Print();
        }
        printf("\n");
    }

    void merge(std::vector<Record> & in, std::vector<Record> & out) {
        uint32_t insize = in.size();

        uint32_t incur = 0, innode_pos = 0, cur_lfcnt = 0;
        Record * tmp = leaf_nodes_[0].recs;
        _key_t k1 = in[0].key, k2 = tmp[0].key;
        while(incur < insize && cur_lfcnt < leaf_cnt_) {
            if(k1 == k2) { 
                out.push_back(in[incur]);

                incur += 1;
                innode_pos += 1;
                if(innode_pos == LEAF_CARD || tmp[innode_pos].key == MAX_KEY) {
                    cur_lfcnt += 1;
                    tmp = leaf_nodes_[cur_lfcnt].recs;
                    innode_pos = 0;
                }

                k1 = in[incur].key;
                k2 = tmp[innode_pos].key;
            } else if(k1 > k2) {
                out.push_back(tmp[innode_pos]);

                innode_pos += 1;
                if(innode_pos == LEAF_CARD || tmp[innode_pos].key == MAX_KEY) {
                    cur_lfcnt += 1;
                    tmp = leaf_nodes_[cur_lfcnt].recs;
                    innode_pos = 0;
                }

                k2 = tmp[innode_pos].key;;
            } else {
                out.push_back(in[incur]);
                
                incur += 1;
                k1 = in[incur].key;
            }
        }

        if(incur < insize) {
            for(int i = incur; i < insize; i++) 
                out.push_back(in[i]);
        }

        if(cur_lfcnt < leaf_cnt_) {
            while(cur_lfcnt < leaf_cnt_) {
                out.push_back(tmp[innode_pos]);

                innode_pos += 1;
                if(innode_pos == LEAF_CARD || tmp[innode_pos].key == MAX_KEY) {
                    cur_lfcnt += 1;
                    tmp = leaf_nodes_[cur_lfcnt].recs;
                    innode_pos = 0;
                }
            }
        }
    }

private:
    INode * inner_nodes_;
    LNode * leaf_nodes_;
    int leaf_cnt_;
    int height_;
    int level_offset_[MAX_HEIGHT];
};

}

#endif //__MORPHTREE2_FIXTREE_H__