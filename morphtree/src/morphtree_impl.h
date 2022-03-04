#ifndef __MORPHTREE_IMPL_H__
#define __MORPHTREE_IMPL_H__

#include "common.h"
#include "node.h"

namespace morphtree {

class MorphtreeImpl {
public:
    explicit MorphtreeImpl();

    ~MorphtreeImpl();
 
    void insert(const _key_t & key, const _val_t val);

    bool update(const _key_t & key, const _val_t val);

    bool remove(const _key_t & key, _val_t & val);

    bool lookup(const _key_t & key, _val_t & v);

    uint64_t scan(const _key_t &startKey, int range, std::vector<_val_t> &result);

    void Print();
    
private:
    bool insert_recursive(BaseNode * n, const _key_t & key, const _val_t val, 
                                _key_t * split_k, BaseNode ** split_n);

    BaseNode * root_;
};

} // namespace morphtree

#endif // __MORPHTREE_IMPL_H__