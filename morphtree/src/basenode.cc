#include "node.h"
#include "epoch.h"

namespace morphtree {

// definitions of global variables
bool do_morphing;
uint64_t gacn;
MorphLogger *morph_log;
EpochBasedMemoryReclamationStrategy *ebr;

// Predict the node type of a leaf node according to its access history
void BaseNode::MorphJudge(bool isWrite) {
    stats = (stats << 1) + (isWrite ? 1 : 0);
    uint8_t one_count = __builtin_popcountl(stats);
    uint8_t new_type = node_type;

    switch(node_type) {
        case NodeType::WOLEAF:
            if(one_count <= 44) 
                new_type = NodeType::ROLEAF;
            break;
        case NodeType::ROLEAF:
            if(one_count >= 56)
                new_type = NodeType::WOLEAF;
            break;
    }
    #ifdef BG_MORPH
        if(new_type != next_node_type) { // add a morph record
            this->next_node_type = new_type; // wait for a node morphing
            this->lsn++;         // the lsn for next morphing
            morph_log.Add(this, this->lsn, new_type);
            return ;
        } else {
            return ;// this node is already waitting for a morph, do nothing
        }
    #else 
        if(new_type != next_node_type && nodelock.TryLock()) { // add a morph record
            this->next_node_type = new_type; // wait for a node morphing
            this->lsn++;         // the lsn for next morphing
            MorphNode(this, this->lsn, (NodeType)new_type);
            return ;
        } else {
            return ;// this node is already waitting for a morph, do nothing
        }
    #endif
}

// Swap the metadata of two nodes
void SwapNode(BaseNode * oldone, BaseNode *newone) {
    static const int HEADER_SIZE = sizeof(ROLeaf);
    char * tmp = new char[HEADER_SIZE];

    memcpy(tmp, oldone, HEADER_SIZE); // record the old node
    memcpy(oldone, newone, HEADER_SIZE); // replace the old node with the newone

    // the old node is recliamed
    ebr->scheduleForDeletion(ReclaimEle{tmp, false});
    ebr->scheduleForDeletion(ReclaimEle{newone, true});
}

// Morph a leaf node from From-type to To-type
void MorphLogger::MorphOneNode(BaseNode * leaf, uint16_t lsn, uint8_t to) {
    #ifdef BG_MORPH
        leaf->nodelock.Lock();
    #endif
    // skip when the record is stale
    if(lsn != leaf->lsn || leaf->node_type == (NodeType)to) {
        leaf->nodelock.UnLock();
        return ;
    }

    BaseNode * newleaf;
    if(leaf->node_type == ROLEAF) {
        ROLeaf * rleaf = (ROLeaf *) leaf;
        // create a new leaf node
        newleaf = new WOLeaf(rleaf->count);
        newleaf->lsn = leaf->lsn;
        newleaf->sibling = leaf->sibling;
        ((WOLeaf *) newleaf)->mysplitkey = rleaf->mysplitkey;
        
        // link it to the old leaf
        leaf->shadow = newleaf;

        // start to migrate
        uint32_t count = 0;
        for(int i = 0; i < GLOBAL_LEAF_SIZE / ROLeaf::PROBE_SIZE; i++) {
            count += rleaf->Dump(i, ((WOLeaf *)newleaf)->recs + count);
        }
    } else {
        // caculate the linear model of the new roleaf
        WOLeaf * wleaf = (WOLeaf *) leaf;
        LinearModelBuilder model;
        for(int i = wleaf->count / 8; i < wleaf->count * 7 / 8; i++) {
            model.add(wleaf->recs[i].key, i);
        }
        model.build();

        // create a new roleaf node
        newleaf = new ROLeaf(model.a_ * GLOBAL_LEAF_SIZE / wleaf->count, model.b_ * GLOBAL_LEAF_SIZE / wleaf->count);
        newleaf->lsn = leaf->lsn;
        newleaf->sibling = leaf->sibling;
        ((ROLeaf *) newleaf)->mysplitkey = wleaf->mysplitkey;
        
        // link it to the old leaf
        leaf->shadow = newleaf;

        // start to migrate
        for(int i = 0; i < wleaf->readable_count; i++) {
            if(wleaf->recs[i].val != 0) {
                ((ROLeaf *)newleaf)->Store(wleaf->recs[i].key, wleaf->recs[i].val, nullptr, nullptr);
            }
        }
    }

    // finish morphing
    newleaf->nodelock.Lock();
    leaf->headerlock.WLock();
    newleaf->headerlock.WLock();
    SwapNode(leaf, newleaf);
    newleaf->headerlock.UnWLock();
    leaf->headerlock.UnWLock();
    leaf->nodelock.UnLock();
    newleaf->nodelock.UnLock();
}

void MorphLogger::Run() {
    while(true) {
        if(size > 0) {
            mtx.Lock();
            MorphRecord r = que.front();
            que.pop();
            size -= 1;
            mtx.UnLock();
            EpochGuard guard;
            MorphOneNode(std::get<0>(r), std::get<1>(r), std::get<2>(r));
        }
    }
}

void MorphNode(BaseNode * leaf, uint8_t lsn, NodeType to) {
    MorphLogger::MorphOneNode(leaf, lsn, to);
}

bool BaseNode::Store(const _key_t & k, uint64_t v, _key_t * split_key, BaseNode ** split_node) {
    if(!Leaf()) {
        return reinterpret_cast<ROInner *>(this)->Store(k, v, split_key, (ROInner **)split_node);
    } else {
        if(do_morphing) {
            MorphJudge(true);
        }

        switch(node_type) {
        case NodeType::ROLEAF: 
            return reinterpret_cast<ROLeaf *>(this)->Store(k, v, split_key, (ROLeaf **)split_node);
        case NodeType::WOLEAF:
            return reinterpret_cast<WOLeaf *>(this)->Store(k, v, split_key, (WOLeaf **)split_node);
        }
        assert(false);
        __builtin_unreachable();
    }
}

bool BaseNode::Lookup(const _key_t & k, uint64_t & v) {
    if(!Leaf()) {
        reinterpret_cast<ROInner *>(this)->Lookup(k, v);
        return true;
    } else {
        if(do_morphing) {
            MorphJudge(false);
        }
        
        switch(node_type) {
        case NodeType::ROLEAF: 
            return reinterpret_cast<ROLeaf *>(this)->Lookup(k, v);
        case NodeType::WOLEAF:
            return reinterpret_cast<WOLeaf *>(this)->Lookup(k, v);
        }
        assert(false);
        __builtin_unreachable();
    }
}

bool BaseNode::Update(const _key_t & k, uint64_t v) {
    switch(node_type) {
    case NodeType::ROLEAF: 
        return reinterpret_cast<ROLeaf *>(this)->Update(k, v);
    case NodeType::WOLEAF:
        return reinterpret_cast<WOLeaf *>(this)->Update(k, v);
    }
    assert(false);
    __builtin_unreachable();
}

bool BaseNode::Remove(const _key_t & k) {
    switch(node_type) {
    case NodeType::ROLEAF: 
        return reinterpret_cast<ROLeaf *>(this)->Remove(k);
    case NodeType::WOLEAF:
        return reinterpret_cast<WOLeaf *>(this)->Remove(k);
    }
    assert(false);
    __builtin_unreachable();
}

int BaseNode::Scan(const _key_t &startKey, int len, Record *result) {
    switch(node_type) {
    case NodeType::ROLEAF: 
        return reinterpret_cast<ROLeaf *>(this)->Scan(startKey, len, result);
    case NodeType::WOLEAF:
        return reinterpret_cast<WOLeaf *>(this)->Scan(startKey, len, result);
    }
    assert(false);
    __builtin_unreachable();
}

void BaseNode::Print(string prefix) {
    if(!Leaf()) {
        reinterpret_cast<ROInner *>(this)->Print(prefix);
    } else {
        return ;
        switch(node_type) {
        case NodeType::ROLEAF: 
            return reinterpret_cast<ROLeaf *>(this)->Print(prefix);
        case NodeType::WOLEAF:
            return reinterpret_cast<WOLeaf *>(this)->Print(prefix);
        }
        assert(false);
        __builtin_unreachable();
    }
}

void BaseNode::Dump(std::vector<Record> & out) {
    switch(node_type) {
    case NodeType::ROLEAF: 
        reinterpret_cast<ROLeaf *>(this)->Dump(out);
        break;
    case NodeType::WOLEAF:
        reinterpret_cast<WOLeaf *>(this)->Dump(out);
        break;
    }
}

void BaseNode::DeleteNode() {
    switch(node_type) {
        case NodeType::ROINNER:
            delete reinterpret_cast<ROInner *>(this);
            break;
        case NodeType::ROLEAF: 
            delete reinterpret_cast<ROLeaf *>(this);
            break;
        case NodeType::WOLEAF:
            delete reinterpret_cast<WOLeaf *>(this);
            break;
    }
    return ;
}

} // namespace morphtree