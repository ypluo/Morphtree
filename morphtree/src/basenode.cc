#include "node.h"
#include "do_morph.h"

namespace morphtree {
// Predict the node type of a leaf node according to its access history
void BaseNode::MorphJudge(bool isWrite) {
    // update sampled history
    stats = (stats << 1) + (isWrite ? 1 : 0);
    
    // determine whether to morph according to read/write tendency
    uint8_t one_count = __builtin_popcountl(stats);
    NodeType new_type = (NodeType)node_type;
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

    /* Normally, the next_node_type is equal to node_type. The node_type is not equal 
     * to the next_node_type when the node is waiting for a logged node morphing.
     * During the waiting, subsequent attemps to the same next_node_type will be ignored.
    */ 
    if(new_type != next_node_type) { // add a morph record
        this->next_node_type = new_type; // wait for a node morphing
        this->lsn = glsn++;         // the lsn for next morphing
        morph_log.AddLog(MorphRecord{this, this->lsn, new_type});
    }
}

bool BaseNode::Store(const _key_t & k, uint64_t v, _key_t * split_key, BaseNode ** split_node) {
    if(!Leaf()) {
        return reinterpret_cast<ROInner *>(this)->Store(k, v, split_key, (ROInner **)split_node);
    } else {
        if(do_morphing && (gacn++ % SAMPLE_FREQ) == 0) {
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
        if(do_morphing && (gacn++ % SAMPLE_FREQ) == 0) {
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
    if(do_morphing && (gacn++ % SAMPLE_FREQ) == 0) {
        MorphJudge(false);
    }
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
    if(do_morphing && (gacn++ % SAMPLE_FREQ) == 0) {
        MorphJudge(true);
    }
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