#include "node.h"

namespace morphtree {

// definitions of global variables
bool do_morphing;
uint64_t global_stats;

// Predict the node type of a leaf node according to its access history
void BaseNode::TypeManager(bool isWrite) {
    stats = (stats << 1) + (isWrite ? 1 : 0);
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

    if(new_type != (NodeType)node_type) {
        MorphNode(this, (NodeType)node_type, new_type);
    }
}

// Morph a leaf node from From-type to To-type
void MorphNode(BaseNode * leaf, NodeType from, NodeType to) {
    BaseNode * newLeaf;
    std::vector<Record> tmp;
    tmp.reserve(GLOBAL_LEAF_SIZE);
    leaf->Dump(tmp);

    switch(to) {
    case NodeType::ROLEAF:
        newLeaf = new ROLeaf(tmp.data(), tmp.size());
        break;
    case NodeType::WOLEAF:
        newLeaf = new WOLeaf(tmp.data(), tmp.size());
        break;
    }
    // swap the header of two nodes
    SwapNode(leaf, newLeaf);

    switch(from) {
    case NodeType::ROLEAF:
        delete reinterpret_cast<ROLeaf *>(newLeaf); 
        return;
    case NodeType::WOLEAF:
        delete reinterpret_cast<WOLeaf *>(newLeaf);
        return;
    }
    assert(false);
    __builtin_unreachable();
}

bool BaseNode::Store(const _key_t & k, uint64_t v, _key_t * split_key, BaseNode ** split_node) {
    if(!Leaf()) {
        return reinterpret_cast<ROInner *>(this)->Store(k, v, split_key, (ROInner **)split_node);
    } else {
        if(do_morphing) {
            TypeManager(true);
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
            TypeManager(false);
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