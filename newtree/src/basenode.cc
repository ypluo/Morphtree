#include "node.h"

namespace newtree {

// definitions of global variables
bool do_morphing;

// Predict the node type of a leaf node according to its access history
void BaseNode::TypeManager(bool isWrite) {
    stats = (stats << 1) + (isWrite ? 1 : 0);
    uint8_t one_count = __builtin_popcountl(stats);
    NodeType new_type = (NodeType)node_type;
    
    switch(node_type) {
        case NodeType::WOLEAF:
            if(one_count < 63) 
                new_type = NodeType::ROLEAF;
            break;
        case NodeType::ROLEAF:
            if(one_count == 64) 
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
    newLeaf->split_k = leaf->split_k;
    newLeaf->sibling = leaf->sibling;
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

bool BaseNode::Store(_key_t k, _val_t v, _key_t * split_key, BaseNode ** split_node) {
    if(do_morphing) {
        TypeManager(true);
    }

    switch(node_type) {
    case NodeType::ROLEAF: 
        return reinterpret_cast<ROLeaf *>(this)->Store(k, v, split_key, (ROLeaf **)split_node);
    case NodeType::WOLEAF:
        return reinterpret_cast<WOLeaf *>(this)->Store(k, v, split_key, (WOLeaf **)split_node);
    }
    return false;
}

bool BaseNode::Lookup(_key_t k, _val_t & v) {
    if(do_morphing) {
        TypeManager(false);
    }
    
    switch(node_type) {
    case NodeType::ROLEAF: 
        return reinterpret_cast<ROLeaf *>(this)->Lookup(k, v);
    case NodeType::WOLEAF:
        return reinterpret_cast<WOLeaf *>(this)->Lookup(k, v);
    }

    return true;
}

void BaseNode::Print(string prefix) {
    switch(node_type) {
    case NodeType::ROLEAF: 
        return reinterpret_cast<ROLeaf *>(this)->Print(prefix);
    case NodeType::WOLEAF:
        return reinterpret_cast<WOLeaf *>(this)->Print(prefix);
    }
    return ;
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
    //assert(out.size() <= GLOBAL_LEAF_SIZE);
}

void BaseNode::DeleteNode() {
    switch(node_type) {
        case NodeType::ROLEAF: 
            delete reinterpret_cast<ROLeaf *>(this);
            break;
        case NodeType::WOLEAF:
            delete reinterpret_cast<WOLeaf *>(this);
            break;
    }
    return ;
}

} // namespace newtree