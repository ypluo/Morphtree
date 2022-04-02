#include "node.h"

namespace morphtree {

// definitions of global variables
uint64_t access_count;
bool do_morphing;

// Predict the node type of a leaf node according to its access history
static NodeType PredictNodeType(uint64_t status) {
    uint8_t zero_count = 64 - __builtin_popcountl(status);
    
    if(zero_count > RO_RW_LINE)
        return NodeType::ROLEAF;
    else if (zero_count > RW_WO_LINE)
        return NodeType::RWLEAF;
    else 
        return NodeType::WOLEAF;
}

// Morph a leaf node from From-type to To-type
void MorphNode(BaseNode * leaf, NodeType from, NodeType to) {
    if(from == to)
        return ; // do not morph a node if FROM-type is equal to TO-type

    BaseNode * newLeaf;
    std::vector<Record> tmp;
    tmp.reserve(GLOBAL_LEAF_SIZE);
    leaf->Dump(tmp);

    switch(to) {
    case NodeType::ROLEAF:
        newLeaf = new ROLeaf(tmp.data(), tmp.size());
        break;
    case NodeType::RWLEAF:
        newLeaf = new RWLeaf(tmp.data(), tmp.size());
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
    case NodeType::RWLEAF:
        delete reinterpret_cast<RWLeaf *>(newLeaf);
        return;
    case NodeType::WOLEAF:
        delete reinterpret_cast<WOLeaf *>(newLeaf);
        return;
    }
    assert(false);
    __builtin_unreachable();
}

bool BaseNode::Store(_key_t k, _val_t v, _key_t * split_key, BaseNode ** split_node) {
    if(!Leaf()) {
        return reinterpret_cast<ROInner *>(this)->Store(k, v, split_key, (ROInner **)split_node);
    } else {
        if(do_morphing) {
            access_count += 1;
            if((access_count & (MORPH_FREQ - 1)) == 0) {
                stats = (stats << 1) + 1; // a write access
                NodeType predict_type = PredictNodeType(stats);
                if (node_type != predict_type) {
                    MorphNode(this, (NodeType)node_type, predict_type);
                }
            }
        }

        switch(node_type) {
        case NodeType::ROLEAF: 
            return reinterpret_cast<ROLeaf *>(this)->Store(k, v, split_key, (ROLeaf **)split_node);
        case NodeType::RWLEAF:
            return reinterpret_cast<RWLeaf *>(this)->Store(k, v, split_key, (RWLeaf **)split_node);
        case NodeType::WOLEAF:
            return reinterpret_cast<WOLeaf *>(this)->Store(k, v, split_key, (WOLeaf **)split_node);
        }
        assert(false);
        __builtin_unreachable();
    }
}

bool BaseNode::Lookup(_key_t k, _val_t & v) {
    if(!Leaf()) {
        reinterpret_cast<ROInner *>(this)->Lookup(k, v);
        return true;
    } else {
        if(do_morphing) {
            access_count += 1;
            if((access_count & (MORPH_FREQ - 1)) == 0) {
                stats = (stats << 1); // a read access
                NodeType predict_type = PredictNodeType(stats);
                if (node_type != predict_type) {
                    MorphNode(this, (NodeType)node_type, predict_type);
                }
            }
        }
        
        switch(node_type) {
        case NodeType::ROLEAF: 
            return reinterpret_cast<ROLeaf *>(this)->Lookup(k, v);
        case NodeType::RWLEAF:
            return reinterpret_cast<RWLeaf *>(this)->Lookup(k, v);
        case NodeType::WOLEAF:
            return reinterpret_cast<WOLeaf *>(this)->Lookup(k, v);
        }
        assert(false);
        __builtin_unreachable();
    }
}

void BaseNode::Print(string prefix) {
    if(!Leaf()) {
        reinterpret_cast<ROInner *>(this)->Print(prefix);
    } else {
        switch(node_type) {
        case NodeType::ROLEAF: 
            return reinterpret_cast<ROLeaf *>(this)->Print(prefix);
        case NodeType::RWLEAF:
            return reinterpret_cast<RWLeaf *>(this)->Print(prefix);
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
    case NodeType::RWLEAF:
        reinterpret_cast<RWLeaf *>(this)->Dump(out);
        break;
    case NodeType::WOLEAF:
        reinterpret_cast<WOLeaf *>(this)->Dump(out);
        break;
    }
    //assert(out.size() <= GLOBAL_LEAF_SIZE);
}

void BaseNode::DeleteNode() {
    switch(node_type) {
        case NodeType::ROINNER:
            delete reinterpret_cast<ROInner *>(this);
            break;
        case NodeType::ROLEAF: 
            delete reinterpret_cast<ROLeaf *>(this);
            break;
        case NodeType::RWLEAF:
            delete reinterpret_cast<RWLeaf *>(this);
            break;
        case NodeType::WOLEAF:
            delete reinterpret_cast<WOLeaf *>(this);
            break;
    }
    return ;
}

} // namespace morphtree