//
// Created by tawnysky on 2020/10/26.
//

#pragma once

#include "piecewise_linear_model.h"
#include "flat_nodes.h"

#define ROOT_LEVEL2_SIZE (ROOT_LEVEL1_SIZE << 3)
#define ROOT_LEVEL3_SIZE (ROOT_LEVEL1_SIZE << 6)

template<typename K, typename P>
class FlatIndex {

    using NODE = FlatNode<K, P>;
    using INNER_NODE = FlatInnerNode<K, P>;
    using LEAF_NODE = FlatLeafNode<K, P>;
    using SIMPLE_INNER_NODE = FlatSimpleInnerNode<K, P>;
    using SIMPLE_LEAF_NODE = FlatSimpleLeafNode<K, P>;

    struct Segment;
    struct LinearModelBuilder;

    // Bit 0-3: the level of index (0-15); bit 4-5: the level of key buffer (0-3, 0 means only one child).
    uint8_t level = 0;
    // Max number: LEVEL1_SIZE * 64
    uint16_t number = 0;

    K min_key;
    K max_key;
    // Used when number = 1
    NODE* child;
    NODE** nodes;
    K keys_l1[ROOT_LEVEL1_SIZE];
    K* keys_l2l3;
    const K key_domain_min = std::numeric_limits<K>::lowest();
    SIMPLE_LEAF_NODE* left_buffer;
    SIMPLE_LEAF_NODE* right_buffer;

    // For double comparison
    inline bool equal(K key1, K key2) const {
        return (key1 == key2);
    }

    inline bool greater_than(K key1, K key2) const {
        return (key1 > key2);
    }

    inline bool less_than(K key1, K key2) const {
        return (key1 < key2);
    }

    bool bitmap_get(const uint8_t* bitmap, size_t offset) const {
        size_t bitmap_array = offset >> 3;
        size_t bitmap_bit = offset & 7;
        return (bool)(bitmap[bitmap_array] & 1 << bitmap_bit);
    }

    void build(K* keys, P* payloads, size_t number) {

        // Make segmentation for leaves
        std::vector<Segment> segments;
        auto in_fun = [keys](auto i) {
            return std::pair<K, size_t>(keys[i],i);
        };
        auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
        size_t last_n = make_segmentation(number, EPSILON_INIT_LEAF_NODE, in_fun, out_fun);
        std::cout<<"Number of leaf nodes: "<<last_n<<std::endl;

        // Build leaf nodes
        size_t start_pos = 0;
        auto leaf_nodes = new LEAF_NODE*[last_n];
        K* first_keys = new K[last_n];
        NODE* prev = left_buffer;
        for(size_t i = 0; i < last_n; i++) {
            leaf_nodes[i] = new LEAF_NODE(1, 64, segments[i].number, start_pos, segments[i].first_key, segments[i].slope, segments[i].intercept, keys, payloads, prev, NULL);
            first_keys[i] = segments[i].first_key;
            start_pos += segments[i].number;
            prev = leaf_nodes[i];
        }
        right_buffer->set_pointer(prev);
        left_buffer->set_pointer(leaf_nodes[0]);
        for(size_t i = 0; i < last_n - 1; i++)
            leaf_nodes[i]->set_next(leaf_nodes[i + 1]);
        leaf_nodes[last_n - 1]->set_next(right_buffer);

        // Build inner nodes recursively
        uint8_t level = 192;
        size_t offset = 0;
        NODE** nodes = (NODE**)leaf_nodes;
        NODE** nodes_tmp = nodes;
        K* first_keys_tmp = first_keys;
        auto in_fun_rec = [&first_keys](auto i){
            return std::pair<K, size_t>(first_keys[i],i);
        };
        // Currently do not allow root level > 1
        while (last_n > ROOT_LEVEL2_SIZE) {
            level++;
            offset += last_n;
            last_n = make_segmentation(last_n, EPSILON_INIT_INNER_NODE, in_fun_rec, out_fun);
            std::cout<<"Number of inner nodes: "<<last_n<<std::endl;
            start_pos = 0;
            nodes = new NODE*[last_n];
            first_keys = new K[last_n];
            for (size_t i = 0; i < last_n; i++) {
                nodes[i] = new INNER_NODE(false, level, segments[offset + i].number, start_pos, segments[offset + i].first_key, segments[offset + i].slope, segments[offset + i].intercept, first_keys_tmp, nodes_tmp);
                first_keys[i] = segments[offset + i].first_key;
                start_pos += segments[offset + i].number;
            }
            delete [] nodes_tmp;
            delete [] first_keys_tmp;
            nodes_tmp = nodes;
            first_keys_tmp = first_keys;
        }

        //Build root
        if (last_n < ROOT_LEVEL1_SIZE) {
            this->level = level - 192 + 16;
            this->number = last_n;
            this->nodes = new NODE*[ROOT_LEVEL1_SIZE]();
            for (size_t i = 0; i < last_n; i++) {
                keys_l1[i] = first_keys[i];
                this->nodes[i] = nodes[i];
            }
            if (last_n == 1) {
                this->level -= 16;
                child = nodes[0];
            }
        }
        else if (last_n < ROOT_LEVEL2_SIZE) {
            this->level = level - 192 + 32;
            this->number = last_n;
            keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * ROOT_LEVEL2_SIZE));
            this->nodes = new NODE*[ROOT_LEVEL2_SIZE]();
            int it = 0;
            int it_l1 = -1;
            for (size_t i = 0; i < ROOT_LEVEL2_SIZE; i++) {
                if (i % 8 == 0) {
                    it_l1++;
                    keys_l1[it_l1] = first_keys[it];
                }
                if (it * ROOT_LEVEL1_SIZE / last_n == it_l1) {
                    keys_l2l3[i] = first_keys[it];
                    this->nodes[i] = nodes[it];
                    it++;
                }
                else {
                    keys_l2l3[i] = key_domain_min;
                }
            }
        }
        else {
            this->level = level - 192 + 48;
            this->number = last_n;
            keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * (ROOT_LEVEL2_SIZE + ROOT_LEVEL3_SIZE)));
            this->nodes = new NODE*[ROOT_LEVEL3_SIZE]();
            int it = 0;
            int it_l1 = -1;
            int it_l2 = -1;
            for (size_t i = 0; i < ROOT_LEVEL3_SIZE; i++) {
                if (i % 64 == 0) {
                    it_l1++;
                    keys_l1[it_l1] = first_keys[it];
                }
                if (i % 8 == 0) {
                    it_l2++;
                    keys_l2l3[it_l2] = first_keys[it];
                }
                if (it * ROOT_LEVEL2_SIZE / last_n == it_l2) {
                    keys_l2l3[i + ROOT_LEVEL2_SIZE] = first_keys[it];
                    this->nodes[i] = nodes[it];
                    it++;
                }
                else {
                    keys_l2l3[i + ROOT_LEVEL2_SIZE] = key_domain_min;
                }
            }
        }
        delete [] first_keys;
        delete [] nodes;
    }

    // Expand level 1 to level 2 (x8)
    void expand_l2() {
        keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * ROOT_LEVEL2_SIZE));
        NODE** nodes_tmp = nodes;
        nodes = new NODE*[ROOT_LEVEL2_SIZE]();
        for (size_t i = 0; i < ROOT_LEVEL1_SIZE; i++) {
            keys_l2l3[i << 3] = keys_l1[i];
            nodes[i << 3] = nodes_tmp[i];
            for (size_t j = 1; j < 8; j++) {
                keys_l2l3[(i << 3) + j] = key_domain_min;
            }
        }
        delete [] nodes_tmp;
        level += 16;
    }

    // Expand level 2 to level 3 (x8)
    void expand_l3() {
        K* keys_l2_tmp = keys_l2l3;
        keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * (ROOT_LEVEL2_SIZE + ROOT_LEVEL3_SIZE)));
        memcpy(keys_l2l3, keys_l2_tmp, ROOT_LEVEL2_SIZE * sizeof(K));
        NODE** nodes_tmp = nodes;
        nodes = new NODE*[ROOT_LEVEL3_SIZE]();
        for (size_t i = 0; i < ROOT_LEVEL2_SIZE; i++) {
            keys_l2l3[(i << 3) + ROOT_LEVEL2_SIZE] = keys_l2_tmp[i];
            nodes[i << 3] = nodes_tmp[i];
            for (size_t j = 1; j < 8; j++) {
                keys_l2l3[(i << 3) + j + ROOT_LEVEL2_SIZE] = key_domain_min;
            }
        }
        free(keys_l2_tmp);
        delete [] nodes_tmp;
        level += 16;
    }

    // Without test
    void evolve_node(SIMPLE_LEAF_NODE* simple_leaf_node, bool type) {
        throw std::logic_error("Without test!");
        size_t size = simple_leaf_node->get_number();
        K* keys = simple_leaf_node->get_keys();
        P* payloads = simple_leaf_node->get_payloads();
        auto linear_model_builder = new LinearModelBuilder(size, false);
        for (size_t it = 0; it < SIMPLE_LEAF_NODE_LEVEL1_SIZE << 6; it++) {
            if (!equal(keys[it], key_domain_min)) {
                linear_model_builder->add(keys[it], payloads[it]);
            }
        }
        linear_model_builder->build_model();
        if (!type) {
            auto buffer_evolved = new LEAF_NODE(6, 64, linear_model_builder->number, 0, linear_model_builder->first_key, linear_model_builder->slope, linear_model_builder->intercept, linear_model_builder->keys, linear_model_builder->payloads, NULL, simple_leaf_node->get_pointer());
            delete left_buffer;
            left_buffer = new SIMPLE_LEAF_NODE(0);
            left_buffer->set_pointer(buffer_evolved);
            buffer_evolved->set_prev(left_buffer);
            ((LEAF_NODE*)buffer_evolved->get_next())->set_prev(buffer_evolved);
            min_key = linear_model_builder->first_key;
            upsert_in_root(linear_model_builder->first_key, buffer_evolved);
        }
        else {
            auto buffer_evolved = new LEAF_NODE(5, 64, linear_model_builder->number, 0, linear_model_builder->first_key, linear_model_builder->slope, linear_model_builder->intercept, linear_model_builder->keys, linear_model_builder->payloads, simple_leaf_node->get_pointer(), NULL);
            delete right_buffer;
            right_buffer = new SIMPLE_LEAF_NODE(16);
            right_buffer->set_pointer(buffer_evolved);
            buffer_evolved->set_next(right_buffer);
            ((LEAF_NODE*)buffer_evolved->get_prev())->set_next(buffer_evolved);
            max_key = linear_model_builder->get_max_key();
            upsert_in_root(linear_model_builder->first_key, buffer_evolved);
        }
        delete linear_model_builder;
    }

    void get_traversal_path(K key, std::vector<NODE*>& traversal_path) const {
        if (level < 16) {
            child->find_split_node(key, traversal_path);
        }
        else if (level < 32) {
            for (size_t i = 1; i < number; i++) {
                if (greater_than(keys_l1[i], key)) {
                    nodes[i - 1]->find_split_node(key, traversal_path);
                    return;
                }
            }
            nodes[number - 1]->find_split_node(key, traversal_path);
        }
        else if (level < 48) {
            int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                if (greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            if (start_pos_l2 >= 0){
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], key) || equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                        nodes[start_pos_l2 + i - 1]->find_split_node(key, traversal_path);
                        return;
                    }
                }
                nodes[start_pos_l2 + 7]->find_split_node(key, traversal_path);
            }
        }
        else {
            int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
            for (int i = 0; i < ROOT_LEVEL1_SIZE; i++) {
                if (greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            if (start_pos_l2 >= 0) {
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key) || equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                        nodes[start_pos_l3 + i - 1]->find_split_node(key, traversal_path);
                        return;
                    }

                }
                nodes[start_pos_l3 + 7]->find_split_node(key, traversal_path);
            }
        }
    }

    void split_node(K key) {
        std::vector<NODE*> traversal_path;
        get_traversal_path(key, traversal_path);

        // Split leaf node
        auto leaf_to_split = (LEAF_NODE*)traversal_path.back();
        traversal_path.pop_back();
        size_t smo_number = leaf_to_split->get_number();
        // Get node data
        K* smo_keys = new K[smo_number];
        P* smo_payloads = new P[smo_number];
        size_t old_size = leaf_to_split->get_size();
        K* old_keys = leaf_to_split->get_keys();
        P* old_payloads = leaf_to_split->get_payloads();
        SIMPLE_LEAF_NODE** old_overflow_blocks = leaf_to_split->get_overflow_blocks();
        size_t count = 0;
        size_t total_blocks = ((old_size - 1) >> 6) + 1;
        for (size_t block = 0; block < total_blocks; block++) {
            // Block it
            size_t itb = 0;
            size_t block_size = 64;
            size_t start_pos = block << 6;
            if (block == total_blocks - 1 && old_size % 64 > 0) {
                block_size = old_size % 64;
            }
            while (equal(old_keys[start_pos + itb], key_domain_min))
                itb++;
            if (!old_overflow_blocks[block]) {
                while (itb < block_size) {
                    if (greater_than(old_keys[start_pos + itb], key_domain_min)) {
                        smo_keys[count] = old_keys[start_pos + itb];
                        smo_payloads[count++] = old_payloads[start_pos + itb];
                        /*
                        if (count > 1)
                            assert(smo_keys[count - 1] > smo_keys[count - 2]);
                        */
                    }
                    itb++;
                }
            }
            else {
                // Overflow block it
                size_t ito = 0;
                size_t ob_size = old_overflow_blocks[block]->get_size();
                K* ob_keys = old_overflow_blocks[block]->get_keys();
                P* ob_payloads = old_overflow_blocks[block]->get_payloads();
                while (itb < block_size || ito < ob_size) {
                    if (itb < block_size && (ito == ob_size || less_than(old_keys[start_pos + itb], ob_keys[ito]))) {
                        smo_keys[count] = old_keys[start_pos + itb];
                        smo_payloads[count++] = old_payloads[start_pos + itb];
                        itb++;
                        while (itb < block_size && equal(old_keys[start_pos + itb], key_domain_min))
                            itb++;
                    }
                    else {
                        smo_keys[count] = ob_keys[ito];
                        smo_payloads[count++] = ob_payloads[ito++];
                        while (ito < ob_size && equal(ob_keys[ito], key_domain_min))
                            ito++;
                    }
                    /*
                    if (count > 1)
                        assert(smo_keys[count - 1] > smo_keys[count - 2]);
                    */
                }
            }
        }
        assert(count == smo_number);

        // Train new models
        std::vector<Segment> segments;
        auto in_fun = [smo_keys](auto i) {
            return std::pair<K, size_t>(smo_keys[i],i);
        };
        auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
        size_t last_n = make_segmentation(count, EPSILON_INIT_LEAF_NODE, in_fun, out_fun);

        // Build leaf nodes
        size_t start_pos = 0;
        auto leaf_nodes = new LEAF_NODE*[last_n];
        K* first_keys = new K[last_n];
        NODE* left_sibling = leaf_to_split->get_prev();
        NODE* right_sibling = leaf_to_split->get_next();
        NODE* prev = left_sibling;
        for(size_t i = 0; i < last_n; i++) {
            leaf_nodes[i] = new LEAF_NODE(1, 64, segments[i].number, start_pos, segments[i].first_key, segments[i].slope, segments[i].intercept, smo_keys, smo_payloads, prev, NULL);
            first_keys[i] = segments[i].first_key;
            start_pos += segments[i].number;
            prev = leaf_nodes[i];
        }
        for(size_t i = 0; i < last_n - 1; i++)
            leaf_nodes[i]->set_next(leaf_nodes[i + 1]);
        leaf_nodes[last_n - 1]->set_next(right_sibling);
        if (left_sibling->level == 0)
            left_buffer->set_pointer(leaf_nodes[0]);
        else
            ((LEAF_NODE*)left_sibling)->set_next(leaf_nodes[0]);
        if (right_sibling->level == 0)
            right_buffer->set_pointer(leaf_nodes[0]);
        else
            ((LEAF_NODE*)right_sibling)->set_prev(leaf_nodes[last_n - 1]);
        
        segments.clear();
        delete leaf_to_split;
        delete[] smo_keys;
        delete[] smo_payloads;

        NODE** nodes = (NODE**)leaf_nodes;

        while (!traversal_path.empty()) {
            // Target node is an inner node
            if (traversal_path.back()->get_level() >= 192) {
                auto target_node = (INNER_NODE*)traversal_path.back();
                traversal_path.pop_back();
                int ref = 0;
                target_node->update_node(first_keys[0], nodes[0]);
                for (size_t i = 1; i < last_n; ++i) {
                    ref = target_node->insert_node(first_keys[i], nodes[i]);
                }

                delete[] nodes;
                delete[] first_keys;

                if (ref >= 0)
                    return;
                // Split inner node
                else {
                    smo_number = target_node->get_number();
                    K* smo_keys = new K[smo_number];
                    NODE** smo_nodes = new NODE*[smo_number];
                    old_keys = target_node->get_keys();
                    NODE** old_nodes = target_node->get_nodes();
                    uint8_t* old_bitmap = target_node->get_bitmap();
                    K cur_key = key_domain_min;
                    count = 0;
                    for (size_t line = 0; line < target_node->get_size() >> 3; line++) {
                        start_pos = line << 3;
                        if (bitmap_get(old_bitmap, line)) {
                            auto temp_inner_node = (SIMPLE_INNER_NODE*)old_nodes[start_pos];
                            size_t temp_inner_node_size = temp_inner_node->get_size();
                            K* temp_inner_node_keys = temp_inner_node->get_keys();
                            NODE** temp_inner_node_nodes = temp_inner_node->get_nodes();
                            for (size_t it_temp = 0; it_temp < temp_inner_node_size; it_temp++) {
                                if (greater_than(temp_inner_node_keys[it_temp], cur_key)) {
                                    cur_key = temp_inner_node_keys[it_temp];         
                                    smo_keys[count] = cur_key;
                                    smo_nodes[count++] = temp_inner_node_nodes[it_temp];
                                    /*
                                    if (count > 1)
                                        assert(smo_keys[count - 1] > smo_keys[count - 2]);
                                    */
                                }
                            }
                        }
                        else {
                            if (greater_than(old_keys[start_pos], cur_key)) {
                                cur_key = old_keys[start_pos];
                                smo_keys[count] = cur_key;
                                smo_nodes[count++] = old_nodes[start_pos];
                                /*
                                if (count > 1)
                                    assert(smo_keys[count - 1] > smo_keys[count - 2]);         
                                */                       
                            }
                        }
                        for (size_t i = 1; i <= 7; i++) {
                            if (greater_than(old_keys[start_pos + i], cur_key)) {
                                cur_key = old_keys[start_pos + i];                            
                                smo_keys[count] = cur_key;
                                smo_nodes[count++] = old_nodes[start_pos + i];
                                /*
                                if (count > 1)
                                    assert(smo_keys[count - 1] > smo_keys[count - 2]);
                                */
                            }
                        }
                    }
                    assert(count == smo_number);
                    
                    // Train new models
                    auto in_fun = [smo_keys](auto i) {
                        return std::pair<K, size_t>(smo_keys[i],i);
                    };
                    auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
                    last_n = make_segmentation(count, EPSILON_INIT_INNER_NODE, in_fun, out_fun);
                    start_pos = 0;
                    nodes = new NODE*[last_n];
                    first_keys = new K[last_n];
                    for (size_t i = 0; i < last_n; i++) {
                        nodes[i] = new INNER_NODE(false, target_node->get_level(), segments[i].number, start_pos, segments[i].first_key, segments[i].slope, segments[i].intercept, smo_keys, smo_nodes);
                        first_keys[i] = segments[i].first_key;
                        start_pos += segments[i].number;
                    }
                    
                    segments.clear();
                    delete target_node; 
                    delete[] smo_keys;
                    delete[] smo_nodes;
                }
            }
            // Target node is a simple inner node
            // Without test
            else {
                throw std::logic_error("Without test!");
                auto target_node = (SIMPLE_INNER_NODE*)traversal_path.back();
                traversal_path.pop_back();
                
                int ref = 0;
                target_node->upsert_node(first_keys[0], nodes[0]);
                for (size_t i = 1; i < last_n; ++i) {
                    ref = target_node->upsert_node(first_keys[i], nodes[i]);
                }

                delete[] nodes;
                delete[] first_keys;
                
                if (ref >= 0)
                    return;
                // Evolve simple inner node
                else {
                    size_t old_number = target_node->get_number();
                    old_keys = target_node->get_keys();
                    NODE** old_nodes = target_node->get_nodes();
                    auto linear_model_builder = new LinearModelBuilder(old_number, true);
                    for (size_t it = 0; it < SIMPLE_INNER_NODE_LEVEL1_SIZE << 6; it++) {
                        if (!equal(old_keys[it], key_domain_min)) {
                            linear_model_builder->add(old_keys[it], old_nodes[it]);
                        }
                    }
                    linear_model_builder->build_model();
                    last_n = 1;
                    nodes = new NODE*[last_n];
                    nodes[0] = new INNER_NODE(true, target_node->get_level() + 64, linear_model_builder->number, 0, linear_model_builder->first_key, linear_model_builder->slope, linear_model_builder->intercept, linear_model_builder->keys, linear_model_builder->nodes);
                    first_keys = new K[last_n];
                    first_keys[0] = linear_model_builder->first_key;
                    
                    delete target_node;
                    delete linear_model_builder;
                }
            }
        }
        for (size_t i = 0; i < last_n; ++i)
            upsert_in_root(first_keys[i], nodes[i]);
        
        delete[] first_keys;
        delete[] nodes;
    }


    void upsert_in_root(K key, NODE* node) {

        // Level 1
        if (level < 32) {
            size_t target_pos = number;
            for (size_t i = 0; i < number; i++) {
                if (greater_than(keys_l1[i], key)) {
                    target_pos = i;
                    break;
                }
            }
            // Update
            if (target_pos > 0 && equal(keys_l1[target_pos - 1], key)) {
                nodes[target_pos - 1] = node;
                if (level < 16) 
                    child = nodes[0];
                return;
            }
            assert(number >= target_pos && number < ROOT_LEVEL1_SIZE);
            memmove(keys_l1 + target_pos + 1, keys_l1 + target_pos, (number - target_pos) * sizeof(K));
            memmove(nodes + target_pos + 1, nodes + target_pos, (number - target_pos) * sizeof(NODE*));
            keys_l1[target_pos] = key;
            nodes[target_pos] = node;
            number++;
            if (level < 16) {
                level += 16;
                child = NULL;
            }
            if (number == ROOT_LEVEL1_SIZE)
                expand_l2();
        }

        // Level 2
        else if (level < 48) {

            // Get start position (line) at level 2
            int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
            for (int i = 0; i < ROOT_LEVEL1_SIZE; i++) {
                if (greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }

            if (start_pos_l2 >= 0) {
                int target_pos = start_pos_l2 + 8;
                int free_pos = target_pos;
                for (size_t i = 0; i < 8; i++) {
                    // Update
                    if (equal(keys_l2l3[start_pos_l2 + i], key)) {
                        nodes[start_pos_l2 + i] = node;
                        return;
                    }
                    else if (equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                        free_pos = start_pos_l2 + i;
                        target_pos = free_pos;
                        break;
                    }
                    else if (greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        target_pos = start_pos_l2 + i;
                        break;
                    }
                }
                if (free_pos != target_pos) {
                    for (int i = target_pos - start_pos_l2 + 1; i < 8; i++) {
                        if (equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                            free_pos = start_pos_l2 + i;
                            break;
                        }
                    }
                }
                // Target line has free position
                if (free_pos < start_pos_l2 + 8 ) {
                    assert(free_pos >= target_pos && free_pos < ROOT_LEVEL2_SIZE);
                    memmove(keys_l2l3 + target_pos + 1, keys_l2l3 + target_pos, (free_pos - target_pos) * sizeof(K));
                    memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                    keys_l2l3[target_pos] = key;
                    nodes[target_pos] = node;
                }
                // Target line is full
                else {
                    for (int i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                        // Borrow from front line
                        if (start_pos_l2 - (i << 3) + 7 > 0 && equal(keys_l2l3[start_pos_l2 - (i << 3) + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l2 - (i << 3) + 8;
                            for (size_t j = 2; j < 9; j++) {
                                if (greater_than(keys_l2l3[influenced_header - j], key_domain_min)) {
                                    free_pos = influenced_header - j + 1;
                                    break;
                                }
                            }
                            keys_l2l3[free_pos] = keys_l2l3[influenced_header];
                            nodes[free_pos] = nodes[influenced_header];
                            assert(target_pos > influenced_header && target_pos <= ROOT_LEVEL2_SIZE);
                            memmove(keys_l2l3 + influenced_header, keys_l2l3 + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(K));
                            memmove(nodes + influenced_header, nodes + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(NODE*));
                            keys_l2l3[target_pos - 1] = key;
                            nodes[target_pos - 1] = node;
                            for (size_t j = influenced_header; j < target_pos; j = j + 8) {
                                keys_l1[j >> 3] = keys_l2l3[j];
                            }
                            break;
                        }
                        // Borrow from latter line
                        if (start_pos_l2 + (i << 3) + 7 < ROOT_LEVEL2_SIZE && equal(keys_l2l3[start_pos_l2 + (i << 3) + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l2 + (i << 3);
                            for (size_t j = 1; j < 8; j++) {
                                if (equal(keys_l2l3[influenced_header + j], key_domain_min)) {
                                    free_pos = influenced_header + j;
                                    break;
                                }
                            }
                            assert(free_pos >= target_pos && free_pos < ROOT_LEVEL2_SIZE);
                            memmove(keys_l2l3 + target_pos + 1, keys_l2l3 + target_pos, (free_pos - target_pos) * sizeof(K));
                            memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                            keys_l2l3[target_pos] = key;
                            nodes[target_pos] = node;
                            for (int j = influenced_header; j >= target_pos; j = j - 8) {
                                keys_l1[j >> 3] = keys_l2l3[j];
                            }
                            break;
                        }
                    }
                }
            }
            // Insert in the first position
            else {
                size_t free_pos = 7;
                for (size_t i = 7; i < ROOT_LEVEL2_SIZE; i = i + 8) {
                    if (equal(keys_l2l3[i], key_domain_min)) {
                        free_pos = i;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[free_pos - i], key_domain_min)) {
                        free_pos = free_pos - i + 1;
                        break;
                    }
                }
                assert(free_pos < ROOT_LEVEL2_SIZE);
                memmove(keys_l2l3 + 1, keys_l2l3, free_pos * sizeof(K));
                memmove(nodes + 1, nodes, free_pos * sizeof(NODE*));
                keys_l2l3[0] = key;
                nodes[0] = node;
                for (size_t i = 0; i << 3 < free_pos; i++) {
                    keys_l1[i] = keys_l2l3[i << 3];
                }
            }

            number++;
            if (number == ROOT_LEVEL2_SIZE)
                expand_l3();
        }

        // Level 3
        else {
            // Get start position (line) at level 2
            int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
            for (int i = 0; i < ROOT_LEVEL1_SIZE; i++) {
                if (greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            

            if (start_pos_l2 >= 0) {

                // Get start position (line) at level 3
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (int i = start_pos_l2; i < start_pos_l2 + 8; i++) {
                    if (greater_than(keys_l2l3[i], key)) {
                        start_pos_l3 = (i - 1) << 3;
                        break;
                    }
                }

                int target_pos = start_pos_l3 + 8;
                int free_pos = target_pos;
                for (size_t i = 0; i < 8; i++) {
                    // Update
                    if (equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key)) {
                        nodes[start_pos_l3 + i] = node;
                        return;
                    }
                    else if (equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = start_pos_l3 + i;
                        target_pos = free_pos;
                        break;
                    }
                    else if (greater_than(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key)) {
                        target_pos = start_pos_l3 + i;
                        break;
                    }
                }
                if (free_pos != target_pos) {
                    for (int i = target_pos - start_pos_l3 + 1; i < 8; i++) {
                        if (equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                            free_pos = start_pos_l3 + i;
                            break;
                        }
                    }
                }
                // Target line has free position
                if (free_pos < start_pos_l3 + 8) {
                    assert(free_pos >= target_pos && free_pos < ROOT_LEVEL3_SIZE);
                    memmove(keys_l2l3 + target_pos + ROOT_LEVEL2_SIZE + 1, keys_l2l3 + target_pos + ROOT_LEVEL2_SIZE, (free_pos - target_pos) * sizeof(K));
                    memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                    keys_l2l3[target_pos + ROOT_LEVEL2_SIZE] = key;
                    nodes[target_pos] = node;
                }
                // Borrow free position
                else {
                    for (int i = 1; i < ROOT_LEVEL2_SIZE; i++) {
                        // Borrow from front line
                        if (start_pos_l3 - (i << 3) + 7 > 0 && equal(keys_l2l3[start_pos_l3 - (i << 3) + ROOT_LEVEL2_SIZE + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l3 - (i << 3) + 8;
                            for (size_t j = 2; j < 9; j++) {
                                if (greater_than(keys_l2l3[influenced_header - j + ROOT_LEVEL2_SIZE], key_domain_min)) {
                                    free_pos = influenced_header - j + 1;
                                    break;
                                }
                            }
                            keys_l2l3[free_pos + ROOT_LEVEL2_SIZE] = keys_l2l3[influenced_header + ROOT_LEVEL2_SIZE];
                            nodes[free_pos] = nodes[influenced_header];
                            assert(target_pos > influenced_header && target_pos <= ROOT_LEVEL3_SIZE);
                            memmove(keys_l2l3 + influenced_header + ROOT_LEVEL2_SIZE, keys_l2l3 + influenced_header + ROOT_LEVEL2_SIZE + 1, (target_pos - influenced_header - 1) * sizeof(K));
                            memmove(nodes + influenced_header, nodes + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(NODE*));
                            keys_l2l3[target_pos + ROOT_LEVEL2_SIZE - 1] = key;
                            nodes[target_pos - 1] = node;
                            for (size_t j = influenced_header; j < target_pos; j = j + 8) {
                                keys_l2l3[j >> 3] = keys_l2l3[j + ROOT_LEVEL2_SIZE];
                            }
                            for (size_t j = ((influenced_header - 1) >> 6) + 1; j << 6 < target_pos; j++) {
                                keys_l1[j] = keys_l2l3[j << 3];
                            }
                            break;
                        }
                        // Borrow from latter line
                        if (start_pos_l3 + (i << 3) + 7 < ROOT_LEVEL3_SIZE && equal(keys_l2l3[start_pos_l3 + (i << 3) + ROOT_LEVEL2_SIZE + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l3 + (i << 3);
                            for (size_t j = 1; j < 8; j++) {
                                if (equal(keys_l2l3[influenced_header + j + ROOT_LEVEL2_SIZE], key_domain_min)) {
                                    free_pos = influenced_header + j;
                                    break;
                                }
                            }
                            assert(free_pos >= target_pos && free_pos < ROOT_LEVEL3_SIZE);
                            memmove(keys_l2l3 + target_pos + ROOT_LEVEL2_SIZE + 1, keys_l2l3 + target_pos + ROOT_LEVEL2_SIZE, (free_pos - target_pos) * sizeof(K));
                            memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                            keys_l2l3[target_pos + ROOT_LEVEL2_SIZE] = key;
                            nodes[target_pos] = node;
                            for (int j = influenced_header; j >= target_pos; j = j - 8) {
                                keys_l2l3[j >> 3] = keys_l2l3[j + ROOT_LEVEL2_SIZE];
                            }
                            for (int j = influenced_header >> 6; j << 6 >= target_pos; j--) {
                                keys_l1[j] = keys_l2l3[j << 3];
                            }
                            break;
                        }
                    }
                }
            }
            // Insert in the first position
            else {
                size_t free_pos = 7;
                for (size_t i = 7; i < ROOT_LEVEL3_SIZE; i = i + 8) {
                    if (equal(keys_l2l3[i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = i;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[free_pos - i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = free_pos - i + 1;
                        break;
                    }
                }
                assert(free_pos < ROOT_LEVEL3_SIZE);
                memmove(keys_l2l3 + ROOT_LEVEL2_SIZE + 1, keys_l2l3 + ROOT_LEVEL2_SIZE, free_pos * sizeof(K));
                memmove(nodes + 1, nodes, free_pos * sizeof(NODE*));
                keys_l2l3[ROOT_LEVEL2_SIZE] = key;
                nodes[0] = node;
                for (size_t i = 0; i << 3 < free_pos; i++) {
                    keys_l2l3[i] = keys_l2l3[(i << 3) + ROOT_LEVEL2_SIZE];
                }
                for (size_t i = 0; i << 6 < free_pos; i++) {
                    keys_l1[i] = keys_l2l3[i << 3];
                }
            }

            number++;
            // Evolve into learned node
            if (number > ROOT_LEVEL2_SIZE * 6) {
                neaten_root();
            }
        }
    }

    // TODO
    void neaten_root() {
        throw std::logic_error("Need to merge root!");
    }

public:

    FlatIndex(K* keys, P* payloads, size_t number)
        :   min_key(keys[0]),
            max_key(keys[number-1]),
            left_buffer(new SIMPLE_LEAF_NODE(0)),
            right_buffer(new SIMPLE_LEAF_NODE(16)) {
        build(keys, payloads, number);
    }

    ~FlatIndex() {
        delete left_buffer;
        delete right_buffer;
        if (level < 16) {
            child->delete_children();
        }
        else if (level < 32) {
            for (size_t i = 0; i < number; i++) {
                nodes[i]->delete_children();
            }
            delete [] nodes;
        }
        else if (level < 48) {
            for (size_t i = 0; i < ROOT_LEVEL2_SIZE; i++)
                if (greater_than(keys_l2l3[i], key_domain_min))
                    nodes[i]->delete_children();
            free(keys_l2l3);
            delete [] nodes;
        }
        else {
            for (size_t i = 0; i < ROOT_LEVEL3_SIZE; i++)
                if (greater_than(keys_l2l3[i + ROOT_LEVEL2_SIZE], key_domain_min))
                    nodes[i]->delete_children();
            free(keys_l2l3);
            delete [] nodes;
        }
    }

    P find(K key) const {
        if (!less_than(key, min_key) && !greater_than(key, max_key)) {
            if (level < 16)
                return child->find(key);
            else if (level < 32) {
                for (size_t i = 1; i < number; i++) {
                    if (greater_than(keys_l1[i], key))
                        return nodes[i - 1]->find(key);
                }
                return nodes[number - 1]->find(key);
            }
            else if (level < 48) {
                int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                    if (greater_than(keys_l1[i], key)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], key) || equal(keys_l2l3[start_pos_l2 + i], key_domain_min))
                        return nodes[start_pos_l2 + i - 1]->find(key);
                }
                return nodes[start_pos_l2 + 7]->find(key);
            }
            else {
                int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                    if (greater_than(keys_l1[i], key)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key) || equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key_domain_min))
                        return nodes[start_pos_l3 + i - 1]->find(key);
                }
                return nodes[start_pos_l3 + 7]->find(key);
            }
        }
        else if (less_than(key, min_key))
            return left_buffer->find(key);
        else
            return right_buffer->find(key);
    }

    void range_query(K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        if (!less_than(lower_bound, min_key) && !greater_than(lower_bound, max_key)) {
            if (level < 16)
                child->range_query(false, lower_bound, upper_bound, answers);
            else if (level < 32) {
                for (size_t i = 1; i < number; i++) {
                    if (greater_than(keys_l1[i], lower_bound)) {
                        nodes[i - 1]->range_query(false, lower_bound, upper_bound, answers);
                        return;
                    }
                }
                nodes[number - 1]->range_query(false, lower_bound, upper_bound, answers);
            }
            else if (level < 48) {
                int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                    if (greater_than(keys_l1[i], lower_bound)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], lower_bound) || equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                        nodes[start_pos_l2 + i - 1]->range_query(false, lower_bound, upper_bound, answers);
                        return;
                    }
                }
                nodes[start_pos_l2 + 7]->range_query(false, lower_bound, upper_bound, answers);
            }
            else {
                int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                    if (greater_than(keys_l1[i], lower_bound)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], lower_bound)) {
                        start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], lower_bound) || equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                        nodes[start_pos_l3 + i - 1]->range_query(false, lower_bound, upper_bound, answers);
                        return;
                    }
                }
                nodes[start_pos_l3 + 7]->range_query(false, lower_bound, upper_bound, answers);
            }
        }
        else if (less_than(lower_bound, min_key))
            left_buffer->range_query(false, lower_bound, upper_bound, answers);
        else
            right_buffer->range_query(false, lower_bound, upper_bound, answers);
    }

    void upsert(K key, P payload) {
        int ref = 0;
        if (!less_than(key, min_key) && !greater_than(key, max_key)) {
            if (level < 16)
                ref = child->upsert(key, payload);
            else if (level < 32) {
                for (size_t i = 1; i < number; i++) {
                    if (greater_than(keys_l1[i], key)) {
                        ref = nodes[i - 1]->upsert(key, payload);
                        break;
                    }
                }
                if (ref == 0)
                    ref = nodes[number - 1]->upsert(key, payload);
            }
            else if (level < 48) {
                int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                    if (greater_than(keys_l1[i], key)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], key) || equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                        ref = nodes[start_pos_l2 + i - 1]->upsert(key, payload);
                        break;
                    }
                }
                if (ref == 0)
                    ref = nodes[start_pos_l2 + 7]->upsert(key, payload);
            }
            else {
                int start_pos_l2 = ROOT_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < ROOT_LEVEL1_SIZE; i++) {
                    if (greater_than(keys_l1[i], key)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (greater_than(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key) || equal(keys_l2l3[start_pos_l3 + i + ROOT_LEVEL2_SIZE], key_domain_min)) {
                        ref = nodes[start_pos_l3 + i - 1]->upsert(key, payload);
                        break;
                    }
                }
                if (ref == 0)
                    ref = nodes[start_pos_l3 + 7]->upsert(key, payload);
            }
            if (ref > 0)
                return;
            else
                split_node(key);
        }
        else if (less_than(key, min_key)) {
            ref = left_buffer->upsert(key, payload);
            if (ref > 0)
                return;
            else
                evolve_node(left_buffer, false);
        }
        else {
            ref = right_buffer->upsert(key, payload);
            if (ref > 0)
                return;
            else
                evolve_node(right_buffer, true);
        }
    }

    // TODO
    // void delete_key(K key) {}

    // Print index info
    void index_size() const {
        size_t model_size = 0;
        size_t slot_size = 0;
        size_t data_size = 0;
        size_t ob_model_size = 0;
        size_t ob_slot_size = 0;
        size_t ob_data_size = 0;
        size_t inner_node = 0;
        size_t data_node = 0;
        size_t index_depth = level % 16 + 1;
        if (level >= 16) {
            index_depth++;
            inner_node++;
        }
        model_size += sizeof(*this);
        left_buffer->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
        right_buffer->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
        if (level < 32) {
            model_size += ROOT_LEVEL1_SIZE * sizeof(NODE*);
            for (size_t i = 0; i < number; i++) {
                nodes[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
        else if (level < 48) {
            model_size += ROOT_LEVEL2_SIZE * (sizeof(K) + sizeof(NODE*));
            for (size_t i = 0; i < ROOT_LEVEL2_SIZE; i++) {
                if (greater_than(keys_l2l3[i], key_domain_min))
                    nodes[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
        else {
            model_size += ROOT_LEVEL2_SIZE * sizeof(K) + ROOT_LEVEL3_SIZE * (sizeof(K) + sizeof(NODE*));
            for (size_t i = 0; i < ROOT_LEVEL3_SIZE; i++) {
                if (greater_than(keys_l2l3[i + ROOT_LEVEL2_SIZE], key_domain_min))
                    nodes[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }

        std::cout << "Total size: " << (double)(model_size+slot_size+ob_model_size+ob_slot_size)/(1024*1024) << " MB" << std::endl;
        std::cout << "Model size: " << (double)model_size/(1024*1024) << " MB" << std::endl;
        std::cout << "Fill rate: " << (double)100*data_size/slot_size << "%" << std::endl;
        std::cout << "OB data rate: " << (double)100*ob_data_size/data_size << "%" << std::endl;
        std::cout << "OB fill rate: " << (double)100*ob_data_size/ob_slot_size << "%" << std::endl;
        std::cout << "Index depth: " << index_depth << std::endl;
        std::cout << "Number of inner nodes: " << inner_node << std::endl;
        std::cout << "Number of data nodes: " << data_node << std::endl;
    }

};

// For storing models, to build nodes
template<typename K, typename P>
struct FlatIndex<K, P>::Segment {
    K first_key;
    double slope;
    double intercept;
    size_t number;
    explicit Segment(const typename OptimalPiecewiseLinearModel<K, size_t>::CanonicalSegment &cs)
            : first_key(cs.get_first_x()),
              number(cs.get_number()) {
        auto [cs_slope, cs_intercept] = cs.get_floating_point_segment(first_key);
        slope = cs_slope;
        intercept = cs_intercept;
    }
};


template<typename K, typename P>
struct FlatIndex<K, P>::LinearModelBuilder {

    K first_key;
    double slope = 0;
    double intercept = 0;
    size_t number = 0;
    long double x_sum = 0;
    long double y_sum = 0;
    long double xx_sum = 0;
    long double xy_sum = 0;
    K* keys;
    P* payloads;
    NODE** nodes;

    // node_type: 0 -> leaf node; 1 -> inner node.
    LinearModelBuilder(size_t size, bool node_type) {
        keys = new K[size];
        if (node_type) {
            payloads = NULL;
            nodes = new NODE*[size];
        }
        else {
            payloads = new P[size];
            nodes = NULL;
        }
    }

    ~LinearModelBuilder() {
        delete [] keys;
        delete [] payloads;
        delete [] nodes;
    }

    void add(K key, P payload) {
        x_sum += static_cast<long double>(key);
        y_sum += static_cast<long double>(number);
        xx_sum += static_cast<long double>(key) * key;
        xy_sum += static_cast<long double>(key) * number;
        keys[number] = key;
        payloads[number] = payload;
        number++;
    }

    void add(K key, NODE* node) {
        x_sum += static_cast<long double>(key);
        y_sum += static_cast<long double>(number);
        xx_sum += static_cast<long double>(key) * key;
        xy_sum += static_cast<long double>(key) * number;
        keys[number] = key;
        nodes[number] = node;
        number++;
    }

    void build_model() {
        first_key = keys[0];
        slope = static_cast<double>((static_cast<long double>(number) * xy_sum - x_sum * y_sum) / (static_cast<long double>(number) * xx_sum - x_sum * x_sum));
        intercept = static_cast<double>((y_sum - static_cast<long double>(slope) * x_sum) / number);
    }

    K get_max_key() const { return keys[number]; }

};

