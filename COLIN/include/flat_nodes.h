//
// Created by tawnysky on 2020/10/26.
//

#pragma once


#include "parameters.h"

#define SIMPLE_LEAF_NODE_LEVEL2_SIZE (SIMPLE_LEAF_NODE_LEVEL1_SIZE << 3)
#define SIMPLE_LEAF_NODE_LEVEL3_SIZE (SIMPLE_LEAF_NODE_LEVEL1_SIZE << 6)
#define SIMPLE_INNER_NODE_LEVEL2_SIZE (SIMPLE_INNER_NODE_LEVEL1_SIZE << 3)
#define SIMPLE_INNER_NODE_LEVEL3_SIZE (SIMPLE_INNER_NODE_LEVEL1_SIZE << 6)


template<typename K, typename P>
class FlatNode{

public:

    // Bit 6-7: node type (0->simple leaf node, 1->leaf node, 2->simple inner node, 3->inner node); bit 4-5: node role
    // (case simple leaf node: 0->left buffer, 1->right buffer, 2->overflow block; case simple inner node: 0->regular
    // node, 1->temporary inner node); bit 0-3: level (0-15).
    // Special number: 0->left buffer, 16->right buffer, 32->overflow block, 64->leaf node, 129->simple inner node at
    // l1, 144->temporary inner node, 193->inner node at l1.
    uint8_t level = 0;

    // For double comparison
    inline bool equal(K key1, K key2) const {
        //return (key1 - key2 <= EPS && key1 - key2 >= -EPS);
        return (key1 == key2);
    }

    inline bool greater_than(K key1, K key2) const {
        //return (key1 - key2 > EPS);
        return (key1 > key2);
    }

    inline bool less_than(K key1, K key2) const {
        //return (key1 - key2 < -EPS);
        return (key1 < key2);
    }

    [[nodiscard]] uint8_t get_level() const {
        return level;
    }

    explicit FlatNode(uint8_t level) : level(level) {}

    virtual ~FlatNode() = default;

    virtual P find(K key) const = 0;

    // Type: 0 -> from father; 1 -> from left brother.
    virtual void range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const = 0;

    virtual void find_split_node(K key, std::vector<FlatNode<K, P>*>& traversal_path) = 0;

    virtual int upsert(K key, P payloads) = 0;

    virtual void delete_children() = 0;

    virtual void node_size(size_t& model_size, size_t& slot_size, size_t& data_size, size_t& ob_model_size, size_t& ob_slot_size, size_t& ob_data_size, size_t& inner_node, size_t& data_node) const = 0;

};

// Left buffer, right buffer, overflow block.
template<typename K, typename P>
class FlatSimpleLeafNode : public FlatNode<K, P> {

    using NODE = FlatNode<K, P>;

protected:

    uint8_t keys_level = 1;
    uint16_t number = 0;
    P* payloads;
    K keys_l1[SIMPLE_LEAF_NODE_LEVEL1_SIZE];
    K* keys_l2l3 = NULL;
    const K key_domain_min = std::numeric_limits<K>::lowest();
    NODE* pointer = NULL;

    // Expand level 1 to level 2 (x8)
    void expand_l2() {
        keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * SIMPLE_LEAF_NODE_LEVEL2_SIZE));
        P* payloads_tmp = payloads;
        payloads = new P[SIMPLE_LEAF_NODE_LEVEL2_SIZE];
        for (size_t i = 0; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
            keys_l2l3[i << 3] = keys_l1[i];
            payloads[i << 3] = payloads_tmp[i];
            for (size_t j = 1; j < 8; j++) {
                keys_l2l3[(i << 3) + j] = key_domain_min;
            }
        }
        delete [] payloads_tmp;
        keys_level = 2;
    }

    // Expand level 2 to level 3 (x8)
    void expand_l3() {
        K* keys_l2_tmp = keys_l2l3;
        keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * (SIMPLE_LEAF_NODE_LEVEL2_SIZE + SIMPLE_LEAF_NODE_LEVEL3_SIZE)));
        memcpy(keys_l2l3, keys_l2_tmp, SIMPLE_LEAF_NODE_LEVEL2_SIZE * sizeof(K));
        P* payloads_tmp = payloads;
        payloads = new P[SIMPLE_LEAF_NODE_LEVEL3_SIZE];
        for (size_t i = 0; i < SIMPLE_LEAF_NODE_LEVEL2_SIZE; i++) {
            keys_l2l3[(i << 3) + SIMPLE_LEAF_NODE_LEVEL2_SIZE] = keys_l2_tmp[i];
            payloads[i << 3] = payloads_tmp[i];
            for (size_t j = 1; j < 8; j++) {
                keys_l2l3[(i << 3) + j + SIMPLE_LEAF_NODE_LEVEL2_SIZE] = key_domain_min;
            }
        }
        free(keys_l2_tmp);
        delete [] payloads_tmp;
        keys_level = 3;
    }

public:

    explicit FlatSimpleLeafNode(uint8_t level)
        :   NODE(level),
            payloads(new P[SIMPLE_LEAF_NODE_LEVEL1_SIZE]) {}

    ~FlatSimpleLeafNode() {
        if (keys_level > 1) {
            assert(keys_l2l3 != NULL);
            free(keys_l2l3);
        }
        delete [] payloads;
    }

    void delete_children() { delete this; }

    size_t get_number() { return number; }

    K* get_keys() {
        if (keys_level == 1)
            return keys_l1;
        else if (keys_level == 2)
            return keys_l2l3;
        else
            return keys_l2l3 + SIMPLE_LEAF_NODE_LEVEL2_SIZE;
    }

    P* get_payloads() { return payloads; }

    NODE* get_pointer() { return pointer; }

    void set_pointer(NODE* node) { pointer = node; }

    size_t get_size() {
        if (keys_level == 1)
            return number;
        else if (keys_level == 2)
            return SIMPLE_LEAF_NODE_LEVEL2_SIZE;
        else
            return SIMPLE_LEAF_NODE_LEVEL3_SIZE;
    }

    P find(K key) const {

        // Level 1
        if (keys_level == 1) {
            for (size_t i = 0; i < number; i++) {
                if (NODE::equal(keys_l1[i], key))
                    return payloads[i];
            }
            return 0;
        }

        // Level 2
        else if (keys_level == 2) {
            int start_pos_l2 = SIMPLE_LEAF_NODE_LEVEL2_SIZE - 8;
            for (int i = 0; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            if (start_pos_l2 >= 0){
                for (size_t i = 0; i < 8; i++) {
                    if (NODE::equal(keys_l2l3[start_pos_l2 + i], key))
                        return payloads[start_pos_l2 + i];
                }
            }
            return 0;
        }

        // Level 3
        else {
            int start_pos_l2 = SIMPLE_LEAF_NODE_LEVEL2_SIZE - 8;
            for (int i = 0; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            if (start_pos_l2 >= 0) {
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (size_t i = 1; i < 8; i++) {
                    if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 0; i < 8; i++) {
                    if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key))
                        return payloads[start_pos_l3 + i];
                }
            }
            return 0;
        }
    }

    void range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        if (type) {
            // Level 1
            if (keys_level == 1) {
                for (size_t it = 0; it < number; it++) {
                    if (!NODE::greater_than(keys_l1[it], upper_bound))
                        answers.emplace_back(keys_l1[it], payloads[it]);
                    else
                        return;
                }
            }

            // Level 2
            else if (keys_level == 2) {
                for (size_t it = 0; it < SIMPLE_LEAF_NODE_LEVEL2_SIZE; it++) {
                    if(!NODE::greater_than(keys_l2l3[it], upper_bound)) {
                        if (!NODE::equal(keys_l2l3[it], key_domain_min))
                            answers.emplace_back(keys_l2l3[it], payloads[it]);
                    }
                    else
                        return;
                }
            }

            // Level 3
            else {
                for (size_t it = 0; it < SIMPLE_LEAF_NODE_LEVEL3_SIZE; it++) {
                    if (!NODE::greater_than(keys_l2l3[it + SIMPLE_LEAF_NODE_LEVEL2_SIZE], upper_bound)) {
                        if (!NODE::equal(keys_l2l3[it + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min))
                            answers.emplace_back(keys_l2l3[it + SIMPLE_LEAF_NODE_LEVEL2_SIZE], payloads[it]);
                    }
                    else
                        return;
                }
            }
        }
        else {
            bool end_flag = false;
            // Level 1
            if (keys_level == 1) {
                for (size_t it = 0; it < number; it++) {
                    if (!NODE::less_than(keys_l1[it], lower_bound)) {
                        while (it < number && !NODE::greater_than(keys_l1[it], upper_bound)) {
                            answers.emplace_back(keys_l1[it], payloads[it]);
                            it++;
                        }
                        if (it < number)
                            end_flag = true;
                        break;
                    }
                }
            }

            // Level 2
            else if (keys_level == 2) {
                int start_pos_l2 = SIMPLE_LEAF_NODE_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                    if (NODE::greater_than(keys_l1[i], lower_bound)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 0; i < 8; i++) {
                    if (!NODE::less_than(keys_l2l3[start_pos_l2 + i], lower_bound)) {
                        size_t it = start_pos_l2 + i;
                        while (it < SIMPLE_LEAF_NODE_LEVEL2_SIZE && !NODE::greater_than(keys_l2l3[it], upper_bound)) {
                            if (!NODE::equal(keys_l2l3[it], key_domain_min))
                                answers.emplace_back(keys_l2l3[it], payloads[it]);
                            it++;
                        }
                        if (it < SIMPLE_LEAF_NODE_LEVEL2_SIZE)
                            end_flag = true;
                        break;
                    }
                }
            }

            // Level 3
            else {
                int start_pos_l2 = SIMPLE_LEAF_NODE_LEVEL2_SIZE - 8;
                for (size_t i = 1; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                    if (NODE::greater_than(keys_l1[i], lower_bound)) {
                        start_pos_l2 = (i - 1) << 3;
                        break;
                    }
                }
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (size_t i = 1; i < 8; i++) {
                    if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], lower_bound)) {
                        start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                        break;
                    }
                }
                for (size_t i = 0; i < 8; i++) {
                    if (!NODE::less_than(keys_l2l3[start_pos_l3 + i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], lower_bound)) {
                        size_t it = start_pos_l3 + i;
                        while (it < SIMPLE_LEAF_NODE_LEVEL3_SIZE && !NODE::greater_than(keys_l2l3[it + SIMPLE_LEAF_NODE_LEVEL2_SIZE], upper_bound)) {
                            if (!NODE::equal(keys_l2l3[it + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min))
                                answers.emplace_back(keys_l2l3[it + SIMPLE_LEAF_NODE_LEVEL2_SIZE], payloads[it]);
                            it++;
                        }
                        if (it < SIMPLE_LEAF_NODE_LEVEL3_SIZE)
                            end_flag = true;
                        break;
                    }
                }
            }
            if (!end_flag && NODE::get_level() == 0)
                pointer->range_query(true, lower_bound, upper_bound, answers);
        }
    }

    void find_split_node(K key, std::vector<NODE*>& traversal_path) {
        throw std::logic_error("Get traversal path error!");
    }

    // 1: insert success; 2: update success; -1: insert fault and evolve node.
    int upsert(K key, P payload) {
        assert(number < SIMPLE_LEAF_NODE_LEVEL3_SIZE);
        // Level 1
        if (keys_level == 1) {
            size_t target_pos = number;
            for (size_t i = 0; i < number; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    target_pos = i;
                    break;
                }
            }
            // Update
            if (target_pos > 0 && NODE::equal(keys_l1[target_pos - 1], key)) {
                payloads[target_pos - 1] = payload;
                return 2;
            }
            assert(number >= target_pos && number < SIMPLE_LEAF_NODE_LEVEL1_SIZE);
            memmove(keys_l1 + target_pos + 1, keys_l1 + target_pos, (number - target_pos) * sizeof(K));
            memmove(payloads + target_pos + 1, payloads + target_pos, (number - target_pos) * sizeof(P));
            keys_l1[target_pos] = key;
            payloads[target_pos] = payload;
            number++;
            if (number == SIMPLE_LEAF_NODE_LEVEL1_SIZE)
                expand_l2();
            return 1;
        }

        // Level 2
        else if (keys_level == 2) {

            // Get start position (line) at level 2
            int start_pos_l2 = SIMPLE_LEAF_NODE_LEVEL2_SIZE - 8;
            for (int i = 0; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            if (start_pos_l2 >= 0) {
                int target_pos = start_pos_l2 + 8;
                int free_pos = target_pos;
                for (size_t i = 0; i < 8; i++) {
                    // Update
                    if (NODE::equal(keys_l2l3[start_pos_l2 + i], key)) {
                        payloads[start_pos_l2 + i] = payload;
                        return 2;
                    }
                    else if (NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                        free_pos = start_pos_l2 + i;
                        target_pos = free_pos;
                        break;
                    }
                    else if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        target_pos = start_pos_l2 + i;
                        break;
                    }
                }
                if (free_pos != target_pos) {
                    for (int i = target_pos - start_pos_l2 + 1; i < 8; i++) {
                        if (NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                            free_pos = start_pos_l2 + i;
                            break;
                        }
                    }
                }
                // Target line has free position
                if (free_pos < start_pos_l2 + 8) {
                    assert(free_pos >= target_pos && free_pos < SIMPLE_LEAF_NODE_LEVEL2_SIZE);
                    memmove(keys_l2l3 + target_pos + 1, keys_l2l3 + target_pos, (free_pos - target_pos) * sizeof(K));
                    memmove(payloads + target_pos + 1, payloads + target_pos, (free_pos - target_pos) * sizeof(P));
                    keys_l2l3[target_pos] = key;
                    payloads[target_pos] = payload;
                }
                // Target line is full
                else {
                    for (int i = 1; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                        // Borrow from front line
                        if (start_pos_l2 - (i << 3) + 7 > 0 && NODE::equal(keys_l2l3[start_pos_l2 - (i << 3) + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l2 - (i << 3) + 8;
                            for (size_t j = 2; j < 9; j++) {
                                if (NODE::greater_than(keys_l2l3[influenced_header - j], key_domain_min)) {
                                    free_pos = influenced_header - j + 1;
                                    break;
                                }
                            }
                            keys_l2l3[free_pos] = keys_l2l3[influenced_header];
                            payloads[free_pos] = payloads[influenced_header];
                            assert(target_pos > influenced_header && target_pos <= SIMPLE_LEAF_NODE_LEVEL2_SIZE);
                            memmove(keys_l2l3 + influenced_header, keys_l2l3 + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(K));
                            memmove(payloads + influenced_header, payloads + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(P));
                            keys_l2l3[target_pos - 1] = key;
                            payloads[target_pos - 1] = payload;
                            for (size_t j = influenced_header; j < target_pos; j = j + 8) {
                                keys_l1[j >> 3] = keys_l2l3[j];
                            }
                            break;
                        }
                        // Borrow from latter line
                        if (start_pos_l2 + (i << 3) + 7 < SIMPLE_LEAF_NODE_LEVEL2_SIZE && NODE::equal(keys_l2l3[start_pos_l2 + (i << 3) + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l2 + (i << 3);
                            for (size_t j = 1; j < 8; j++) {
                                if (NODE::equal(keys_l2l3[influenced_header + j], key_domain_min)) {
                                    free_pos = influenced_header + j;
                                    break;
                                }
                            }
                            assert(free_pos >= target_pos && free_pos < SIMPLE_LEAF_NODE_LEVEL2_SIZE);
                            memmove(keys_l2l3 + target_pos + 1, keys_l2l3 + target_pos, (free_pos - target_pos) * sizeof(K));
                            memmove(payloads + target_pos + 1, payloads + target_pos, (free_pos - target_pos) * sizeof(P));
                            keys_l2l3[target_pos] = key;
                            payloads[target_pos] = payload;
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
                for (size_t i = 7; i < SIMPLE_LEAF_NODE_LEVEL2_SIZE; i = i + 8) {
                    if (NODE::equal(keys_l2l3[i], key_domain_min)) {
                        free_pos = i;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (NODE::greater_than(keys_l2l3[free_pos - i], key_domain_min)) {
                        free_pos = free_pos - i + 1;
                        break;
                    }
                }
                assert(free_pos < SIMPLE_LEAF_NODE_LEVEL2_SIZE);
                memmove(keys_l2l3 + 1, keys_l2l3, free_pos * sizeof(K));
                memmove(payloads + 1, payloads, free_pos * sizeof(P));
                keys_l2l3[0] = key;
                payloads[0] = payload;
                for (size_t i = 0; i << 3 < free_pos; i++) {
                    keys_l1[i] = keys_l2l3[i << 3];
                }
            }
            number++;
            if (number == SIMPLE_LEAF_NODE_LEVEL2_SIZE) {
                expand_l3();
            }
                
            return 1;
        }

        // Level 3
        else {

            // Get start position (line) at level 2
            int start_pos_l2 = SIMPLE_LEAF_NODE_LEVEL2_SIZE - 8;
            for (int i = 0; i < SIMPLE_LEAF_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }

            if (start_pos_l2 >= 0) {
                
                // Get start position (line) at level 3
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (int i = start_pos_l2; i < start_pos_l2 + 8; i++) {
                    if (NODE::greater_than(keys_l2l3[i], key)) {
                        start_pos_l3 = (i - 1) << 3;
                        break;
                    }
                }

                int target_pos = start_pos_l3 + 8;
                int free_pos = target_pos;
                for (size_t i = 0; i < 8; i++) {
                    // Update
                    if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key)) {
                        payloads[start_pos_l3 + i] = payload;
                        return 2;
                    }
                    else if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = start_pos_l3 + i;
                        target_pos = free_pos;
                        break;
                    }
                    else if (NODE::greater_than(keys_l2l3[start_pos_l3 + i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key)) {
                        target_pos = start_pos_l3 + i;
                        break;
                    }
                }
                if (free_pos != target_pos) {
                    for (int i = target_pos - start_pos_l3 + 1; i < 8; i++) {
                        if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min)) {
                            free_pos = start_pos_l3 + i;
                            break;
                        }
                    }
                }
                // Target line has free position
                if (free_pos < start_pos_l3 + 8) {
                    assert(free_pos >= target_pos && free_pos < SIMPLE_LEAF_NODE_LEVEL3_SIZE);
                    memmove(keys_l2l3 + target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE + 1, keys_l2l3 + target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE, (free_pos - target_pos) * sizeof(K));
                    memmove(payloads + target_pos + 1, payloads + target_pos, (free_pos - target_pos) * sizeof(P));
                    keys_l2l3[target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE] = key;
                    payloads[target_pos] = payload;
                }
                // Borrow free position
                else {
                    for (int i = 1; i < SIMPLE_LEAF_NODE_LEVEL2_SIZE; i++) {
                        // Borrow from front line
                        if (start_pos_l3 - (i << 3) + 7 > 0 && NODE::equal(keys_l2l3[start_pos_l3 - (i << 3) + SIMPLE_LEAF_NODE_LEVEL2_SIZE + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l3 - (i << 3) + 8;
                            for (size_t j = 2; j < 9; j++) {
                                if (NODE::greater_than(keys_l2l3[influenced_header - j + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min)) {
                                    free_pos = influenced_header - j + 1;
                                    break;
                                }
                            }
                            keys_l2l3[free_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE] = keys_l2l3[influenced_header + SIMPLE_LEAF_NODE_LEVEL2_SIZE];
                            payloads[free_pos] = payloads[influenced_header];
                            assert(target_pos > influenced_header && target_pos <= SIMPLE_LEAF_NODE_LEVEL3_SIZE);
                            memmove(keys_l2l3 + influenced_header + SIMPLE_LEAF_NODE_LEVEL2_SIZE, keys_l2l3 + influenced_header + SIMPLE_LEAF_NODE_LEVEL2_SIZE + 1, (target_pos - influenced_header - 1) * sizeof(K));
                            memmove(payloads + influenced_header, payloads + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(P));
                            keys_l2l3[target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE - 1] = key;
                            payloads[target_pos - 1] = payload;
                            for (size_t j = influenced_header; j < target_pos; j = j + 8) {
                                keys_l2l3[j >> 3] = keys_l2l3[j + SIMPLE_LEAF_NODE_LEVEL2_SIZE];
                            }
                            for (size_t j = ((influenced_header - 1) >> 6) + 1; j << 6 < target_pos; j++) {
                                keys_l1[j] = keys_l2l3[j << 3];
                            }
                            break;
                        }
                        // Borrow from latter line
                        if (start_pos_l3 + (i << 3) + 7 < SIMPLE_LEAF_NODE_LEVEL3_SIZE && NODE::equal(keys_l2l3[start_pos_l3 + (i << 3) + SIMPLE_LEAF_NODE_LEVEL2_SIZE + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l3 + (i << 3);
                            for (size_t j = 1; j < 8; j++) {
                                if (NODE::equal(keys_l2l3[influenced_header + j + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min)) {
                                    free_pos = influenced_header + j;
                                    break;
                                }
                            }
                            assert(free_pos >= target_pos && free_pos < SIMPLE_LEAF_NODE_LEVEL3_SIZE);
                            memmove(keys_l2l3 + target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE + 1, keys_l2l3 + target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE, (free_pos - target_pos) * sizeof(K));
                            memmove(payloads + target_pos + 1, payloads + target_pos, (free_pos - target_pos) * sizeof(P));
                            keys_l2l3[target_pos + SIMPLE_LEAF_NODE_LEVEL2_SIZE] = key;
                            payloads[target_pos] = payload;
                            for (int j = influenced_header; j >= target_pos; j = j - 8) {
                                keys_l2l3[j >> 3] = keys_l2l3[j + SIMPLE_LEAF_NODE_LEVEL2_SIZE];
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
                for (size_t i = 7; i < SIMPLE_LEAF_NODE_LEVEL3_SIZE; i = i + 8) {
                    if (NODE::equal(keys_l2l3[i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = i;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (NODE::greater_than(keys_l2l3[free_pos - i + SIMPLE_LEAF_NODE_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = free_pos - i + 1;
                        break;
                    }
                }
                assert(free_pos < SIMPLE_LEAF_NODE_LEVEL3_SIZE);
                memmove(keys_l2l3 + SIMPLE_LEAF_NODE_LEVEL2_SIZE + 1, keys_l2l3 + SIMPLE_LEAF_NODE_LEVEL2_SIZE, free_pos * sizeof(K));
                memmove(payloads + 1, payloads, free_pos * sizeof(P));
                keys_l2l3[SIMPLE_LEAF_NODE_LEVEL2_SIZE] = key;
                payloads[0] = payload;
                for (size_t i = 0; i << 3 < free_pos; i++) {
                    keys_l2l3[i] = keys_l2l3[(i << 3) + SIMPLE_LEAF_NODE_LEVEL2_SIZE];
                }
                for (size_t i = 0; i << 6 < free_pos; i++) {
                    keys_l1[i] = keys_l2l3[i << 3];
                }
            }

            number++;
            // Evolve into learned node
            if (number <= SIMPLE_LEAF_NODE_LEVEL2_SIZE * 6)
                return 1;
            else
                return -1;
        }
    }

    void node_size(size_t& model_size, size_t& slot_size, size_t& data_size, size_t& ob_model_size, size_t& ob_slot_size, size_t& ob_data_size, size_t& inner_node, size_t& data_node) const {
        if (NODE::get_level() == 32) {
            ob_model_size += sizeof(*this);
            ob_data_size += number * (sizeof(K) + sizeof(P));
            if (keys_level == 1) {
                ob_model_size -= SIMPLE_LEAF_NODE_LEVEL1_SIZE * sizeof(K);
                ob_slot_size += SIMPLE_LEAF_NODE_LEVEL1_SIZE * (sizeof(K) + sizeof(P));
            }
            else if (keys_level == 2) {
                ob_slot_size += SIMPLE_LEAF_NODE_LEVEL2_SIZE * (sizeof(K) + sizeof(P));
            }
            else {
                ob_model_size += SIMPLE_LEAF_NODE_LEVEL2_SIZE * sizeof(K);
                ob_slot_size += SIMPLE_LEAF_NODE_LEVEL3_SIZE * (sizeof(K) + sizeof(P));
            }
        }
        else {
            model_size += sizeof(*this);
            data_size += number * (sizeof(K) + sizeof(P));
            if (keys_level == 1) {
                model_size -= SIMPLE_LEAF_NODE_LEVEL1_SIZE * sizeof(K);
                slot_size += SIMPLE_LEAF_NODE_LEVEL1_SIZE * (sizeof(K) + sizeof(P));
            }
            else if (keys_level == 2) {
                slot_size += SIMPLE_LEAF_NODE_LEVEL2_SIZE * (sizeof(K) + sizeof(P));
            }
            else {
                model_size += SIMPLE_LEAF_NODE_LEVEL2_SIZE * sizeof(K);
                slot_size += SIMPLE_LEAF_NODE_LEVEL3_SIZE * (sizeof(K) + sizeof(P));
            }
        }
    }

};

// Simple inner node, temporary inner node.
template<typename K, typename P>
class FlatSimpleInnerNode : public FlatNode<K, P> {

    using NODE = FlatNode<K, P>;

protected:

    uint8_t keys_level = 1;
    uint16_t number = 0;
    NODE** nodes;
    K keys_l1[SIMPLE_INNER_NODE_LEVEL1_SIZE];
    K* keys_l2l3 = NULL;
    const K key_domain_min = std::numeric_limits<K>::lowest();



    // Expand level 1 to level 2 (x8)
    void expand_l2() {
        keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * SIMPLE_INNER_NODE_LEVEL2_SIZE));
        NODE** nodes_tmp = nodes;
        nodes = new NODE*[SIMPLE_INNER_NODE_LEVEL2_SIZE]();
        for (size_t i = 0; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
            keys_l2l3[i << 3] = keys_l1[i];
            nodes[i << 3] = nodes_tmp[i];
            for (size_t j = 1; j < 8; j++) {
                keys_l2l3[(i << 3) + j] = key_domain_min;
            }
        }
        delete [] nodes_tmp;
        keys_level = 2;
    }

    // Expand level 2 to level 3 (x8)
    void expand_l3() {
        K* keys_l2_tmp = keys_l2l3;
        keys_l2l3 = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * (SIMPLE_INNER_NODE_LEVEL2_SIZE + SIMPLE_INNER_NODE_LEVEL3_SIZE)));
        memcpy(keys_l2l3, keys_l2_tmp, SIMPLE_INNER_NODE_LEVEL2_SIZE * sizeof(K));
        NODE** nodes_tmp = nodes;
        nodes = new NODE*[SIMPLE_INNER_NODE_LEVEL3_SIZE]();
        for (size_t i = 0; i < SIMPLE_INNER_NODE_LEVEL2_SIZE; i++) {
            keys_l2l3[(i << 3) + SIMPLE_INNER_NODE_LEVEL2_SIZE] = keys_l2_tmp[i];
            nodes[i << 3] = nodes_tmp[i];
            for (size_t j = 1; j < 8; j++) {
                keys_l2l3[(i << 3) + j + SIMPLE_INNER_NODE_LEVEL2_SIZE] = key_domain_min;
            }
        }
        free(keys_l2_tmp);
        delete [] nodes_tmp;
        keys_level = 3;
    }

public:

    explicit FlatSimpleInnerNode(uint8_t level)
            :   NODE(level),
                nodes(new NODE*[SIMPLE_INNER_NODE_LEVEL1_SIZE]) {}

    ~FlatSimpleInnerNode() {
        if (keys_level >= 2)
            free(keys_l2l3);
        delete [] nodes;
    }

    void delete_children() {
        if (keys_level == 1) {
            for (size_t i = 0; i < number; i++)
                nodes[i]->delete_children();
        }
        else if (keys_level == 2) {
            for (size_t i = 0; i < SIMPLE_INNER_NODE_LEVEL2_SIZE; i++)
                if (NODE::greater_than(keys_l2l3[i], key_domain_min))
                    nodes[i]->delete_children();
        }
        else {
            for (size_t i = 0; i < SIMPLE_INNER_NODE_LEVEL3_SIZE; i++)
                if (NODE::greater_than(keys_l2l3[i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min))
                    nodes[i]->delete_children();
        }
        delete this;
    }

    size_t get_number() { return number; }

    K* get_keys() {
        if (keys_level == 1)
            return keys_l1;
        else if (keys_level == 2)
            return keys_l2l3;
        else
            return keys_l2l3 + SIMPLE_INNER_NODE_LEVEL2_SIZE;
    }

    NODE** get_nodes() { return nodes; }

    size_t get_size() {
        if (keys_level == 1)
            return number;
        else if (keys_level == 2)
            return SIMPLE_INNER_NODE_LEVEL2_SIZE;
        else
            return SIMPLE_INNER_NODE_LEVEL3_SIZE;
    }

    P find(K key) const {

        // Level 1
        if (keys_level == 1) {
            assert(!NODE::greater_than(keys_l1[0], key));
            for (size_t i = 1; i < number; i++) {
                if (NODE::greater_than(keys_l1[i], key))
                    return nodes[i - 1]->find(key);
            }
            return nodes[number - 1]->find(key);
        }

        // Level 2
        else if (keys_level == 2) {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key) || NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min))
                    return nodes[start_pos_l2 + i - 1]->find(key);
            }
            return nodes[start_pos_l2 + 7]->find(key);
        }

        // Level 3
        else {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            int start_pos_l3 = (start_pos_l2 << 3) + 56;
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                    start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key) || NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min))
                    return nodes[start_pos_l3 + i - 1]->find(key);
            }
            return nodes[start_pos_l3 + 7]->find(key);
        }
    }

    void range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        // Level 1
        if (keys_level == 1) {
            for (size_t i = 1; i < number; i++) {
                if (NODE::greater_than(keys_l1[i], lower_bound)) {
                    nodes[i - 1]->range_query(false, lower_bound, upper_bound, answers);
                    return;
                }
            }
            nodes[number - 1]->range_query(false, lower_bound, upper_bound, answers);
        }

        // Level 2
        else if (keys_level == 2) {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], lower_bound)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], lower_bound) || NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                    nodes[start_pos_l2 + i - 1]->range_query(false, lower_bound, upper_bound, answers);
                    return;
                }
            }
            nodes[start_pos_l2 + 7]->range_query(false, lower_bound, upper_bound, answers);
        }

        // Level 3
        else {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], lower_bound)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            int start_pos_l3 = (start_pos_l2 << 3) + 56;
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], lower_bound)) {
                    start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], lower_bound) || NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                    nodes[start_pos_l3 + i - 1]->range_query(false, lower_bound, upper_bound, answers);
                    return;
                }
            }
            nodes[start_pos_l3 + 7]->range_query(false, lower_bound, upper_bound, answers);
        }
    }

    void find_split_node(K key, std::vector<NODE*>& traversal_path) {
        if (NODE::get_level() < 144)
            traversal_path.push_back(this);
        // Level 1
        if (keys_level == 1) {
            for (size_t i = 1; i < number; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    nodes[i - 1]->find_split_node(key, traversal_path);
                    return;
                }
            }
            nodes[number - 1]->find_split_node(key, traversal_path);
        }

        // Level 2
        else if (keys_level == 2) {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key) || NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                    nodes[start_pos_l2 + i - 1]->find_split_node(key, traversal_path);
                    return;
                }
            }
            nodes[start_pos_l2 + 7]->find_split_node(key, traversal_path);
        }
        // Level 3
        else {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;

            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            int start_pos_l3 = (start_pos_l2 << 3) + 56;
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                    start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key) || NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                    nodes[start_pos_l3 + i - 1]->find_split_node(key, traversal_path);
                    return;
                }
            }
            nodes[start_pos_l3 + 7]->find_split_node(key, traversal_path);
        }
    }

    int upsert(K key, P payload) {
        // Level 1
        if (keys_level == 1) {
            for (size_t i = 1; i < number; i++) {
                if (NODE::greater_than(keys_l1[i], key))
                    return nodes[i - 1]->upsert(key, payload);
            }
            return nodes[number - 1]->upsert(key, payload);
        }

        // Level 2
        else if (keys_level == 2) {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key) || NODE:: equal(keys_l2l3[start_pos_l2 + i], key_domain_min))
                    return nodes[start_pos_l2 + i - 1]->upsert(key, payload);
            }
            return nodes[start_pos_l2 + 7]->upsert(key, payload);
        }

        // Level 3
        else {
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (size_t i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }
            int start_pos_l3 = (start_pos_l2 << 3) + 56;
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                    start_pos_l3 = (start_pos_l2 + i - 1) << 3;
                    break;
                }
            }
            for (size_t i = 1; i < 8; i++) {
                if (NODE::greater_than(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key) || NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min))
                    return nodes[start_pos_l3 + i - 1]->upsert(key, payload);
            }
            return nodes[start_pos_l3 + 7]->upsert(key, payload);
        }
    }

    // 1: insert success; 2: update success; -1: insert success and evolve node (case simple inner node).
    int upsert_node(K key, FlatNode<K, P>* node) {

        assert(number < SIMPLE_INNER_NODE_LEVEL3_SIZE);

        // Level 1
        if (keys_level == 1) {
            size_t target_pos = number;
            for (size_t i = 0; i < number; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    target_pos = i;
                    break;
                }
            }
            // Update
            if (target_pos > 0 && NODE::equal(keys_l1[target_pos - 1], key)) {
                nodes[target_pos - 1] = node;
                return 2;
            }
            assert(number >= target_pos && number < SIMPLE_INNER_NODE_LEVEL1_SIZE);
            memmove(keys_l1 + target_pos + 1, keys_l1 + target_pos, (number - target_pos) * sizeof(K));
            memmove(nodes + target_pos + 1, nodes + target_pos, (number - target_pos) * sizeof(NODE*));
            keys_l1[target_pos] = key;
            nodes[target_pos] = node;
            number++;
            if (number == SIMPLE_INNER_NODE_LEVEL1_SIZE)
                expand_l2();
            return 1;
        }

        // Level 2
        else if (keys_level == 2) {

            // Get start position (line) at level 2
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (int i = 0; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }

            if (start_pos_l2 >= 0) {
                int target_pos = start_pos_l2 + 8;
                int free_pos = target_pos;
                for (size_t i = 0; i < 8; i++) {
                    // Update
                    if (NODE::equal(keys_l2l3[start_pos_l2 + i], key)) {
                        nodes[start_pos_l2 + i] = node;
                        return 2;
                    }
                    else if (NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                        free_pos = start_pos_l2 + i;
                        target_pos = free_pos;
                        break;
                    }
                    else if (NODE::greater_than(keys_l2l3[start_pos_l2 + i], key)) {
                        target_pos = start_pos_l2 + i;
                        break;
                    }
                }
                if (free_pos != target_pos) {
                    for (int i = target_pos - start_pos_l2 + 1; i < 8; i++) {
                        if (NODE::equal(keys_l2l3[start_pos_l2 + i], key_domain_min)) {
                            free_pos = start_pos_l2 + i;
                            break;
                        }
                    }
                }
                // Target line has free position
                if (free_pos < start_pos_l2 + 8) {
                    assert(free_pos >= target_pos && free_pos < SIMPLE_INNER_NODE_LEVEL2_SIZE);
                    memmove(keys_l2l3 + target_pos + 1, keys_l2l3 + target_pos, (free_pos - target_pos) * sizeof(K));
                    memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                    keys_l2l3[target_pos] = key;
                    nodes[target_pos] = node;
                }
                // Target line is full
                else {
                    for (int i = 1; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                        // Borrow from front line
                        if (start_pos_l2 - (i << 3) + 7 > 0 && NODE::equal(keys_l2l3[start_pos_l2 - (i << 3) + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l2 - (i << 3) + 8;
                            for (size_t j = 2; j < 9; j++) {
                                if (NODE::greater_than(keys_l2l3[influenced_header - j], key_domain_min)) {
                                    free_pos = influenced_header - j + 1;
                                    break;
                                }
                            }
                            keys_l2l3[free_pos] = keys_l2l3[influenced_header];
                            nodes[free_pos] = nodes[influenced_header];
                            assert(target_pos > influenced_header && target_pos <= SIMPLE_INNER_NODE_LEVEL2_SIZE);
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
                        if (start_pos_l2 + (i << 3) + 7 < SIMPLE_INNER_NODE_LEVEL2_SIZE && NODE::equal(keys_l2l3[start_pos_l2 + (i << 3) + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l2 + (i << 3);
                            for (size_t j = 1; j < 8; j++) {
                                if (NODE::equal(keys_l2l3[influenced_header + j], key_domain_min)) {
                                    free_pos = influenced_header + j;
                                    break;
                                }
                            }
                            assert(free_pos >= target_pos && free_pos < SIMPLE_INNER_NODE_LEVEL2_SIZE);
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
                for (size_t i = 7; i < SIMPLE_INNER_NODE_LEVEL2_SIZE; i = i + 8) {
                    if (NODE::equal(keys_l2l3[i], key_domain_min)) {
                        free_pos = i;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (NODE::greater_than(keys_l2l3[free_pos - i], key_domain_min)) {
                        free_pos = free_pos - i + 1;
                        break;
                    }
                }
                assert(free_pos < SIMPLE_INNER_NODE_LEVEL2_SIZE);
                memmove(keys_l2l3 + 1, keys_l2l3, free_pos * sizeof(K));
                memmove(nodes + 1, nodes, free_pos * sizeof(NODE*));
                keys_l2l3[0] = key;
                nodes[0] = node;
                for (size_t i = 0; i << 3 < free_pos; i++) {
                    keys_l1[i] = keys_l2l3[i << 3];
                }
            }

            number++;
            if (number == SIMPLE_INNER_NODE_LEVEL2_SIZE)
                expand_l3();
            return 1;
        }

        // Level 3
        else {

            // Get start position (line) at level 2
            int start_pos_l2 = SIMPLE_INNER_NODE_LEVEL2_SIZE - 8;
            for (int i = 0; i < SIMPLE_INNER_NODE_LEVEL1_SIZE; i++) {
                if (NODE::greater_than(keys_l1[i], key)) {
                    start_pos_l2 = (i - 1) << 3;
                    break;
                }
            }


            if (start_pos_l2 >= 0) {

                // Get start position (line) at level 3
                int start_pos_l3 = (start_pos_l2 << 3) + 56;
                for (int i = start_pos_l2; i < start_pos_l2 + 8; i++) {
                    if (NODE::greater_than(keys_l2l3[i], key)) {
                        start_pos_l3 = (i - 1) << 3;
                        break;
                    }
                }

                int target_pos = start_pos_l3 + 8;
                int free_pos = target_pos;
                for (size_t i = 0; i < 8; i++) {
                    // Update
                    if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key)) {
                        nodes[start_pos_l3 + i] = node;
                        return 2;
                    }
                    else if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = start_pos_l3 + i;
                        target_pos = free_pos;
                        break;
                    }
                    else if (NODE::greater_than(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key)) {
                        target_pos = start_pos_l3 + i;
                        break;
                    }
                }
                if (free_pos != target_pos) {
                    for (int i = target_pos - start_pos_l3 + 1; i < 8; i++) {
                        if (NODE::equal(keys_l2l3[start_pos_l3 + i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                            free_pos = start_pos_l3 + i;
                            break;
                        }
                    }
                }
                // Target line has free position
                if (free_pos < start_pos_l3 + 8) {
                    assert(free_pos >= target_pos && free_pos < SIMPLE_INNER_NODE_LEVEL3_SIZE);
                    memmove(keys_l2l3 + target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE + 1, keys_l2l3 + target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE, (free_pos - target_pos) * sizeof(K));
                    memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                    keys_l2l3[target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE] = key;
                    nodes[target_pos] = node;
                }
                // Borrow free position
                else {
                    for (int i = 1; i < SIMPLE_INNER_NODE_LEVEL2_SIZE; i++) {
                        // Borrow from front line
                        if (start_pos_l3 - (i << 3) + 7 > 0 && NODE::equal(keys_l2l3[start_pos_l3 - (i << 3) + SIMPLE_INNER_NODE_LEVEL2_SIZE + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l3 - (i << 3) + 8;
                            for (size_t j = 2; j < 9; j++) {
                                if (NODE::greater_than(keys_l2l3[influenced_header - j + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                                    free_pos = influenced_header - j + 1;
                                    break;
                                }
                            }
                            keys_l2l3[free_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE] = keys_l2l3[influenced_header + SIMPLE_INNER_NODE_LEVEL2_SIZE];
                            nodes[free_pos] = nodes[influenced_header];
                            assert(target_pos > influenced_header && target_pos <= SIMPLE_INNER_NODE_LEVEL3_SIZE);
                            memmove(keys_l2l3 + influenced_header + SIMPLE_INNER_NODE_LEVEL2_SIZE, keys_l2l3 + influenced_header + SIMPLE_INNER_NODE_LEVEL2_SIZE + 1, (target_pos - influenced_header - 1) * sizeof(K));
                            memmove(nodes + influenced_header, nodes + influenced_header + 1, (target_pos - influenced_header - 1) * sizeof(NODE*));
                            keys_l2l3[target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE - 1] = key;
                            nodes[target_pos - 1] = node;
                            for (size_t j = influenced_header; j < target_pos; j = j + 8) {
                                keys_l2l3[j >> 3] = keys_l2l3[j + SIMPLE_INNER_NODE_LEVEL2_SIZE];
                            }
                            for (size_t j = ((influenced_header - 1) >> 6) + 1; j << 6 < target_pos; j++) {
                                keys_l1[j] = keys_l2l3[j << 3];
                            }
                            break;
                        }
                        // Borrow from latter line
                        if (start_pos_l3 + (i << 3) + 7 < SIMPLE_INNER_NODE_LEVEL3_SIZE && NODE::equal(keys_l2l3[start_pos_l3 + (i << 3) + SIMPLE_INNER_NODE_LEVEL2_SIZE + 7], key_domain_min)) {
                            size_t influenced_header = start_pos_l3 + (i << 3);
                            for (size_t j = 1; j < 8; j++) {
                                if (NODE::equal(keys_l2l3[influenced_header + j + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                                    free_pos = influenced_header + j;
                                    break;
                                }
                            }
                            assert(free_pos >= target_pos && free_pos < SIMPLE_INNER_NODE_LEVEL3_SIZE);
                            memmove(keys_l2l3 + target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE + 1, keys_l2l3 + target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE, (free_pos - target_pos) * sizeof(K));
                            memmove(nodes + target_pos + 1, nodes + target_pos, (free_pos - target_pos) * sizeof(NODE*));
                            keys_l2l3[target_pos + SIMPLE_INNER_NODE_LEVEL2_SIZE] = key;
                            nodes[target_pos] = node;
                            for (int j = influenced_header; j >= target_pos; j = j - 8) {
                                keys_l2l3[j >> 3] = keys_l2l3[j + SIMPLE_INNER_NODE_LEVEL2_SIZE];
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
                for (size_t i = 7; i < SIMPLE_INNER_NODE_LEVEL3_SIZE; i = i + 8) {
                    if (NODE::equal(keys_l2l3[i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = i;
                        break;
                    }
                }
                for (size_t i = 1; i < 8; i++) {
                    if (NODE::greater_than(keys_l2l3[free_pos - i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min)) {
                        free_pos = free_pos - i + 1;
                        break;
                    }
                }
                assert(free_pos < SIMPLE_INNER_NODE_LEVEL3_SIZE);
                memmove(keys_l2l3 + SIMPLE_INNER_NODE_LEVEL2_SIZE + 1, keys_l2l3 + SIMPLE_INNER_NODE_LEVEL2_SIZE, free_pos * sizeof(K));
                memmove(nodes + 1, nodes, free_pos * sizeof(NODE*));
                keys_l2l3[SIMPLE_INNER_NODE_LEVEL2_SIZE] = key;
                nodes[0] = node;
                for (size_t i = 0; i << 3 < free_pos; i++) {
                    keys_l2l3[i] = keys_l2l3[(i << 3) + SIMPLE_INNER_NODE_LEVEL2_SIZE];
                }
                for (size_t i = 0; i << 6 < free_pos; i++) {
                    keys_l1[i] = keys_l2l3[i << 3];
                }
            }

            number++;
            // Evolve into learned node
            if (number <= SIMPLE_INNER_NODE_LEVEL2_SIZE * 6) {
                return 1;
            }
            else
                return -1;
        }
    }

    void node_size(size_t& model_size, size_t& slot_size, size_t& data_size, size_t& ob_model_size, size_t& ob_slot_size, size_t& ob_data_size, size_t& inner_node, size_t& data_node) const {
        model_size += sizeof(*this);
        if (keys_level == 1) {
            model_size += SIMPLE_INNER_NODE_LEVEL1_SIZE * sizeof(NODE*);
            for (size_t i = 0; i < number; i++) {
                nodes[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
        else if (keys_level == 2) {
            model_size += SIMPLE_INNER_NODE_LEVEL2_SIZE * (sizeof(K) + sizeof(NODE*));
            for (size_t i = 0; i < SIMPLE_INNER_NODE_LEVEL2_SIZE; i++) {
                if (NODE::greater_than(keys_l2l3[i], key_domain_min))
                    nodes[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
        else {
            model_size += SIMPLE_INNER_NODE_LEVEL2_SIZE * sizeof(K) + SIMPLE_INNER_NODE_LEVEL3_SIZE * (sizeof(K) + sizeof(NODE*));
            for (size_t i = 0; i < SIMPLE_INNER_NODE_LEVEL3_SIZE; i++) {
                if (NODE::greater_than(keys_l2l3[i + SIMPLE_INNER_NODE_LEVEL2_SIZE], key_domain_min))
                    nodes[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
    }

};

// Learned inner node.
template<typename K, typename P>
class FlatInnerNode : public FlatNode<K, P> {

    using NODE = FlatNode<K, P>;
    using SIMPLE_INNER_NODE = FlatSimpleInnerNode<K, P>;

protected:

    size_t size;
    double slope;
    double intercept;
    NODE** nodes;
    K* keys;
    uint8_t* bitmap;
    size_t number = 0;
    size_t overflow_number = 0;
    size_t max_number;
    size_t max_overflow_number;

    inline K get_key(size_t position) const {
        return keys[position];
    }

    inline NODE* get_node(size_t position) const {
        return nodes[position];
    }

    [[nodiscard]] bool bitmap_get(size_t offset) const {
        size_t bitmap_array = offset >> 3;
        size_t bitmap_bit = offset & 7;
        return (bool)(bitmap[bitmap_array] & 1 << bitmap_bit);
    }

    void bitmap_set (size_t offset) {
        size_t bitmap_array = offset >> 3;
        size_t bitmap_bit = offset & 7;
        bitmap[bitmap_array] |= 1 << bitmap_bit;
    }

    inline void insert_key(K key, NODE* node, size_t position) {
        keys[position] = key;
        nodes[position] = node;
    }

    void model_correction(K min_key, K max_key) {
        double min_pos = min_key * slope + intercept;
        double max_pos = max_key * slope + intercept + 1;
        if (min_pos > 0 && max_pos < size - 1) {
            slope = ((double)size - 1) / (max_key - min_key);
            intercept = 0 - slope * min_key;
        }
        else if (min_pos > 0) {
            slope = (double)max_pos / (max_key - min_key);
            intercept = 0 - slope * min_key;
        }
        else if (max_pos < size) {
            slope = ((double)size - min_pos - 1) / (max_key - min_key);
            intercept = size - 1 - slope * max_key;
        }
    }

    inline size_t predict_pos(K key) const {
        int position = key * slope + intercept + 0.5;
        if (position < 0)
            return 0;
        else if (position >= size)
            return size - 1;
        return position;
    }

    void init_keys(size_t number, K* keys, NODE** nodes) {
        int start_offset = 0;
        int end_offset = 0;
        int prev_it = 0;
        for (int line_offset = 0; line_offset < size >> 3; line_offset++) {
            while (end_offset < number && predict_pos(keys[end_offset]) < (line_offset << 3) + 8) {
                end_offset++;
            }
            assert(end_offset - start_offset < SIMPLE_INNER_NODE_LEVEL3_SIZE);
            if (end_offset - start_offset == 0) {
                for (size_t i = 0; i < 8; i++) {
                    insert_key(keys[prev_it], nodes[prev_it], (line_offset << 3) + i);
                }
            }
            else if (end_offset - start_offset <= 8) {
                int it = end_offset - 1;
                int residual = 8;
                while (it >= start_offset && it - start_offset + 1 < residual) {
                    int predicted_pos = predict_pos(keys[it]);
                    insert_key(keys[it], nodes[it], (line_offset << 3) + residual - 1);
                    residual--;
                    while ((line_offset << 3) + residual > predicted_pos) {
                        insert_key(keys[it], nodes[it], (line_offset << 3) + residual - 1);
                        residual--;
                    }
                    it--;
                }
                if (it >= start_offset) {
                    while (it >= start_offset) {
                        insert_key(keys[it], nodes[it], (line_offset << 3) + it - start_offset);
                        it--;
                    }
                    residual = 0;
                }
                while (residual > 0) {
                    insert_key(keys[prev_it], nodes[prev_it], (line_offset << 3) + residual - 1);
                    residual--;
                }
                prev_it = end_offset - 1;
            }
            else {
                bitmap_set(line_offset);
                this->keys[line_offset << 3] = keys[start_offset];
                this->nodes[line_offset << 3] = new SIMPLE_INNER_NODE(144);
                size_t it = start_offset;
                while (end_offset - it > 7) {
                    ((SIMPLE_INNER_NODE*) this->nodes[line_offset << 3])->upsert_node(keys[it], nodes[it]);
                    overflow_number++;
                    it++;
                }
                while (it < end_offset) {
                    insert_key(keys[it], nodes[it], (line_offset << 3) - end_offset + it + 8);
                    it++;
                }
                prev_it = end_offset - 1;
            }
            start_offset = end_offset;
        }
    }

    // Expand inner node
    void expand_node() {
        K* old_keys = new K[number + 1];
        NODE** old_nodes = new NODE*[number + 1];
        int it = 1;
        old_keys[0] = std::numeric_limits<K>::lowest();
        for (size_t line = 0; line < size >> 3; line++) {
            size_t start_pos = line << 3;
            if (bitmap_get(line)) {
                auto temp_inner_node = (SIMPLE_INNER_NODE*)nodes[start_pos];
                size_t temp_inner_node_size = temp_inner_node->get_size();
                K* temp_inner_node_keys = temp_inner_node->get_keys();
                NODE** temp_inner_node_nodes = temp_inner_node->get_nodes();
                for (size_t it_temp = 0; it_temp < temp_inner_node_size; it_temp++) {
                    if (NODE::greater_than(temp_inner_node_keys[it_temp], old_keys[it - 1])) {
                        old_keys[it] = temp_inner_node_keys[it_temp];
                        old_nodes[it] = temp_inner_node_nodes[it_temp];
                        it++;
                    }
                }
                delete temp_inner_node;
            }
            else {
                if (NODE::greater_than(keys[start_pos], old_keys[it - 1])) {
                    old_keys[it] = keys[start_pos];
                    old_nodes[it] = nodes[start_pos];
                    it++;
                }
            }
            for (size_t i = 1; i <= 7; i++) {
                if (NODE::greater_than(keys[start_pos + i], old_keys[it - 1])) {
                    old_keys[it] = keys[start_pos + i];
                    old_nodes[it] = nodes[start_pos + i];
                    it++;
                }
            }
        }
        size_t old_size = size;
        size = (((size_t) (number / INNER_NODE_INIT_RATIO) >> 3) + 1) << 3;
        overflow_number = 0;
        max_number = size * INNER_NODE_MAX_RATIO;
        max_overflow_number = size * INNER_NODE_OVERFLOW_MAX_RATIO;
        slope = slope * ((double) size / old_size);
        intercept = intercept * ((double) size / old_size);
        free(keys);
        delete [] nodes;
        delete [] bitmap;
        keys = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * size));
        nodes = new NODE*[size];
        bitmap = new uint8_t[((size - 1) >> 6) + 1]();
        init_keys(number, old_keys + 1, old_nodes + 1);
        delete [] old_keys;
        delete [] old_nodes;
    }

public:
    // build_type: 0 -> bulk load with optimal plr, 1 -> use least square method
    FlatInnerNode(bool build_type, uint8_t level, size_t number, size_t start_pos, K first_key, double slope, double intercept, K* keys, NODE** nodes)
        :   FlatNode<K, P>(level),
            size((((size_t) (number / INNER_NODE_INIT_RATIO) >> 3) + 1) << 3),
            max_number(size * INNER_NODE_MAX_RATIO),
            max_overflow_number(std::max((int)(size * INNER_NODE_OVERFLOW_MAX_RATIO), OVERFLOW_MIN_BOUND)),
            slope(slope / INNER_NODE_INIT_RATIO),
            number(number) {
        if (!build_type) {
            this->intercept = (intercept - start_pos - first_key * slope) / INNER_NODE_INIT_RATIO;
        }
        else {
            this->intercept = intercept / INNER_NODE_INIT_RATIO;
        }
        model_correction(first_key, keys[start_pos + number - 1]);
        this->keys = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * size));
        this->nodes = new NODE*[size];
        this->bitmap = new uint8_t[((size - 1) >> 6) + 1]();
        init_keys(number, keys + start_pos, nodes + start_pos);
    }

    ~FlatInnerNode() {
        if (size > 0) {
            for (size_t line = 0; line < size >> 3; line++) {
                if (bitmap_get(line)) {
                    delete nodes[line << 3];
                }
            }
        }
        free(keys);
        delete [] nodes;
        delete [] bitmap;
    }

    void delete_children() {
        nodes[0]->delete_children();
        for (size_t i = 1; i < size; i++) {
            if (NODE::greater_than(keys[i], keys[i - 1]))
                nodes[i]->delete_children();
        }
        size = 0;
        delete this;
    }

    size_t get_number() { return number; }

    size_t get_size() { return size; }

    K* get_keys() { return keys; }

    NODE** get_nodes() { return nodes; }

    uint8_t* get_bitmap() { return bitmap; }

    P find(K key) const {
        int predicted_pos = predict_pos(key);
        int line_head = predicted_pos - predicted_pos % 8;
        if (NODE::greater_than(get_key(predicted_pos), key)) {
            for (int it = predicted_pos - 1; it >= line_head - 1; it--) {
                if (!NODE::greater_than(get_key(it), key))
                    return get_node(it)->find(key);
            }
            return 0;
        }
        else {
            for (int it = predicted_pos + 1; it < line_head + 8; it++) {
                if (NODE::greater_than(get_key(it), key))
                    return get_node(it - 1)->find(key);
            }
            return get_node(line_head + 7)->find(key);
        }
    }

    void range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        int predicted_pos = predict_pos(lower_bound);
        int line_head = predicted_pos - predicted_pos % 8;
        if (NODE::greater_than(get_key(predicted_pos), lower_bound)) {
            for (int it = predicted_pos - 1; it >= line_head - 1; it--) {
                if (!NODE::greater_than(get_key(it), lower_bound)) {
                    get_node(it)->range_query(false, lower_bound, upper_bound, answers);
                    return;
                }
            }
        }
        else {
            for (int it = predicted_pos + 1; it < line_head + 8; it++) {
                if (NODE::greater_than(get_key(it), lower_bound)) {
                    get_node(it - 1)->range_query(false, lower_bound, upper_bound, answers);
                    return;
                }
            }
            get_node(line_head + 7)->range_query(false, lower_bound, upper_bound, answers);
        }
    }

    void find_split_node(K key, std::vector<NODE*>& traversal_path) {
        traversal_path.push_back(this);
        int predicted_pos = predict_pos(key);
        int line_head = predicted_pos - predicted_pos % 8;
        if (NODE::greater_than(get_key(predicted_pos), key)) {
            for (int it = predicted_pos - 1; it >= line_head - 1; it--) {
                if (!NODE::greater_than(get_key(it), key)) {
                    get_node(it)->find_split_node(key, traversal_path);
                    return;
                }
            }
        }
        else {
            for (int it = predicted_pos + 1; it < line_head + 8; it++) {
                if (NODE::greater_than(get_key(it), key)) {
                    get_node(it - 1)->find_split_node(key, traversal_path);
                    return;
                }
            }
            get_node(line_head + 7)->find_split_node(key, traversal_path);
        }
    }

    int upsert(K key, P payload) {
        int predicted_pos = predict_pos(key);
        int line_head = predicted_pos - predicted_pos % 8;
        if (NODE::greater_than(get_key(predicted_pos), key)) {
            for (int it = predicted_pos - 1; it >= line_head - 1; it--) {
                if (!NODE::greater_than(get_key(it), key))
                    return get_node(it)->upsert(key, payload);
            }
            return 0;
        }
        else {
            for (int it = predicted_pos + 1; it < line_head + 8; it++) {
                if (NODE::greater_than(get_key(it), key))
                    return get_node(it - 1)->upsert(key, payload);
            }
            return get_node(line_head + 7)->upsert(key, payload);
        }
    }

    int insert_node(K key, NODE* node) {
        int predicted_pos = predict_pos(key);
        int line_head = predicted_pos - predicted_pos % 8;
        int position = predicted_pos;
        int redundancy = 0;
        int ref = 1;
        // Calculate the start position and the redundancy
        if (NODE::greater_than(get_key(predicted_pos), key)) {
            for(int it = predicted_pos - 1; it >= line_head; it--) {
                if (!NODE::greater_than(get_key(it), key)) {
                    position = it;
                    break;
                }
            }
            if (position != predicted_pos) {
                for(int it = position - 1; it >= line_head - 1; it--) {
                    if (it >= 0 && NODE::equal(get_key(it), get_key(position))) {
                        redundancy++;
                    }
                    else
                        break;
                }
                redundancy = (redundancy + 1) >> 1;
                position = position - redundancy + 1;
            }
            else
                position = line_head;
        }
        else {
            for(int it = predicted_pos + 1; it < line_head + 8; it++) {
                if (NODE::greater_than(get_key(it), key)) {
                    position = it;
                    break;
                }
            }
            if (position != predicted_pos)
                position--;
            else
                position = line_head + 7;
            for(int it = position - 1; it >= line_head - 1; it--) {
                if (it >= 0 && NODE::equal(get_key(it), get_key(position))) {
                    redundancy++;
                }
                else
                    break;
            }
            redundancy = (redundancy + 1) >> 1;
            position = position - redundancy + 1;
        }
        if (redundancy > 0) {
            for (size_t i = 0; i < redundancy; i++) {
                insert_key(key, node, position + i);
            }
            if (position % 8 + redundancy == 8 && line_head + 8 < size && NODE::equal(get_key(position - 1), get_key(line_head + 8))) {
                insert_key(key, node, line_head + 8);
                size_t it = line_head + 9;
                while (it < size && NODE::equal(get_key(position - 1), get_key(it))) {
                    insert_key(key, node, it);
                    it++;
                }
            }
        }
        else {
            // Find free position in the line
            int free_pos_left = position - 1;
            int free_pos_right = position - 1;
            for (int it = free_pos_left - 1; it >= line_head; it--) {
                if (it > 0 && NODE::equal(get_key(it), get_key(it - 1))) {
                    free_pos_left = it;
                    break;
                }
            }
            for (int it = free_pos_right + 1; it < line_head + 7; it++) {
                if (it + 1 < size && NODE::equal(get_key(it), get_key(it + 1))) {
                    free_pos_right = it;
                    break;
                }
            }
            // Use left free position
            if (free_pos_left != position - 1 && (free_pos_right != position - 1 && free_pos_left + free_pos_right <= (position << 1) - 2 || free_pos_right == position - 1)) {
                assert(position > free_pos_left && position <= size);
                memmove(keys + free_pos_left, keys + free_pos_left + 1, (position - free_pos_left - 1) * sizeof(K));
                memmove(nodes + free_pos_left, nodes + free_pos_left + 1, (position - free_pos_left - 1) * sizeof(NODE*));
                insert_key(key, node, position - 1);
                if (position % 8 == 0 && position < size && NODE::equal(get_key(position - 2), get_key(position))) {
                    insert_key(key, node, position);
                    size_t it = position + 1;
                    while (it < size && NODE::equal(get_key(position - 2), get_key(it))) {
                        insert_key(key, node, it);
                        it++;
                    }
                }
            }
            // Use right free position
            else if (free_pos_right != position - 1) {
                assert(free_pos_right >= position && free_pos_right < size);
                memmove(keys + position + 1, keys + position, (free_pos_right - position) * sizeof(K));
                memmove(nodes + position + 1, nodes + position, (free_pos_right - position) * sizeof(NODE*));
                insert_key(key, node, position);
            }
            // No free position in the line
            else {
                if (!bitmap_get(line_head >> 3)) {
                    NODE* node_temp = get_node(line_head);
                    nodes[line_head] = new SIMPLE_INNER_NODE(144);
                    ((SIMPLE_INNER_NODE*)nodes[line_head])->upsert_node(get_key(line_head), node_temp);
                    bitmap_set(line_head >> 3);
                }
                if (position == line_head + 1) {
                    ref = ((SIMPLE_INNER_NODE*) nodes[line_head])->upsert_node(key, node);
                }
                else if (position == line_head) {
                    keys[line_head] = key;
                    ref = ((SIMPLE_INNER_NODE*) nodes[line_head])->upsert_node(key, node);
                }
                else {
                    ref = ((SIMPLE_INNER_NODE*) nodes[line_head])->upsert_node(get_key(line_head + 1), get_node(line_head + 1));
                    assert(position > line_head && position > line_head + 1 && position <= size);
                    memmove(keys + line_head + 1, keys + line_head + 2, (position - line_head - 2) * sizeof(K));
                    memmove(nodes + line_head + 1, nodes + line_head + 2, (position - line_head - 2) * sizeof(NODE*));
                    insert_key(key, node, position - 1);
                    if (position == line_head + 8) {
                        size_t it = position;
                        while (it < size && NODE::equal(get_key(position - 2), get_key(it))) {
                            insert_key(key, node, it);
                            it++;
                        }
                    }
                }
                overflow_number++;
            }
        }

        number++;
        if (number >= max_number) {
            if (overflow_number >= max_overflow_number || number / INNER_NODE_INIT_RATIO > INNER_NODE_MAX_SIZE)
                return -2;
            else
                expand_node();
        }
        return ref;
    }

    void update_node(K key, NODE* node) {
        int predicted_pos = predict_pos(key);
        int line_head = predicted_pos - predicted_pos % 8;
        if (NODE::greater_than(get_key(line_head + 1), key)) {
            if (bitmap_get(line_head >> 3))
                ((SIMPLE_INNER_NODE*) nodes[line_head])->upsert_node(key, node);
            else
                nodes[line_head] = node;
        }
        else {
            for (int it = line_head; it <= line_head + 7; it++) {
                if (NODE::equal(get_key(it), key)) {
                    nodes[it] = node;
                    it++;
                    while (it < size && NODE::equal(get_key(it), key)) {
                        nodes[it] = node;
                        it++;
                    }
                    break;
                }
            }
        }
    }

    void node_size(size_t& model_size, size_t& slot_size, size_t& data_size, size_t& ob_model_size, size_t& ob_slot_size, size_t& ob_data_size, size_t& inner_node, size_t& data_node) const {
        model_size += sizeof(*this) + size * (sizeof(K) + sizeof(NODE*)) + ((size - 1) >> 6) + 1;
        inner_node++;
        get_node(0)->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
        for (size_t i = 1; i < size; i++) {
            if (!NODE::equal(get_key(i), get_key(i - 1))) {
                get_node(i)->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
    }

};

// Learned leaf node
template<typename K, typename P>
class FlatLeafNode : public FlatNode<K, P> {

    using NODE = FlatNode<K, P>;
    using SIMPLE_LEAF_NODE = FlatSimpleLeafNode<K, P>;

protected:

    size_t size;
    double slope;
    double intercept;
    K first_key;
    P* payloads;
    K* keys;
    const K key_domain_min = std::numeric_limits<K>::lowest();
    SIMPLE_LEAF_NODE** overflow_blocks;
    NODE* next = NULL;
    size_t number = 0;
    size_t overflow_number = 0;
    size_t max_number;
    size_t max_overflow_number;
    NODE* prev = NULL;

    inline K get_key(size_t position) const {
        return keys[position];
    }

    inline P get_payload(size_t position) const {
        return payloads[position];
    }

    inline void insert_key(K key, P payload, size_t position) {
        keys[position] = key;
        payloads[position] = payload;
        number++;
    }

    inline void update_payload(P payload, size_t position) {
        payloads[position] = payload;
    }

    inline SIMPLE_LEAF_NODE* get_overflow_block(size_t position) const {
        return overflow_blocks[position >> 6];
    }

    inline void build_overflow_block(size_t position) {
        overflow_blocks[position >> 6] = new SIMPLE_LEAF_NODE(32);
    }

    void model_correction(K min_key, K max_key) {
        double min_pos = min_key * slope + intercept;
        double max_pos = max_key * slope + intercept + 1;
        if (min_pos > 0 && max_pos < size - 1) {
            slope = ((double)size - 1) / (max_key - min_key);
            intercept = 0 - slope * min_key;
        }
        else if (min_pos > 0) {
            slope = (double)max_pos / (max_key - min_key);
            intercept = 0 - slope * min_key;
        }
        else if (max_pos < size) {
            slope = ((double)size - min_pos - 1) / (max_key - min_key);
            intercept = size - 1 - slope * max_key;
        }
        assert(slope > 0);
    }

    inline size_t predict_pos(K key) const {
        int64_t position = static_cast<int64_t>(key * slope + intercept + 0.5);
        if (position < 0)
            return 0;
        else if (position >= size)
            return size - 1;
        return position;
    }

    [[nodiscard]] int get_free_pos(size_t position) const {
        size_t step = 1;
        size_t line_offset = position % 8;
        while(step <= line_offset || step + line_offset < 8) {
            if (step <= line_offset && NODE::equal(get_key(position - step), key_domain_min))
                return position - step;
            else if (step + line_offset < 8 && NODE::equal(get_key(position + step), key_domain_min))
                return position + step;
            else
                step++;
        }
        return -1;
    }

    void expand_node() {
        size_t old_size = size;
        size = (((size_t) (number / LEAF_NODE_INIT_RATIO) >> 3) + 1) << 3;
        number = 0;
        overflow_number = 0;
        max_number = size * LEAF_NODE_MAX_RATIO;
        max_overflow_number = size * LEAF_NODE_OVERFLOW_MAX_RATIO;
        slope = slope * ((double) size / old_size);
        intercept = intercept * ((double) size / old_size);
        K* old_keys = keys;
        keys = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * size));
        P* old_payloads = payloads;
        payloads = new P[size];
        SIMPLE_LEAF_NODE** old_overflow_blocks = overflow_blocks;
        overflow_blocks = new SIMPLE_LEAF_NODE*[((size - 1) >> 6) + 1]();
        for (size_t i = 0; i < size; i++) {
            this->keys[i] = key_domain_min;
        }
        size_t total_blocks = ((old_size - 1) >> 6) + 1;
        for (size_t block = 0; block < total_blocks; block++) {
            size_t itb = 0;
            size_t block_size = 64;
            size_t start_pos = block << 6;
            if (block == total_blocks - 1 && old_size % 64 > 0) {
                block_size = old_size % 64;
            }
            while (NODE::equal(old_keys[start_pos + itb], key_domain_min))
                itb++;
            if (!old_overflow_blocks[block]) {
                while (itb < block_size) {
                    if (NODE::greater_than(old_keys[start_pos + itb], key_domain_min)) {
                        int ref = upsert(old_keys[start_pos + itb], old_payloads[start_pos + itb]);
                        assert(ref == 1);
                    }
                    itb++;
                }
            }
            else {
                size_t ito = 0;
                size_t ob_size = old_overflow_blocks[block]->get_size();
                K* ob_keys = old_overflow_blocks[block]->get_keys();
                P* ob_payloads = old_overflow_blocks[block]->get_payloads();
                while (itb < block_size || ito < ob_size) {
                    if (itb < block_size && (ito == ob_size || NODE::less_than(old_keys[start_pos + itb], ob_keys[ito]))) {
                        int ref = upsert(old_keys[start_pos + itb], old_payloads[start_pos + itb]);
                        assert(ref == 1);
                        itb++;
                        while (itb < block_size && NODE::equal(old_keys[start_pos + itb], key_domain_min))
                            itb++;
                    }
                    else {
                        int ref = upsert(ob_keys[ito], ob_payloads[ito]);
                        assert(ref == 1);
                        ito++;
                        while (ito < ob_size && NODE::equal(ob_keys[ito], key_domain_min))
                            ito++;
                    }
                }
            }
        }
        for (size_t i = 0; i < total_blocks; i++) {
            if (old_overflow_blocks[i]) {
                delete old_overflow_blocks[i];
            }
        }
        delete [] old_overflow_blocks;
        free(old_keys);
        delete [] old_payloads;
    }

public:

    // build_type: bit 2: 0 -> bulk load with optimal plr, 1 -> use least square method; bit 1 & 0: next & prev
    FlatLeafNode(uint8_t build_type, uint8_t level, size_t number, size_t start_pos, K first_key, double slope, double intercept, K* keys, P* payloads, NODE* prev, NODE* next)
        :   FlatNode<K, P>(level),
            first_key(first_key),
            size((((size_t) (number / LEAF_NODE_INIT_RATIO) >> 3) + 1) << 3),
            max_number(size * LEAF_NODE_MAX_RATIO),
            max_overflow_number(std::max((int)(size * LEAF_NODE_OVERFLOW_MAX_RATIO), OVERFLOW_MIN_BOUND)),
            slope(slope / LEAF_NODE_INIT_RATIO) {
        if (build_type < 4) {
            this->intercept = (intercept - start_pos - first_key * slope) / LEAF_NODE_INIT_RATIO ;
        }
        else {
            this->intercept = intercept / LEAF_NODE_INIT_RATIO;
        }
        if (build_type % 2 == 1) {
            this->prev = prev;
        }
        if (build_type % 4 > 1) {
            this->next = next;
        }
        model_correction(first_key, keys[start_pos + number - 1]);
        this->keys = static_cast<K*>(std::aligned_alloc(64, sizeof(K) * size));
        this->payloads = new P[size];
        overflow_blocks = new SIMPLE_LEAF_NODE*[((size - 1) >> 6) + 1]();
        for (size_t i = 0; i < size; i++) {
            this->keys[i] = key_domain_min;
        }
        
        for (size_t i = 0; i < number; i++) {
            int ref = upsert(keys[i + start_pos], payloads[i + start_pos]);
            assert(ref == 1);
        }
        assert(this->number == number);
    }

    ~FlatLeafNode() {
        for (size_t i = 0; i <= (size - 1) >> 6; i++) {
            if (overflow_blocks[i]) {
                delete overflow_blocks[i];
            }
        }
        delete [] overflow_blocks;
        free(keys);
        delete [] payloads;
    }

    void delete_children() { delete this; }

    void set_prev(NODE* node) { prev = node; }

    void set_next(NODE* node) { next = node; }

    size_t get_number() { return number; }

    size_t get_size() { return size; }

    K* get_keys() { return keys; }

    P* get_payloads() { return payloads; }

    SIMPLE_LEAF_NODE** get_overflow_blocks() { return overflow_blocks; }

    NODE* get_prev() { return prev; }

    NODE* get_next() { return next; }

    K node_upper_bound() {
        if (next && next->level == 64)
            return reinterpret_cast<FlatLeafNode*>(next)->first_key;
        else
            return std::numeric_limits<K>::max();
    }

    P find(K key) const {

        int predicted_pos = predict_pos(key);
        if (NODE::equal(get_key(predicted_pos), key)) {
            return get_payload(predicted_pos);
        }
        else if (NODE::equal(get_key(predicted_pos), key_domain_min)) {
            return 0;
        }
        else if (NODE::greater_than(get_key(predicted_pos), key)) {
            int line_head = predicted_pos - predicted_pos % 8;
            for (int pos = predicted_pos; pos >= line_head; pos--) {
                if (NODE::equal(get_key(pos), key)) {
                    return get_payload(pos);
                }
                else if (NODE::equal(get_key(pos), key_domain_min)) {
                    return 0;
                }
                else if (NODE::less_than(get_key(pos), key)) {
                    break;
                }
            }
            if (get_overflow_block(predicted_pos))
                return get_overflow_block(predicted_pos)->find(key);
            else
                return 0;
        }
        else {
            int line_head = predicted_pos - predicted_pos % 8;
            for (size_t pos = predicted_pos; pos < line_head + 8; pos++) {
                if (NODE::equal(get_key(pos), key)) {
                    return get_payload(pos);
                }
                else if (NODE::equal(get_key(pos), key_domain_min)) {
                    return 0;
                }
                else if (NODE::greater_than(get_key(pos), key)) {
                    break;
                }
            }
            if (get_overflow_block(predicted_pos))
                return get_overflow_block(predicted_pos)->find(key);
            else
                return 0;
        }
    }

    void range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        size_t scan_start_pos = 0;
        bool end_flag = false;
        if (!type) {
            size_t predicted_pos =  predict_pos(lower_bound);
            if (get_overflow_block(predicted_pos)) {
                get_overflow_block(predicted_pos)->range_query(false, lower_bound, upper_bound, answers);
            }
            scan_start_pos = std::min(((predicted_pos >> 6) + 1) << 6, size);
            size_t line_head = predicted_pos - predicted_pos % 8;
            for (size_t it = line_head; it < scan_start_pos; it++) {
                if (!NODE::less_than(get_key(it), lower_bound)) {
                    while (it < scan_start_pos && !NODE::greater_than(get_key(it), upper_bound)) {
                        if (!NODE::equal(get_key(it), key_domain_min)) {
                            answers.emplace_back(get_key(it), get_payload(it));
                        }
                        it++;
                    }
                    if (it < scan_start_pos)
                        end_flag = true;
                    break;
                }
            }
        }
        while (!end_flag && scan_start_pos < size) {
            if (get_overflow_block(scan_start_pos)) {
                get_overflow_block(scan_start_pos)->range_query(true, lower_bound, upper_bound, answers);
            }
            size_t it = scan_start_pos;
            scan_start_pos = std::min(scan_start_pos + 64, size);
            while (it < scan_start_pos && !NODE::greater_than(get_key(it), upper_bound)) {
                if (!NODE::equal(get_key(it), key_domain_min)) {
                    answers.emplace_back(get_key(it), get_payload(it));
                }
                it++;
            }
            if (it < scan_start_pos)
                end_flag = true;
        }
        if (!end_flag) {
            next->range_query(true, lower_bound, upper_bound, answers);
        }
    }

    void find_split_node(K key, std::vector<NODE*>& traversal_path) {
        traversal_path.push_back(this);
    }

    int upsert(K key, P payload) {
        assert(key >= first_key);
        assert(key < node_upper_bound());
        int ref = 0;
        int predicted_pos = predict_pos(key);
        // Predicted position is free
        if (NODE::equal(get_key(predicted_pos), key_domain_min)) {
            insert_key(key, payload, predicted_pos);
            ref = 1;
        }
        // Predicted position is the target key
        else if (NODE::equal(get_key(predicted_pos), key)) {
            update_payload(payload, predicted_pos);
            return 2;
        }
        else {
            int line_head = predicted_pos - predicted_pos % 8;
            // Search to left
            if (NODE::greater_than(get_key(predicted_pos), key)) {
                for(int pos = predicted_pos; pos >= line_head; pos--) {
                    if (NODE::equal(get_key(pos), key)) {
                        update_payload(payload, pos);
                        return 2;
                    }
                    else if (NODE::equal(get_key(pos), key_domain_min)) {
                        insert_key(key, payload, pos);
                        ref = 1;
                        break;
                    }
                    else if (NODE::less_than(get_key(pos), key) || pos == line_head) {
                        if(NODE::greater_than(get_key(pos), key)) {
                            pos--;
                        }
                        int free_pos = get_free_pos(predicted_pos);
                        if(free_pos >= 0) {
                            if (free_pos > pos) {
                                assert(free_pos > pos && free_pos < size);
                                memmove(keys + pos + 2, keys + pos + 1, (free_pos - pos - 1) * sizeof(K));
                                memmove(payloads + pos + 2, payloads + pos + 1, (free_pos - pos - 1) * sizeof(P));
                                insert_key(key, payload, pos + 1);
                            }
                            else {
                                assert(pos >= free_pos && pos < size);
                                memmove(keys + free_pos, keys + free_pos + 1, (pos - free_pos) * sizeof(K));
                                memmove(payloads + free_pos, payloads + free_pos + 1, (pos - free_pos) * sizeof(P));
                                insert_key(key, payload, pos);
                            }
                            ref = 1;
                            break;
                        }
                        else {
                            if (!get_overflow_block(predicted_pos))
                                build_overflow_block(predicted_pos);
                            ref = get_overflow_block(predicted_pos)->upsert(key, payload);
                            if (ref != 2) {
                                number++;
                                overflow_number++;
                            }
                            break;
                        }
                    }
                }
            }
            // Search to right
            else {
                for(int pos = predicted_pos; pos < line_head + 8; pos++) {
                    if (NODE::equal(get_key(pos), key)) {
                        update_payload(payload, pos);
                        return 2;
                    }
                    else if (NODE::equal(get_key(pos), key_domain_min)) {
                        insert_key(key, payload, pos);
                        ref = 1;
                        break;
                    }
                    else if (NODE::greater_than(get_key(pos), key) || pos == line_head + 7) {
                        if(NODE::less_than(get_key(pos), key)) {
                            pos++;
                        }
                        int free_pos = get_free_pos(predicted_pos);
                        if(free_pos >= 0) {
                            if (free_pos > pos) {
                                assert(free_pos >= pos && free_pos < size);
                                memmove(keys + pos + 1, keys + pos, (free_pos - pos) * sizeof(K));
                                memmove(payloads + pos + 1, payloads + pos, (free_pos - pos) * sizeof(P));
                                insert_key(key, payload, pos);
                            }
                            else {
                                assert(pos > free_pos && pos <= size);
                                memmove(keys + free_pos, keys + free_pos + 1, (pos - free_pos - 1) * sizeof(K));
                                memmove(payloads + free_pos, payloads + free_pos + 1, (pos - free_pos - 1) * sizeof(P));
                                insert_key(key, payload, pos - 1);
                            }
                            ref = 1;
                            break;
                        }
                        else {
                            if (!get_overflow_block(predicted_pos))
                                build_overflow_block(predicted_pos);
                            ref = get_overflow_block(predicted_pos)->upsert(key, payload);
                            if (ref != 2) {
                                number++;
                                overflow_number++;
                            }
                            break;
                        }
                    }
                }
            }
        }

        if (ref == 1) {
            if (number > max_number) {
                if (overflow_number > max_overflow_number || number / LEAF_NODE_INIT_RATIO > LEAF_NODE_MAX_SIZE) {
                    return -2;
                }
                else
                    expand_node();
            }
        }
        return ref;
    }

    void node_size(size_t& model_size, size_t& slot_size, size_t& data_size, size_t& ob_model_size, size_t& ob_slot_size, size_t& ob_data_size, size_t& inner_node, size_t& data_node) const {
        model_size += sizeof(*this) + (((size - 1) >> 6) + 1) * sizeof(NODE*);
        slot_size += size * (sizeof(K) + sizeof(P));
        data_size += (number - overflow_number) * (sizeof(K) + sizeof(P));
        data_node++;
        for (size_t i = 0; i <= (size - 1) >> 6; i++) {
            if (overflow_blocks[i]) {
                overflow_blocks[i]->node_size(model_size, slot_size, data_size, ob_model_size, ob_slot_size, ob_data_size, inner_node, data_node);
            }
        }
    }

};
