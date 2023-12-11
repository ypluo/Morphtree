### Morphtree

Implementation of the paper "Morphtree: a polymorphic main-memory learned index for dynamic workloads", which is to appear in The VLDB Journal, 2023.

Morphtree is a high-performed learned index that capable of adapting to query workloads with different read/write ratios. The main idea of Morphtree is to divide the index into a read-optimized inner search tree and an evolving data layer. The read-optimized inner search tree is novel for reducing the average tree height and inner node probing cost. The evolving data layer introduces two types of node layouts optimized for lookup and insertion queries, respectively. When tasked with different queries, Morphtree can change the leaf node layout efficiently at the granularity of one leaf node.  

The concurrent version of Morphtree is implemented in branch [concurrent](https://github.com/ypluo/Morphtree/tree/concurrent). Users can refer to `morphtree/test/ycsbtest.c` for example usage of Morphtree. 