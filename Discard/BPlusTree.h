#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include <iostream>
#include <vector>

class BPlusTree {
public:
    void insert(int key, int position);
    int search(int key);
};

#endif
