#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>

#define MAX_POINTERS_PER_BLOCK ((BF_BLOCK_SIZE - sizeof(int) * 3) / sizeof(bplustree))

typedef struct
{
    int id;
    int blockno; // counter to block with larger value than the id stored
} bplustree;

typedef struct
{
    int is_innernode;
    int no_of_keys;
    int previousblock; // counter of block that has a smaller value than all ids stored on node
    int parent;
    bplustree bplusstructure[MAX_POINTERS_PER_BLOCK];
} BPLUS_INDEX_NODE;

void bplus_inner_node_print(int blocknumber, BPLUS_INDEX_NODE *node);

int bplus_index_node_insert(BPLUS_INFO *bplus_info, int currentBlock, BPLUS_INDEX_NODE *node, int id, int leftBlockNo, int rightBlockNo);

#endif
