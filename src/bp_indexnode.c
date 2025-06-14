#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bp_indexnode.h"
#include "bf.h"
#include "bp_file.h"
#include "record.h"

// void *mallocindex(bplustree* bplusstructure, int newsize){
// bplusstructure = (bplustree*) malloc(newsize*sizeof(bplustree));
// if (bplusstructure == NULL)
// {
//     printf("Error: Could not allocate memory.\n");
// }
// }

int bplus_index_node_insert(BPLUS_INFO *bplus_info, int currentBlock, BPLUS_INDEX_NODE *node, int id, int leftBlockNo, int rightBlockNo)
{
    if (node->no_of_keys < MAX_POINTERS_PER_BLOCK)
    {
        int insertPos = 0;
        while (insertPos < node->no_of_keys && node->bplusstructure[insertPos].id < id)
        {
            insertPos++;
        }

        for (int i = MAX_POINTERS_PER_BLOCK - 1; i > insertPos; i--)
        {

            node->bplusstructure[i].id = node->bplusstructure[i - 1].id;
            node->bplusstructure[i].blockno = node->bplusstructure[i - 1].blockno;
        }
        if (insertPos == 0)
        {
            node->previousblock = leftBlockNo;
        }
        node->bplusstructure[insertPos].id = id;
        node->bplusstructure[insertPos].blockno = rightBlockNo;
        node->no_of_keys++;

        return BPLUS_OK;
    }
    else
    {
        return BPLUS_ERROR;
    }
}

void bplus_inner_node_print(int blocknumber, BPLUS_INDEX_NODE *node)
{
    printf(" * Inner Block: %d, Parent: %d \n", blocknumber, node->parent);

    printf(" %d ", node->previousblock);

    for (int i = 0; i < node->no_of_keys; i++)
    {
        printf(" [#%d] %d ", node->bplusstructure[i].id, node->bplusstructure[i].blockno);
    }

    printf("\n");
}
