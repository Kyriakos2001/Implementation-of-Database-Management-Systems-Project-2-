#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bp_datanode.h"

#include "bf.h"
#include "bp_file.h"
#include "record.h"

#define CALL_BF(call)             \
    {                             \
        BF_ErrorCode code = call; \
        if (code != BF_OK)        \
        {                         \
            BF_PrintError(code);  \
            return BPLUS_ERROR;   \
        }                         \
    }

// void *mallocdata(Record *allrecords, int newsize){
// allrecords = (Record*) malloc(newsize*sizeof(Record));
// if (allrecords==NULL){
//     printf("Error: Could not allocate memory.\n");
// }
// }

void bplus_data_node_print(int blocknumber, BPLUS_DATA_NODE *node)
{
    printf(" * Data Block: %d, Parent: %d, Next: %d \n", blocknumber, node->parent, node->nextdatanode);
    for (int i = 0; i < node->no_of_records; i++)
    {
        printf(" %d/%d ", blocknumber, i + 1);

        printRecord(node->allrecords[i]);
    }
}

int bplus_data_node_insert(BPLUS_INFO *bplus_info, int currentBlock, BPLUS_DATA_NODE *node, Record record)
{
    if (node->no_of_records < bplus_info->max_recs_in_block)
    {
        int insertPos = 0;
        while (insertPos < node->no_of_records && node->allrecords[insertPos].id < record.id)
        {
            insertPos++;
        }

        for (int i = bplus_info->max_recs_in_block - 1; i > insertPos; i--)
        {
            node->allrecords[i] = node->allrecords[i - 1];
        }

        node->allrecords[insertPos] = record;
        node->no_of_records++;

        return BPLUS_OK;
    }
    else
    {
        return BPLUS_ERROR;
    }
}