#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"
#include <stdbool.h>
#include <assert.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return BPLUS_ERROR;     \
    }                         \
  }

int create_new_root(int, BPLUS_INFO *, int, int, int);
int split_parent(int, BPLUS_INFO *, int, BPLUS_INDEX_NODE *, int, int);
int insert_into_parent(int, BPLUS_INFO *, int, int, int, int);
int update_root(int, BPLUS_INFO *, int, int, int, int);
static int update_children_parents(int file_desc, BPLUS_INFO* bplus_info, int parentBlockNo, BPLUS_INDEX_NODE* parentNode);
static int update_indexnode_child_parent(int file_desc, BPLUS_INFO* bplus_info, int childBlockNo, int newParentBlockNo);

int BP_CreateFile(char *fileName) {
    BF_Block *headerblock;
    BF_Block *rootblock;
    BF_Block_Init(&headerblock);
    BF_Block_Init(&rootblock);
    int fileds;

    CALL_BF(BF_CreateFile(fileName));
    CALL_BF(BF_OpenFile(fileName, &fileds));
    CALL_BF(BF_AllocateBlock(fileds, headerblock));
    CALL_BF(BF_AllocateBlock(fileds, rootblock));

    BPLUS_INFO *metadata = (BPLUS_INFO *) BF_Block_GetData(headerblock);
    metadata->tree_depth = 1;
    metadata->file_name = fileName;
    metadata->max_recs_in_block = MAX_RECORDS_PER_BLOCK;
    metadata->max_pointers_in_block = MAX_POINTERS_PER_BLOCK;
    int block_no;
    BF_GetBlockCounter(fileds, &block_no);
    metadata->rootblock_no = block_no - 1;

    BPLUS_INDEX_NODE *rootdata = (BPLUS_INDEX_NODE *) BF_Block_GetData(rootblock);
    rootdata->no_of_keys = 0;
    rootdata->previousblock = -1;
    rootdata->parent = -1;
    rootdata->is_innernode = 0;
    BF_Block_SetDirty(headerblock);
    BF_Block_SetDirty(rootblock);

    CALL_BF(BF_UnpinBlock(headerblock));
    CALL_BF(BF_UnpinBlock(rootblock));

    BF_Block_Destroy(&headerblock);
    BF_Block_Destroy(&rootblock);

    CALL_BF(BF_CloseFile(fileds));

    return BPLUS_OK;
}

BPLUS_INFO *BP_OpenFile(char *fileName, int *file_desc) {
    BPLUS_INFO *metadata;
    BF_OpenFile(fileName, file_desc);
    BF_Block *headerBlock;
    BF_Block_Init(&headerBlock);
    BF_GetBlock(*file_desc, 0, headerBlock);
    metadata = (BPLUS_INFO *) BF_Block_GetData(headerBlock);
    BF_UnpinBlock(headerBlock);
    BF_Block_Destroy(&headerBlock);
    return metadata;
}

int BP_CloseFile(int file_desc, BPLUS_INFO *info) {
    BF_Block *headerBlock;
    BF_Block_Init(&headerBlock);
    BF_GetBlock(file_desc, 0, headerBlock);
    BPLUS_INFO *newmetadata = (BPLUS_INFO *) BF_Block_GetData(headerBlock);
    memcpy(newmetadata, info, sizeof (BPLUS_INFO));
    BF_Block_SetDirty(headerBlock);
    CALL_BF(BF_UnpinBlock(headerBlock));
    BF_Block_Destroy(&headerBlock);
    CALL_BF(BF_CloseFile(file_desc));
    return BPLUS_OK;
}

int BP_InsertEntry(int file_desc, BPLUS_INFO *bplus_info, Record record) {
    BF_Block *currentBlock;
    BF_Block_Init(&currentBlock);
    int currentBlockNo = bplus_info->rootblock_no;

    if (BP_GetEntry(file_desc, bplus_info, record.id, NULL) == BPLUS_OK) {
        return BPLUS_ERROR;
    }

    while (1) {
        CALL_BF(BF_GetBlock(file_desc, currentBlockNo, currentBlock));
        char *blockData = BF_Block_GetData(currentBlock);
        BPLUS_INDEX_NODE *testNode = (BPLUS_INDEX_NODE *) blockData;
        //if (bplus_info->tree_depth == 1) {
        if (testNode->is_innernode == 0) {
            // At leaf level (root is leaf)
            BPLUS_DATA_NODE *currentDataNode = (BPLUS_DATA_NODE *) blockData;

            if (bplus_data_node_insert(bplus_info, currentBlockNo, currentDataNode, record) == 0) { // insert successful
                BF_Block_SetDirty(currentBlock);
                break;
            } else {
                Record records[MAX_RECORDS_PER_BLOCK + 1] = {0};

                for (int i = 0; i < MAX_RECORDS_PER_BLOCK; i++) {
                    records[i] = currentDataNode->allrecords[i];
                }

                int insertPos = 0;
                while (insertPos < MAX_RECORDS_PER_BLOCK && records[insertPos].id < record.id) {
                    insertPos++;
                }

                for (int i = MAX_RECORDS_PER_BLOCK; i > insertPos; i--) {
                    records[i] = records[i - 1];
                }

                records[insertPos] = record;

                int mid = records[(MAX_RECORDS_PER_BLOCK + 1) / 2].id;

                for (int i = 1; i < MAX_RECORDS_PER_BLOCK + 1; i++) { // check ordering
                    assert(records[i - 1].id < records[i].id);
                }

                BF_Block *newDataBlock;
                BF_Block_Init(&newDataBlock);
                CALL_BF(BF_AllocateBlock(file_desc, newDataBlock));

                BPLUS_DATA_NODE *newDataNode = (BPLUS_DATA_NODE *) BF_Block_GetData(newDataBlock);
                int newDataBlockNo;
                CALL_BF(BF_GetBlockCounter(file_desc, &newDataBlockNo));
                newDataBlockNo -= 1;
                newDataNode->is_innernode = 0;
                newDataNode->no_of_records = 0;
                newDataNode->parent = currentDataNode->parent; // same parent as old leaf
                //newDataNode->parent = bplus_info->rootblock_no;
                newDataNode->nextdatanode = currentDataNode->nextdatanode;
                memset(newDataNode->allrecords, 0, sizeof (newDataNode->allrecords));

                currentDataNode->no_of_records = 0;
                currentDataNode->nextdatanode = newDataBlockNo;
                memset(currentDataNode->allrecords, 0, sizeof (currentDataNode->allrecords));


                for (int i = 0; i < MAX_RECORDS_PER_BLOCK + 1; i++) {
                    Record rec = records[i];

                    if (rec.id <= mid) {
                        bplus_data_node_insert(bplus_info, currentBlockNo, currentDataNode, rec);
                    } else {
                        bplus_data_node_insert(bplus_info, newDataBlockNo, newDataNode, rec);
                    }
                }

                bool parent_exists = currentDataNode->parent != -1;


                if (!parent_exists) {
                    int newRootBlockNo = create_new_root(file_desc, bplus_info, currentBlockNo, mid, newDataBlockNo);
                    newDataNode->parent = newRootBlockNo;
                    currentDataNode->parent = newRootBlockNo;
                } else {
                    int leftBlockNo = currentBlockNo;
                    int key = mid;
                    int rightBlockNo = newDataBlockNo;

                    insert_into_parent(file_desc, bplus_info, currentDataNode->parent, leftBlockNo, key, rightBlockNo);
                }


                BF_Block_SetDirty(currentBlock);
                BF_Block_SetDirty(newDataBlock);
                CALL_BF(BF_UnpinBlock(newDataBlock));
                BF_Block_Destroy(&newDataBlock);

                break;
            }
        } else {
            // Not at leaf, go down the tree
            BPLUS_INDEX_NODE *currentIndexNode = (BPLUS_INDEX_NODE *) blockData;
            // if (currentIndexNode->parent == 0 && currentBlockNo == bplus_info->rootblock_no) {
            //     currentIndexNode->parent = -1;
            // }

            if (currentIndexNode->no_of_keys == 0) {
                printf("initializing root\n");
                BF_Block *leftDataBlock;
                BF_Block_Init(&leftDataBlock);
                CALL_BF(BF_AllocateBlock(file_desc, leftDataBlock));

                BPLUS_DATA_NODE *leftDataNode = (BPLUS_DATA_NODE *) BF_Block_GetData(leftDataBlock);
                int leftDataBlockNo;
                CALL_BF(BF_GetBlockCounter(file_desc, &leftDataBlockNo));
                leftDataBlockNo -= 1;
                leftDataNode->is_innernode = 0;
                leftDataNode->no_of_records = 0;
                leftDataNode->parent = currentBlockNo;
                leftDataNode->nextdatanode = -1;
                memset(leftDataNode->allrecords, 0, sizeof (leftDataNode->allrecords));

                BF_Block *rightDataBlock;
                BF_Block_Init(&rightDataBlock);
                CALL_BF(BF_AllocateBlock(file_desc, rightDataBlock));
                BPLUS_DATA_NODE *rightDataNode = (BPLUS_DATA_NODE *) BF_Block_GetData(rightDataBlock);
                int rightDataBlockNo;
                CALL_BF(BF_GetBlockCounter(file_desc, &rightDataBlockNo));
                rightDataBlockNo -= 1;
                rightDataNode->is_innernode = 0;
                rightDataNode->no_of_records = 0;
                rightDataNode->parent = currentBlockNo;
                rightDataNode->nextdatanode = -1;
                memset(rightDataNode->allrecords, 0, sizeof (rightDataNode->allrecords));

                leftDataNode->nextdatanode = rightDataBlockNo;

                currentIndexNode->previousblock = leftDataBlockNo;
                currentIndexNode->bplusstructure[0].blockno = rightDataBlockNo;
                currentIndexNode->bplusstructure[0].id = record.id;
                currentIndexNode->no_of_keys++;

                //int nextBlockNo = currentBlockNo;

                BF_Block_SetDirty(leftDataBlock);
                CALL_BF(BF_UnpinBlock(leftDataBlock));
                BF_Block_Destroy(&leftDataBlock);
                BF_Block_SetDirty(rightDataBlock);
                CALL_BF(BF_UnpinBlock(rightDataBlock));
                BF_Block_Destroy(&rightDataBlock);

            } else {
                if (record.id <= currentIndexNode->bplusstructure[0].id) {
                    currentBlockNo = currentIndexNode->previousblock;
                } else {
                    int found = 0;

                    for (int i = 1; i < currentIndexNode->no_of_keys; i++) {
                        if (record.id <= currentIndexNode->bplusstructure[i].id) {
                            currentBlockNo = currentIndexNode->bplusstructure[i - 1].blockno;
                            found = 1;
                            break;
                        }
                    }

                    if (found == 0) {
                        currentBlockNo = currentIndexNode->bplusstructure[currentIndexNode->no_of_keys - 1].blockno;
                    }
                }

                CALL_BF(BF_UnpinBlock(currentBlock));
            }

        }
    }


    CALL_BF(BF_UnpinBlock(currentBlock));

    BF_Block_Destroy(&currentBlock);

    return BPLUS_OK;
}

int insert_into_parent(int file_desc, BPLUS_INFO *bplus_info, int parentBlockNo, int leftBlockNo, int key, int rightBlockNo) {
    BF_Block *pBlock = NULL;
    BF_Block_Init(&pBlock);

    CALL_BF(BF_GetBlock(file_desc, parentBlockNo, pBlock));

    BPLUS_INDEX_NODE *parentNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(pBlock);

    int insertPos = 0;

    while (insertPos < parentNode->no_of_keys && parentNode->bplusstructure[insertPos].id < key) {
        insertPos++;
    }

    if (parentNode->no_of_keys < bplus_info->max_pointers_in_block) {
        for (int i = parentNode->no_of_keys; i > insertPos; i--) {
            parentNode->bplusstructure[i] = parentNode->bplusstructure[i - 1];
        }
        parentNode->bplusstructure[insertPos].id = key;
        parentNode->bplusstructure[insertPos].blockno = rightBlockNo;
        parentNode->no_of_keys++;

        BF_Block_SetDirty(pBlock);

        CALL_BF(BF_UnpinBlock(pBlock));

        BF_Block_Destroy(&pBlock);

        return BPLUS_OK;
    }

    //    int ret = split_parent(file_desc, bplus_info, parentBlockNo, parentNode, key, rightBlockNo);
    //    CALL_BF(BF_UnpinBlock(pBlock));
    //    BF_Block_Destroy(&pBlock);
    //    return ret

    CALL_BF(BF_UnpinBlock(pBlock));

    BF_Block_Destroy(&pBlock);

    return BPLUS_ERROR;
}
//
// int insert_into_parent(int file_desc, BPLUS_INFO *bplus_info, int leftBlockNo, int key, int rightBlockNo) {
//     BF_Block_Init(&childBlock);
//     CALL_BF(BF_GetBlock(file_desc, leftBlockNo, childBlock));
//     void *leftData = BF_Block_GetData(childBlock);
//
//     int parentBlockNo;
//     // Distinguish leaf or index by checking no_of_records
//     BPLUS_DATA_NODE *leafCheck = (BPLUS_DATA_NODE *) leftData;
//     if (leafCheck->no_of_records >= 0 && leafCheck->no_of_records <= bplus_info->max_recs_in_block) {
//         parentBlockNo = leafCheck->parent;
//     } else {
//         parentBlockNo = ((BPLUS_INDEX_NODE *) leftData)->parent;
//     }
//
//     CALL_BF(BF_UnpinBlock(childBlock));
//     BF_Block_Destroy(&childBlock);
//
//     if (parentBlockNo == -1) {
//         return create_new_root(file_desc, bplus_info, leftBlockNo, key, rightBlockNo);
//     }
//
//     BF_Block *pBlock;
//     BF_Block_Init(&pBlock);
//     CALL_BF(BF_GetBlock(file_desc, parentBlockNo, pBlock));
//     BPLUS_INDEX_NODE *parentNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(pBlock);
//
//     int insertPos = 0;
//     while (insertPos < parentNode->no_of_keys && parentNode->bplusstructure[insertPos].id < key) {
//         insertPos++;
//     }
//
//     if (parentNode->no_of_keys < bplus_info->max_pointers_in_block) {
//         for (int i = parentNode->no_of_keys; i > insertPos; i--) {
//             parentNode->bplusstructure[i] = parentNode->bplusstructure[i - 1];
//         }
//         parentNode->bplusstructure[insertPos].id = key;
//         parentNode->bplusstructure[insertPos].blockno = rightBlockNo;
//         parentNode->no_of_keys++;
//
//         // Update the parent of the right child
//         update_indexnode_child_parent(file_desc, bplus_info, rightBlockNo, parentBlockNo);
//
//         BF_Block_SetDirty(pBlock);
//         CALL_BF(BF_UnpinBlock(pBlock));
//         BF_Block_Destroy(&pBlock);
//         return BPLUS_OK;
//     }
//
//     int ret = split_parent(file_desc, bplus_info, parentBlockNo, parentNode, key, rightBlockNo);
//     CALL_BF(BF_UnpinBlock(pBlock));
//     BF_Block_Destroy(&pBlock);
//     return ret;
// }

// int split_parent(int file_desc, BPLUS_INFO *bplus_info, int parentBlockNo, BPLUS_INDEX_NODE *parentNode, int key, int rightBlockNo) {
//     BF_Block *parentBlock;
//     BF_Block_Init(&parentBlock);
//     CALL_BF(BF_GetBlock(file_desc, parentBlockNo, parentBlock));

//     int original_size = parentNode->no_of_keys;
//     int total_keys = original_size + 1;
//     int *tempKeys = malloc((bplus_info->max_pointers_in_block + 1) * sizeof (int));
//     int *tempPointers = malloc((bplus_info->max_pointers_in_block + 2) * sizeof (int));
//     if (!tempKeys || !tempPointers) {
//         perror("Memory allocation failed in split_parent");
//         exit(EXIT_FAILURE);
//     }

//     tempPointers[0] = parentNode->previousblock;
//     for (int i = 0; i < original_size; i++) {
//         tempKeys[i] = parentNode->bplusstructure[i].id;
//         tempPointers[i + 1] = parentNode->bplusstructure[i].blockno;
//     }

//     int insertPos = 0;
//     while (insertPos < original_size && tempKeys[insertPos] < key) {
//         insertPos++;
//     }

//     for (int i = original_size; i > insertPos; i--) {
//         tempKeys[i] = tempKeys[i - 1];
//         tempPointers[i + 1] = tempPointers[i];
//     }

//     tempKeys[insertPos] = key;
//     tempPointers[insertPos + 1] = rightBlockNo;

//     int mid = total_keys / 2;
//     int newKey = tempKeys[mid];

//     parentNode->no_of_keys = mid;
//     parentNode->previousblock = tempPointers[0];
//     for (int i = 0; i < mid; i++) {
//         parentNode->bplusstructure[i].id = tempKeys[i];
//         parentNode->bplusstructure[i].blockno = tempPointers[i + 1];
//     }

//     BF_Block *newParentBlock;
//     BF_Block_Init(&newParentBlock);
//     CALL_BF(BF_AllocateBlock(file_desc, newParentBlock));
//     BPLUS_INDEX_NODE *newParentNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(newParentBlock);

//     newParentNode->no_of_keys = total_keys - mid - 1;
//     newParentNode->previousblock = tempPointers[mid + 1];
//     newParentNode->parent = parentNode->parent; // Same parent as old parent node

//     for (int i = mid + 1, j = 0; i < total_keys; i++, j++) {
//         newParentNode->bplusstructure[j].id = tempKeys[i];
//         newParentNode->bplusstructure[j].blockno = tempPointers[i + 1];
//     }

//     free(tempKeys);
//     free(tempPointers);

//     BF_Block_SetDirty(parentBlock);
//     CALL_BF(BF_UnpinBlock(parentBlock));
//     BF_Block_Destroy(&parentBlock);

//     BF_Block_SetDirty(newParentBlock);
//     CALL_BF(BF_UnpinBlock(newParentBlock));
//     int newParentBlockNo;
//     CALL_BF(BF_GetBlockCounter(file_desc, &newParentBlockNo));
//     newParentBlockNo -= 1;
//     BF_Block_Destroy(&newParentBlock);

//     // Update children of parentNode and newParentNode
//     update_children_parents(file_desc, bplus_info, parentBlockNo, parentNode);

//     // Re-read the new parent node to update its children
//     BF_Block *rBlock;
//     BF_Block_Init(&rBlock);
//     CALL_BF(BF_GetBlock(file_desc, newParentBlockNo, rBlock));
//     BPLUS_INDEX_NODE *reReadNewParentNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(rBlock);
//     update_children_parents(file_desc, bplus_info, newParentBlockNo, reReadNewParentNode);
//     CALL_BF(BF_UnpinBlock(rBlock));
//     BF_Block_Destroy(&rBlock);

//     return insert_into_parent(file_desc, bplus_info, parentBlockNo, newKey, newParentBlockNo);
// }

int update_root(int file_desc, BPLUS_INFO *bplus_info, int leftBlockNo, int key, int rightBlockNo, int RootBlockNo) {
    BF_Block *RootBlock;
    BF_Block_Init(&RootBlock);
    BF_GetBlock(file_desc, RootBlockNo, RootBlock);

    BPLUS_INDEX_NODE *newRootNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(RootBlock);
    // newRootNode->no_of_keys++;
    // //newRootNode->parent = -1;
    // newRootNode->bplusstructure[newRootNode->no_of_keys].id = key;
    // newRootNode->bplusstructure[newRootNode->no_of_keys].blockno = rightBlockNo;
    // newRootNode->previousblock = leftBlockNo;


    bplus_index_node_insert(bplus_info, RootBlockNo, newRootNode, key, leftBlockNo, rightBlockNo);
    ///newRootBlockNo -= 1;
    // bplus_info->rootblock_no = newRootBlockNo;
    //bplus_info->tree_depth++;




    // Update children's parent to the new root
    update_indexnode_child_parent(file_desc, bplus_info, leftBlockNo, RootBlockNo);
    update_indexnode_child_parent(file_desc, bplus_info, rightBlockNo, RootBlockNo);

    BF_Block_SetDirty(RootBlock);
    CALL_BF(BF_UnpinBlock(RootBlock));
    BF_Block_Destroy(&RootBlock);
    return BPLUS_OK;
}

int create_new_root(int file_desc, BPLUS_INFO *bplus_info, int leftBlockNo, int key, int rightBlockNo) {
    BF_Block *newRootBlock;
    BF_Block_Init(&newRootBlock);
    CALL_BF(BF_AllocateBlock(file_desc, newRootBlock));

    BPLUS_INDEX_NODE *newRootNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(newRootBlock);
    newRootNode->no_of_keys = 1;
    newRootNode->parent = -1;
    newRootNode->bplusstructure[0].id = key;
    newRootNode->bplusstructure[0].blockno = rightBlockNo;
    newRootNode->previousblock = leftBlockNo;
    newRootNode->is_innernode = 1;

    int newRootBlockNo;
    BF_GetBlockCounter(file_desc, &newRootBlockNo);
    newRootBlockNo -= 1;
    bplus_info->rootblock_no = newRootBlockNo;
    //bplus_info->tree_depth++;

    BF_Block_SetDirty(newRootBlock);
    CALL_BF(BF_UnpinBlock(newRootBlock));
    BF_Block_Destroy(&newRootBlock);

    return newRootBlockNo;
}

int BP_GetEntry(int file_desc, BPLUS_INFO *bplus_info, int value, Record **record) {
    BF_Block *block = NULL;
    BF_Block_Init(&block);

    int currentBlockNo = bplus_info->rootblock_no;
    //int depth = 0;

    while (1) {
        CALL_BF(BF_GetBlock(file_desc, currentBlockNo, block));

        void *blockData = BF_Block_GetData(block);

        BPLUS_INDEX_NODE *testNode = (BPLUS_INDEX_NODE *) blockData;

        //if (bplus_info->tree_depth == 1) {
        if (testNode->is_innernode == 1) {
            BPLUS_INDEX_NODE *currentIndexNode = (BPLUS_INDEX_NODE *) blockData;

            if (value <= currentIndexNode->bplusstructure[0].id) {
                currentBlockNo = currentIndexNode->previousblock;
            } else {
                int found = 0;

                for (int i = 1; i < currentIndexNode->no_of_keys; i++) {
                    if (value <= currentIndexNode->bplusstructure[i].id) {
                        currentBlockNo = currentIndexNode->bplusstructure[i - 1].blockno;
                        found = 1;
                        break;
                    }
                }

                if (found == 0) {
                    currentBlockNo = currentIndexNode->bplusstructure[currentIndexNode->no_of_keys - 1].blockno;
                }
            }
        } else {
            BPLUS_DATA_NODE *currentDataNode = (BPLUS_DATA_NODE *) blockData;
            for (int i = 0; i < currentDataNode->no_of_records; i++) {
                if (currentDataNode->allrecords[i].id == value) {
                    if (record != NULL) {
                        *record = malloc(sizeof (Record));

                        if (*record == NULL) {
                            printf("Error: Could not allocate memory for result record.\n");
                            exit(1);
                        }

                        memcpy(*record, &currentDataNode->allrecords[i], sizeof (Record));
                    }

                    CALL_BF(BF_UnpinBlock(block));
                    BF_Block_Destroy(&block);

                    return BPLUS_OK;
                }
            }

            CALL_BF(BF_UnpinBlock(block));

            break;
        }

        CALL_BF(BF_UnpinBlock(block));
    }

    BF_Block_Destroy(&block);

    return BPLUS_ERROR;
}

static int update_children_parents(int file_desc, BPLUS_INFO* bplus_info, int parentBlockNo, BPLUS_INDEX_NODE* parentNode) {
    // Update children in previousblock
    if (parentNode->previousblock != -1) {
        update_indexnode_child_parent(file_desc, bplus_info, parentNode->previousblock, parentBlockNo);
    }

    // Update children in bplusstructure
    for (int i = 0; i < parentNode->no_of_keys; i++) {
        update_indexnode_child_parent(file_desc, bplus_info, parentNode->bplusstructure[i].blockno, parentBlockNo);
    }

    return 0;
}

static int update_indexnode_child_parent(int file_desc, BPLUS_INFO* bplus_info, int childBlockNo, int newParentBlockNo) {
    BF_Block *cBlock;
    BF_Block_Init(&cBlock);
    CALL_BF(BF_GetBlock(file_desc, childBlockNo, cBlock));
    void *cData = BF_Block_GetData(cBlock);

    // Distinguish leaf or index by no_of_records
    BPLUS_DATA_NODE *leafCheck = (BPLUS_DATA_NODE *) cData;
    if (leafCheck->no_of_records >= 0 && leafCheck->no_of_records <= bplus_info->max_recs_in_block) {
        leafCheck->parent = newParentBlockNo;
    } else {
        ((BPLUS_INDEX_NODE *) cData)->parent = newParentBlockNo;
    }

    BF_Block_SetDirty(cBlock);
    CALL_BF(BF_UnpinBlock(cBlock));
    BF_Block_Destroy(&cBlock);

    return 0;
}

int BP_Print(int file_desc, BPLUS_INFO *bplus_info) {
    BF_Block *currentBlock;
    BF_Block_Init(&currentBlock);

    int total_blocks;
    CALL_BF(BF_GetBlockCounter(file_desc, &total_blocks));

    for (int currentBlockNo = 1; currentBlockNo < total_blocks; currentBlockNo++) {
        printf("====================================================\n");

        CALL_BF(BF_GetBlock(file_desc, currentBlockNo, currentBlock));

        BPLUS_INDEX_NODE *testNode = (BPLUS_INDEX_NODE *) BF_Block_GetData(currentBlock);

        if (testNode->is_innernode == 1) {
            bplus_inner_node_print(currentBlockNo, testNode);
        } else {
            BPLUS_DATA_NODE *blockData = (BPLUS_DATA_NODE*) BF_Block_GetData(currentBlock);
            bplus_data_node_print(currentBlockNo, blockData);

        }

        CALL_BF(BF_UnpinBlock(currentBlock));
    }

    printf("====================================================\n");


    BF_Block_Destroy(&currentBlock);

    return total_blocks;
}