#ifndef BP_DATANODE_H
#define BP_DATANODE_H
#include <record.h>
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include <bp_indexnode.h>

// Define maximum records per data node
#define MAX_RECORDS_PER_BLOCK ((BF_BLOCK_SIZE - sizeof(int) * 3) / sizeof(Record))

typedef struct
{
    int is_innernode;
    int no_of_records;
    int nextdatanode;
    int parent;
    Record allrecords[MAX_RECORDS_PER_BLOCK]; // Fixed-size array
} BPLUS_DATA_NODE;

void bplus_data_node_print(int blocknumber, BPLUS_DATA_NODE *node);

int bplus_data_node_insert(BPLUS_INFO *bplus_info, int blocknumber, BPLUS_DATA_NODE *node, Record record);

#endif