#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

#define size_of_attribute(Struct,Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE= size_of_attribute(Row,id);
const uint32_t USERNAME_SIZE= size_of_attribute(Row,username);
const uint32_t EMAIL_SIZE= size_of_attribute(Row,email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET=ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET=USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE=ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;

typedef struct {
    char* buffer;
    size_t bufferLength;
    ssize_t inputLength;
} InputBuffer;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct {
    StatementType type;
    //used by insert statement
    Row rowToInsert;
} Statement;

const uint32_t PAGE_SIZE=4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROW_PER_PAGE=PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS=ROW_PER_PAGE*TABLE_MAX_PAGES;

typedef struct {
    uint32_t numRows;
    void* pages[TABLE_MAX_PAGES];
} Table;

void* rowSlot(Table* table,uint32_t rowNum){
    //根据行号除以每页行总数获得所在页的偏移
    uint32_t pageNum=rowNum/ROW_PER_PAGE;
    void* page=table->pages[pageNum];
    if (page==NULL){
        //如果该偏移页为空，分配一页
        page=table->pages[pageNum]= malloc(PAGE_SIZE);
    }
    //rowNum行号是该页的第几行
    uint32_t rowOffset=rowNum%ROW_PER_PAGE;
    //因为有第0行，所以这里需要考虑0行，因此这里直接做乘法就可以得到改行起始地址了
    uint32_t byteOffset=rowOffset*ROW_SIZE;
    return page+byteOffset;
}

void serializeRow(Row* source, void* destination){
    memcpy(destination+ID_OFFSET,&(source->id),ID_SIZE);
    memcpy(destination+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    memcpy(destination+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);
}

void deserializeRow(void* source,Row* destination){
    memcpy(&(destination->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(destination->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(destination->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}

InputBuffer* newInputBuffer(){
    InputBuffer* inputBuffer=(InputBuffer*)malloc(sizeof(InputBuffer));
    inputBuffer->buffer=NULL;
    inputBuffer->bufferLength=0;
    inputBuffer->inputLength=0;

    return inputBuffer;
}

void printPrompt(){
    printf("db > ");
}

void readInput(InputBuffer* inputBuffer){
    ssize_t bytesRead= getline(&(inputBuffer->buffer),&(inputBuffer->bufferLength),stdin);
    if (bytesRead<=0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    //忽略换行符
    inputBuffer->inputLength=bytesRead-1;
    inputBuffer->buffer[bytesRead-1]=0;
}

void closeInputBuffer(InputBuffer* inputBuffer){
    free(inputBuffer->buffer);
    free(inputBuffer);
}

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer){
    if (strcmp(inputBuffer->buffer, ".exit") == 0) {
        closeInputBuffer(inputBuffer);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareStatement(InputBuffer* inputBuffer,Statement* statement){
    if (strncmp(inputBuffer->buffer,"insert",6)==0){
        statement->type=STATEMENT_INSERT;
        int argsAssigned = sscanf(inputBuffer->buffer,"insert %d %s %s",&(statement->rowToInsert.id),
               statement->rowToInsert.username,statement->rowToInsert.email);
        if (argsAssigned<3){
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strncmp(inputBuffer->buffer,"select",6)==0){
        statement->type=STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}


void printRow(Row* row){
    printf("(%d,%s,%s)\n",row->id,row->username,row->email);
}

ExecuteResult executeInsert(Statement* statement,Table* table){
    if (table->numRows==TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row* rowToInsert=&(statement->rowToInsert);
    serializeRow(rowToInsert, rowSlot(table,table->numRows));
    table->numRows+=1;
    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement,Table* table){
    Row row;
    for (uint32_t i= 0; i < table->numRows; i++) {
        deserializeRow(rowSlot(table,i),&row);
        printRow(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement,Table* table){
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return executeInsert(statement,table);
        case (STATEMENT_SELECT):
            return executeSelect(statement,table);
    }
}


Table* newTable(){
    Table* table= malloc(sizeof(Table));
    table->numRows=0;
    for (uint32_t i = 0;i<TABLE_MAX_PAGES;i++){
        table->pages[i]=NULL;
    }
    return table;
}

void freeTable(Table* table){
    for (int i = 0; table->pages[i]; i++) {
        free(table->pages[i]);
    }
    free(table);
}


int main() {
    Table* table=newTable();
    InputBuffer* inputBuffer = newInputBuffer();
    while(true){
        printPrompt();
        readInput(inputBuffer);
        if (inputBuffer->buffer[0]=='.'){
            switch (doMetaCommand(inputBuffer)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n",inputBuffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepareStatement(inputBuffer,&statement)) {
            case(PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error,Couldn't parse statement.\n");
                continue;
            case(PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n",inputBuffer->buffer);
                continue;
        }

        switch (executeStatement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error:Table full.\n");
                break;
        }
    }
    return 0;
}

