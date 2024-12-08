#ifndef BUFFER_H
#define BUFFER_H

#include "replication.h"

typedef struct ReplBuffer {
    char **commands;
    long long *offsets;
    int size;
    int capacity;
    long long min_offset;
    long long max_offset;
} ReplBuffer;

ReplBuffer *create_repl_buffer(int capacity);
void free_repl_buffer(ReplBuffer *buffer);
bool add_to_repl_buffer(ReplBuffer *buffer, const char *command, long long offset);
char **get_commands_from_offset(ReplBuffer *buffer, long long offset, int *count);

#endif // BUFFER_H