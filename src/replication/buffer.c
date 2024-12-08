#include "../include/buffer.h"
#include <stdlib.h>
#include <string.h>

#define DEFAULT_REPL_BACKLOG_SIZE 1024 * 1024  // 1MB default size

ReplBuffer *create_repl_buffer(int capacity) {
    ReplBuffer *buffer = malloc(sizeof(ReplBuffer));
    if (!buffer) return NULL;

    buffer->commands = malloc(capacity * sizeof(char*));
    buffer->offsets = malloc(capacity * sizeof(long long));
    if (!buffer->commands || !buffer->offsets) {
        free(buffer->commands);
        free(buffer->offsets);
        free(buffer);
        return NULL;
    }

    buffer->size = 0;
    buffer->capacity = capacity;
    buffer->min_offset = 0;
    buffer->max_offset = 0;

    return buffer;
}

bool add_to_repl_buffer(ReplBuffer *buffer, const char *command, long long offset) {
    if (buffer->size >= buffer->capacity) {
        // Circular buffer: remove oldest entry
        free(buffer->commands[0]);
        memmove(buffer->commands, buffer->commands + 1, 
                (buffer->size - 1) * sizeof(char*));
        memmove(buffer->offsets, buffer->offsets + 1, 
                (buffer->size - 1) * sizeof(long long));
        buffer->size--;
        buffer->min_offset = buffer->offsets[0];
    }

    buffer->commands[buffer->size] = strdup(command);
    buffer->offsets[buffer->size] = offset;
    buffer->size++;
    buffer->max_offset = offset;

    return true;
}

char **get_commands_from_offset(ReplBuffer *buffer, long long offset, int *count) {
    if (offset < buffer->min_offset || offset > buffer->max_offset) {
        *count = 0;
        return NULL;
    }

    int start_idx = 0;
    while (start_idx < buffer->size && buffer->offsets[start_idx] < offset) {
        start_idx++;
    }

    *count = buffer->size - start_idx;
    if (*count <= 0) return NULL;

    char **commands = malloc(*count * sizeof(char*));
    for (int i = 0; i < *count; i++) {
        commands[i] = strdup(buffer->commands[start_idx + i]);
    }

    return commands;
}

void free_repl_buffer(ReplBuffer *buffer) {
    if (!buffer) return;
    
    for (int i = 0; i < buffer->size; i++) {
        free(buffer->commands[i]);
    }
    free(buffer->commands);
    free(buffer->offsets);
    free(buffer);
}