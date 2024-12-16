#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include "protocol.h"

static int parse_length(char **ptr) {
    char *endptr;
    long length = strtol(*ptr, &endptr, 10);
    if (endptr == *ptr || length < 0 || length > MAX_BULK_LENGTH) {
        return -1;
    }
    *ptr = endptr;
    return length;
}

static int skip_crlf(char **ptr) {
    if (**ptr == '\r' && *(*ptr + 1) == '\n') {
        *ptr += 2;
        return 1;
    }
    return 0;
}

static RedisString parse_bulk_string(char **ptr) {
    RedisString result = {NULL, 0};
    
    if (**ptr != '$') {
        return result;
    }
    (*ptr)++;

    int length = parse_length(ptr);
    if (length < 0 || !skip_crlf(ptr)) {
        return result;
    }

    result.data = *ptr;
    result.length = length;
    *ptr += length;

    if (!skip_crlf(ptr)) {
        result.data = NULL;
        result.length = 0;
    }

    return result;
}

RedisCommand parse_redis_array(char *input) {
    RedisCommand cmd = {NULL, 0};
    char *ptr = input;
    
    // Handle simple text commands
    if (*ptr != '*') {
        // Remove trailing \r\n
        size_t len = strlen(ptr);
        while (len > 0 && (ptr[len-1] == '\r' || ptr[len-1] == '\n')) {
            ptr[--len] = '\0';
        }
        
        if (len == 0) return cmd;
        
        // Count words
        int word_count = 1;
        for (size_t i = 0; i < len; i++) {
            if (ptr[i] == ' ') word_count++;
        }
        
        if (word_count > MAX_ARGS) return cmd;
        
        cmd.argv = malloc(word_count * sizeof(RedisString));
        if (!cmd.argv) return cmd;
        cmd.argc = word_count;
        
        // Parse words
        char *word = strtok(ptr, " \r\n");
        int i = 0;
        while (word && i < word_count) {
            cmd.argv[i].data = word;
            cmd.argv[i].length = strlen(word);
            word = strtok(NULL, " \r\n");
            i++;
        }
        
        return cmd;
    }
    
    // Original RESP parsing code
    ptr++;
    int length = parse_length(&ptr);
    if (length < 0 || length > MAX_ARGS || !skip_crlf(&ptr)) {
        return cmd;
    }

    cmd.argv = malloc(length * sizeof(RedisString));
    if (!cmd.argv) {
        return cmd;
    }
    cmd.argc = length;

    for (int i = 0; i < length; i++) {
        cmd.argv[i] = parse_bulk_string(&ptr);
        if (!cmd.argv[i].data) {
            free(cmd.argv);
            cmd.argv = NULL;
            cmd.argc = 0;
            return cmd;
        }
    }

    return cmd;
}

void free_command(RedisCommand *cmd) {
    if (cmd->argv) {
        free(cmd->argv);
        cmd->argv = NULL;
        cmd->argc = 0;
    }
}

void send_redis_string(int socket, const char *str) {
    char response[MAX_BULK_LENGTH];
    snprintf(response, sizeof(response), "+%s\r\n", str);
    write(socket, response, strlen(response));
}

void send_redis_bulk_string(int socket, const char *str) {
    char response[MAX_BULK_LENGTH];
    size_t len = strlen(str);
    snprintf(response, sizeof(response), "$%zu\r\n%s\r\n", len, str);
    write(socket, response, strlen(response));
}

void send_redis_error(int socket, const char *str) {
    char response[MAX_BULK_LENGTH];
    snprintf(response, sizeof(response), "-ERR %s\r\n", str);
    write(socket, response, strlen(response));
}

void send_redis_integer(int socket, int value) {
    char response[MAX_BULK_LENGTH];
    snprintf(response, sizeof(response), ":%d\r\n", value); // Format as Redis integer
    write(socket, response, strlen(response));  // Send to the client
}