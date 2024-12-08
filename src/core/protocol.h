#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdlib.h>

#define MAX_BULK_LENGTH 512
#define MAX_ARGS 32

typedef struct {
    char *data;
    size_t length;
} RedisString;

typedef struct {
    RedisString *argv;
    int argc;
} RedisCommand;

// Protocol parsing functions
RedisCommand parse_redis_array(char *input);
void free_command(RedisCommand *cmd);

// Response functions
void send_redis_string(int socket, const char *str);
void send_redis_bulk_string(int socket, const char *str);
void send_redis_error(int socket, const char *str);
void send_redis_integer(int socket, int value);

#endif // PROTOCOL_H