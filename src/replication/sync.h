#ifndef SYNC_H
#define SYNC_H

#include "replication.h"

// Sync types
typedef enum {
    SYNC_FULL,
    SYNC_PARTIAL
} SyncType;

// Sync result
typedef struct SyncResult {
    bool success;
    long long offset;
    char *error_message;
} SyncResult;

SyncResult perform_full_sync(int slave_socket);
SyncResult perform_partial_sync(int slave_socket, long long offset);
bool is_partial_sync_possible(long long offset);
void handle_sync_command(int client_socket, RedisCommand *cmd);
void handle_psync_command(int client_socket, RedisCommand *cmd);

#endif // SYNC_H