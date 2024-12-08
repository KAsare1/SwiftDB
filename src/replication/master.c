#include "../include/master.h"
#include "../include/protocol.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>


#define REPL_BACKLOG_SIZE 1024 * 1024  // 1MB backlog
#define SLAVE_TIMEOUT 60  // Seconds before slave is considered disconnected
#define HEARTBEAT_INTERVAL 10 

typedef struct ReplicationBacklog {
    char *buffer;
    size_t size;
    size_t capacity;
    long long start_offset;
    long long current_offset;
} ReplicationBacklog;

typedef struct MasterState {
    SlaveNode *slaves;
    int slave_count;
    ReplicationBacklog *backlog;
    pthread_mutex_t backlog_mutex;
} MasterState;

static ReplicationBacklog* create_backlog() {
    ReplicationBacklog *backlog = malloc(sizeof(ReplicationBacklog));
    if (!backlog) return NULL;

    backlog->buffer = malloc(REPL_BACKLOG_SIZE);
    if (!backlog->buffer) {
        free(backlog);
        return NULL;
    }

    backlog->capacity = REPL_BACKLOG_SIZE;
    backlog->size = 0;
    backlog->start_offset = 0;
    backlog->current_offset = 0;
    return backlog;
}

void init_master(ReplicationState *repl_state) {
    MasterState *master = malloc(sizeof(MasterState));
    if (!master) return;

    master->slaves = NULL;
    master->slave_count = 0;
    master->backlog = create_backlog();
    pthread_mutex_init(&master->backlog_mutex, NULL);

    repl_state->role_specific_state = master;
}

static void add_to_backlog(MasterState *master, const char *command, size_t len) {
    pthread_mutex_lock(&master->backlog_mutex);

    ReplicationBacklog *backlog = master->backlog;
    
    // If backlog would overflow, remove old entries
    if (backlog->size + len > backlog->capacity) {
        size_t remove_size = (backlog->size + len) - backlog->capacity;
        memmove(backlog->buffer, 
                backlog->buffer + remove_size, 
                backlog->size - remove_size);
        backlog->size -= remove_size;
        backlog->start_offset += remove_size;
    }

    // Add new command
    memcpy(backlog->buffer + backlog->size, command, len);
    backlog->size += len;
    backlog->current_offset += len;

    pthread_mutex_unlock(&master->backlog_mutex);
}

void handle_slave_connection(int slave_socket) {
    //TODO Implementation
}


void propagate_command_to_slaves(RedisCommand *cmd) {
    if (!repl_state || repl_state->role != ROLE_MASTER) return;

    MasterState *master = (MasterState *)repl_state->role_specific_state;
    if (!master || !master->slaves) return;

    // Format command for replication
    char buffer[MAX_BULK_LENGTH];
    int pos = 0;
    
    // Format as RESP array
    pos += sprintf(buffer + pos, "*%d\r\n", cmd->argc);
    for (int i = 0; i < cmd->argc; i++) {
        pos += sprintf(buffer + pos, "$%zu\r\n", cmd->argv[i].length);
        memcpy(buffer + pos, cmd->argv[i].data, cmd->argv[i].length);
        pos += cmd->argv[i].length;
        pos += sprintf(buffer + pos, "\r\n");
    }

    // Add to replication backlog
    add_to_backlog(master, buffer, pos);

    // Send to all connected slaves
    SlaveNode *slave, *tmp;
    HASH_ITER(hh, master->slaves, slave, tmp) {
        if (!slave->sync_in_progress) {
            ssize_t written = write(slave->socket, buffer, pos);
            if (written > 0) {
                slave->offset += written;
            }
        }
    }
}

void propagate_command_to_slaves(RedisCommand *cmd) {
    MasterState *master = (MasterState *)repl_state->role_specific_state;
    if (!master || !master->slaves) return;

    // Format command for replication
    char command_buffer[MAX_BULK_LENGTH];
    int len = format_redis_command(command_buffer, cmd);
    
    // Add to replication buffer
    add_to_repl_buffer(master->repl_buffer, command_buffer, 
                       master->repl_buffer->max_offset + len);

    // Send to all slaves
    SlaveNode *slave, *tmp;
    HASH_ITER(hh, master->slaves, slave, tmp) {
        if (!slave->sync_in_progress) {
            write(slave->socket, command_buffer, len);
        }
    }
}


// Helper function to format Redis command
int format_redis_command(char *buffer, RedisCommand *cmd) {
    int pos = 0;
    pos += sprintf(buffer + pos, "*%d\r\n", cmd->argc);
    
    for (int i = 0; i < cmd->argc; i++) {
        pos += sprintf(buffer + pos, "$%zu\r\n", cmd->argv[i].length);
        memcpy(buffer + pos, cmd->argv[i].data, cmd->argv[i].length);
        pos += cmd->argv[i].length;
        pos += sprintf(buffer + pos, "\r\n");
    }
    
    return pos;
}

void cleanup_master(ReplicationState *repl_state) {
    if (!repl_state || !repl_state->role_specific_state) return;

    MasterState *master = (MasterState *)repl_state->role_specific_state;
    
    // Cleanup slaves
    SlaveNode *slave, *tmp;
    HASH_ITER(hh, master->slaves, slave, tmp) {
        HASH_DEL(master->slaves, slave);
        close(slave->socket);
        free(slave->slave_id);
        free(slave);
    }

    // Cleanup backlog
    if (master->backlog) {
        free(master->backlog->buffer);
        free(master->backlog);
    }

    // Cleanup mutex
    pthread_mutex_destroy(&master->backlog_mutex);

    // Free master state
    free(master);
}


void send_slave_heartbeat(SlaveNode *slave) {
    char ping[] = "*1\r\n$4\r\nPING\r\n";
    ssize_t written = write(slave->socket, ping, strlen(ping));
    
    if (written <= 0) {
        // Mark slave for cleanup if write fails
        slave->last_heartbeat = 0;
        return;
    }
    
    // Update last heartbeat time
    slave->last_heartbeat = time(NULL);
}

void check_slave_timeouts(MasterState *master) {
    time_t now = time(NULL);
    SlaveNode *slave, *tmp;
    
    HASH_ITER(hh, master->slaves, slave, tmp) {
        // Check if slave has timed out
        if (now - slave->last_heartbeat > SLAVE_TIMEOUT) {
            printf("Slave %s timed out, removing...\n", slave->slave_id);
            
            // Remove from hash table
            HASH_DEL(master->slaves, slave);
            master->slave_count--;
            
            // Close socket and cleanup
            close(slave->socket);
            free(slave->slave_id);
            free(slave);
        }
    }
}

void *replication_heartbeat(void *arg) {
    if (!repl_state) return NULL;

    while (cleanup_running) {  // Use existing cleanup_running flag
        if (repl_state->role == ROLE_MASTER) {
            MasterState *master = (MasterState *)repl_state->role_specific_state;
            if (!master) continue;

            // Send heartbeat to all slaves
            SlaveNode *slave, *tmp;
            HASH_ITER(hh, master->slaves, slave, tmp) {
                send_slave_heartbeat(slave);
            }

            // Check for timed out slaves
            check_slave_timeouts(master);
        }
        else if (repl_state->role == ROLE_SLAVE) {
            SlaveState *slave = (SlaveState *)repl_state->role_specific_state;
            if (!slave) continue;

            // Send REPLCONF ACK to master
            char ack[64];
            snprintf(ack, sizeof(ack), "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%zu\r\n%lld\r\n",
                     snprintf(NULL, 0, "%lld", slave->replication_offset),
                     slave->replication_offset);
            
            if (write(slave->master_socket, ack, strlen(ack)) <= 0) {
                // Connection to master lost
                printf("Lost connection to master, attempting reconnect...\n");
                close(slave->master_socket);
                
                // Attempt reconnection
                if (connect_to_master() == 0) {
                    // Request partial resync
                    request_partial_resync(slave->replication_offset);
                }
            }
        }

        // Sleep before next heartbeat
        sleep(HEARTBEAT_INTERVAL);
    }

    return NULL;
}