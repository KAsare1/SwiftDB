#include "../include/replication.h"
#include "../include/master.h"
#include "../include/slave.h"
#include "../include/replconf.h"
#include "../include/buffer.h"
#include "../include/sync.h"
#include "../include/protocol.h"
#include <stdlib.h>
#include <string.h>

ReplicationState *repl_state = NULL;

void init_replication(ReplicationConfig *config) {
    repl_state = malloc(sizeof(ReplicationState));
    if (!repl_state) return;

    repl_state->config = config;
    repl_state->role = config->role;
    repl_state->replication_enabled = true;
    repl_state->processing_master_command = false;

    if (config->role == ROLE_MASTER) {
        init_master(repl_state);
    } else if (config->role == ROLE_SLAVE) {
        init_slave(repl_state);
    }
}

void add_slave(int socket, long long offset) {
    if (!repl_state || repl_state->role != ROLE_MASTER) return;

    MasterState *master = (MasterState *)repl_state->role_specific_state;
    SlaveNode *slave = malloc(sizeof(SlaveNode));
    if (!slave) return;

    slave->socket = socket;
    slave->offset = offset;
    slave->last_heartbeat = time(NULL);
    slave->sync_in_progress = false;
    
    HASH_ADD_INT(master->slaves, socket, slave);
    master->slave_count++;
}

void update_slave_ack(int socket, RedisCommand *cmd) {
    if (!repl_state || repl_state->role != ROLE_MASTER || cmd->argc != 3) return;

    MasterState *master = (MasterState *)repl_state->role_specific_state;
    SlaveNode *slave;
    HASH_FIND_INT(master->slaves, &socket, slave);
    
    if (slave) {
        slave->offset = strtoll(cmd->argv[2].data, NULL, 10);
        slave->last_heartbeat = time(NULL);
    }
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

    // Send to all slaves
    SlaveNode *slave, *tmp;
    HASH_ITER(hh, master->slaves, slave, tmp) {
        if (!slave->sync_in_progress) {
            write(slave->socket, buffer, pos);
        }
    }
}