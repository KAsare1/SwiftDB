#ifndef SLAVE_H
#define SLAVE_H

#include "replication.h"

typedef struct SlaveState {
    int master_socket;
    char *master_host;
    int master_port;
    long long replication_offset;
    bool sync_in_progress;
    time_t last_master_interaction;
} SlaveState;

void init_slave(ReplicationState *repl_state);
void cleanup_slave(ReplicationState *repl_state);
int connect_to_master(void);
void process_master_update(char *update);
bool is_master_connection_healthy(void);

#endif // SLAVE_H