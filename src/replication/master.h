// #ifndef MASTER_H
// #define MASTER_H

// #include <time.h>
// #include <pthread.h>
// #include "replication.h"

// typedef struct ReplicationBacklog ReplicationBacklog;


// typedef struct SlaveNode {
//     int socket;
//     char *slave_id;
//     long long offset;
//     time_t last_heartbeat;
//     bool sync_in_progress;
//     UT_hash_handle hh;
// } SlaveNode;

// typedef struct MasterState {
//     SlaveNode *slaves;
//     int slave_count;
//     ReplicationBacklog *backlog;
//     pthread_mutex_t backlog_mutex;
// } MasterState;

// void init_master(ReplicationState *repl_state);
// void cleanup_master(ReplicationState *repl_state);
// void handle_slave_connection(int slave_socket);
// void propagate_command_to_slaves(RedisCommand *cmd);
// void remove_slave(SlaveNode *slave);

// #endif // MASTER_H