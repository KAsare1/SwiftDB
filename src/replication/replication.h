// #ifndef REPLICATION_H
// #define REPLICATION_H
// #define REPL_ACK "ACK"
// #define REPL_LISTENING_PORT "LISTENING-PORT"
// #define REPL_CAPA "CAPA"
// #define REPL_GETACK "GETACK"

// #include <stdbool.h>
// #include "../core/protocol.h"
// #include "../../include/uthash.h"

// // Replication roles
// typedef enum {
//     ROLE_MASTER,
//     ROLE_SLAVE,
//     ROLE_SENTINEL
// } ReplicationRole;

// // Replication configuration
// typedef struct ReplicationConfig {
//     ReplicationRole role;
//     char *master_host;
//     int master_port;
//     int replication_timeout;
//     char *replication_id;
//     long long replication_offset;
// } ReplicationConfig;

// // Replication state
// typedef struct ReplicationState {
//     ReplicationRole role;
//     ReplicationConfig *config;
//     void *role_specific_state;  // Cast to MasterState or SlaveState based on role
//     bool replication_enabled;
//     bool processing_master_command;
//     time_t last_interaction;
// } ReplicationState;

// // Global replication state
// extern ReplicationState *repl_state;

// // Core replication functions
// void init_replication(ReplicationConfig *config);
// void init_master(ReplicationState *repl_state);
// void init_slave(ReplicationState *repl_state);
// void handle_slave_connection(int slave_socket);
// void cleanup_master(ReplicationState *repl_state);
// void cleanup_slave(ReplicationState *repl_state);
// int connect_to_master();
// void propagate_command_to_slaves(RedisCommand *cmd);
// void cleanup_replication();

// typedef struct ReplConfCapabilities {
//     bool psync2;        // Supports PSYNC2
//     bool eof;          // Supports EOF marking
//     bool multi_bulk;   // Supports multi-bulk transfers
// } ReplConfCapabilities;

// typedef enum ReplConfType {
//     REPLCONF_ACK,
//     REPLCONF_LISTENING_PORT,
//     REPLCONF_CAPA,
//     REPLCONF_GETACK,
//     REPLCONF_UNKNOWN
// } ReplConfType;


// #endif // REPLICATION_H