#include "../include/replconf.h"
#include "../include/protocol.h"
#include <string.h>
#include <stdio.h>

void handle_replconf(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "ERR wrong number of arguments for REPLCONF");
        return;
    }

    // Extract subcommand
    char subcommand[MAX_BULK_LENGTH];
    strncpy(subcommand, cmd->argv[1].data, cmd->argv[1].length);
    subcommand[cmd->argv[1].length] = '\0';

    ReplConfType type = parse_replconf_type(subcommand);
    
    // Validate arguments
    if (!validate_replconf_args(cmd, type)) {
        send_redis_error(client_socket, "ERR invalid REPLCONF arguments");
        return;
    }

    // Handle different REPLCONF subcommands
    switch (type) {
        case REPLCONF_ACK:
            handle_replconf_ack(client_socket, cmd);
            break;
        case REPLCONF_LISTENING_PORT:
            handle_replconf_listening_port(client_socket, cmd);
            break;
        case REPLCONF_CAPA:
            handle_replconf_capa(client_socket, cmd);
            break;
        case REPLCONF_GETACK:
            handle_replconf_getack(client_socket, cmd);
            break;
        default:
            send_redis_error(client_socket, "ERR unknown REPLCONF subcommand");
            break;
    }
}

void handle_replconf_ack(int client_socket, RedisCommand *cmd) {
    if (repl_state->role != ROLE_MASTER) {
        send_redis_error(client_socket, "ERR REPLCONF ACK only valid on master");
        return;
    }

    // Parse replication offset from slave
    long long offset = strtoll(cmd->argv[2].data, NULL, 10);
    
    // Update slave's acknowledged offset
    MasterState *master = (MasterState *)repl_state->role_specific_state;
    SlaveNode *slave;
    HASH_FIND_INT(master->slaves, &client_socket, slave);
    
    if (slave) {
        slave->offset = offset;
        slave->last_heartbeat = time(NULL);
        send_redis_string(client_socket, "OK");
    } else {
        send_redis_error(client_socket, "ERR slave not found");
    }
}

void handle_replconf_listening_port(int client_socket, RedisCommand *cmd) {
    if (repl_state->role != ROLE_MASTER) {
        send_redis_error(client_socket, "ERR REPLCONF LISTENING-PORT only valid on master");
        return;
    }

    // Parse port number
    int port = atoi(cmd->argv[2].data);
    if (port <= 0 || port > 65535) {
        send_redis_error(client_socket, "ERR invalid port number");
        return;
    }

    // Update slave's listening port
    MasterState *master = (MasterState *)repl_state->role_specific_state;
    SlaveNode *slave;
    HASH_FIND_INT(master->slaves, &client_socket, slave);
    
    if (slave) {
        // Store the port if needed
        send_redis_string(client_socket, "OK");
    } else {
        send_redis_error(client_socket, "ERR slave not found");
    }
}

void handle_replconf_capa(int client_socket, RedisCommand *cmd) {
    ReplConfCapabilities capabilities = {0};
    
    // Parse capabilities
    for (int i = 2; i < cmd->argc; i++) {
        if (strncmp(cmd->argv[i].data, "psync2", 6) == 0) {
            capabilities.psync2 = true;
        } else if (strncmp(cmd->argv[i].data, "eof", 3) == 0) {
            capabilities.eof = true;
        } else if (strncmp(cmd->argv[i].data, "multi-bulk", 10) == 0) {
            capabilities.multi_bulk = true;
        }
    }

    // Store capabilities for the connection
    if (repl_state->role == ROLE_MASTER) {
        MasterState *master = (MasterState *)repl_state->role_specific_state;
        SlaveNode *slave;
        HASH_FIND_INT(master->slaves, &client_socket, slave);
        if (slave) {
            // Store capabilities with slave info
            send_redis_string(client_socket, "OK");
            return;
        }
    }

    send_redis_string(client_socket, "OK");
}

void handle_replconf_getack(int client_socket, RedisCommand *cmd) {
    if (repl_state->role != ROLE_MASTER) {
        send_redis_error(client_socket, "ERR REPLCONF GETACK only valid on master");
        return;
    }

    // Request immediate ACK from slave
    MasterState *master = (MasterState *)repl_state->role_specific_state;
    SlaveNode *slave;
    HASH_FIND_INT(master->slaves, &client_socket, slave);
    
    if (slave) {
        // Send REPLCONF ACK request to slave
        char ack_request[] = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n1\r\n";
        write(client_socket, ack_request, strlen(ack_request));
        send_redis_string(client_socket, "OK");
    } else {
        send_redis_error(client_socket, "ERR slave not found");
    }
}

ReplConfType parse_replconf_type(const char *subcommand) {
    if (strcasecmp(subcommand, REPL_ACK) == 0) return REPLCONF_ACK;
    if (strcasecmp(subcommand, REPL_LISTENING_PORT) == 0) return REPLCONF_LISTENING_PORT;
    if (strcasecmp(subcommand, REPL_CAPA) == 0) return REPLCONF_CAPA;
    if (strcasecmp(subcommand, REPL_GETACK) == 0) return REPLCONF_GETACK;
    return REPLCONF_UNKNOWN;
}

bool validate_replconf_args(RedisCommand *cmd, ReplConfType type) {
    switch (type) {
        case REPLCONF_ACK:
            return cmd->argc == 3;  // REPLCONF ACK <offset>
        case REPLCONF_LISTENING_PORT:
            return cmd->argc == 3;  // REPLCONF LISTENING-PORT <port>
        case REPLCONF_CAPA:
            return cmd->argc >= 3;  // REPLCONF CAPA <capability> [capability ...]
        case REPLCONF_GETACK:
            return cmd->argc == 2;  // REPLCONF GETACK
        default:
            return false;
    }
}