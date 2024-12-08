#ifndef REPLCONF_H
#define REPLCONF_H

#include "replication.h"
#include "protocol.h"

// REPLCONF command handler
void handle_replconf(int client_socket, RedisCommand *cmd);

// REPLCONF subcommand handlers
void handle_replconf_ack(int client_socket, RedisCommand *cmd);
void handle_replconf_listening_port(int client_socket, RedisCommand *cmd);
void handle_replconf_capa(int client_socket, RedisCommand *cmd);
void handle_replconf_getack(int client_socket, RedisCommand *cmd);

// Helper functions
ReplConfType parse_replconf_type(const char *subcommand);
bool validate_replconf_args(RedisCommand *cmd, ReplConfType type);
void send_replconf_response(int client_socket, bool success, const char *message);

#endif // REPLCONF_H