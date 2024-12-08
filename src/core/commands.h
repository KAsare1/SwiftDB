#ifndef COMMANDS_H
#define COMMANDS_H

#include "protocol.h"

void register_commands();
void cleanup_commands();
void execute_command(int client_socket, RedisCommand *cmd);
void cleanup_expired_keys();
void check_memory_and_evict();


#endif // COMMANDS_H
