#include "../include/slave.h"
#include "../include/protocol.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>




typedef struct SlaveState {
    int master_socket;
    char *master_host;
    int master_port;
    long long replication_offset;
    bool sync_in_progress;
    pthread_t read_thread;
    bool running;
} SlaveState;



static void *slave_read_thread(void *arg) {
    SlaveState *slave = (SlaveState *)arg;
    char buffer[MAX_BULK_LENGTH];

    while (slave->running) {
        ssize_t bytes_read = read(slave->master_socket, buffer, sizeof(buffer));
        if (bytes_read <= 0) {
            if (bytes_read == 0 || errno != EINTR) {
                // Connection lost, attempt reconnection
                close(slave->master_socket);
                sleep(1);
                if (connect_to_master() == 0) {
                    // Reestablished connection, request partial resync
                    request_partial_resync(slave->replication_offset);
                }
            }
            continue;
        }

        // Process received commands
        process_master_update(buffer, bytes_read);
        slave->replication_offset += bytes_read;

        // Send ACK to master
        send_slave_ack(slave->replication_offset);
    }

    return NULL;
}




void init_slave(ReplicationState *repl_state) {
    SlaveState *slave = malloc(sizeof(SlaveState));
    if (!slave) {
        // Handle error
        return;
    }

    slave->master_socket = -1;
    slave->replication_offset = 0;
    slave->sync_in_progress = false;

    repl_state->role_specific_state = slave;
}

int connect_to_master() {
    if (!repl_state || repl_state->role != ROLE_SLAVE) return -1;

    SlaveState *slave = (SlaveState *)repl_state->role_specific_state;
    
    // Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    // Connect to master
    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(slave->master_port);
    if (inet_pton(AF_INET, slave->master_host, &master_addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
        close(sock);
        return -1;
    }

    slave->master_socket = sock;

    // Send PING to verify connection
    char ping[] = "*1\r\n$4\r\nPING\r\n";
    write(sock, ping, strlen(ping));

    // Request initial sync
    request_initial_sync();

    return 0;
}

void process_master_update(const char *update, size_t len) {
    if (!repl_state || repl_state->role != ROLE_SLAVE) return;

    SlaveState *slave = (SlaveState *)repl_state->role_specific_state;
    
    // Parse and execute command
    RedisCommand cmd = parse_redis_array(update);
    if (cmd.argv) {
        // Set flag to allow writes from master
        repl_state->processing_master_command = true;
        execute_command(slave->master_socket, &cmd);
        repl_state->processing_master_command = false;
        free_command(&cmd);
    }
}



void send_slave_ack(long long offset) {
    if (!repl_state || repl_state->role != ROLE_SLAVE) return;

    SlaveState *slave = (SlaveState *)repl_state->role_specific_state;
    
    char ack[64];
    snprintf(ack, sizeof(ack), "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%zu\r\n%lld\r\n",
             snprintf(NULL, 0, "%lld", offset), offset);
    
    write(slave->master_socket, ack, strlen(ack));
}



void request_partial_resync(long long offset) {
    if (!repl_state || repl_state->role != ROLE_SLAVE) return;

    SlaveState *slave = (SlaveState *)repl_state->role_specific_state;
    
    char psync[64];
    snprintf(psync, sizeof(psync), "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$%zu\r\n%lld\r\n",
             snprintf(NULL, 0, "%lld", offset), offset);
    
    write(slave->master_socket, psync, strlen(psync));
}

void cleanup_slave(ReplicationState *repl_state) {
    if (!repl_state || !repl_state->role_specific_state) return;

    SlaveState *slave = (SlaveState *)repl_state->role_specific_state;
    
    // Stop read thread
    slave->running = false;
    
    // Close connection to master
    if (slave->master_socket >= 0) {
        close(slave->master_socket);
    }

    // Free allocated memory
    free(slave->master_host);
    free(slave);
}