#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <pthread.h>
#include "Server.h"
#include "include/protocol.h"
#include "include/commands.h"
#include "include/sdb.h"
#include "include/replication.h"
#include "include/master.h"
#include "include/slave.h"

pthread_mutex_t cleanup_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile sig_atomic_t cleanup_running = 1;

ReplicationConfig repl_config;
ReplicationState *repl_state = NULL;


void server_shutdown();
void handle_zombies(int sig);
void handle_shutdown(int sig);
void *replication_heartbeat(void *arg);




void handle_zombies(int sig) {
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

// Graceful shutdown on SIGINT
void handle_shutdown(int sig) {
    printf("\nSIGINT received. Shutting down the server...\n");
    server_shutdown();
    exit(0);
}

void handle_client(int client_socket) {
    char buffer[30000];
    ssize_t bytes_read;

    printf("Handling client (PID: %d)\n", getpid());

    // Check if this is a replication connection
    if (repl_state && repl_state->role == ROLE_MASTER) {
        handle_slave_connection(client_socket);
        return;
    }

    while (1) {
        memset(buffer, 0, sizeof(buffer));
        bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);

        if (bytes_read <= 0) {
            if (bytes_read == 0) {
                printf("Client disconnected (PID: %d)\n", getpid());
            } else {
                perror("Read error");
            }
            break;
        }

        RedisCommand cmd = parse_redis_array(buffer);
        if (cmd.argv) {
            execute_command(client_socket, &cmd);
            free_command(&cmd);
        } else {
            send_redis_error(client_socket, "protocol error");
        }
    }

    close(client_socket);
}


void *client_thread(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);  // Free dynamically allocated socket
    handle_client(client_socket);
    return NULL;
}

void launch(struct Server *server) {
    int address_length = sizeof(server->address);
    while (1) {
        printf("====WAITING FOR CONNECTION (PID: %d)=====\n", getpid());
        int *client_socket = malloc(sizeof(int));
        if (!client_socket) {
            fprintf(stderr, "Error: Failed to allocate memory for client socket.\n");
            continue;
        }

        *client_socket = accept(server->socket, (struct sockaddr *)&server->address, (socklen_t *)&address_length);

        if (*client_socket < 0) {
            perror("Accept failed");
            free(client_socket);
            continue;
        }

        printf("CLIENT CONNECTED\n");
        pthread_t thread;
        if (pthread_create(&thread, NULL, client_thread, client_socket) != 0) {
            perror("Failed to create thread");
            close(*client_socket);
            free(client_socket);
        }
        pthread_detach(thread);  // Auto-cleanup for the thread
    }
}

void server_shutdown() {
    pthread_mutex_lock(&cleanup_mutex);
    cleanup_running = 0;
    pthread_mutex_unlock(&cleanup_mutex);
    
    // Cleanup replication
    if (repl_state) {
        if (repl_state->role == ROLE_MASTER) {
            cleanup_master(repl_state);
        } else if (repl_state->role == ROLE_SLAVE) {
            cleanup_slave(repl_state);
        }
        
        // Cleanup replication config
        if (repl_state->config) {
            free(repl_state->config->master_host);
            free(repl_state->config->replication_id);
            free(repl_state->config);
        }
        
        free(repl_state);
    }

    sleep(1);
    cleanup_commands();
    printf("Server shut down. Resources cleaned up.\n");
}


void *background_cleanup(void *arg) {
    while (1) {
        pthread_mutex_lock(&cleanup_mutex);
        if (!cleanup_running) {
            pthread_mutex_unlock(&cleanup_mutex);
            break;
        }
        pthread_mutex_unlock(&cleanup_mutex);
        sleep(10);
        cleanup_expired_keys();
        check_memory_and_evict();
    }
    return NULL;
}

void start_background_cleanup() {
    pthread_t cleanup_thread;
    if (pthread_create(&cleanup_thread, NULL, background_cleanup, NULL) != 0) {
        fprintf(stderr, "Error: Failed to start cleanup thread.\n");
        exit(EXIT_FAILURE);
    }
    pthread_detach(cleanup_thread);  // Detach thread to avoid manual joins
}


void init_replication_mode(int argc, char *argv[]) {
    // Default to master mode
        ReplicationConfig *config = malloc(sizeof(ReplicationConfig));
    if (!config) {
        fprintf(stderr, "Failed to allocate replication config\n");
        exit(EXIT_FAILURE);
    }


    config->role = ROLE_MASTER;
    config->master_host = NULL;
    config->master_port = 0;
    config->replication_timeout = 60;
    config->replication_id = strdup("master-01");
    config->replication_offset = 0;
    

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--slave") == 0 && i + 2 < argc) {
            config->role = ROLE_SLAVE;
            config->master_host = strdup(argv[i + 1]);
            config->master_port = atoi(argv[i + 2]);
            break;
        }
    }


    pthread_t heartbeat_thread;
    pthread_create(&heartbeat_thread, NULL, replication_heartbeat, NULL);
    pthread_detach(heartbeat_thread);

    init_replication(&repl_config);
}




int main(int argc, char *argv[]) {
    printf("Starting server...\n");


    struct sigaction sa;
    sa.sa_handler = handle_shutdown;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);


    if (initialize_sdb() != 0) {
        fprintf(stderr, "Failed to initialize SDB file. Exiting.\n");
        return EXIT_FAILURE;
    }


    init_replication_mode(argc, argv);


    register_commands();

    start_background_cleanup();

    pthread_t heartbeat_thread;
    if (pthread_create(&heartbeat_thread, NULL, replication_heartbeat, NULL) != 0) {
        fprintf(stderr, "Error: Failed to start heartbeat thread.\n");
        return EXIT_FAILURE;
    }
    pthread_detach(heartbeat_thread);


    struct Server server = server_constructor(
        AF_INET, 
        SOCK_STREAM, 
        0, 
        INADDR_ANY, 
        repl_config.role == ROLE_SLAVE ? 6380 : 6379,  
        10, 
        launch
    );
    
    if (repl_config.role == ROLE_SLAVE) {
        if (connect_to_master() != 0) {
            fprintf(stderr, "Failed to connect to master. Exiting.\n");
            return EXIT_FAILURE;
        }
    }

    server.launch(&server);

    server_shutdown();
    return 0;
}