#include "commands.h"
#include "protocol.h"
#include "include/uthash.h"
#include "include/replication.h"
#include "include/master.h"
#include "include/slave.h"
#include "include/sync.h"
#include "include/replconf.h"
#include "include/buffer.h"
#include "include/sdb.h"
#include "include/sdb.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <time.h>
#include <pthread.h>



static pthread_mutex_t set_table_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t versioned_set_table_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t command_table_mutex = PTHREAD_MUTEX_INITIALIZER;

#define MAX_DATABASES 16

struct SetEntryDB *db_table[MAX_DATABASES] = {NULL};


// Command handler type
typedef void (*CommandHandler)(int client_socket, RedisCommand *cmd);


// Define the base entry structure
struct SetEntryDB {
    char *key;
    char *value;
};

// Versioned entry adds versioning information
struct VersionedSetEntryDB {
    struct SetEntryDB entry;  // Embed SetEntry
    int version;
};
// Hashtable entry for commands
typedef struct CommandEntry {
    char name[MAX_BULK_LENGTH]; // Command name (key)
    CommandHandler handler;      // Command handler (value)
    UT_hash_handle hh;           // Hashtable handle
} CommandEntry;

static CommandEntry *command_table = NULL; // Global command hashtable

// Hashtable entry for SET key-value pairs
struct SetEntry {
    char key[MAX_BULK_LENGTH];    // Key
    char value[MAX_BULK_LENGTH];  // Value
    time_t expiration;            // Expiration timestamp (0 if no expiration)
    UT_hash_handle hh;            // Hashtable handle
};

static struct SetEntry *set_table = NULL; // Global key-value hashtable


struct VersionedSetEntry {
    char key[MAX_BULK_LENGTH];           // Key of the versioned entry
    char value[MAX_BULK_LENGTH];         // Value of the versioned entry
    struct VersionedSetEntry *next;      // Linked list to keep version history
    UT_hash_handle hh;                   // Hash handle used by uthash
};
struct VersionedSetEntry *versioned_set_table = NULL;


// Data structure to store time-series data
typedef struct TimeSeriesEntry {
    time_t timestamp;
    double value;
} TimeSeriesEntry;


typedef struct GeoDataEntry {
    double latitude;
    double longitude;
    char *location_name;
} GeoDataEntry;




// Function to retrieve a database by its ID
struct SetEntryDB *get_database(int db_id) {
    if (db_id < 0 || db_id >= MAX_DATABASES) {
        return NULL;  // Invalid database ID
    }
    return db_table[db_id];
}

// Function to set a database by its ID
void set_database(int db_id, struct SetEntryDB *new_db) {
    if (db_id < 0 || db_id >= MAX_DATABASES) {
        return;  // Invalid database ID
    }
    db_table[db_id] = new_db;
}


static int is_key_expired(struct SetEntry *entry);
static void delete_key(struct SetEntry *entry);


extern ReplicationState *repl_state; 

// Register a command
// Precompute uppercase command names during registration
void register_command(const char *name, CommandHandler handler) {
    CommandEntry *entry = malloc(sizeof(CommandEntry));
    if (!entry) {
        fprintf(stderr, "Error: Failed to allocate memory for command '%s'.\n", name);
        exit(EXIT_FAILURE);
    }
    memset(entry->name, 0, MAX_BULK_LENGTH);  // Ensure the name array is null-terminated
    for (int i = 0; name[i] && i < MAX_BULK_LENGTH - 1; i++) {
        entry->name[i] = toupper(name[i]);
    }
    entry->handler = handler;
    HASH_ADD_STR(command_table, name, entry);
}

// Command handlers
void handle_ping(int client_socket, RedisCommand *cmd) {
    if (cmd->argc == 1) {
        send_redis_string(client_socket, "PONG");
    } else {
        char response[MAX_BULK_LENGTH];
        strncpy(response, cmd->argv[1].data, cmd->argv[1].length);
        response[cmd->argv[1].length] = '\0';
        send_redis_bulk_string(client_socket, response);
    }
}

void handle_echo(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'echo' command");
    } else {
        char response[MAX_BULK_LENGTH];
        strncpy(response, cmd->argv[1].data, cmd->argv[1].length);
        response[cmd->argv[1].length] = '\0';
        send_redis_bulk_string(client_socket, response);
    }
}

void handle_set(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "Invalid number of arguments");
        return;
    }

    // Check if we're a slave and this isn't from master
    if (repl_state && repl_state->role == ROLE_SLAVE && 
        !repl_state->processing_master_command) {
        send_redis_error(client_socket, "READONLY You can't write against a read only slave.");
        return;
    }

    const char *key = cmd->argv[1].data;
    const char *value = cmd->argv[2].data;
    size_t key_len = cmd->argv[1].length;
    size_t value_len = cmd->argv[2].length;
    int expiration = 0;
    int cas_value = -1; // Default: No CAS

    // Check for optional arguments like EX, NX, XX, COND, CAS
    for (int i = 3; i < cmd->argc; i++) {
        if (strncmp(cmd->argv[i].data, "EX", 2) == 0) {
            if (i + 1 < cmd->argc) {
                expiration = atoi(cmd->argv[i + 1].data);
                i++; // Skip next argument (expiration time)
            } else {
                send_redis_error(client_socket, "Missing expiration time for EX");
                return;
            }
        } else if (strncmp(cmd->argv[i].data, "CAS", 3) == 0) {
            if (i + 1 < cmd->argc) {
                cas_value = atoi(cmd->argv[i + 1].data);
                i++; // Skip the expected value argument
            } else {
                send_redis_error(client_socket, "CAS requires a value");
                return;
            }
        }
        // ... other options handling ...
    }

    // Handle CAS: Ensure atomicity
    pthread_mutex_lock(&set_table_mutex);
    struct SetEntry *entry;
    HASH_FIND(hh, set_table, key, key_len, entry);
    
    if (cas_value != -1 && (!entry || atoi(entry->value) != cas_value)) {
        pthread_mutex_unlock(&set_table_mutex);
        send_redis_error(client_socket, "CAS failed: value does not match");
        return;
    }

    // Add or update key-value pair
    if (entry) {
        strncpy(entry->value, value, MAX_BULK_LENGTH - 1);
        entry->value[MAX_BULK_LENGTH - 1] = '\0';
    } else {
        entry = malloc(sizeof(struct SetEntry));
        if (!entry) {
            pthread_mutex_unlock(&set_table_mutex);
            send_redis_error(client_socket, "Out of memory");
            return;
        }
        strncpy(entry->key, key, key_len);
        entry->key[key_len] = '\0';
        strncpy(entry->value, value, value_len);
        entry->value[value_len] = '\0';
        entry->expiration = 0;
        HASH_ADD_KEYPTR(hh, set_table, entry->key, key_len, entry);
    }

    // Handle expiration (EX)
    if (expiration > 0) {
        entry->expiration = time(NULL) + expiration;
    }

    pthread_mutex_unlock(&set_table_mutex);

    // If we're the master, propagate to slaves
    if (repl_state && repl_state->role == ROLE_MASTER) {
        propagate_command_to_slaves(cmd);
    }

    send_redis_string(client_socket, "OK");
}


void handle_get(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'GET' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    pthread_mutex_lock(&set_table_mutex);

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);

    // Check if the key is expired
    if (entry && is_key_expired(entry)) {
        HASH_DEL(set_table, entry);
        free(entry);
        entry = NULL;
    }

    // Return value if found in memory
    if (entry) {
        send_redis_bulk_string(client_socket, entry->value);
        pthread_mutex_unlock(&set_table_mutex);
        return;
    }

    pthread_mutex_unlock(&set_table_mutex);

    // If not found in memory, try reading from SDB
    SDBEntry sdb_entry;
    if (read_from_sdb(key, &sdb_entry) == 0) {
        // Add to in-memory cache
        entry = malloc(sizeof(struct SetEntry));
        if (entry) {
            strncpy(entry->key, sdb_entry.key, MAX_BULK_LENGTH);
            strncpy(entry->value, sdb_entry.value, MAX_BULK_LENGTH);
            entry->expiration = sdb_entry.ttl ? time(NULL) + sdb_entry.ttl : 0; // Calculate expiration
            pthread_mutex_lock(&set_table_mutex);
            HASH_ADD_STR(set_table, key, entry);
            pthread_mutex_unlock(&set_table_mutex);
        }
        send_redis_bulk_string(client_socket, sdb_entry.value);
    } else {
        send_redis_bulk_string(client_socket, "nil");
    }
}




void handle_setex(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 4) {
        send_redis_error(client_socket, "wrong number of arguments for 'SETEX' command");
        return;
    }

    char key[MAX_BULK_LENGTH], value[MAX_BULK_LENGTH];
    int expiration_time;

    // Extract key, value, and expiration time from the command
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    strncpy(value, cmd->argv[2].data, cmd->argv[2].length);
    value[cmd->argv[2].length] = '\0';

    expiration_time = atoi(cmd->argv[3].data);  // Convert expiration time to integer

    // Get current time and calculate expiration time
    time_t current_time = time(NULL);
    time_t expiration_timestamp = current_time + expiration_time;

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);

    if (entry && is_key_expired(entry)) {
        delete_key(entry);
        send_redis_bulk_string(client_socket, "nil");
        strncpy(entry->value, value, MAX_BULK_LENGTH);
        entry->expiration = expiration_timestamp;  // Update expiration time
        return;
    } else {
        entry = malloc(sizeof(struct SetEntry));
        if (!entry) {
            fprintf(stderr, "Error: Memory allocation failed for command registration.\n");
            exit(EXIT_FAILURE);  // Exit gracefully or handle the error appropriately
        }
        strncpy(entry->key, key, MAX_BULK_LENGTH);
        strncpy(entry->value, value, MAX_BULK_LENGTH);
        entry->expiration = expiration_timestamp;

        HASH_ADD_STR(set_table, key, entry);
    }


        // Save to SDB
    if (save_to_sdb(key, value, expiration_time) != 0) {
        send_redis_error(client_socket, "Failed to persist data");
        return;
    }

    send_redis_string(client_socket, "OK");
}




void handle_expire(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'EXPIRE' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    int expiration_time;

    // Extract key and expiration time
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    expiration_time = atoi(cmd->argv[2].data);  // Convert expiration time to integer

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);
    if (entry && is_key_expired(entry)) {

        delete_key(entry);
        send_redis_bulk_string(client_socket, "nil");
        
        // Update expiration time
        time_t current_time = time(NULL);
        entry->expiration = current_time + expiration_time;

        if (save_to_sdb(key, entry->value, expiration_time) != 0) {
            send_redis_error(client_socket, "Failed to persist expiration");
            return;
        }

        send_redis_integer(client_socket, 1);  // Return 1 for successful expiration update
        return;
    } else {
        send_redis_integer(client_socket, 0);  // Return 0 if key does not exist
    }
}


void handle_incr(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'INCR' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);

    if (entry) {
        long value = atol(entry->value);  // Convert value to long
        value++;  // Increment the value
        snprintf(entry->value, MAX_BULK_LENGTH, "%ld", value);  // Store back the incremented value

        send_redis_integer(client_socket, value);  // Return the new incremented value
    } else {
        send_redis_error(client_socket, "key does not exist");
    }
}

void handle_mget(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'MGET' command");
        return;
    }

    for (int i = 1; i < cmd->argc; i++) {
        char key[MAX_BULK_LENGTH];
        strncpy(key, cmd->argv[i].data, cmd->argv[i].length);
        key[cmd->argv[i].length] = '\0';

        struct SetEntry *entry;
        HASH_FIND_STR(set_table, key, entry);

        if (entry && is_key_expired(entry)) {
            // Check if the key is expired
            delete_key(entry);
            send_redis_bulk_string(client_socket, "nil");
            

            // Key exists and is not expired
            send_redis_bulk_string(client_socket, entry->value);
            return;
        } else {
            // Key does not exist
            send_redis_bulk_string(client_socket, "nil");
        }
    }
}


void handle_getex(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'GETEX' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);

    if (entry && is_key_expired(entry)) {
        // Check if the key is expired
        delete_key(entry);
        send_redis_bulk_string(client_socket, "nil");
             

        // Key exists and is not expired
        entry->expiration = time(NULL) + 3600; // Reset TTL (e.g., 1 hour)
        send_redis_bulk_string(client_socket, entry->value);
        return; 
    } else {
        // Key does not exist
        send_redis_bulk_string(client_socket, "nil");
    }
}


void handle_del(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'DEL' command");
        return;
    }

    int deleted_count = 0;
    for (int i = 1; i < cmd->argc; i++) {
        char key[MAX_BULK_LENGTH];
        strncpy(key, cmd->argv[i].data, cmd->argv[i].length);
        key[cmd->argv[i].length] = '\0';

        struct SetEntry *entry;
        HASH_FIND_STR(set_table, key, entry);
        if (entry) {
            // Optional: Handle DEL_IF (delete based on a condition)
            if (cmd->argc > 2 && strncmp(cmd->argv[1].data, "DEL_IF", 6) == 0) {
                // Example: DEL_IF key > 10
                // Here we would parse the condition, e.g., key-value comparison
                int condition_met = 1; // Placeholder condition check
                if (condition_met) {
                    HASH_DEL(set_table, entry);
                    free(entry);
                    deleted_count++;
                } else {
                    send_redis_error(client_socket, "Condition not met for DEL_IF");
                    return;
                }
            } else {
                HASH_DEL(set_table, entry);
                free(entry);
                deleted_count++;
            }
        }
        save_to_sdb(key, "", 1);
    }

    send_redis_integer(client_socket, deleted_count);  // Return the number of deleted keys
}

void handle_getttl(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'GETTTL' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);
    if (entry) {
        send_redis_bulk_string(client_socket, entry->value);

        // Simulate TTL logic (e.g., placeholder TTL of 3600 seconds or expiration timestamp logic)
        send_redis_integer(client_socket, 3600);  // Placeholder TTL value (1 hour)
    } else {
        send_redis_bulk_string(client_socket, "nil");
        send_redis_integer(client_socket, -1);  // No TTL if the key doesn't exist
    }
}


void handle_copy(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'COPY' command");
        return;
    }

    char key[MAX_BULK_LENGTH], new_key[MAX_BULK_LENGTH];
    int expiration = 0;

    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';
    strncpy(new_key, cmd->argv[2].data, cmd->argv[2].length);
    new_key[cmd->argv[2].length] = '\0';

    // Optional EX argument for expiration time
    if (cmd->argc > 3 && strncmp(cmd->argv[3].data, "EX", 2) == 0 && cmd->argc > 4) {
        expiration = atoi(cmd->argv[4].data);
    }

    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);
    if (entry) {
        struct SetEntry *new_entry = malloc(sizeof(struct SetEntry));
        if (!new_entry) {
            fprintf(stderr, "Error: Memory allocation failed for command registration.\n");
            exit(EXIT_FAILURE);  // Exit gracefully or handle the error appropriately
        }
        strncpy(new_entry->key, new_key, MAX_BULK_LENGTH);
        strncpy(new_entry->value, entry->value, MAX_BULK_LENGTH);
        // Implement expiration (EX) here if needed
        HASH_ADD_STR(set_table, key, new_entry);
        send_redis_string(client_socket, "OK");
    } else {
        send_redis_error(client_socket, "Source key does not exist");
    }
}

void handle_aggregate(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'AGGREGATE' command");
        return;
    }

    char operation[MAX_BULK_LENGTH];
    strncpy(operation, cmd->argv[1].data, cmd->argv[1].length);
    operation[cmd->argv[1].length] = '\0';

    int result = 0;
    for (int i = 2; i < cmd->argc; i++) {
        char key[MAX_BULK_LENGTH];
        strncpy(key, cmd->argv[i].data, cmd->argv[i].length);
        key[cmd->argv[i].length] = '\0';

        struct SetEntry *entry;
        HASH_FIND_STR(set_table, key, entry);
        if (entry) {
            result += atoi(entry->value);
        } else {
            send_redis_error(client_socket, "One or more keys do not exist");
            return;
        }
    }

    // Return the result of aggregation (e.g., SUM)
    send_redis_integer(client_socket, result);
}

void handle_query(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'QUERY' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    char condition[MAX_BULK_LENGTH];
    strncpy(condition, cmd->argv[2].data, cmd->argv[2].length);
    condition[cmd->argv[2].length] = '\0';

    // In a real implementation, this would involve parsing the condition and querying a structured dataset (e.g., hash fields)
    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);
    if (entry) {
        // If condition matches (for simplicity, we assume it’s always true)
        send_redis_bulk_string(client_socket, entry->value);
    } else {
        send_redis_bulk_string(client_socket, "nil");
    }
}


void handle_stream(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 4) {
        send_redis_error(client_socket, "wrong number of arguments for 'STREAM' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    int start = atoi(cmd->argv[2].data);
    int count = atoi(cmd->argv[3].data);

    // Simulate streaming logic (in a real implementation, this would fetch ranges from a sorted set or list)
    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);
    if (entry) {
        // Simulate streaming by splitting the value into chunks
        send_redis_bulk_string(client_socket, entry->value);  // Placeholder for actual stream logic
    } else {
        send_redis_bulk_string(client_socket, "nil");
    }
}


void handle_hsearch(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'HSEARCH' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    char pattern[MAX_BULK_LENGTH];
    strncpy(pattern, cmd->argv[2].data, cmd->argv[2].length);
    pattern[cmd->argv[2].length] = '\0';

    // Simulate hash field search (e.g., use pattern matching on key fields)
    struct SetEntry *entry;
    HASH_FIND_STR(set_table, key, entry);
    if (entry) {
        send_redis_bulk_string(client_socket, entry->value);  // Return matched value
    } else {
        send_redis_bulk_string(client_socket, "nil");
    }
}

void handle_setv(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'SETV' command");
        return;
    }

    char key[MAX_BULK_LENGTH], value[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';
    strncpy(value, cmd->argv[2].data, cmd->argv[2].length);
    value[cmd->argv[2].length] = '\0';

    // Check if the key already exists in versioned set
    struct VersionedSetEntry *entry;
    HASH_FIND_STR(versioned_set_table, key, entry);

    if (entry) {
        // Create a new version and add it to the linked list
        struct VersionedSetEntry *new_entry = malloc(sizeof(struct VersionedSetEntry));
        if (!new_entry) {
            fprintf(stderr, "Error: Memory allocation failed for command registration.\n");
            exit(EXIT_FAILURE);  // Exit gracefully or handle the error appropriately
        }
        strncpy(new_entry->key, key, MAX_BULK_LENGTH);
        strncpy(new_entry->value, entry->value, MAX_BULK_LENGTH);
        new_entry->next = entry;  // Point to previous version
        HASH_ADD_STR(versioned_set_table, key, new_entry);  // Add new version to hash table
    } else {
        struct VersionedSetEntry *new_entry = malloc(sizeof(struct VersionedSetEntry));
        if (!new_entry) {
            fprintf(stderr, "Error: Memory allocation failed for command registration.\n");
            exit(EXIT_FAILURE);  // Exit gracefully or handle the error appropriately
        }
        strncpy(new_entry->key, key, MAX_BULK_LENGTH);
        strncpy(new_entry->value, value, MAX_BULK_LENGTH);
        new_entry->next = NULL;
        HASH_ADD_STR(versioned_set_table, key, new_entry);  // Add first version
    }

    send_redis_string(client_socket, "OK");
}

// Handle the HISTORY command to retrieve the history of a key
void handle_history(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'HISTORY' command");
        return;
    }

    char key[MAX_BULK_LENGTH];
    strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
    key[cmd->argv[1].length] = '\0';

    struct VersionedSetEntry *entry;
    HASH_FIND_STR(versioned_set_table, key, entry);
    
    if (!entry) {
        send_redis_bulk_string(client_socket, "nil");
        return;
    }

    // Iterate through the version history
    while (entry) {
        send_redis_bulk_string(client_socket, entry->value);
        entry = entry->next;
    }
}


void handle_bulk_set(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3 || cmd->argc % 2 != 1) {
        send_redis_error(client_socket, "wrong number of arguments for 'BULK_SET' command");
        return;
    }

    for (int i = 1; i < cmd->argc; i += 2) {
        char key[MAX_BULK_LENGTH], value[MAX_BULK_LENGTH];
        strncpy(key, cmd->argv[i].data, cmd->argv[i].length);
        key[cmd->argv[i].length] = '\0';
        strncpy(value, cmd->argv[i + 1].data, cmd->argv[i + 1].length);
        value[cmd->argv[i + 1].length] = '\0';

        struct SetEntry *entry = malloc(sizeof(struct SetEntry));
        if (!entry) {
            fprintf(stderr, "Error: Memory allocation failed for command registration.\n");
            exit(EXIT_FAILURE);  // Exit gracefully or handle the error appropriately
        }
        strncpy(entry->key, key, MAX_BULK_LENGTH);
        strncpy(entry->value, value, MAX_BULK_LENGTH);

        HASH_ADD_STR(set_table, key, entry);
    }

    send_redis_string(client_socket, "OK");
}

void handle_bulk_get(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'BULK_GET' command");
        return;
    }

    // Iterate over the keys provided in the command
    for (int i = 1; i < cmd->argc; i++) {
        char key[MAX_BULK_LENGTH];
        strncpy(key, cmd->argv[i].data, cmd->argv[i].length);
        key[cmd->argv[i].length] = '\0';

        struct SetEntry *entry;
        HASH_FIND_STR(set_table, key, entry);
        if (entry) {
            send_redis_bulk_string(client_socket, entry->value);
        } else {
            send_redis_bulk_string(client_socket, "nil");
        }
    }
}

// Handle the FLUSHALL command to remove all keys
void handle_flushall(int client_socket, RedisCommand *cmd) {
    // Iterate through all the databases and clear them.
    // Assuming you have a global or database-specific structure to track the data
    // e.g., a hash table of key-value pairs for each database.

    struct VersionedSetEntry *entry, *tmp;
    HASH_ITER(hh, versioned_set_table, entry, tmp) {
        HASH_DEL(versioned_set_table, entry);  // Remove entry from hash table
        free(entry);  // Free the memory allocated for the entry
    }

    send_redis_string(client_socket, "OK");
}

// Handle the BACKUP command to trigger a backup
void handle_backup(int client_socket, RedisCommand *cmd) {
    FILE *backup_file = fopen("backup.rdb", "wb");
    if (!backup_file) {
        send_redis_error(client_socket, "failed to open backup file");
        return;
    }

    pthread_mutex_lock(&set_table_mutex);

    struct SetEntry *entry, *tmp;
    HASH_ITER(hh, set_table, entry, tmp) {
        fwrite(entry, sizeof(struct SetEntry), 1, backup_file);
    }

    pthread_mutex_unlock(&set_table_mutex);

    fclose(backup_file);
    send_redis_string(client_socket, "Backup completed");
}


//TODO: FIX INCOMPATIBLE TYPES
// Handle the SWAPDB command to swap two databases

/*
void handle_swapdb(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 3) {
        send_redis_error(client_socket, "wrong number of arguments for 'SWAPDB' command");
        return;
    }

    int db1 = atoi(cmd->argv[1].data);  // Convert database 1 to integer
    int db2 = atoi(cmd->argv[2].data);  // Convert database 2 to integer

    if (db1 < 0 || db2 < 0) {
        send_redis_error(client_socket, "invalid database number");
        return;
    }

    // Assume we have a global array of databases (each database being a hash table)
    // Example: Database 1 and Database 2 are represented by two hash tables
    
    // Get the pointers to the databases
    struct VersionedSetEntryDB *db1_table = get_database(db1);  // Retrieve database 1
    struct VersionedSetEntryDB *db2_table = get_database(db2);  // Retrieve database 2

    // Ensure both databases exist before swapping
    if (db1_table == NULL || db2_table == NULL) {
        send_redis_error(client_socket, "one or both databases do not exist");
        return;
    }

    // Swap the databases
    // For this example, we assume that set_database() takes a database index and the corresponding database structure.
    set_database(db1, db2_table);  // Swap contents of db1 with db2
    set_database(db2, db1_table);  // Swap contents of db2 with db1

    send_redis_string(client_socket, "OK");
}
*/



// Handle the SELECT command to select the current database
void handle_select(int client_socket, RedisCommand *cmd) {
    if (cmd->argc < 2) {
        send_redis_error(client_socket, "wrong number of arguments for 'SELECT' command");
        return;
    }

    int db = atoi(cmd->argv[1].data);  // Get the database index

    if (db < 0 || db >= MAX_DATABASES) {
        send_redis_error(client_socket, "invalid database index");
        return;
    }

    // Set the current database for this client session
    // current_db = db;

    send_redis_string(client_socket, "OK");
}

// TODO:Time-Series add command (TS.ADD)
// void handle_ts_add(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc != 4) {  // Corrected to 4 arguments: series_name, timestamp, value
//         send_redis_error(client_socket, "wrong number of arguments for 'TS.ADD' command");
//         return;
//     }

//     char series_name[MAX_BULK_LENGTH];
//     strncpy(series_name, cmd->argv[1].data, cmd->argv[1].length);
//     series_name[cmd->argv[1].length] = '\0';  // Null-terminate the series name

//     long timestamp = strtol(cmd->argv[2].data, NULL, 10);  // Convert timestamp to long
//     double value = atof(cmd->argv[3].data);  // Convert value to double (time-series data value)

//     // Retrieve or create the sorted set for the series
//     ZSet *zset = get_zset(series_name);  // Fetch or create a ZSet for this series
//     if (zset == NULL) {
//         zset = create_zset();  // Create a new ZSet if it doesn't exist
//         set_zset(series_name, zset);  // Store the ZSet in the global database (or similar)
//     }

//     // Add the timestamp and value to the ZSet
//     redisZAdd(zset, series_name, timestamp);  // You may want to store timestamp and value in separate places

//     send_redis_string(client_socket, "OK");  // Send success response
// }

//TODO: Time-Series range command (TS.RANGE)
// void handle_ts_range(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc < 3) {
//         send_redis_error(client_socket, "wrong number of arguments for 'TS.RANGE' command");
//         return;
//     }

//     char series_name[MAX_BULK_LENGTH];
//     strncpy(series_name, cmd->argv[1].data, cmd->argv[1].length);
//     series_name[cmd->argv[1].length] = '\0';

//     long start_timestamp = strtol(cmd->argv[2].data, NULL, 10);
//     long end_timestamp = strtol(cmd->argv[3].data, NULL, 10);

//     // Optionally handle aggregation type (e.g., SUM, AVG)
//     const char *aggregation_type = cmd->argc > 4 ? cmd->argv[4].data : NULL;

//     // Retrieve time-series data from a ZSET (sorted by timestamp)
//     // Simplified example: get range from sorted set
//     redisZRange(series_name, start_timestamp, end_timestamp, aggregation_type); // Example

//     send_redis_string(client_socket, "OK");
// }

//TODO: Implement GEOFILTER
// void handle_geo_filter(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc < 3) {
//         send_redis_error(client_socket, "wrong number of arguments for 'GEOFILTER' command");
//         return;
//     }

//     char key[MAX_BULK_LENGTH];
//     strncpy(key, cmd->argv[1].data, cmd->argv[1].length);
//     key[cmd->argv[1].length] = '\0';

//     double radius = atof(cmd->argv[2].data);
//     // Optionally handle additional condition (e.g., filter by location or other fields)
//     const char *condition = cmd->argc > 3 ? cmd->argv[3].data : NULL;

//     // Filter geo-data (simplified example)
//     redisGeoFilter(key, radius, condition); // This would filter the geo-data from the sorted set

//     send_redis_string(client_socket, "OK");
// }

// TODO: Handle the ZADD command to add a member to a sorted set.
// void handle_zadd(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc < 4) {
//         send_redis_error(client_socket, "wrong number of arguments for 'ZADD' command");
//         return;
//     }

//     const char *key = cmd->argv[1].data;
//     double score = atof(cmd->argv[2].data);  // Convert score to double
//     const char *member = cmd->argv[3].data;

//     // Get or create the sorted set (this assumes you have a function to retrieve a sorted set from a database or cache).
//     ZSet *zset = get_zset(key);
//     if (zset == NULL) {
//         zset = create_zset();  // Create the sorted set if it doesn't exist.
//         set_zset(key, zset);   // Store the sorted set in the database.
//     }

//     // Add the member to the sorted set.
//     int result = redisZAdd(zset, member, score);
//     if (result == 1) {
//         send_redis_integer(client_socket, 1);  // Return 1 for a successful addition.
//     } else {
//         send_redis_integer(client_socket, 0);  // Return 0 if the member already exists and no update is made.
//     }
// }


//TODO: Handle the ZREM command to remove a member from a sorted set.
// void handle_zrem(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc < 3) {
//         send_redis_error(client_socket, "wrong number of arguments for 'ZREM' command");
//         return;
//     }

//     const char *key = cmd->argv[1].data;
//     const char *member = cmd->argv[2].data;

//     // Get the sorted set from the database.
//     ZSet *zset = get_zset(key);
//     if (zset == NULL) {
//         send_redis_integer(client_socket, 0);  // Return 0 if the sorted set doesn't exist.
//         return;
//     }

//     // Remove the member from the sorted set.
//     int result = redisZRem(zset, member);
//     if (result == 1) {
//         send_redis_integer(client_socket, 1);  // Return 1 if the member was removed.
//     } else {
//         send_redis_integer(client_socket, 0);  // Return 0 if the member doesn't exist.
//     }
// }

//TODO: Handle the ZRANGE command to retrieve members in a given score range.
// void handle_zrange(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc < 4) {
//         send_redis_error(client_socket, "wrong number of arguments for 'ZRANGE' command");
//         return;
//     }

//     const char *key = cmd->argv[1].data;
//     double min_score = atof(cmd->argv[2].data);
//     double max_score = atof(cmd->argv[3].data);

//     // Get the sorted set from the database.
//     ZSet *zset = get_zset(key);
//     if (zset == NULL) {
//         send_redis_error(client_socket, "sorted set not found");
//         return;
//     }

//     // Retrieve members in the specified score range.
//     ZSetEntry *entry = redisZRange(zset, min_score, max_score);
//     if (entry == NULL) {
//         send_redis_bulk_string(client_socket, "nil");
//         return;
//     }

//     // Return the found members.
//     while (entry != NULL) {
//         char member_response[MAX_BULK_LENGTH];
//         snprintf(member_response, sizeof(member_response), "$%zu\r\n%s\r\n", strlen(entry->member), entry->member);
//         write(client_socket, member_response, strlen(member_response));
//         entry = entry->next;
//     }
// }


void handle_sync(int client_socket, RedisCommand *cmd) {
    handle_sync_command(client_socket, cmd);
}

void handle_psync(int client_socket, RedisCommand *cmd) {
    handle_psync_command(client_socket, cmd);
}




void register_commands() {
    register_command("PING", handle_ping);
    register_command("ECHO", handle_echo);
    register_command("SET", handle_set);
    register_command("GET", handle_get);
    register_command("SETEX", handle_setex);
    register_command("GETEX", handle_getex);  
    register_command("DEL", handle_del);      
    register_command("EXPIRE", handle_expire); 
    register_command("INCR", handle_incr);    
    register_command("MGET", handle_mget);
    register_command("GETTTL", handle_getttl);  
    register_command("COPY", handle_copy);      
    register_command("AGGREGATE", handle_aggregate); 
    register_command("QUERY", handle_query);    
    register_command("STREAM", handle_stream);  
    register_command("HSEARCH", handle_hsearch);
    register_command("SETV", handle_setv);
    register_command("HISTORY", handle_history);  
    register_command("BULK_SET", handle_bulk_set);
    register_command("BULK_GET", handle_bulk_get);
    register_command("FLUSHALL", handle_flushall);  
    register_command("BACKUP", handle_backup);
    register_command("SYNC", handle_sync);
    register_command("PSYNC", handle_psync);
    register_command("REPLCONF", handle_replconf);
    // register_command("SWAPDB", handle_swapdb);
    register_command("SELECT", handle_select);
    // register_command("TS.ADD", handle_ts_add);
    // register_command("TS.RANGE", handle_ts_range);
    // register_command("GEOFILTER", handle_geo_filter);
}


// Cleanup commands
void cleanup_commands() {
    // Cleanup command table
    CommandEntry *entry, *tmp;
    if (command_table) {
        HASH_ITER(hh, command_table, entry, tmp) {
            HASH_DEL(command_table, entry);
            free(entry);  // Free command table entry
        }
        command_table = NULL;  // Clear global pointer
    }

    // Cleanup set table
    struct SetEntry *set_entry, *set_tmp;
    if (set_table) {
        HASH_ITER(hh, set_table, set_entry, set_tmp) {
            HASH_DEL(set_table, set_entry);
            free(set_entry);  // Free set entry
        }
        set_table = NULL;  // Clear global pointer
    }

    // Cleanup versioned set table
    struct VersionedSetEntry *ver_entry, *ver_tmp;
    if (versioned_set_table) {
        HASH_ITER(hh, versioned_set_table, ver_entry, ver_tmp) {
            HASH_DEL(versioned_set_table, ver_entry);
            free(ver_entry);  // Free versioned set entry
        }
        versioned_set_table = NULL;  // Clear global pointer
    }

    // Cleanup db_table
    for (int i = 0; i < MAX_DATABASES; i++) {
        if (db_table[i]) {
            free(db_table[i]);  // Free database entry if dynamically allocated
            db_table[i] = NULL; // Clear the entry
        }
    }

    printf("All commands and data structures have been cleaned up.\n");
}

void execute_command(int client_socket, RedisCommand *cmd) {
    if (cmd->argc == 0) {
        send_redis_error(client_socket, "empty command");
        return;
    }

    // Extract command name
    char command[MAX_BULK_LENGTH];
    strncpy(command, cmd->argv[0].data, cmd->argv[0].length);
    command[cmd->argv[0].length] = '\0';

    // Convert to uppercase
    for (int i = 0; command[i]; i++) {
        command[i] = toupper(command[i]);
    }

    // Lookup the command in the hashtable
    CommandEntry *entry;
    HASH_FIND_STR(command_table, command, entry);

    if (entry) {
        entry->handler(client_socket, cmd);
    } else {
        send_redis_error(client_socket, "unknown command");
    }
}


void cleanup_expired_keys() {
    pthread_mutex_lock(&set_table_mutex);

    struct SetEntry *entry, *tmp;
    time_t now = time(NULL);

    HASH_ITER(hh, set_table, entry, tmp) {
        if (entry->expiration > 0 && entry->expiration <= now) {
            HASH_DEL(set_table, entry);
            free(entry);
        }
    }

    pthread_mutex_unlock(&set_table_mutex);
    printf("Expired keys cleaned up.\n");
}

void evict_random_key() {
    struct SetEntry *entry, *tmp;
    int key_count = HASH_COUNT(set_table);

    if (key_count == 0) return;  // Nothing to evict

    int random_index = rand() % key_count;  // Pick a random key index
    int i = 0;

    HASH_ITER(hh, set_table, entry, tmp) {
        if (i == random_index) {
            HASH_DEL(set_table, entry);
            free(entry);
            printf("Evicted a random key.\n");
            return;
        }
        i++;
    }
}

void check_memory_and_evict() {
    int key_count = HASH_COUNT(set_table);
    const int MAX_KEYS = 1000;  // Example threshold

    while (key_count > MAX_KEYS) {
        evict_random_key();
        key_count = HASH_COUNT(set_table);
    }
}


int is_key_expired(struct SetEntry *entry) {
    if (entry->expiration > 0 && time(NULL) > entry->expiration) {
        return 1; // Expired
    }
    return 0; // Not expired
}

void delete_key(struct SetEntry *entry) {
    HASH_DEL(set_table, entry);
    free(entry);
}