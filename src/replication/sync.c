// #include "sync.h"
// #include "../core/protocol.h"
// #include "../persistence/sdb.h"
// #include "replication.h"
// #include "sync.h"
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <unistd.h>
// #include <fcntl.h>
// #include <sys/stat.h>

// #define SYNC_BUFFER_SIZE 4096
// #define TEMP_RDB_FILE "temp.sdb"

// static SyncResult create_sync_result(bool success, long long offset, const char *message) {
//     SyncResult result = {
//         .success = success,
//         .offset = offset,
//         .error_message = message ? strdup(message) : NULL
//     };
//     return result;
// }

// SyncResult perform_partial_sync(int slave_socket, long long offset) {
//     MasterState *master = (MasterState *)repl_state->role_specific_state;
    
//     if (!is_partial_sync_possible(offset)) {
//         return create_sync_result(false, 0, "Partial sync not possible");
//     }

//     // Get missing commands
//     int command_count;
//     char **commands = get_commands_from_offset(master->repl_buffer, offset, &command_count);
    
//     if (!commands) {
//         return create_sync_result(false, 0, "Failed to get commands");
//     }

//     // Send commands to slave
//     for (int i = 0; i < command_count; i++) {
//         write(slave_socket, commands[i], strlen(commands[i]));
//     }

//     // Clean up
//     for (int i = 0; i < command_count; i++) {
//         free(commands[i]);
//     }
//     free(commands);

//     return create_sync_result(true, master->repl_buffer->max_offset, NULL);
// }

// bool is_partial_sync_possible(long long offset) {
//     MasterState *master = (MasterState *)repl_state->role_specific_state;
//     return offset >= master->repl_buffer->min_offset && 
//            offset <= master->repl_buffer->max_offset;
// }

// void handle_sync_command(int client_socket, RedisCommand *cmd) {
//     if (!repl_state || repl_state->role != ROLE_MASTER) {
//         send_redis_error(client_socket, "ERR not a master");
//         return;
//     }

//     SyncResult result = perform_full_sync(client_socket);
//     if (!result.success) {
//         send_redis_error(client_socket, result.error_message ? 
//                         result.error_message : "ERR sync failed");
//         if (result.error_message) free(result.error_message);
//         return;
//     }

//     // Add slave to master's slave list
//     MasterState *master = (MasterState *)repl_state->role_specific_state;
//     SlaveNode *slave = malloc(sizeof(SlaveNode));
//     slave->socket = client_socket;
//     slave->offset = result.offset;
//     slave->last_heartbeat = time(NULL);
//     slave->sync_in_progress = false;

//     HASH_ADD_INT(master->slaves, socket, slave);

//     add_slave(client_socket, result.offset);
//     send_redis_string(client_socket, "OK");
// }

// void handle_psync_command(int client_socket, RedisCommand *cmd) {
//     if (cmd->argc != 3) {
//         send_redis_error(client_socket, "ERR wrong number of arguments for PSYNC");
//         return;
//     }

//     long long offset = strtoll(cmd->argv[2].data, NULL, 10);
    
//     if (is_partial_sync_possible(offset)) {
//         // Perform partial sync
//         SyncResult result = perform_partial_sync(client_socket, offset);
//         if (result.success) {
//             send_redis_string(client_socket, "CONTINUE");
//             return;
//         }
//     }

//     // Fall back to full sync
//     handle_sync_command(client_socket, cmd);
// }




// SyncResult perform_full_sync(int slave_socket) {
//     pthread_mutex_lock(&sdb_mutex);  // Use existing mutex from sdb.c

//     // 1. Read current database entries
//     FILE *source = fopen(SDB_FILE, "rb");
//     if (!source) {
//         pthread_mutex_unlock(&sdb_mutex);
//         return create_sync_result(false, 0, "Failed to open source SDB file");
//     }

//     // Read header to get entry count
//     SDBHeader header;
//     fread(&header, sizeof(SDBHeader), 1, source);
    
//     // Read all entries into memory
//     SDBEntry *entries = malloc(header.entry_count * sizeof(SDBEntry));
//     SDBIndex *indices = malloc(header.entry_count * sizeof(SDBIndex));
    
//     if (!entries || !indices) {
//         free(entries);
//         free(indices);
//         fclose(source);
//         pthread_mutex_unlock(&sdb_mutex);
//         return create_sync_result(false, 0, "Memory allocation failed");
//     }

//     // Read indices and entries
//     fread(indices, sizeof(SDBIndex), header.entry_count, source);
//     for (int i = 0; i < header.entry_count; i++) {
//         fseek(source, indices[i].offset, SEEK_SET);
//         fread(&entries[i].ttl, sizeof(uint32_t), 1, source);
//         fread(&entries[i].type, sizeof(uint32_t), 1, source);
//         fread(entries[i].key, MAX_KEY_LENGTH, 1, source);
//         fread(entries[i].value, MAX_VALUE_LENGTH, 1, source);
//     }

//     fclose(source);

//     // 2. Create temporary file for sync
//     FILE *temp = fopen(TEMP_SDB_FILE, "wb");
//     if (!temp) {
//         free(entries);
//         free(indices);
//         pthread_mutex_unlock(&sdb_mutex);
//         return create_sync_result(false, 0, "Failed to create temp file");
//     }

//     // 3. Write to temp file using your exact format
//     // Update header with current timestamp
//     time_t now = time(NULL);
//     strftime(header.created_at, sizeof(header.created_at), 
//              "%Y-%m-%dT%H:%M:%S", localtime(&now));
    
//     fwrite(&header, sizeof(SDBHeader), 1, temp);

//     // Recalculate and write indices
//     uint64_t data_offset = sizeof(SDBHeader) + (header.entry_count * sizeof(SDBIndex));
//     for (int i = 0; i < header.entry_count; i++) {
//         indices[i].offset = data_offset;
//         indices[i].length = strlen(entries[i].key) + strlen(entries[i].value) + 
//                            sizeof(uint32_t) * 2;
//         data_offset += indices[i].length;
//     }
//     fwrite(indices, sizeof(SDBIndex), header.entry_count, temp);

//     // Write entries
//     for (int i = 0; i < header.entry_count; i++) {
//         fwrite(&entries[i].ttl, sizeof(uint32_t), 1, temp);
//         fwrite(&entries[i].type, sizeof(uint32_t), 1, temp);
//         fwrite(entries[i].key, strlen(entries[i].key) + 1, 1, temp);
//         fwrite(entries[i].value, strlen(entries[i].value) + 1, 1, temp);
//     }

//     // Write footer
//     SDBFooter footer = { .checksum = "00000000000000000000000000000000" };
//     fwrite(&footer, sizeof(SDBFooter), 1, temp);

//     fclose(temp);
//     free(entries);
//     free(indices);

//     // 4. Send file size to slave
//     struct stat st;
//     stat(TEMP_SDB_FILE, &st);
//     char size_header[64];
//     snprintf(size_header, sizeof(size_header), "$%lld\r\n", (long long)st.st_size);
//     write(slave_socket, size_header, strlen(size_header));

//     // 5. Send file contents
//     FILE *sync_file = fopen(TEMP_SDB_FILE, "rb");
//     char buffer[SYNC_BUFFER_SIZE];
//     size_t bytes_read;
//     while ((bytes_read = fread(buffer, 1, SYNC_BUFFER_SIZE, sync_file)) > 0) {
//         write(slave_socket, buffer, bytes_read);
//     }
//     fclose(sync_file);
//     unlink(TEMP_SDB_FILE);

//     pthread_mutex_unlock(&sdb_mutex);
//     return create_sync_result(true, data_offset, NULL);
// }

// // Function for slave to receive and process sync
// int receive_sdb_sync(int master_socket) {
//     pthread_mutex_lock(&sdb_mutex);

//     // Read size header
//     char size_buf[64];
//     int size_len = 0;
//     while (size_len < sizeof(size_buf) - 1) {
//         char c;
//         if (read(master_socket, &c, 1) != 1) {
//             pthread_mutex_unlock(&sdb_mutex);
//             return -1;
//         }
//         if (c == '\n' && size_buf[size_len-1] == '\r') break;
//         size_buf[size_len++] = c;
//     }
//     size_buf[size_len] = '\0';

//     // Parse size
//     long long file_size = strtoll(size_buf + 1, NULL, 10);
//     if (file_size <= 0) {
//         pthread_mutex_unlock(&sdb_mutex);
//         return -1;
//     }

//     // Receive to temporary file
//     FILE *temp = fopen(TEMP_SDB_FILE, "wb");
//     if (!temp) {
//         pthread_mutex_unlock(&sdb_mutex);
//         return -1;
//     }

//     char buffer[SYNC_BUFFER_SIZE];
//     long long remaining = file_size;
//     while (remaining > 0) {
//         size_t to_read = remaining > SYNC_BUFFER_SIZE ? SYNC_BUFFER_SIZE : remaining;
//         size_t bytes_read = read(master_socket, buffer, to_read);
//         if (bytes_read <= 0) {
//             fclose(temp);
//             unlink(TEMP_SDB_FILE);
//             pthread_mutex_unlock(&sdb_mutex);
//             return -1;
//         }
//         fwrite(buffer, 1, bytes_read, temp);
//         remaining -= bytes_read;
//     }
//     fclose(temp);

//     // Validate received file
//     if (read_sdb(TEMP_SDB_FILE) != 0) {
//         unlink(TEMP_SDB_FILE);
//         pthread_mutex_unlock(&sdb_mutex);
//         return -1;
//     }

//     // Replace current database file
//     rename(TEMP_SDB_FILE, SDB_FILE);
    
//     pthread_mutex_unlock(&sdb_mutex);
//     return 0;
// }