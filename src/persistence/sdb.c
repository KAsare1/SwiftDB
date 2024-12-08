#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "include/sdb.h"

pthread_mutex_t sdb_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *SDB_FILE = "database.sdb";

// Initialize the SDB file if it doesn't exist
int initialize_sdb() {
    FILE *file = fopen(SDB_FILE, "rb");
    if (!file) {
        printf("Initializing new SDB file: %s\n", SDB_FILE);
        return write_sdb(SDB_FILE, NULL, 0);  // Create an empty SDB file
    }
    fclose(file);
    return 0;
}

// Write the SDB file (binary format)
int write_sdb(const char *filename, SDBEntry *entries, int entry_count) {
    pthread_mutex_lock(&sdb_mutex);  // Ensure thread safety

    FILE *file = fopen(filename, "wb");
    if (!file) {
        perror("Error opening file");
        pthread_mutex_unlock(&sdb_mutex);
        return -1;
    }

    // Write Header
    SDBHeader header = {
        .version = "1.0",
        .created_at = "2024-11-19T12:00:00", // Ideally generated dynamically
        .entry_count = entry_count,
        .compression = "None",
        .encryption = "None"
    };
    fwrite(&header, sizeof(SDBHeader), 1, file);

    // Write Index Section
    uint64_t data_offset = sizeof(SDBHeader) + (entry_count * sizeof(SDBIndex));
    SDBIndex *index = malloc(entry_count * sizeof(SDBIndex));
    for (int i = 0; i < entry_count; i++) {
        index[i].offset = data_offset;
        index[i].length = strlen(entries[i].key) + strlen(entries[i].value) + sizeof(uint32_t) * 2;
        data_offset += index[i].length;
    }
    fwrite(index, sizeof(SDBIndex), entry_count, file);

    // Write Data Section
    for (int i = 0; i < entry_count; i++) {
        fwrite(&entries[i].ttl, sizeof(uint32_t), 1, file);
        fwrite(&entries[i].type, sizeof(uint32_t), 1, file);
        fwrite(entries[i].key, strlen(entries[i].key) + 1, 1, file);
        fwrite(entries[i].value, strlen(entries[i].value) + 1, 1, file);
    }

    // Write Footer
    SDBFooter footer = { .checksum = "00000000000000000000000000000000" };  // Placeholder checksum
    fwrite(&footer, sizeof(SDBFooter), 1, file);

    free(index);
    fclose(file);
    pthread_mutex_unlock(&sdb_mutex);
    return 0;
}

// Read the SDB file (binary format)
int read_sdb(const char *filename) {
    pthread_mutex_lock(&sdb_mutex);

    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Error opening file");
        pthread_mutex_unlock(&sdb_mutex);
        return -1;
    }

    // Read Header
    SDBHeader header;
    fread(&header, sizeof(SDBHeader), 1, file);
    printf("Version: %s\nCreated At: %s\nEntries: %u\n",
           header.version, header.created_at, header.entry_count);

    // Read Index Section
    SDBIndex *index = malloc(header.entry_count * sizeof(SDBIndex));
    fread(index, sizeof(SDBIndex), header.entry_count, file);

    // Read Data Section
    for (int i = 0; i < header.entry_count; i++) {
        SDBEntry entry;
        fseek(file, index[i].offset, SEEK_SET);
        fread(&entry.ttl, sizeof(uint32_t), 1, file);
        fread(&entry.type, sizeof(uint32_t), 1, file);
        fread(entry.key, MAX_KEY_LENGTH, 1, file);
        fread(entry.value, MAX_VALUE_LENGTH, 1, file);

        printf("Key: %s, Value: %s, TTL: %u, Type: %u\n",
               entry.key, entry.value, entry.ttl, entry.type);
    }

    // Read Footer
    SDBFooter footer;
    fseek(file, -(long)sizeof(SDBFooter), SEEK_END);
    fread(&footer, sizeof(SDBFooter), 1, file);
    printf("Checksum: %s\n", footer.checksum);

    free(index);
    fclose(file);
    pthread_mutex_unlock(&sdb_mutex);
    return 0;
}

// Save a key-value pair to the SDB file (fallback for legacy/plain-text operations)
int save_to_sdb(const char *key, const char *value, int expiration) {
    FILE *file = fopen(SDB_FILE, "rb+");
    if (!file) {
        perror("Error opening SDB file");
        return -1;
    }

    SDBEntry entry;
    strncpy(entry.key, key, MAX_KEY_LENGTH - 1);
    entry.key[MAX_KEY_LENGTH - 1] = '\0';
    strncpy(entry.value, value, MAX_VALUE_LENGTH - 1);
    entry.value[MAX_VALUE_LENGTH - 1] = '\0';
    entry.ttl = expiration > 0 ? time(NULL) + expiration : 0;
    entry.type = 0;  // Extendable for types like string, list, etc.

    // Read the SDB file to see if the key exists
    SDBHeader header;
    fread(&header, sizeof(SDBHeader), 1, file);

    SDBIndex *indices = malloc(header.entry_count * sizeof(SDBIndex));
    fread(indices, sizeof(SDBIndex), header.entry_count, file);

    // Check if the key exists
    for (int i = 0; i < header.entry_count; i++) {
        fseek(file, indices[i].offset, SEEK_SET);
        SDBEntry existing_entry;
        fread(&existing_entry, sizeof(SDBEntry), 1, file);

        if (strcmp(existing_entry.key, key) == 0) {
            // Update the existing entry
            fseek(file, indices[i].offset, SEEK_SET);
            fwrite(&entry, sizeof(SDBEntry), 1, file);
            free(indices);
            fclose(file);
            return 0;
        }
    }

    // Add a new entry
    fseek(file, 0, SEEK_END);
    uint64_t offset = ftell(file);
    fwrite(&entry, sizeof(SDBEntry), 1, file);

    // Update the index and header
    SDBIndex new_index = { .offset = offset, .length = sizeof(SDBEntry) };
    indices = realloc(indices, (header.entry_count + 1) * sizeof(SDBIndex));
    indices[header.entry_count] = new_index;

    header.entry_count++;
    fseek(file, 0, SEEK_SET);
    fwrite(&header, sizeof(SDBHeader), 1, file);
    fwrite(indices, sizeof(SDBIndex), header.entry_count, file);

    free(indices);
    fclose(file);
    return 0;
}


int read_from_sdb(const char *key, SDBEntry *entry) {
    FILE *file = fopen(SDB_FILE, "rb");
    if (!file) {
        perror("Error opening SDB file");
        return -1;
    }

    SDBHeader header;
    fread(&header, sizeof(SDBHeader), 1, file);

    SDBIndex *indices = malloc(header.entry_count * sizeof(SDBIndex));
    fread(indices, sizeof(SDBIndex), header.entry_count, file);

    for (int i = 0; i < header.entry_count; i++) {
        fseek(file, indices[i].offset, SEEK_SET);
        fread(entry, sizeof(SDBEntry), 1, file);

        if (strcmp(entry->key, key) == 0) {
            if (entry->ttl > 0 && entry->ttl < time(NULL)) {
                free(indices);
                fclose(file);
                return -1; // Key expired
            }
            free(indices);
            fclose(file);
            return 0;
        }
    }

    free(indices);
    fclose(file);
    return -1; // Key not found
}