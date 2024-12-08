#ifndef SDB_H
#define SDB_H

#include <stdint.h>

// Maximum key/value length
#define MAX_KEY_LENGTH 256
#define MAX_VALUE_LENGTH 1024

typedef struct {
    char key[MAX_KEY_LENGTH];
    char value[MAX_VALUE_LENGTH];
    uint32_t ttl; // Time-to-live in seconds (0 means no expiration)
    uint32_t type; // Type of the entry (e.g., 0 = String, 1 = Hash)
} SDBEntry;

// SDB File structure
typedef struct {
    char version[8];
    char created_at[20];
    uint32_t entry_count;
    char compression[16];
    char encryption[16];
} SDBHeader;

typedef struct {
    uint64_t offset;
    uint32_t length;
} SDBIndex;

typedef struct {
    char checksum[64]; // SHA256 checksum
} SDBFooter;

// Function prototypes
int write_sdb(const char *filename, SDBEntry *entries, int entry_count);
int read_sdb(const char *filename);
int initialize_sdb();
int save_to_sdb(const char *key, const char *value, int expiration);
int read_from_sdb(const char *key, SDBEntry *entry);
#endif
