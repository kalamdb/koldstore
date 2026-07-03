#ifndef PG_KOLDSTORE_CUSTOM_SCAN_H
#define PG_KOLDSTORE_CUSTOM_SCAN_H

#include <stddef.h>

typedef struct KoldstoreCustomScanCallbackState
{
    unsigned int custom_path_calls;
    unsigned int custom_scan_calls;
    unsigned int begin_custom_scan_calls;
    unsigned int exec_custom_scan_calls;
    unsigned int end_custom_scan_calls;
    unsigned int rescan_custom_scan_calls;
} KoldstoreCustomScanCallbackState;

typedef void (*KoldstoreCustomScanCallback)(void *state);

typedef struct KoldstoreCustomScanCallbacks
{
    const char *name;
    KoldstoreCustomScanCallback custom_path;
    KoldstoreCustomScanCallback custom_scan;
    KoldstoreCustomScanCallback begin_custom_scan;
    KoldstoreCustomScanCallback exec_custom_scan;
    KoldstoreCustomScanCallback end_custom_scan;
    KoldstoreCustomScanCallback rescan_custom_scan;
} KoldstoreCustomScanCallbacks;

void koldstore_register_custom_scan(void);
const KoldstoreCustomScanCallbacks *koldstore_custom_scan_callbacks(void);
size_t koldstore_custom_scan_callback_count(void);
const char *koldstore_custom_scan_callback_name(size_t index);
void koldstore_custom_path(void *state);
void koldstore_custom_scan(void *state);
void koldstore_begin_custom_scan(void *state);
void koldstore_exec_custom_scan(void *state);
void koldstore_end_custom_scan(void *state);
void koldstore_rescan_custom_scan(void *state);

#endif
