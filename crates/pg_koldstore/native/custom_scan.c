#include "custom_scan.h"

#define KOLDSTORE_CUSTOM_SCAN_CALLBACK_COUNT 6

static const char *KOLDSTORE_CALLBACK_NAMES[KOLDSTORE_CUSTOM_SCAN_CALLBACK_COUNT] = {
    "CustomPath",
    "CustomScan",
    "BeginCustomScan",
    "ExecCustomScan",
    "EndCustomScan",
    "RescanCustomScan",
};

static void
koldstore_increment_counter(unsigned int *counter)
{
    if (counter != (void *)0) {
        *counter = *counter + 1;
    }
}

void
koldstore_register_custom_scan(void)
{
    (void)koldstore_custom_scan_callbacks();
}

void
koldstore_custom_path(void *state)
{
    KoldstoreCustomScanCallbackState *scan_state = state;
    if (scan_state != (void *)0) {
        koldstore_increment_counter(&scan_state->custom_path_calls);
    }
}

void
koldstore_custom_scan(void *state)
{
    KoldstoreCustomScanCallbackState *scan_state = state;
    if (scan_state != (void *)0) {
        koldstore_increment_counter(&scan_state->custom_scan_calls);
    }
}

void
koldstore_begin_custom_scan(void *state)
{
    KoldstoreCustomScanCallbackState *scan_state = state;
    if (scan_state != (void *)0) {
        koldstore_increment_counter(&scan_state->begin_custom_scan_calls);
    }
}

void
koldstore_exec_custom_scan(void *state)
{
    KoldstoreCustomScanCallbackState *scan_state = state;
    if (scan_state != (void *)0) {
        koldstore_increment_counter(&scan_state->exec_custom_scan_calls);
    }
}

void
koldstore_end_custom_scan(void *state)
{
    KoldstoreCustomScanCallbackState *scan_state = state;
    if (scan_state != (void *)0) {
        koldstore_increment_counter(&scan_state->end_custom_scan_calls);
    }
}

void
koldstore_rescan_custom_scan(void *state)
{
    KoldstoreCustomScanCallbackState *scan_state = state;
    if (scan_state != (void *)0) {
        koldstore_increment_counter(&scan_state->rescan_custom_scan_calls);
    }
}

const KoldstoreCustomScanCallbacks *
koldstore_custom_scan_callbacks(void)
{
    static const KoldstoreCustomScanCallbacks callbacks = {
        "KoldMergeScan",
        koldstore_custom_path,
        koldstore_custom_scan,
        koldstore_begin_custom_scan,
        koldstore_exec_custom_scan,
        koldstore_end_custom_scan,
        koldstore_rescan_custom_scan,
    };

    return &callbacks;
}

size_t
koldstore_custom_scan_callback_count(void)
{
    return KOLDSTORE_CUSTOM_SCAN_CALLBACK_COUNT;
}

const char *
koldstore_custom_scan_callback_name(size_t index)
{
    if (index >= KOLDSTORE_CUSTOM_SCAN_CALLBACK_COUNT) {
        return (void *)0;
    }

    return KOLDSTORE_CALLBACK_NAMES[index];
}
