#include "custom_scan.h"

void
koldstore_register_custom_scan(void)
{
    /*
     * Real PostgreSQL Custom Scan methods are installed from the pgrx build.
     * This shim keeps the FFI boundary explicit while Rust-side planning and
     * execution glue is developed behind the feature-gated extension build.
     */
}

void
koldstore_begin_custom_scan(void)
{
}

void
koldstore_exec_custom_scan(void)
{
}

void
koldstore_end_custom_scan(void)
{
}

void
koldstore_rescan_custom_scan(void)
{
}
