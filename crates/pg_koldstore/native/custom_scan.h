#ifndef PG_KOLDSTORE_CUSTOM_SCAN_H
#define PG_KOLDSTORE_CUSTOM_SCAN_H

void koldstore_register_custom_scan(void);
void koldstore_begin_custom_scan(void);
void koldstore_exec_custom_scan(void);
void koldstore_end_custom_scan(void);
void koldstore_rescan_custom_scan(void);

#endif
