-- Preinstall koldstore for the published try-it image.
-- pg_cron is packaged in the image but not enabled by default.
-- koldstore must already be in shared_preload_libraries (image default).
CREATE EXTENSION IF NOT EXISTS koldstore;
