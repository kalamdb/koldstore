-- Preinstall koldstore for the published try-it image.
-- pg_cron is packaged in the image but not enabled by default.
CREATE EXTENSION IF NOT EXISTS koldstore;
