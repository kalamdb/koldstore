CREATE TABLE iot.telemetry (
  tenant_id text NOT NULL,
  device_id text NOT NULL,
  id bigint PRIMARY KEY,
  ts timestamptz NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  speed double precision NOT NULL,
  temperature double precision NOT NULL,
  battery double precision NOT NULL,
  event_type text NOT NULL
);
