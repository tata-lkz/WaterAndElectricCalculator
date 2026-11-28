BEGIN;

CREATE TABLE IF NOT EXISTS log_keys (
  id serial PRIMARY KEY,
  name text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS log_readings (
  kid int NOT NULL REFERENCES log_keys(id) ON DELETE CASCADE,
  t timestamptz NOT NULL,
  v_avg double precision NOT NULL,
  v_min double precision NOT NULL,
  v_max double precision NOT NULL,
  v_last double precision NOT NULL,
  PRIMARY KEY (kid, t)
);

ALTER TABLE log_readings
  ADD COLUMN IF NOT EXISTS v_n int NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS log_readings_t ON log_readings(t);

CREATE TABLE IF NOT EXISTS log_hourly (
  kid int NOT NULL REFERENCES log_keys(id) ON DELETE CASCADE,
  t timestamptz NOT NULL,
  v_avg double precision NOT NULL,
  v_min double precision NOT NULL,
  v_max double precision NOT NULL,
  v_last double precision NOT NULL,
  v_n int NOT NULL DEFAULT 1,
  mins int NOT NULL DEFAULT 1,
  PRIMARY KEY (kid, t)
);

CREATE INDEX IF NOT EXISTS log_hourly_t ON log_hourly(t);

CREATE TABLE IF NOT EXISTS mqtt_messages (
  id bigserial PRIMARY KEY,
  received_at timestamptz NOT NULL,
  topic text NOT NULL,
  payload bytea NOT NULL,
  qos smallint,
  retain boolean,
  dup boolean,
  message_id integer
);

CREATE INDEX IF NOT EXISTS mqtt_messages_received_at ON mqtt_messages(received_at);
CREATE INDEX IF NOT EXISTS mqtt_messages_topic_received_at ON mqtt_messages(topic, received_at);

CREATE OR REPLACE VIEW log_all AS
  SELECT kid, t, v_avg, v_min, v_max, v_last, v_n, 1 AS mins
  FROM log_readings
  UNION ALL
  SELECT kid, t, v_avg, v_min, v_max, v_last, v_n, mins
  FROM log_hourly;

COMMIT;
