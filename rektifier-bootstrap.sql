-- Matching PG schema for rektifier.toml.example.
--
-- Apply with:  just psql -f rektifier-bootstrap.sql
-- Or:          docker compose exec -T postgres psql -U rektifier rektifier < rektifier-bootstrap.sql
--
-- Rektifier introspects information_schema at boot and refuses to start
-- if the actual schema doesn't match what the config declares. Each pk/sk
-- column must be `GENERATED ALWAYS AS (...) STORED`. The expression form
-- is operator's choice — these examples use `#>>` for clarity.

DROP TABLE IF EXISTS users;
CREATE TABLE users (
  data jsonb NOT NULL,
  id   text  GENERATED ALWAYS AS (data#>>'{id,S}') STORED PRIMARY KEY
);

DROP TABLE IF EXISTS device_events;
CREATE TABLE device_events (
  doc       jsonb NOT NULL,
  device_id text    GENERATED ALWAYS AS (doc#>>'{device_id,S}')           STORED,
  ts        numeric GENERATED ALWAYS AS ((doc#>>'{ts,N}')::numeric)       STORED,
  PRIMARY KEY (device_id, ts)
);

-- Uncomment if you enable the `blobs` table in rektifier.toml:
-- DROP TABLE IF EXISTS blobs;
-- CREATE TABLE blobs (
--   meta jsonb NOT NULL,
--   hash bytea GENERATED ALWAYS AS (decode(meta#>>'{hash,B}', 'base64')) STORED PRIMARY KEY
-- );
