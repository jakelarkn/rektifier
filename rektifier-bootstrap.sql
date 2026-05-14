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

-- Hash-only N-PK. Mirrors rektifier.toml.example's `counters` table —
-- the diff layer uses it to confirm parity on non-string PKs.
DROP TABLE IF EXISTS counters;
CREATE TABLE counters (
  data jsonb   NOT NULL,
  id   numeric GENERATED ALWAYS AS ((data#>>'{id,N}')::numeric) STORED PRIMARY KEY
);

-- Hash-only B-PK. The generated column decodes the base64 wire form of
-- DDB `B` attributes into PG `bytea`.
DROP TABLE IF EXISTS blobs;
CREATE TABLE blobs (
  data    jsonb NOT NULL,
  binmark bytea GENERATED ALWAYS AS (decode(data#>>'{binmark,B}', 'base64')) STORED PRIMARY KEY
);

-- Composite S + B sort key.
DROP TABLE IF EXISTS binsorted;
CREATE TABLE binsorted (
  data    jsonb NOT NULL,
  id      text  GENERATED ALWAYS AS (data#>>'{id,S}')                              STORED,
  binmark bytea GENERATED ALWAYS AS (decode(data#>>'{binmark,B}', 'base64'))      STORED,
  PRIMARY KEY (id, binmark)
);

-- Composite S + S. Used by Query parity tests for `begins_with(sk, :v)`
-- and other S-typed sort-key predicates.
DROP TABLE IF EXISTS messages;
CREATE TABLE messages (
  data   jsonb NOT NULL,
  thread text  GENERATED ALWAYS AS (data#>>'{thread,S}') STORED,
  ts     text  GENERATED ALWAYS AS (data#>>'{ts,S}')     STORED,
  PRIMARY KEY (thread, ts)
);
