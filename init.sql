CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
DROP TRIGGER IF EXISTS ts_insert_blocker ON ops.inventory;
DROP FUNCTION IF EXISTS _timescaledb_functions.insert_blocker();
