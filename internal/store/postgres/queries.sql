-- name: CreateDefaultSchema :exec
CREATE SCHEMA IF NOT EXISTS slack_hubspot;

-- name: CreateRecords :exec
CREATE TABLE IF NOT EXISTS records (
    key TEXT PRIMARY KEY NOT NULL,
    value BYTEA NOT NULL
);

-- name: GetRecord :one
SELECT value FROM records WHERE key = $1;

-- name: PutRecord :exec
INSERT INTO records (key, value) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- name: ScanRecords :many
SELECT key, value FROM records WHERE key >= $1 AND key < $2 ORDER BY key;

-- name: SchemaExists :one
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1);
