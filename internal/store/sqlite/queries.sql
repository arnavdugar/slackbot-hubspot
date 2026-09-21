-- name: GetRecord :one
SELECT value FROM records WHERE key = ?;

-- name: PutRecord :exec
INSERT INTO records (key, value) VALUES (?, ?)
ON CONFLICT (key) DO UPDATE SET value = excluded.value;

-- name: ScanRecords :many
SELECT key, value FROM records WHERE key >= ? AND key < ? ORDER BY key;
