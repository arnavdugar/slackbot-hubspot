# Storage

The domain coordinator persists individual event, thread, notification, and
workspace-wide file records through serializable backend transactions. It does
not persist conversation transcripts. SQLite and PostgreSQL statements are
generated from checked-in SQL with sqlc. Spanner uses the official client's
native reads and buffered mutations; its read-write transaction callback handles
aborted transactions. PostgreSQL retries serialization and deadlock failures.
Worker remote requests happen outside these short transactions, so unrelated
threads can synchronize concurrently.

`store.Open` accepts the selected driver name and nested driver configuration:

```go
db, err := store.Open(ctx, store.Config{
    "driver": "sqlite",
    "sqlite": map[string]any{"path": "/data/slack-hubspot.db"},
})
```

Only the selected driver's configuration is validated. PostgreSQL accepts `dsn`,
`max_idle_connections` (default 5), `max_open_connections` (default 20), and
`schema` (default `slack_hubspot`). Every pooled connection uses only that
application schema for its search path; it never falls back to `public`.
Schema names must be lowercase SQL identifiers of at most 63 characters. Names
beginning with `pg_` and `information_schema` are rejected.

Spanner accepts `project_id`, `instance_id`, and `database_id`, or a complete
`database` resource name. Its `table_prefix` defaults to `SlackHubSpot`; the
current table appends `Records`, preserving the default `SlackHubSpotRecords`
name. Future tables and indexes use the same prefix. A nonempty prefix must
begin with an ASCII letter and contain only ASCII letters, digits, or
underscores. Its 64-character limit reserves another 64 characters for future
suffixes. Explicit `table_prefix: ""` selects the legacy unprefixed `Records`
table. Spanner names are case insensitive, so a change of letter case does not
create a separate namespace. `emulator_host` or `SPANNER_EMULATOR_HOST` selects
an emulator. Production authentication uses Application Default Credentials.

Separate PostgreSQL schemas or Spanner prefixes isolate every deployment's
events, thread mappings, commands, leases, file mappings, outbox, and schema
version. Use a distinct namespace for each Slack–HubSpot account pairing that
shares a physical database; replicas of one deployment share its namespace.
PostgreSQL's schema also namespaces future tables. Namespaces organize data;
database IAM and grants still determine access between deployments.

`Open` does not migrate. The migration command explicitly invokes `Migrate`;
normal startup invokes `Check`, which rejects an absent or incompatible schema.
The initial embedded migration is additive and idempotent. PostgreSQL migrations
create the default `slack_hubspot` schema if absent using a static sqlc query.
A custom schema must be provisioned by a database administrator first; migration
fails clearly if it is missing. The migration role needs schema creation
permission only when creating the default schema, and table creation permission
within the selected schema. The runtime role needs schema usage and table data
access. Custom identifiers are validated and quoted through pgx connection
configuration; none are interpolated into PostgreSQL queries.

A Spanner instance and database must already exist. Its migration creates only
the application table derived from the prefix and requires database DDL
permissions; runtime credentials only need database data access. Back up
databases before applying future irreversible migrations.

The application defaults do not discover, rename, import, or mutate a previous
generic `public.records` or `Records` table. For an existing deployment that
used those names, explicitly set PostgreSQL `schema: public` or Spanner
`table_prefix: ""` to continue using its original state. The old Spanner `table`
setting is rejected: replace `table: CustomRecords` with `table_prefix: Custom`.
The default physical table remains `SlackHubSpotRecords`. Previously configured
names that cannot be expressed as a valid prefix plus `Records` require a
separately planned data migration to the new naming convention. Moving existing
state into the application defaults also requires a planned migration; simply
changing the namespace selects a different store and does not migrate data.

SQLite enables foreign keys, WAL, a five-second busy timeout, immediate write
transactions, and one database connection. Use one application replica and a
persistent volume. PostgreSQL and Spanner support independent application
replicas without local persistent volumes.

## Conformance

SQLite always runs in `go test ./internal/store/...`. The same suite can also
exercise real PostgreSQL and Spanner (including the official emulator):

```sh
export STORE_TEST_POSTGRES_DSN="${TEST_DATABASE_URL}"
export STORE_TEST_POSTGRES_OTHER_SCHEMA="public"
export SPANNER_EMULATOR_HOST="127.0.0.1:9010"
export STORE_TEST_SPANNER_DATABASE="projects/test/instances/test/databases/test"
go test ./internal/store/... -count=1
```

Use dedicated test databases. The suite initializes their schema and isolates
each scenario with a random record prefix. Test records remain until the test
database is removed. It covers concurrent event deduplication, thread and global
file lease exclusivity, expiry and recovery, generations, stale commands,
stop/resume invalidation, preservation and reuse of mappings, partial attachment
retry, permanent failures, read-only status/help, durable notification outbox,
and process restart behavior. Namespace tests use identical external IDs in
the same PostgreSQL database's default schema and a second preexisting schema
(`public` by default, configurable with `STORE_TEST_POSTGRES_OTHER_SCHEMA`), and
in the same Spanner database's default table and
`SlackHubSpotIsolationTestRecords`. They verify that events, commands,
thread/note/file mappings, work leases, and notifications remain independent.
Additional checks reject invalid names and prove that a missing custom
PostgreSQL schema cannot fall back to `public`.

To add a driver, implement `domain.Store` and register its factory with
`store.Register`, including configuration validation and migration support.
Alternatively, implement `backend.Database` and use `store.New` to reuse the
coordinator. Run `conformance.Run` against independent connections sharing each
test's isolated dataset, and document the deployment requirements.

The initial portable backend intentionally uses prefix scans when claiming
work. This keeps behavior consistent across drivers; large installations may
need driver-specific indexed eligibility queries or a native Store
implementation while retaining the same conformance suite.
