# Slack–HubSpot synchronization bot

A Go service connecting one Slack workspace to one HubSpot account. Mention the bot with an existing ticket in a new private-channel root message. It posts your configured ticket summary and maintains one integration-owned transcript note associated with that ticket. Invite the bot manually; public channels and DMs are intentionally ignored.

```text
@bot 12345
@bot https://app.hubspot.com/contacts/123456/record/0-5/12345
```

All eligible messages, including the root, are retrieved from Slack and rendered in timestamp order. Entries include display names, stable identities, UTC timestamps, message permalinks, readable Slack formatting, and attachment outcomes. Edits and deletions trigger full reconciliation. Bot control messages, this bot's replies, and unsupported system subtypes are excluded.

## Run

Requires Go 1.25 or later, or Docker. Start with [config.example.yaml](config.example.yaml), copying it to the ignored `config.yaml`. Set the actual Slack workspace/bot IDs and HubSpot account ID, and supply credentials through the environment or mounted secret files. Never put credentials in the image or tracked files.

```sh
cp config.example.yaml config.yaml
make build
# For local binaries, set storage.sqlite.path to a writable local path.
bin/migrate -config config.yaml
bin/bot -config config.yaml
```

The migration command reads only storage settings; it does not need Slack or HubSpot credentials. The bot checks storage/schema compatibility and verifies that the configured Slack and HubSpot identities match its credentials at startup. It never migrates the database automatically.

The included Compose configuration uses SQLite, one application replica, a durable named volume, non-root execution, and a read-only root filesystem:

```sh
docker compose build
docker compose run --rm --entrypoint /migrate bot -config /config/config.yaml
docker compose up -d
```

Place a TLS ingress/reverse proxy in front of port 8080. Configure the Slack Events API request URL as `https://YOUR_HOST/slack/events`. Every Slack event and URL-verification request requires a valid signature and a timestamp within five minutes. Only expose `/slack/events` publicly; restrict `/metrics`, `/livez`, and `/readyz` to your monitoring and orchestration network.

## Credentials and scopes

Create a Slack app installed in the one configured workspace. Subscribe to `app_mention` and `message.groups` bot events. The bot needs `app_mentions:read`, `chat:write`, `groups:history`, `groups:read`, `reactions:write`, and `users:read`. Add `files:read` when attachments are enabled. Invite it into each supported private channel.

Some Slack installations require a separate user token with `groups:history` for private-channel `conversations.replies`. Configure `slack.history_token_env` or `slack.history_token_file` in that case; the authorizing user must have access to the relevant channels. Only history reads use this token; bot actions still use the bot token. Both tokens must belong to the configured workspace. See Slack's [thread-retrieval reference](https://docs.slack.dev/reference/methods/conversations.replies/) and [token guide](https://docs.slack.dev/authentication/tokens/).

Use a HubSpot private-app access token for the configured account. Grant `tickets` for ticket/note operations and `files` when copying attachments. The account identity endpoint documents `oauth` access. Optional summary variables require extra read permissions only when used: `crm.objects.owners.read` for Owner, `crm.objects.companies.read` for Company, and `crm.objects.contacts.read` for Contact. Status and Pipeline resolve ticket-pipeline labels. Verify these permissions against your app's available scopes; permissions vary with HubSpot app generation. The implementation uses the [Notes API](https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide), [Files API](https://developers.hubspot.com/docs/api-reference/files-files-v3/guide), and [account information API](https://developers.hubspot.com/docs/api-reference/legacy/account/account-information/guide).

HubSpot URLs are accepted only for the configured account on supported HubSpot application hosts. The access token's account is verified independently at startup.

## Commands

Any channel member can mention the bot in the associated thread:

| Command | Behavior |
| --- | --- |
| `@bot help` | Examples and command help, even without a linked ticket. |
| `@bot resume` | Reactivates a stopped thread and queues a full backfill into its existing note, reusing copied files. Already-active calls do not enqueue work. |
| `@bot status` | Reads the ticket/link, tracking and sync states, last successful text sync, current error, retry time, partial attachment failures, and separately labeled historical error. Does not schedule a sync. |
| `@bot sync` | Queues full reconciliation. A stopped thread remains stopped and suggests `resume`. |
| `@bot untrack` | Durably stops tracking and invalidates pending/running work. Keeps ticket, note, file mappings, remote content, and existing reactions. Repeated calls are harmless. |

Commands execute before ticket detection. Unknown or malformed commands return usage without changing tracking state. Unlinked threads report “not tracked”; `resume` explains how to begin. State-changing commands are deduplicated atomically and ordered by their Slack message timestamp. Older commands cannot undo newer commands. Edited command messages do not execute commands again. Confirmations enter a durable outbox in the same transaction as state changes and are posted after commit.

## Configuration

YAML is strict for service settings. `${ENV}` substitution, `field_env: ENVIRONMENT_VARIABLE`, and `field_file: /run/secrets/file` are supported. Value sources are mutually exclusive. Missing secret sources fail startup without printing values. Only the selected storage driver's settings and secret references are validated.

The mounted `summary_template` supports Slack mrkdwn and **only** these case-sensitive substitutions:

| Variable | Value |
| --- | --- |
| `{{.Company}}` | First associated company's name, chosen by ID order. |
| `{{.Contact}}` | First associated contact's name, chosen by ID order. |
| `{{.CreatedAt}}` | Ticket creation timestamp. |
| `{{.ID}}` | Ticket ID. |
| `{{.Owner}}` | Owner display name, or ID when no name is present. |
| `{{.Pipeline}}` | Ticket pipeline label. |
| `{{.Priority}}` | Ticket priority. |
| `{{.Status}}` | Ticket pipeline stage label. |
| `{{.Subject}}` | Ticket subject. |
| `{{.URL}}` | Account-scoped ticket URL. |
| `{{.UpdatedAt}}` | Ticket modification timestamp. |

There are no executable functions, loops, includes, or general Go template evaluation. Unknown fields or malformed templates fail startup. Values are escaped for Slack; the deployer chooses which properties may be disclosed in the channel. Empty optional properties render as empty strings.

Worker configuration controls concurrency, coalescing delay, lease duration, maximum attempts, polling, exponential backoff with jitter, and shutdown grace. `Retry-After` is respected. Sync configuration controls whether files are copied, the exact MIME allowlist, byte limit (up to 1 GiB), and temporary-file directory. Default successful reactions use `white_check_mark`; an optional failure reaction can be configured.

## Storage and coordination

`domain.Store` is the application boundary; SQL, database handles, and vendor-specific transactions stay in driver packages. A shared coordinator persists small operational records and implements all state/lease semantics uniformly. It stores no conversation archive. SQLite/PostgreSQL queries are generated with sqlc; Spanner uses official native reads/mutations and retried read-write transactions.

| Driver | Configuration | Deployment |
| --- | --- | --- |
| PostgreSQL | `storage.postgres.dsn_env`, `max_open_connections`, optional `max_idle_connections`, `schema` (default `slack_hubspot`) | Multiple stateless replicas. Use a TLS-enabled production DSN and an application-specific database role. |
| Spanner | `project_id`, `instance_id`, `database_id` or full `database` resource; `table_prefix` (default `SlackHubSpot`) | Multiple stateless replicas. Application Default Credentials/Workload Identity; runtime database read/write permissions and separate migration DDL permissions. Provision instance/database first. |
| SQLite | `storage.sqlite.path` | Exactly one replica (`worker.replicas: 1`), durable writable volume. Foreign keys, WAL, busy timeout, and one connection. |

The database stores unique event IDs, thread/ticket/note mappings, tracking revision and command history metadata, sync generations, leases/retries/status, global workspace/file mappings, and notification outbox entries. Each new eligible event increments the requested generation atomically. A claim snapshots that generation and holds an expiring opaque token. A completion cannot consume later generations. Stop/resume changes the revision and invalidates old tokens; every write checks the current token, revision, active state, and expiry. File uploads have independent global leases so one stopped thread cannot cancel work another active thread needs.

Shared production databases are supported through explicit namespaces. PostgreSQL stores its table in `slack_hubspot.records` by default and confines each pooled connection's search path to the configured schema. The migration creates the default schema; a DBA must provision a custom schema before migration. The schema also namespaces future tables. Spanner derives table and future index names from `table_prefix`: the default `SlackHubSpot` prefix plus the `Records` suffix preserves the existing `SlackHubSpotRecords` table. A nonempty prefix must begin with an ASCII letter, contain only ASCII letters, digits, or underscores, and be at most 64 characters to leave room for future suffixes.

Replicas of the same deployment must share the same namespace. Separate deployments, environments, or workspace/account pairs must use different schemas/prefixes (or different databases), since their migrations, leases, deduplication, and file mappings must be isolated. Spanner names are case insensitive, so changing only letter case does not create a separate namespace. Names prevent collisions; database permissions remain the access-control boundary. Changing a namespace selects a different store; it does not migrate data.

The old Spanner `table` setting is rejected. Replace `table: CustomRecords` with `table_prefix: Custom`; the default physical table is unchanged. To use a previous generic store intentionally, set PostgreSQL `schema: public` or Spanner `table_prefix: ""` explicitly (which selects `Records`). Previously configured Spanner names that cannot be expressed as a valid prefix plus `Records` require a planned data migration to the new naming convention; the service does not rename them automatically.

Claim scans use an ordered record-key range and short serializable transactions. This is intentionally simple; at very large thread counts a backend can implement an indexed work queue behind the same domain contract. Deduplication records are retained indefinitely to preserve replay protection; size and back up the database accordingly.

Add a driver by implementing `domain.Store` and registering its factory/configuration validation with `store.Register`, or by implementing the internal serializable backend contract and reusing the coordinator. Supply explicit migrations, deployment documentation, and pass `internal/store/conformance.Run`. Slack, HubSpot, rendering, and synchronization logic do not change.

## Remote content, retries, and limits

**Manual edits to the integration-owned HubSpot note are overwritten on reconciliation.** Each note contains the stable marker `slack-thread:WORKSPACE:CHANNEL:ROOT_TS`. A worker checks associated notes for that marker before creating a note and saves the note mapping before subsequent operations. Unchanged-content rebuilds safely replace the same note. If an in-flight remote request finishes after a lease is invalidated, a later reconciliation repairs its contents. Remote APIs do not participate in the database transaction, so a crash around remote creation has an unavoidable ambiguity window; source markers and file duplicate-detection options minimize duplicates.

Files are downloaded using authenticated Slack access, restricted to Slack-owned HTTPS hosts, checked against configured MIME/size limits, staged in private temporary files, and uploaded to HubSpot with PRIVATE access. Temporary files are removed on return; give `/tmp` enough space for concurrent downloads (at least concurrency × maximum file size). Uploaded IDs are attached through the note's `hs_attachment_ids` property and reused across threads. Stable source filenames and HubSpot's duplicate detection recover interrupted uploads where possible. Exactly-once remote upload cannot be guaranteed across a crash after HubSpot accepts the bytes.

An attachment failure never blocks eligible text or other attachments. Partial status is persisted separately from text sync success. Transient attachment failures schedule another bounded reconciliation; permanent failures are reported and do not upload indefinitely. Permanent thread failures can be retried with `sync`; stopped threads require `resume`. Slack notifications are delivered from a leased outbox with stable `client_msg_id` values; crash-time remote notification delivery is at least once.

HubSpot limits note bodies to 65,536 characters. Oversized complete transcripts fail explicitly as `note_too_large`; they are never silently truncated or split into multiple notes. A deployment that needs longer histories must address this upstream constraint. Slack pagination and rate limits can make large reconciliations take multiple retries; keep leases comfortably longer than an individual network request. Lease heartbeats allow long active work.

## Operations

The image builds static Go binaries in an Alpine builder (`CGO_ENABLED=0`) and runs them in distroless static as UID/GID 65532. The runtime includes CA certificates for outbound HTTPS, timezone data, and a nonroot identity without a shell or package manager; using `scratch` would require maintaining those essentials ourselves. The smaller builder reduces build-image downloads, not the size of the final runtime image. It contains the service `/bot` and the separate `/migrate` maintenance binary. It supports a read-only root filesystem with only the configured SQLite and temporary paths writable. PostgreSQL/Spanner deployments do not need persistent application volumes or session affinity. On SIGTERM, readiness drops, new claims stop, HTTP drains, and workers finish or release work within the configured grace period; an unavailable database can leave leases to expire safely.

`/livez` reports process liveness; `/readyz` checks readiness and storage schema/connectivity. `/metrics` exposes Prometheus counters for received/rejected/duplicate events, retries/permanent failures, recovered leases, note creates/updates, uploaded/skipped/failed attachments, and upstream rate limits; work gauges, a sync-duration histogram (including the existing sums/counts), and Go/process metrics are also available. The official Prometheus Go client owns collection and exposition through a private registry. The strict OpenAPI route delegates encoding to `promhttp`, which negotiates Prometheus text, OpenMetrics text, or protobuf from the scraper’s `Accept` header. Work gauges use a fresh, request-scoped storage snapshot; storage failure returns HTTP 503 instead of stale or zero counts. No workspace, channel, ticket, or other unbounded identifiers become metric labels. JSON logs use `log/slog` and include available event/workspace/channel/thread/message/ticket/worker/lease identifiers. Tokens, authorization headers, file URLs, message text, and ticket content are never logged by default.

Run migrations as a separate deployment step. Version 1 only initializes missing tables and refuses incompatible schema versions; it performs no destructive changes. Back up the database and test restoration before every future irreversible migration. For SQLite, take an online-consistent backup or stop the application before copying its database/WAL files. Do not reuse a storage namespace for a different workspace/HubSpot account pair.

Build a versioned multi-architecture OCI image:

```sh
docker buildx build --build-arg VERSION=0.1.0 \
  --platform linux/amd64,linux/arm64 --tag YOUR_REGISTRY/slackhubspot:0.1.0 \
  --output type=oci,dest=slackhubspot.oci.tar .
```

## Development and verification

[GitHub Actions](.github/workflows/build.yml) runs on pull requests, branch pushes, `v*` tag pushes, and manual dispatch. It checks formatting, runs vet and race tests against SQLite, PostgreSQL, and the Spanner emulator, then builds both Linux architectures. Every successful build offers `slackhubspot-oci-COMMIT_SHA` containing `slackhubspot.oci.tar` and its SHA-256 checksum in the workflow's downloadable artifacts (retained for 14 days).

Pushes to `main` and version tags additionally publish `ghcr.io/OWNER/REPOSITORY` with immutable `sha-COMMIT_SHA` tags. `main` also receives a `main` tag; a release such as `v1.2.3` receives `1.2.3`, `1.2`, and `latest` tags. Use the immutable tag or digest in deployments. Publishing uses the built-in `GITHUB_TOKEN`; allow package writes for the repository's Actions, and grant package access if attaching to an existing GHCR package. Pull-request builds and manual dispatch never publish. Action versions are pinned to verified commit SHAs, and package-write permission is confined to the publishing job. OCI provenance and SBOM attestations are enabled.

The workflow starts once this project is pushed to a GitHub repository; it does not require Slack or HubSpot credentials to build or test.

```sh
make generate   # pinned oapi-codegen strict server and sqlc
make check      # vet and race tests
make build
```

The shared conformance suite always exercises SQLite, including independent connections, concurrent deduplication and claims, lease recovery, generation races, command ordering, durable stop/resume, upload reuse, outbox recovery, and partial-failure status. Set the following to run the identical suite against disposable PostgreSQL and Spanner databases:

```sh
export STORE_TEST_POSTGRES_DSN='postgres://USER:PASSWORD@127.0.0.1:5432/TEST_DATABASE?sslmode=disable'
export SPANNER_EMULATOR_HOST='127.0.0.1:9010'
export STORE_TEST_SPANNER_DATABASE='projects/TEST_PROJECT/instances/TEST_INSTANCE/databases/TEST_DATABASE'
go test -race ./internal/store/...
```

Use only isolated test databases; the suite initializes schemas and writes operational test records. Provision the emulator instance/database before running it. HTTP integration tests use local mock servers; worker tests use real SQLite coordination and fake remote services to exercise complete transcripts, in-flight stops, backfill, interrupted-create recovery, partial attachments, and events arriving during a note write. Live Slack/HubSpot acceptance requires your deployment credentials.
