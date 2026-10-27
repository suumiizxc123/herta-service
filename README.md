# HERTA Operations Desktop 1.9.24

Монгол хэрэглэгчийн гарын авлага: [Markdown](docs/HERTA_Operations_1.9.22_Guide_MN.md).

Connectors has exactly two types: **REST connector** and **Kafka connector**, with separate add forms and saved connection controls. PostgreSQL full database backup configuration is managed in **Backup & restore**. Existing saved configurations are preserved.

The Import workspace includes Files, REST API and Kafka sources. Manage saved source connections, fetch REST data, select received items and preview or import a batch on one screen. Run **Dashboard → Apply / start** after updating the desktop app to build the worker with connector-specific queue metadata and duplicate checks for manual REST fetches.

Electron is the control interface. A Docker Compose worker handles REST intake, Kafka consumption, the persistent review inbox, and scheduled NIST ZIP backups independently of the Electron window. Windows requires Docker Desktop with its engine running; install/start Docker Desktop before using **Start workers** on the Dashboard page.

## Background platform

- The core `herta-worker` container uses `restart: unless-stopped`, persists inbox files in a Docker volume, and exposes its control API only on `127.0.0.1:18127`.
- REST sources can push NIST payloads to `http://127.0.0.1:18127/api/v1/nist`; no Authorization header or receiver token is required. The worker imports files directly into HERTA; failed imports stay in the persistent inbox for review. The worker control API still requires its own API token.
- The Connectors page stores named REST pull, REST receiver and Kafka definitions; PostgreSQL backup definitions are managed in Backup & restore. Active REST polling, REST intake, and Kafka consumption run in the Docker worker, independently of the Electron window. Connector changes are persisted in the worker volume and resumed after a worker restart.
- Kafka messages are consumed by the worker with manual offset commits after the matching NIST inbox items are imported or discarded.
- Desktop connector settings are encrypted in the desktop profile. The worker also stores its active connector configuration and HERTA access token in its persistent Docker volume with owner-only file permissions so scheduled work can continue while Electron is closed. Signing out clears the worker's HERTA token. Protect the Docker volume and selected backup folder as sensitive data.
- REST pull connectors poll the configured endpoint every 60 seconds, require no authentication, de-duplicate repeated NIST files, and import new files directly into the selected HERTA group. Test validates without importing; Fetch now fetches and imports. Kafka messages are consumed by the worker with manual offset commits after the matching NIST inbox items are imported or discarded.
- Telegram can be configured with a bot token and chat ID; the worker sends a notification when it receives NIST files from REST or Kafka and supports a test message from the Telegram page.
- Scheduled backups run inside the worker, write to the chosen Windows folder, retry a failed run up to three times, and continue when the Electron window closes while Docker remains running. HERTA snapshots contain exported NIST subject files in ZIP format. Active PostgreSQL backup connectors run `pg_dump` in custom format and save a complete logical dump of the configured database as `.dump`; they are included in both scheduled and manual backups. Database dumps are not physical cluster images and do not include cluster-wide roles or tablespaces. Choose a retention limit in Backup settings; old timestamped snapshots and database dumps are pruned to that limit.
- The Dashboard can additionally start a local Kafka broker (`localhost:19092`) or a single-node Elasticsearch instance (`localhost:19200`). These are optional local services; the worker can also connect to an existing Kafka broker.

Electron stores HERTA credentials using operating system safeStorage encryption. The worker encrypts saved credentials with AES-256-GCM using a key derived from its worker API token; its access token remains in the session file with owner-only permissions. The worker renews the HERTA token every hour through the login API. Desktop HERTA operations share the authenticated worker session when available. On HTTP 401/403, the operation signs in again and retries once. Concurrent renewal requests share one login. Signing out clears saved HERTA credentials and sessions from both the app and worker. Sign in again after changing the HERTA password or worker API token. Docker Desktop normally starts when a Windows user signs in, so this setup is not a headless Windows Server deployment.

The Kafka broker and Elasticsearch are optional infrastructure. PostgreSQL metadata storage, Redis, MinIO/S3 archive, Debezium CDC, Prometheus/Grafana, and production failover are not included in this local desktop stack. Elasticsearch is a local single-node development service with security disabled and its port bound to loopback; do not expose it to a network.

## Desktop workflows

- Browse/search/group HERTA subjects, inspect records and images, export NIST files, and review profile metadata.
- Select NIST files, preview the batch, and explicitly confirm imports by typing `IMPORT`.
- Fetch NIST envelopes from REST or receive unauthenticated REST posts for direct HERTA import. Kafka raw NIST or JSON/Base64 messages enter the worker inbox for manual review. Telegram credentials are synced to the worker when it is started from the desktop app.
- Create and restore local NIST snapshots. Add and activate a PostgreSQL connector to back up a full configured database alongside NIST subject exports.
- Schedule ZIP snapshots with a five-field cron expression. `0 2 * * *` runs daily at 02:00 local time; `*/15 * * * *` is useful for testing.

## Run from source

Install Node.js 22 or newer, pnpm 11, and Docker Desktop. Then run:

```powershell
pnpm install
pnpm dev
```

The default HERTA API address is `https://localhost:8005`; set `HERTA_BASE_URL` before launching to use another address. Start the worker stack from Dashboard. The local Compose definition is `platform/compose.yaml`.

## Build a Windows installer

Run `pnpm run dist:win`. The installer and portable executable are written to `release/`. Docker Desktop is installed separately; the HERTA installer does not install Docker or start containers automatically.

## Updating to 1.9.22

Install the latest executable, sign in to HERTA again once to enable hourly renewal, then run **Dashboard → Apply / start** to rebuild the worker. Closing Electron does not stop jobs; the computer must remain awake and Docker must keep running.

REST connector settings include the HERTA destination group (default `0`). Send a JSON envelope such as `{"fileName":"subject.nist","fileContent":"BASE64_NIST_BYTES"}` to the active receiver. Raw bytes are also accepted with `Content-Type: application/octet-stream` and optional `X-File-Name`. Arbitrary subject JSON is not converted into NIST.

Receiver responses include `accepted`, `imported`, and per-file `results`. HTTP 200 means all import requests succeeded; HTTP 202 means some files remain queued; HTTP 409 means the receiver is inactive. Verify resulting subjects when validating a new NIST source. The endpoint is published only on localhost.

## Verification and artifacts

Version 1.9.22 passed 22 automated tests and REST/Kafka UI smoke checks, including mock HERTA direct import, token renewal, encrypted credentials, and persistent queues. Live Docker/HERTA/Kafka deployment validation remains environment-specific. Run `pnpm test` for automated tests.

Only the latest local release is retained. `release/` is ignored by Git; source and documentation are pushed to the repository, while EXE files remain local.
