# HERTA Operations Desktop 1.9.20

The Import workspace includes Files, REST API and Kafka sources. Manage saved source connections, fetch REST data, select received items and preview or import a batch on one screen. Run **Dashboard → Apply / start** after updating the desktop app to build the worker with connector-specific queue metadata and duplicate checks for manual REST fetches.

Electron is the control interface. A Docker Compose worker handles REST intake, Kafka consumption, the persistent review inbox, and scheduled NIST ZIP backups independently of the Electron window. Windows requires Docker Desktop with its engine running; install/start Docker Desktop before using **Start workers** on the Dashboard page.

## Background platform

- The core `herta-worker` container uses `restart: unless-stopped`, persists inbox files in a Docker volume, and exposes its control API only on `127.0.0.1:18127`.
- REST sources can push NIST payloads to `http://127.0.0.1:18127/api/v1/nist`; the desktop shows a temporary bearer token. Files stay in the persistent inbox until imported successfully or discarded.
- The Connectors page stores named REST pull, REST receiver, Kafka, and PostgreSQL backup definitions. Active REST polling, REST intake, and Kafka consumption run in the Docker worker, independently of the Electron window. Connector changes are persisted in the worker volume and resumed after a worker restart.
- Kafka messages are consumed by the worker with manual offset commits after the matching NIST inbox items are imported or discarded.
- Desktop connector settings are encrypted in the desktop profile. The worker also stores its active connector configuration and HERTA access token in its persistent Docker volume with owner-only file permissions so scheduled work can continue while Electron is closed. Signing out clears the worker's HERTA token. Protect the Docker volume and selected backup folder as sensitive data.
- REST pull connectors poll the configured endpoint every 60 seconds, send any custom request headers, and de-duplicate repeated NIST files. Kafka messages are consumed by the worker with manual offset commits after the matching NIST inbox items are imported or discarded.
- Telegram can be configured with a bot token and chat ID; the worker sends a notification when it receives NIST files from REST or Kafka and supports a test message from the Telegram page.
- Scheduled backups run inside the worker, write to the chosen Windows folder, retry a failed run up to three times, and continue when the Electron window closes while Docker remains running. HERTA snapshots contain exported NIST subject files in ZIP format. Active PostgreSQL backup connectors run `pg_dump` in custom format and save a complete logical dump of the configured database as `.dump`; they are included in both scheduled and manual backups. Database dumps are not physical cluster images and do not include cluster-wide roles or tablespaces. Choose a retention limit in Backup settings; old timestamped snapshots and database dumps are pruned to that limit.
- The Dashboard can additionally start a local Kafka broker (`localhost:19092`) or a single-node Elasticsearch instance (`localhost:19200`). These are optional local services; the worker can also connect to an existing Kafka broker.

Electron stores the HERTA access token in an encrypted per-user session file using the operating system encryption provider, never the password. The worker keeps its own token copy in its persistent Docker volume so NIST backups can continue after a worker restart while Electron is closed. Signing out clears both copies. Docker Desktop normally starts when a Windows user signs in, so this setup is not a headless Windows Server deployment.

The Kafka broker and Elasticsearch are optional infrastructure. PostgreSQL metadata storage, Redis, MinIO/S3 archive, Debezium CDC, Prometheus/Grafana, and production failover are not included in this local desktop stack. Elasticsearch is a local single-node development service with security disabled and its port bound to loopback; do not expose it to a network.

## Desktop workflows

- Browse/search/group HERTA subjects, inspect records and images, export NIST files, and review profile metadata.
- Select NIST files, preview the batch, and explicitly confirm imports by typing `IMPORT`.
- Fetch NIST envelopes from REST, receive authenticated REST posts, and consume Kafka raw NIST or JSON/Base64 envelopes into the worker inbox. Telegram credentials are synced to the worker when it is started from the desktop app.
- Create and restore local NIST snapshots. Add and activate a PostgreSQL connector to back up a full configured database alongside NIST subject exports.
- Schedule ZIP snapshots with a five-field cron expression. `0 2 * * *` runs daily at 02:00 local time; `*/15 * * * *` is useful for testing.

## Run from source

Install Node.js 22 or newer, pnpm 11, and Docker Desktop. Then run:

```powershell
pnpm install
pnpm dev
```

The default HERTA API address is `https://localhost:8005`; set `HERTA_BASE_URL` before launching to use another address. Start the worker stack from Overview. The local Compose definition is `platform/compose.yaml`.

## Build a Windows installer

Run `pnpm run dist:win`. The installer and portable executable are written to `release/`. Docker Desktop is installed separately; the HERTA installer does not install Docker or start containers automatically.
