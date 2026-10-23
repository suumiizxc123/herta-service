# HERTA Operations Desktop 1.9.2

Electron is the control interface. A Docker Compose worker handles REST intake, Kafka consumption, the persistent review inbox, and scheduled NIST ZIP backups independently of the Electron window. Windows requires Docker Desktop with its engine running; install/start Docker Desktop before using **Start workers** on the Overview page.

## Background platform

- The core `herta-worker` container uses `restart: unless-stopped`, persists inbox files in a Docker volume, and exposes its control API only on `127.0.0.1:18127`.
- REST sources can push NIST payloads to `http://127.0.0.1:18127/api/v1/nist`; the desktop shows a temporary bearer token. Files stay in the persistent inbox until imported successfully or discarded.
- Kafka messages are consumed by the worker with manual offset commits after the matching NIST inbox items are imported or discarded.
- Scheduled backups run inside the worker, write ZIP snapshots to the chosen Windows folder, retry a failed run up to three times, and continue when the Electron window closes while Docker remains running. Choose a retention limit in Backup settings; after a successful run, older timestamped HERTA ZIP snapshots are pruned to that limit.
- The Overview page can additionally start a local Kafka broker (`localhost:19092`) or a single-node Elasticsearch instance (`localhost:19200`). These are optional local services; the worker can also connect to an existing Kafka broker.

Electron stores the HERTA access token in an encrypted per-user session file using the operating system encryption provider, never the password. On app startup it verifies the saved token with HERTA and refreshes the worker session; a rejected token is cleared, and signing out clears both saved and worker sessions. If Docker restarts while Electron is closed, the worker can start again but must wait for Electron to reopen and restore the session before authenticated backups can run. Docker Desktop normally starts when a Windows user signs in, so this setup is not a headless Windows Server deployment.

The Kafka broker and Elasticsearch are optional infrastructure. PostgreSQL metadata storage, Redis, MinIO/S3 archive, Debezium CDC, Prometheus/Grafana, and production failover are not included in this local desktop stack. Elasticsearch is a local single-node development service with security disabled and its port bound to loopback; do not expose it to a network.

## Desktop workflows

- Browse/search/group HERTA subjects, inspect records and images, export NIST files, and review profile metadata.
- Select NIST files, preview the batch, and explicitly confirm imports by typing `IMPORT`.
- Fetch NIST envelopes from REST, receive authenticated REST posts, and consume Kafka raw NIST or JSON/Base64 envelopes into the worker inbox.
- Create and restore local NIST snapshots. A snapshot exports HERTA subject records; it is not a physical database backup or failover replica.
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
