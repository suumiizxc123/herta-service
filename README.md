# HERTA Operations Desktop 1.6.0

An Electron desktop application for the everyday HERTA BioCom workflows in the supplied PowerShell scripts. It runs locally on the operator's computer and connects to the existing HERTA API. It does not require a browser service, Python, Docker, or a separate user database.

## Included workflows

- Sign in to HERTA and keep the access token in memory for the current app session.
- Browse and search subjects, export selected `.nist` files (up to 256 MiB per package), and save a ZIP with a CSV report.
- Choose `.nist` files through a native file picker, preview the selection, and explicitly confirm imports by typing `IMPORT`.
- Inspect profile length and JSON/Base64-like indicators without showing profile contents.
- Create and restore local NIST snapshot ZIPs with a manifest and export report.
- Schedule automatic NIST snapshot ZIPs into a selected folder with a five-field cron expression. The desktop must remain open and signed in to HERTA at the scheduled time.
- Preview JPEG/PNG images found in subject data or in the subject's exported NIST record.

Automatic backup cron example: `0 2 * * *` runs daily at 02:00 local time. Use `*/15 * * * *` for every 15 minutes while testing. Automatic backups run inside the desktop app, so they pause when the app is closed or signed out.
- Check HERTA API responsiveness and review a local audit history of desktop operations.
- Inspect subject metadata and reveal a subject's profile on demand.
- Fetch NIST file envelopes from a configured REST JSON endpoint, receive authenticated NIST POSTs on a loopback-only REST listener, and connect to a Kafka topic to receive raw NIST bytes or JSON Base64 file envelopes.
- Test Kafka broker connectivity and create topics on an existing cluster. Topic creation does not provision a Kafka broker.

The app never saves the HERTA password or token to disk. It uses Electron's isolated renderer and a narrow preload API; HERTA requests and local file access run in the main process. Certificate verification is off by default for HERTA installations using a private or self-signed certificate. Turn on **Verify server certificate** at sign-in when the HERTA server has a publicly trusted certificate. Activity history is stored locally and excludes biometric contents, subject identifiers, credentials, and tokens.

Kafka, Debezium, databases, Redis, Elasticsearch, object storage, Prometheus/Grafana, and Keycloak are server-side infrastructure. The desktop app reports that boundary; it does not install or pretend to manage those services without corresponding HERTA management APIs.

REST source responses use `{ "files": [{ "code": "SUB001", "fileContent": "BASE64_NIST" }] }` (or a single file object). The local receiver listens only on `127.0.0.1`, uses a random in-memory bearer token, and accepts `POST /api/v1/nist`. Kafka SASL credentials and REST tokens stay in memory. Incoming data is held in the desktop inbox until an operator previews and confirms the NIST import. Kafka offsets are committed only after successful imports; invalid records remain pending until explicitly discarded.

## Run from source

Install Node.js 22 or newer and pnpm 11, then run:

```powershell
pnpm install
pnpm dev
```

`pnpm dev` runs the Vite renderer and Electron desktop window. `pnpm start` builds the local renderer and opens the desktop app. The default API address is `https://localhost:8005`; set `HERTA_BASE_URL` before launching to use another address.

## Build a desktop installer

On Windows, run `pnpm run dist:win`. The installer and portable executable are written to `release/`. On macOS use `pnpm run dist:mac`; on Linux use `pnpm run dist:linux`. Build each platform on that platform.
