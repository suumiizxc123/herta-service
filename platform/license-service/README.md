# HERTA license service (Render)

This separate Node service issues and validates license keys for the Docker worker. License keys are shown only once to the admin and stored on the service as SHA-256 hashes. The worker sends a validation request every 60 seconds over HTTPS. A successful check gives it a 75-second local lease; it stops licensed operations when that lease or the license expiration ends. The 15-second margin covers a short network delay. A service outage longer than the lease pauses operations until the next successful check.

## Deploy on Render

1. Push the repository to GitHub.
2. In Render, choose **New → Blueprint**, select this repository, and deploy its root `render.yaml`.
3. Set the prompted `ADMIN_TOKEN` to a fresh random value with at least 32 characters. Generate one in PowerShell with:

   ```powershell
   [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
   ```

4. Deploy `herta-license-service` on the Free plan. This is for testing only: Free services can spin down and have ephemeral storage, so issued and revoked keys can be lost after a restart, sleep, or deploy. Do not use this configuration for production licensing. Move to a paid service with a persistent disk before relying on issued keys.
5. Copy its HTTPS URL, for example `https://herta-license-service.onrender.com`.

The service exposes `/health`, `POST /v1/validate`, and admin-only license issue/list/revoke endpoints. It does not provide an admin web page, so use the PowerShell commands below.

## Issue and manage a license

```powershell
$base = 'https://herta-license-service.onrender.com'
$admin = 'PASTE_YOUR_RENDER_ADMIN_TOKEN'
$expires = '2027-10-23T00:00:00.000Z'
$license = Invoke-RestMethod -Method Post -Uri "$base/v1/licenses" -Headers @{ Authorization = "Bearer $admin" } -ContentType 'application/json' -Body (@{ label = 'HERTA Operations'; expiresAt = $expires } | ConvertTo-Json)
$license | Format-List id,label,expiresAt,licenseKey
```

Save `licenseKey` securely at once; the API never returns it again. List records without keys:

```powershell
Invoke-RestMethod "$base/v1/licenses" -Headers @{ Authorization = "Bearer $admin" }
```

Revoke immediately:

```powershell
Invoke-RestMethod -Method Delete -Uri "$base/v1/licenses/$($license.id)" -Headers @{ Authorization = "Bearer $admin" }
```

Extend a license while keeping the same customer key:

```powershell
$expires = '2028-10-23T00:00:00.000Z'
Invoke-RestMethod -Method Patch -Uri "$base/v1/licenses/$($license.id)" -Headers @{ Authorization = "Bearer $admin" } -ContentType 'application/json' -Body (@{ expiresAt = $expires } | ConvertTo-Json)
```

The Render Admin token can read, issue, and revoke every key. Keep it private and rotate it in Render if it is exposed.

## Connect HERTA Operations

Open **Dashboard → Docker background workers → License**, enter the Render HTTPS URL and the issued key, then select **Save and restart worker**. The worker restarts, validates its license, and reports its status and expiry in the Dashboard. All desktop HERTA data operations and background REST, Kafka, Telegram, and backup workers require a valid license.

Create the license with its intended expiration date. Changing or revoking a license takes effect at the next worker validation, normally within 60 seconds. A device whose worker data volume is recreated gets a new instance ID but can use the same key.

## Local service development

```powershell
$env:ADMIN_TOKEN = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
$env:LICENSE_STORE = (Join-Path $PWD 'data/licenses.json')
npm start
```

Production clients accept only HTTPS license service URLs. Local worker tests may use HTTP on loopback. Never publish real license keys, admin tokens, or database files in Git.
