const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');

async function freePort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', resolve).once('error', reject));
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

test('Docker worker exposes an authenticated status API and persists REST inbox files', async (t) => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'herta-worker-test-'));
  const port = await freePort();
  const apiToken = 'worker-test-token-12345678901234567890';
  const child = spawn(process.execPath, [path.resolve(__dirname, '../platform/worker/server.js')], {
    env: { ...process.env, PORT: String(port), DATA_DIR: dataDir, BACKUP_DIR: path.join(dataDir, 'backups'), WORKER_API_TOKEN: apiToken },
    stdio: 'ignore',
  });
  t.after(async () => { child.kill(); await fs.rm(dataDir, { recursive: true, force: true }); });

  const base = `http://127.0.0.1:${port}`;
  let health;
  for (let attempt = 0; attempt < 40; attempt += 1) {
    try { health = await fetch(`${base}/health`); if (health.ok) break; } catch { /* Worker has not bound its port yet. */ }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.equal(health?.status, 200);
  const denied = await fetch(`${base}/api/status`);
  assert.equal(denied.status, 401);

  const controlHeaders = { Authorization: `Bearer ${apiToken}`, 'Content-Type': 'application/json' };
  const settingsResponse = await fetch(`${base}/api/backup/settings`, { method: 'POST', headers: controlHeaders, body: JSON.stringify({ enabled: true, cron: '0 2 * * *', retentionCount: 7 }) });
  assert.equal(settingsResponse.status, 200);
  assert.equal((await settingsResponse.json()).retentionCount, 7);
  const status = await (await fetch(`${base}/api/status`, { headers: { Authorization: `Bearer ${apiToken}` } })).json();
  assert.equal(status.backup.retentionCount, 7);
  assert.equal(status.kafka.listening, false);
  assert.equal(status.receiver.running, false);
  assert.equal(status.telegram.configured, false);

  const receiverResponse = await fetch(`${base}/api/receiver/start`, { method: 'POST', headers: controlHeaders, body: '{}' });
  const receiver = await receiverResponse.json();
  assert.equal(receiverResponse.status, 200);

  const intakeResponse = await fetch(`${base}/api/v1/nist`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${receiver.token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: 'SUB001', fileContent: Buffer.from('sample nist').toString('base64') }),
  });
  assert.equal(intakeResponse.status, 202);
  const items = await (await fetch(`${base}/api/inbox`, { headers: { Authorization: `Bearer ${apiToken}` } })).json();
  assert.equal(items.length, 1);
  assert.equal(items[0].name, 'SUB001.nist');

  child.kill();
  await new Promise((resolve) => child.once('exit', resolve));
  const inboxFile = JSON.parse(await fs.readFile(path.join(dataDir, 'inbox.json'), 'utf8'));
  assert.equal(inboxFile.length, 1);
  assert.ok(await fs.readFile(path.join(dataDir, 'inbox', inboxFile[0].file)));
  assert.equal(JSON.parse(await fs.readFile(path.join(dataDir, 'backup-settings.json'), 'utf8')).retentionCount, 7);
});
