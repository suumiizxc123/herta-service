const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const http = require('node:http');

function startWorker(port, dataDir, apiToken) {
  return spawn(process.execPath, [path.resolve(__dirname, '../platform/worker/server.js')], {
    env: { ...process.env, PORT: String(port), DATA_DIR: dataDir, BACKUP_DIR: path.join(dataDir, 'backups'), WORKER_API_TOKEN: apiToken },
    stdio: 'ignore',
  });
}

async function waitForWorker(base) {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    try { const health = await fetch(`${base}/health`); if (health.ok) return; } catch { /* Worker has not bound its port yet. */ }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('Worker did not become healthy.');
}

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
  const telegramReset = await fetch(`${base}/api/telegram/config`, { method: 'POST', headers: controlHeaders, body: JSON.stringify({ botToken: '', chatId: '' }) });
  assert.equal(telegramReset.status, 200);
  assert.equal((await telegramReset.json()).connected, false);
  const invalidTelegramLookup = await fetch(`${base}/api/telegram/chats`, { method: 'POST', headers: controlHeaders, body: JSON.stringify({ botToken: 'not-a-bot-token' }) });
  assert.equal(invalidTelegramLookup.status, 400);
  assert.match((await invalidTelegramLookup.json()).error, /valid Telegram bot token/i);

  const receiverResponse = await fetch(`${base}/api/receiver/start`, { method: 'POST', headers: controlHeaders, body: JSON.stringify({ connectorId: 'receiver-test' }) });
  const receiver = await receiverResponse.json();
  assert.equal(receiverResponse.status, 200);
  assert.equal(receiver.connectorId, 'receiver-test');

  const intakeResponse = await fetch(`${base}/api/v1/nist`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${receiver.token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: 'SUB001', fileContent: Buffer.from('sample nist').toString('base64') }),
  });
  assert.equal(intakeResponse.status, 202);
  const items = await (await fetch(`${base}/api/inbox`, { headers: { Authorization: `Bearer ${apiToken}` } })).json();
  assert.equal(items.length, 1);
  assert.equal(items[0].name, 'SUB001.nist');
  assert.equal(items[0].sourceType, 'rest');
  assert.equal(items[0].connectorId, 'receiver-test');

  child.kill();
  await new Promise((resolve) => child.once('exit', resolve));
  const inboxFile = JSON.parse(await fs.readFile(path.join(dataDir, 'inbox.json'), 'utf8'));
  assert.equal(inboxFile.length, 1);
  assert.ok(await fs.readFile(path.join(dataDir, 'inbox', inboxFile[0].file)));
  assert.equal(JSON.parse(await fs.readFile(path.join(dataDir, 'backup-settings.json'), 'utf8')).retentionCount, 7);
});

test('worker-owned connector and HERTA session survive worker restarts', async (t) => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'herta-worker-restart-test-'));
  const port = await freePort();
  const apiToken = 'worker-restart-test-token-1234567890';
  const base = `http://127.0.0.1:${port}`;
  const controlHeaders = { Authorization: `Bearer ${apiToken}`, 'Content-Type': 'application/json' };
  let child = startWorker(port, dataDir, apiToken);
  t.after(async () => { child.kill(); await fs.rm(dataDir, { recursive: true, force: true }); });
  await waitForWorker(base);

  const sessionResponse = await fetch(`${base}/api/session`, { method: 'POST', headers: controlHeaders, body: JSON.stringify({ baseUrl: 'http://example.test', token: 'worker-session-token', verifyTls: false }) });
  assert.equal(sessionResponse.status, 200);
  const saved = await fetch(`${base}/api/connectors`, { method: 'PUT', headers: controlHeaders, body: JSON.stringify([
    { id: 'rest-poll', name: 'REST pull', type: 'rest', mode: 'pull', active: true, config: { url: 'http://127.0.0.1:1/nist', pollEverySeconds: 15 } },
    { id: 'rest-receiver', name: 'REST receiver', type: 'rest', mode: 'receiver', active: true, config: {} },
  ]) });
  assert.equal(saved.status, 200);
  const savedConnectors = JSON.parse(await fs.readFile(path.join(dataDir, 'connectors.json'), 'utf8'));
  assert.equal(savedConnectors[0].active, true);

  child.kill();
  await new Promise((resolve) => child.once('exit', resolve));
  child = startWorker(port, dataDir, apiToken);
  await waitForWorker(base);
  const status = await (await fetch(`${base}/api/status`, { headers: controlHeaders })).json();
  assert.equal(status.authenticated, true);
  assert.equal(status.connectors[0].id, 'rest-poll');
  assert.equal(status.connectors[0].active, true);
  assert.equal(status.receiver.connectorId, 'rest-receiver');
  const received = await fetch(`${base}/api/v1/nist`, { method: 'POST', headers: { Authorization: `Bearer ${status.receiver.token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ code: 'SUB002', fileContent: Buffer.from('receiver nist').toString('base64') }) });
  assert.equal(received.status, 202);
  const receivedBody = await received.json();
  const updatedStatus = await (await fetch(`${base}/api/status`, { headers: controlHeaders })).json();
  assert.equal(updatedStatus.connectors.find((item) => item.id === 'rest-receiver').received, 1);
  assert.ok(updatedStatus.connectors.find((item) => item.id === 'rest-receiver').lastRunAt);
  let queue = await (await fetch(`${base}/api/queue/history`, { headers: controlHeaders })).json();
  assert.ok(queue.some((item) => item.outcome === 'received' && item.source === 'REST receiver'));
  const imported = await fetch(`${base}/api/inbox/${encodeURIComponent(receivedBody.ids[0])}/complete`, { method: 'POST', headers: controlHeaders, body: '{}' });
  assert.equal(imported.status, 200);
  queue = await (await fetch(`${base}/api/queue/history`, { headers: controlHeaders })).json();
  assert.ok(queue.some((item) => item.outcome === 'imported'));
  const logs = await (await fetch(`${base}/api/connectors/logs`, { headers: controlHeaders })).json();
  assert.ok(logs.some((item) => item.connectorId === 'rest-receiver' && item.event === 'Files received'));

  const logout = await fetch(`${base}/api/session`, { method: 'DELETE', headers: controlHeaders });
  assert.equal(logout.status, 200);
  await assert.rejects(fs.access(path.join(dataDir, 'herta-session.json')));
});

test('REST connector test validates a NIST batch without enqueueing it', async (t) => {
  const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'herta-worker-rest-test-'));
  const workerPort = await freePort();
  const restPort = await freePort();
  const apiToken = 'worker-rest-test-token-1234567890';
  const sample = Buffer.from('sample ANSI-NIST').toString('base64');
  const restServer = http.createServer((_request, response) => {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ files: [{ fileName: 'sample.nist', fileContent: sample }] }));
  });
  await new Promise((resolve, reject) => restServer.listen(restPort, '127.0.0.1', resolve).once('error', reject));
  const child = startWorker(workerPort, dataDir, apiToken);
  t.after(async () => {
    child.kill();
    await new Promise((resolve) => restServer.close(resolve));
    await fs.rm(dataDir, { recursive: true, force: true });
  });
  const base = `http://127.0.0.1:${workerPort}`;
  await waitForWorker(base);
  const headers = { Authorization: `Bearer ${apiToken}`, 'Content-Type': 'application/json' };
  const saved = await fetch(`${base}/api/connectors`, { method: 'PUT', headers, body: JSON.stringify([{ id: 'rest-check', name: 'REST check', type: 'rest', mode: 'pull', active: false, config: { url: `http://127.0.0.1:${restPort}/nist`, verifyTls: true } }]) });
  assert.equal(saved.status, 200);
  const tested = await fetch(`${base}/api/connectors/test`, { method: 'POST', headers, body: JSON.stringify({ connectorId: 'rest-check' }) });
  assert.equal(tested.status, 200);
  const result = await tested.json();
  assert.equal(result.ok, true);
  assert.equal(result.count, 1);
  assert.equal(result.bytes, Buffer.from('sample ANSI-NIST').length);
  assert.deepEqual(await (await fetch(`${base}/api/inbox`, { headers })).json(), []);
  const fetched = await fetch(`${base}/api/rest/fetch`, { method: 'POST', headers, body: JSON.stringify({ connectorId: 'rest-check' }) });
  assert.equal(fetched.status, 200);
  const batch = await fetched.json();
  assert.equal(batch.count, 1);
  assert.equal(batch.files[0].sourceType, 'rest');
  assert.equal(batch.files[0].connectorId, 'rest-check');
  const content = await (await fetch(`${base}/api/inbox/${batch.files[0].id}/content`, { headers })).json();
  assert.equal(content.fileContent, sample);
  assert.equal(content.connectorId, 'rest-check');
  const repeated = await (await fetch(`${base}/api/rest/fetch`, { method: 'POST', headers, body: JSON.stringify({ connectorId: 'rest-check' }) })).json();
  assert.equal(repeated.count, 0);
  assert.equal((await (await fetch(`${base}/api/inbox`, { headers })).json()).length, 1);
});
