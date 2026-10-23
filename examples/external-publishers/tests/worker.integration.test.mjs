import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { publishRest } from '../rest-publisher.mjs';

// Resolve against repository root, while allowing this folder to work standalone.
const repositoryWorker = fileURLToPath(new URL('../../../platform/worker/server.js', import.meta.url));

test('REST publisher delivers exact binary bytes to an isolated real worker inbox', { skip: !existsSync(repositoryWorker) }, async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'herta-external-worker-'));
  const listener = net.createServer();
  listener.listen(0, '127.0.0.1'); await once(listener, 'listening');
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  const token = 'publisher-integration-test-control-token';
  const child = spawn(process.execPath, [repositoryWorker], {
    env: { ...process.env, PORT: String(port), DATA_DIR: dir, BACKUP_DIR: path.join(dir, 'backups'), WORKER_API_TOKEN: token },
    stdio: 'ignore',
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) { child.kill(); await once(child, 'exit'); }
    await rm(dir, { recursive: true, force: true });
  });
  const base = `http://127.0.0.1:${port}`;
  let ready = false;
  for (let i = 0; i < 60; i++) {
    try { if ((await fetch(`${base}/health`)).ok) { ready = true; break; } } catch { /* starting */ }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert.equal(ready, true, 'isolated worker must become healthy');
  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
  const receiverResponse = await fetch(`${base}/api/receiver/start`, { method: 'POST', headers, body: '{}' });
  assert.equal(receiverResponse.status, 200);
  const receiver = await receiverResponse.json();
  const sample = { name: 'transport-fixture.nist', data: Buffer.from([0, 128, 255, 4]) };
  const result = await publishRest([sample], { REST_URL: `${base}/api/v1/nist`, REST_TOKEN: receiver.token || '' });
  assert.equal(result.accepted, 1);
  assert.equal(result.httpStatus, 202); // No HERTA session: the file remains in inbox.
  const contentResponse = await fetch(`${base}/api/inbox/${result.ids[0]}/content`, { headers });
  assert.equal(contentResponse.status, 200);
  const content = await contentResponse.json();
  assert.equal(content.name, sample.name);
  assert.deepEqual(Buffer.from(content.fileContent, 'base64'), sample.data);
});
