const { spawn } = require('node:child_process');
const fsSync = require('node:fs');
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

const PROJECT = 'herta-operations';
const WORKER_URL = 'http://127.0.0.1:18127';
const DEFAULT_BACKUP = '/backups';

function dockerCommand() {
  const programFiles = process.env.ProgramW6432 || process.env.ProgramFiles || 'C:\\Program Files';
  const candidates = [
    process.env.DOCKER_CLI_PATH,
    path.join(programFiles, 'Docker', 'Docker', 'resources', 'bin', 'docker.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Docker', 'Docker', 'resources', 'bin', 'docker.exe'),
  ].filter(Boolean);
  return candidates.find((candidate) => fsSync.existsSync(candidate)) || 'docker';
}

function composeDirectory(app) {
  return app.isPackaged ? path.join(process.resourcesPath, 'platform') : path.resolve(__dirname, '../platform');
}

function run(command, args, { cwd, timeout = 120000 } = {}) {
  return new Promise((resolve, reject) => {
    const executable = command === 'docker' ? dockerCommand() : command;
    const child = spawn(executable, args, { cwd, windowsHide: true, shell: false, env: { ...process.env, DOCKER_CLI_HINTS: 'false' } });
    let stdout = ''; let stderr = ''; let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; child.kill(); }, timeout);
    child.stdout.on('data', (chunk) => { stdout += chunk; if (stdout.length > 2_000_000) child.kill(); });
    child.stderr.on('data', (chunk) => { stderr += chunk; if (stderr.length > 2_000_000) child.kill(); });
    child.on('error', (error) => { clearTimeout(timer); reject(new Error(error.code === 'ENOENT' ? 'Docker CLI was not found. Install Docker Desktop and start its engine.' : error.message)); });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (timedOut) return reject(new Error('Docker operation timed out. Check Docker Desktop and try again.'));
      if (code !== 0) return reject(new Error((stderr || stdout || `Docker exited with code ${code}.`).trim().slice(-1200)));
      resolve({ stdout: stdout.trim(), stderr: stderr.trim() });
    });
  });
}

async function environmentFile(app) {
  const folder = app.getPath('userData');
  const file = path.join(folder, 'docker-platform.env');
  await fs.mkdir(folder, { recursive: true });
  let values = {};
  try {
    const text = await fs.readFile(file, 'utf8');
    for (const line of text.split(/\r?\n/)) { const match = line.match(/^([A-Z_]+)=(.*)$/); if (match) values[match[1]] = match[2].replace(/^"|"$/g, ''); }
  } catch { /* Create the first-run runtime settings below. */ }
  if (!/^[A-Za-z0-9_-]{40,}$/.test(values.HERTA_WORKER_API_TOKEN || '')) values.HERTA_WORKER_API_TOKEN = crypto.randomBytes(32).toString('base64url');
  if (!values.HERTA_BACKUP_PATH) values.HERTA_BACKUP_PATH = path.join(folder, 'backups').replace(/\\/g, '/');
  await fs.mkdir(values.HERTA_BACKUP_PATH, { recursive: true });
  const escape = (value) => `"${String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\$/g, '\\$')}"`;
  await fs.writeFile(file, `HERTA_WORKER_API_TOKEN=${escape(values.HERTA_WORKER_API_TOKEN)}\nHERTA_BACKUP_PATH=${escape(values.HERTA_BACKUP_PATH)}\n`, { mode: 0o600 });
  return { file, token: values.HERTA_WORKER_API_TOKEN, backupPath: values.HERTA_BACKUP_PATH };
}

function composeArgs(app, envFile, args) {
  return ['compose', '--project-name', PROJECT, '--env-file', envFile, '-f', path.join(composeDirectory(app), 'compose.yaml'), '--project-directory', composeDirectory(app), ...args];
}

async function workerRequest(app, pathname, { method = 'GET', body, timeout = 15000 } = {}) {
  const env = await environmentFile(app);
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(`${WORKER_URL}${pathname}`, { method, headers: { Authorization: `Bearer ${env.token}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) }, body: body === undefined ? undefined : JSON.stringify(body), signal: controller.signal });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(result.error || `Worker request failed (HTTP ${response.status}).`);
    return result;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error('HERTA worker request timed out.');
    if (error.cause?.code === 'ECONNREFUSED' || error.code === 'ECONNREFUSED') throw new Error('HERTA background worker is not running. Start the worker stack first.');
    throw error;
  } finally { clearTimeout(timer); }
}

async function platformStatus(app) {
  const env = await environmentFile(app);
  let engine = { available: false, version: '', error: '' };
  try {
    const info = await run('docker', ['version', '--format', '{{.Server.Version}}'], { cwd: composeDirectory(app), timeout: 12000 });
    engine = { available: true, version: info.stdout, error: '' };
  } catch (error) { engine.error = error.message; }
  if (!engine.available) return { engine, services: [], worker: null, backupPath: env.backupPath };
  let services = [];
  try {
    const output = await run('docker', composeArgs(app, env.file, ['ps', '--all', '--format', 'json']), { cwd: composeDirectory(app), timeout: 15000 });
    try {
      const parsed = JSON.parse(output.stdout);
      services = Array.isArray(parsed) ? parsed : [parsed];
    } catch {
      services = output.stdout.split(/\r?\n/).filter(Boolean).map((line) => { try { return JSON.parse(line); } catch { return null; } }).filter(Boolean);
    }
  } catch { /* The compose project has not been created yet. */ }
  let worker = null;
  if (services.some((service) => service.Service === 'herta-worker' && /up|running/i.test(service.State || service.Status || ''))) {
    try { worker = await workerRequest(app, '/api/status'); } catch (error) { worker = { ok: false, error: error.message }; }
  }
  return { engine, services, worker, backupPath: env.backupPath };
}

async function startPlatform(app, options = {}) {
  const env = await environmentFile(app);
  const before = await platformStatus(app);
  const services = ['herta-worker'];
  if (options.localKafka) services.push('kafka');
  if (options.search) services.push('elasticsearch');
  await run('docker', composeArgs(app, env.file, ['up', '-d', ...services]), { cwd: composeDirectory(app), timeout: 15 * 60 * 1000 });
  const running = (name) => before.services.some((service) => service.Service === name && /running|up/i.test(service.State || service.Status || ''));
  for (const [name, enabled] of [['kafka', Boolean(options.localKafka)], ['elasticsearch', Boolean(options.search)]]) {
    if (!enabled && running(name)) await run('docker', composeArgs(app, env.file, ['stop', name]), { cwd: composeDirectory(app), timeout: 120000 });
  }
  let status;
  for (let attempt = 0; attempt < 45; attempt += 1) {
    status = await platformStatus(app);
    if (status.worker?.ok) return status;
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
  return status;
}

async function configureBackupMount(app, directory) {
  const env = await environmentFile(app);
  const folder = path.resolve(String(directory || env.backupPath));
  await fs.mkdir(folder, { recursive: true });
  const next = `HERTA_WORKER_API_TOKEN="${env.token}"\nHERTA_BACKUP_PATH="${folder.replace(/\\/g, '/')}"\n`;
  await fs.writeFile(env.file, next, { mode: 0o600 });
  return folder;
}

async function recreateWorker(app) {
  const env = await environmentFile(app);
  await run('docker', composeArgs(app, env.file, ['up', '-d', '--force-recreate', 'herta-worker']), { cwd: composeDirectory(app), timeout: 15 * 60 * 1000 });
  return platformStatus(app);
}

async function stopPlatform(app) {
  const env = await environmentFile(app);
  await run('docker', composeArgs(app, env.file, ['stop']), { cwd: composeDirectory(app), timeout: 120000 });
  return platformStatus(app);
}

async function platformLogs(app, service = '') {
  const env = await environmentFile(app);
  const allowed = new Set(['herta-worker', 'kafka', 'elasticsearch']);
  if (service && !allowed.has(service)) throw new Error('Unknown platform service.');
  const args = ['logs', '--no-color', '--tail', '100']; if (service) args.push(service);
  return (await run('docker', composeArgs(app, env.file, args), { cwd: composeDirectory(app), timeout: 20000 })).stdout;
}

async function setWorkerSession(app, value) {
  const env = await environmentFile(app);
  let baseUrl = value?.baseUrl;
  if (baseUrl) {
    const target = new URL(baseUrl);
    if (target.hostname === 'localhost' || target.hostname === '127.0.0.1') target.hostname = 'host.docker.internal';
    baseUrl = target.toString().replace(/\/$/, '');
  }
  const body = value ? { baseUrl, token: value.token, verifyTls: Boolean(value.verifyTls) } : undefined;
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const response = await fetch(`${WORKER_URL}/api/session`, { method: value ? 'POST' : 'DELETE', headers: { Authorization: `Bearer ${env.token}`, ...(body ? { 'Content-Type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined, signal: controller.signal });
    if (!response.ok) return false;
    return true;
  } catch { return false; } finally { clearTimeout(timer); }
}

module.exports = { platformStatus, startPlatform, stopPlatform, platformLogs, workerRequest, setWorkerSession, configureBackupMount, recreateWorker };
