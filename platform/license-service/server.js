'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const http = require('node:http');
const path = require('node:path');

const PORT = Number(process.env.PORT || 10000);
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '');
const STORE = process.env.LICENSE_STORE || '/var/data/licenses.json';
const MAX_BODY = 16 * 1024;
const rates = new Map();

function timingSafeText(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  return left.length === right.length && left.length > 0 && crypto.timingSafeEqual(left, right);
}

async function loadLicenses() {
  try {
    const value = JSON.parse(await fs.readFile(STORE, 'utf8'));
    return Array.isArray(value) ? value : [];
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

let licenses = [];
let writeQueue = Promise.resolve();
async function saveLicenses() {
  const snapshot = JSON.stringify(licenses, null, 2);
  writeQueue = writeQueue.catch(() => {}).then(async () => {
    await fs.mkdir(path.dirname(STORE), { recursive: true });
    const temporary = `${STORE}.tmp`;
    await fs.writeFile(temporary, snapshot, { mode: 0o600 });
    await fs.rename(temporary, STORE);
  });
  await writeQueue;
}

function json(response, status, body) {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
  response.end(JSON.stringify(body));
}

async function bodyJson(request) {
  let text = '';
  for await (const chunk of request) {
    text += chunk;
    if (Buffer.byteLength(text) > MAX_BODY) throw new Error('Request body is too large.');
  }
  if (!text) return {};
  return JSON.parse(text);
}

function rateAllowed(request) {
  const now = Date.now();
  const address = request.socket.remoteAddress || 'unknown';
  const state = rates.get(address) || { start: now, count: 0 };
  if (now - state.start > 60_000) { state.start = now; state.count = 0; }
  state.count += 1;
  rates.set(address, state);
  if (rates.size > 10_000) rates.clear();
  return state.count <= 120;
}

function authenticated(request) {
  return timingSafeText(String(request.headers.authorization || '').replace(/^Bearer\s+/i, ''), ADMIN_TOKEN);
}

async function handle(request, response) {
  if (!rateAllowed(request)) return json(response, 429, { error: 'Too many requests.' });
  const url = new URL(request.url, 'http://localhost');
  if (request.method === 'GET' && url.pathname === '/health') return json(response, 200, { ok: true, service: 'herta-license-service' });

  if (request.method === 'POST' && url.pathname === '/v1/validate') {
    let input;
    try { input = await bodyJson(request); } catch { return json(response, 400, { error: 'Invalid request body.' }); }
    const candidate = String(input.licenseKey || '');
    const hash = crypto.createHash('sha256').update(candidate).digest('hex');
    const license = licenses.find(item => timingSafeText(item.keyHash, hash));
    const now = Date.now();
    if (!license) return json(response, 403, { valid: false, reason: 'invalid' });
    if (license.revokedAt) return json(response, 403, { valid: false, reason: 'revoked', expiresAt: license.expiresAt });
    if (!Number.isFinite(Date.parse(license.expiresAt)) || Date.parse(license.expiresAt) <= now) {
      return json(response, 403, { valid: false, reason: 'expired', expiresAt: license.expiresAt });
    }
    const instanceId = String(input.instanceId || '').slice(0, 100);
    return json(response, 200, { valid: true, licenseId: license.id, expiresAt: license.expiresAt, serverTime: new Date(now).toISOString(), checkedAt: new Date(now).toISOString(), nextCheckSeconds: 60, instanceId });
  }

  if (url.pathname.startsWith('/v1/licenses')) {
    if (!ADMIN_TOKEN || !authenticated(request)) return json(response, 401, { error: 'Admin authorization failed.' });
    if (request.method === 'GET' && url.pathname === '/v1/licenses') {
      return json(response, 200, { licenses: licenses.map(({ keyHash, ...item }) => item) });
    }
    if (request.method === 'POST' && url.pathname === '/v1/licenses') {
      let input;
      try { input = await bodyJson(request); } catch { return json(response, 400, { error: 'Invalid request body.' }); }
      const expiresTimestamp = Date.parse(input.expiresAt || '');
      if (!Number.isFinite(expiresTimestamp) || expiresTimestamp <= Date.now()) return json(response, 400, { error: 'expiresAt must be a future ISO-8601 date.' });
      const expiresAt = new Date(expiresTimestamp).toISOString();
      const licenseKey = `herta_${crypto.randomBytes(32).toString('base64url')}`;
      const item = { id: crypto.randomUUID(), label: String(input.label || '').trim().slice(0, 100), keyHash: crypto.createHash('sha256').update(licenseKey).digest('hex'), expiresAt, createdAt: new Date().toISOString(), revokedAt: null };
      licenses.push(item);
      try { await saveLicenses(); } catch { licenses = licenses.filter(row => row.id !== item.id); return json(response, 500, { error: 'Could not save license. Check persistent storage.' }); }
      return json(response, 201, { id: item.id, label: item.label, expiresAt: item.expiresAt, createdAt: item.createdAt, licenseKey });
    }
    const match = url.pathname.match(/^\/v1\/licenses\/([0-9a-f-]{36})$/i);
    if (request.method === 'PATCH' && match) {
      let input;
      try { input = await bodyJson(request); } catch { return json(response, 400, { error: 'Invalid request body.' }); }
      const license = licenses.find(item => item.id === match[1]);
      if (!license) return json(response, 404, { error: 'License not found.' });
      const expiresTimestamp = Date.parse(input.expiresAt || '');
      if (!Number.isFinite(expiresTimestamp) || expiresTimestamp <= Date.now()) return json(response, 400, { error: 'expiresAt must be a future ISO-8601 date.' });
      if (license.revokedAt) return json(response, 409, { error: 'Revoke cannot be undone. Issue a new license instead.' });
      const previous = license.expiresAt;
      license.expiresAt = new Date(expiresTimestamp).toISOString();
      try { await saveLicenses(); } catch { license.expiresAt = previous; return json(response, 500, { error: 'Could not save license renewal.' }); }
      return json(response, 200, { id: license.id, label: license.label, expiresAt: license.expiresAt, createdAt: license.createdAt, revokedAt: license.revokedAt });
    }
    if (request.method === 'DELETE' && match) {
      const license = licenses.find(item => item.id === match[1]);
      if (!license) return json(response, 404, { error: 'License not found.' });
      license.revokedAt ||= new Date().toISOString();
      try { await saveLicenses(); } catch { return json(response, 500, { error: 'Could not save license revocation.' }); }
      return json(response, 200, { id: license.id, revokedAt: license.revokedAt });
    }
  }
  return json(response, 404, { error: 'Endpoint not found.' });
}

async function start() {
  if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) throw new Error('PORT must be a valid TCP port.');
  if (ADMIN_TOKEN.length < 32) throw new Error('Set ADMIN_TOKEN to a random secret of at least 32 characters.');
  licenses = await loadLicenses();
  const server = http.createServer((request, response) => { void handle(request, response).catch(() => { if (!response.headersSent) json(response, 500, { error: 'License service error.' }); }); });
  server.listen(PORT, '0.0.0.0', () => console.log(`HERTA license service listening on ${PORT}`));
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close(() => process.exit(0)));
}

start().catch(error => { console.error(error.message); process.exit(1); });
