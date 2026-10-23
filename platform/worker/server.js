const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { Kafka, logLevel } = require('kafkajs');
const { createZip } = require('../../electron/backup-archive');
const { MAX_FILE_BYTES, MAX_BATCH_BYTES, normalizeNistSourcePayload, safeNistName } = require('../../electron/source-files');
const { parseCron, matchesCron, nextCronDate } = require('../../electron/cron');

const PORT = Number(process.env.PORT || 8127);
const DATA_DIR = process.env.DATA_DIR || '/data';
const BACKUP_DIR = process.env.BACKUP_DIR || '/backups';
const API_TOKEN = process.env.WORKER_API_TOKEN || '';
const INBOX_DIR = path.join(DATA_DIR, 'inbox');
const MAX_BODY = 350 * 1024 * 1024;
const inbox = new Map();
let session = null;
let kafkaConsumer = null;
let kafkaInfo = null;
let receiverToken = '';
let telegramConfig = { botToken: '', chatId: '' };
let telegramStatus = { configured: false, connected: false, botName: '', checkedAt: null, lastError: '' };
let backupSettings = { enabled: false, cron: '0 2 * * *', retentionCount: 30, lastScheduledKey: '', lastRunAt: null, lastRunStatus: 'idle', lastRunMessage: '' };
let backupBusy = false;
let scheduler;
const startedAt = new Date().toISOString();

async function ensureFolders() { await fs.mkdir(INBOX_DIR, { recursive: true }); await fs.mkdir(BACKUP_DIR, { recursive: true }); }
async function readJson(file, fallback) { try { return JSON.parse(await fs.readFile(file, 'utf8')); } catch { return fallback; } }
async function writeJson(file, value) {
  const temporary = `${file}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(value, null, 2), { mode: 0o600 });
  await fs.rename(temporary, file);
}

async function loadInbox() {
  const entries = await readJson(path.join(DATA_DIR, 'inbox.json'), []);
  for (const entry of entries) {
    const file = entry.file ? path.join(INBOX_DIR, entry.file) : '';
    try { inbox.set(entry.id, { ...entry, data: file ? await fs.readFile(file) : null, file }); }
    catch { inbox.set(entry.id, { ...entry, data: null, error: 'Stored NIST file is missing.' }); }
  }
}
async function saveInbox() {
  const entries = [...inbox.values()].map(({ data, file, ...entry }) => ({ ...entry, ...(file ? { file: path.basename(file) } : {}) }));
  await writeJson(path.join(DATA_DIR, 'inbox.json'), entries);
}
function inboxSummary(entry) { return { id: entry.id, name: entry.name, size: entry.data?.length || 0, source: entry.source, error: entry.error || '', kafka: entry.kafka || null, createdAt: entry.createdAt || null }; }

async function addFiles(files, source, kafka = null) {
  if (inbox.size + files.length > 500) throw new Error('The worker inbox is limited to 500 files.');
  let batchBytes = 0;
  for (const file of files) { batchBytes += file.data.length; if (file.data.length > MAX_FILE_BYTES || batchBytes > MAX_BATCH_BYTES) throw new Error('NIST intake is limited to 32 MiB per file and 256 MiB per batch.'); }
  const ids = [];
  for (const item of files) {
    const id = crypto.randomUUID();
    const name = safeNistName(item.name, 'received');
    const filename = `${id}.nist`;
    const file = path.join(INBOX_DIR, filename);
    await fs.writeFile(file, item.data, { mode: 0o600, flag: 'wx' });
    const entry = { id, name, size: item.data.length, source, kafka, file: filename, createdAt: new Date().toISOString() };
    inbox.set(id, { ...entry, data: item.data, file }); ids.push(id);
  }
  await saveInbox();
  void sendTelegramMessage(`HERTA Operations received ${ids.length} NIST file${ids.length === 1 ? '' : 's'} from ${source}.`);
  return ids;
}

async function telegramRequest(method, body = {}) {
  const response = await fetch(`https://api.telegram.org/bot${telegramConfig.botToken}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(12000) });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || !result.ok) throw new Error(String(result.description || `Telegram API request failed (HTTP ${response.status}).`).slice(0, 240));
  return result.result;
}

async function checkTelegram() {
  if (!telegramConfig.botToken || !telegramConfig.chatId) throw new Error('Telegram bot token and chat ID are required.');
  try {
    const bot = await telegramRequest('getMe');
    telegramStatus = { configured: true, connected: true, botName: bot.username || bot.first_name || 'Telegram bot', checkedAt: new Date().toISOString(), lastError: '' };
    return telegramStatus;
  } catch (error) {
    telegramStatus = { ...telegramStatus, configured: true, connected: false, checkedAt: new Date().toISOString(), lastError: error.message };
    throw error;
  }
}

async function sendTelegramMessage(text) {
  if (!telegramConfig.botToken || !telegramConfig.chatId) return;
  try {
    await telegramRequest('sendMessage', { chat_id: telegramConfig.chatId, text });
    telegramStatus = { ...telegramStatus, configured: true, connected: true, checkedAt: new Date().toISOString(), lastError: '' };
  } catch (error) {
    telegramStatus = { ...telegramStatus, configured: true, connected: false, checkedAt: new Date().toISOString(), lastError: error.message };
    console.error('Telegram notification failed:', error.message);
  }
}

function requestJson(baseUrl, verifyTls, pathname, { method = 'GET', token, json } = {}) {
  const target = new URL(pathname, `${baseUrl}/`);
  const payload = json === undefined ? null : Buffer.from(JSON.stringify(json));
  const headers = { Accept: 'application/json' };
  if (payload) { headers['Content-Type'] = 'application/json'; headers['Content-Length'] = payload.length; }
  if (token) headers.Authorization = `Bearer ${token}`;
  const transport = target.protocol === 'https:' ? https : http;
  return new Promise((resolve, reject) => {
    const request = transport.request(target, { method, headers, rejectUnauthorized: target.protocol !== 'https:' || verifyTls, timeout: 45000 }, (response) => {
      const chunks = []; let size = 0;
      response.on('data', (chunk) => { size += chunk.length; if (size > 64 * 1024 * 1024) { request.destroy(new Error('HERTA response exceeded the 64 MiB limit.')); return; } chunks.push(chunk); });
      response.on('end', () => {
        let result = {};
        try { result = chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}; }
        catch { return reject(new Error(`HERTA returned unreadable JSON (HTTP ${response.statusCode}).`)); }
        if (response.statusCode < 200 || response.statusCode >= 300) return reject(new Error(`HERTA request failed (HTTP ${response.statusCode}). ${String(result.detail || result.message || '').slice(0, 300)}`));
        resolve(result);
      });
    });
    request.on('timeout', () => request.destroy(new Error('HERTA request timed out.')));
    request.on('error', reject);
    if (payload) request.write(payload);
    request.end();
  });
}
function requireSession() { if (!session?.token) throw new Error('Sign in to HERTA from the desktop app first.'); return session; }
async function hertaJson(pathname, options) { const active = requireSession(); return requestJson(active.baseUrl, active.verifyTls, pathname, { ...options, token: active.token }); }
async function getAllSubjects() {
  const records = []; let offset = 0; let count = Infinity;
  while (offset < count) {
    const page = await hertaJson(`/api/v1/subjects?offset=${offset}&size=50`);
    if (!Array.isArray(page.subjects)) throw new Error('HERTA returned an unexpected subjects response.');
    records.push(...page.subjects); count = Number(page.count) || (offset + page.subjects.length); offset += page.subjects.length;
    if (!page.subjects.length) break;
    if (records.length > 500) throw new Error('Backup limit is 500 subjects per snapshot.');
  }
  return records;
}
async function createBackup(scheduled = false) {
  const records = await getAllSubjects();
  if (!records.length) throw new Error('HERTA has no subjects to back up.');
  const files = []; const results = []; let totalBytes = 0;
  for (const subject of records) {
    const code = String(subject.code || '').trim();
    if (!code) continue;
    try {
      const response = await hertaJson(`/api/v1/subjects/${encodeURIComponent(code)}/export`, { method: 'POST', json: { format: 'nist' } });
      const encoded = String(response.fileContent || '').replace(/\s+/g, '');
      if (!encoded || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) throw new Error('Invalid NIST payload.');
      const data = Buffer.from(encoded, 'base64');
      if (!data.length || data.length > MAX_FILE_BYTES) throw new Error('NIST file is empty or too large.');
      if (totalBytes + data.length > MAX_BATCH_BYTES) throw new Error('Snapshot exceeds the 256 MiB size limit.');
      totalBytes += data.length;
      files.push({ name: safeNistName(code), data }); results.push({ code, ok: true });
    } catch (error) { results.push({ code, ok: false, message: error.message }); }
  }
  if (!files.length) throw new Error('No subject records could be exported.');
  files.push({ name: 'backup-manifest.json', data: Buffer.from(JSON.stringify({ application: 'HERTA Operations', createdAt: new Date().toISOString(), subjectCount: records.length, successfulExports: results.filter((r) => r.ok).length, format: 'NIST' }, null, 2)) });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const destination = path.join(BACKUP_DIR, `herta-snapshot-${stamp}.zip`);
  const archive = createZip(files);
  if (archive.length > MAX_BATCH_BYTES) throw new Error('Snapshot ZIP exceeds the 256 MiB size limit.');
  await fs.writeFile(destination, archive, { mode: 0o600, flag: 'wx' });
  const snapshots = (await fs.readdir(BACKUP_DIR, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && /^herta-snapshot-.*\.zip$/i.test(entry.name))
    .map((entry) => path.join(BACKUP_DIR, entry.name));
  const dated = await Promise.all(snapshots.map(async (file) => ({ file, stat: await fs.stat(file) })));
  dated.sort((a, b) => b.stat.mtimeMs - a.stat.mtimeMs);
  await Promise.all(dated.slice(backupSettings.retentionCount).map(({ file }) => fs.rm(file, { force: true })));
  const event = { at: new Date().toISOString(), action: scheduled ? 'Automatic snapshot created' : 'Snapshot created', summary: `${files.length - 1} NIST files saved to ${destination}.` };
  const logPath = path.join(DATA_DIR, 'activity-log.json');
  const log = await readJson(logPath, []); await writeJson(logPath, [event, ...log].slice(0, 500));
  return { canceled: false, path: destination, rows: results.map((result) => ({ Code: result.code, Status: result.ok ? 'Success' : 'Failed', Bytes: 0, Details: result.message || 'Exported to snapshot.' })) };
}

async function persistSchedule() { await writeJson(path.join(DATA_DIR, 'backup-settings.json'), backupSettings); }
async function tickBackup() {
  if (backupBusy || !backupSettings.enabled || !session?.token) return;
  const now = new Date();
  if (!matchesCron(backupSettings.cron, now)) return;
  const key = `${now.getFullYear()}-${now.getMonth()}-${now.getDate()}-${now.getHours()}-${now.getMinutes()}`;
  if (key === backupSettings.lastScheduledKey) return;
  backupBusy = true;
  backupSettings.lastScheduledKey = key;
  backupSettings.lastRunStatus = 'running';
  backupSettings.lastRunAt = new Date().toISOString();
  await persistSchedule();
  try {
    let failure;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try { await createBackup(true); failure = null; break; }
      catch (error) { failure = error; if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, attempt * 2000)); }
    }
    if (failure) throw failure;
    backupSettings.lastRunStatus = 'success'; backupSettings.lastRunMessage = 'Snapshot created successfully.';
  } catch (error) {
    backupSettings.lastRunStatus = 'failed'; backupSettings.lastRunMessage = error.message;
    const logPath = path.join(DATA_DIR, 'activity-log.json'); const log = await readJson(logPath, []);
    await writeJson(logPath, [{ at: new Date().toISOString(), action: 'Automatic snapshot failed', summary: error.message }, ...log].slice(0, 500));
  } finally { await persistSchedule(); backupBusy = false; }
}

function parseKafkaConfig(input = {}) {
  const brokers = String(input.brokers || '').split(',').map((entry) => entry.trim()).filter(Boolean);
  const topic = String(input.topic || '').trim();
  if (!brokers.length || !topic) throw new Error('Enter at least one Kafka broker and a topic.');
  let sasl;
  const mechanism = String(input.mechanism || 'none').toLowerCase();
  if (mechanism !== 'none') {
    if (!['plain', 'scram-sha-256', 'scram-sha-512'].includes(mechanism) || !input.username || !input.password) throw new Error('Choose a supported Kafka SASL mode and enter credentials.');
    sasl = { mechanism, username: String(input.username), password: String(input.password) };
  }
  return { brokers, topic, groupId: String(input.groupId || 'herta-operations-worker'), mechanism, tls: Boolean(input.tls), verifyTls: Boolean(input.verifyTls), sasl, messageFormat: input.messageFormat === 'raw-nist' ? 'raw-nist' : 'json-base64' };
}
function kafkaClient(config) { return new Kafka({ clientId: 'herta-operations-worker', brokers: config.brokers, ssl: config.tls ? { rejectUnauthorized: config.verifyTls } : undefined, sasl: config.sasl, logLevel: logLevel.ERROR, connectionTimeout: 8000, requestTimeout: 15000 }); }
async function startKafka(input) {
  await stopKafka();
  const config = parseKafkaConfig(input); const consumer = kafkaClient(config).consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
  await consumer.connect(); await consumer.subscribe({ topic: config.topic, fromBeginning: false });
  kafkaConsumer = consumer; kafkaInfo = { topic: config.topic, groupId: config.groupId, listening: true, brokers: config.brokers, messageFormat: config.messageFormat };
  await consumer.run({ autoCommit: false, eachMessage: async ({ topic, partition, message }) => {
    try {
      let files;
      if (config.messageFormat === 'raw-nist') files = [{ name: safeNistName(message.key?.toString() || `${topic}-${partition}-${message.offset}`), data: Buffer.from(message.value || []) }];
      else {
        let payload;
        try { payload = JSON.parse(Buffer.from(message.value || []).toString('utf8')); }
        catch { throw new Error('This message is not valid JSON. Expected a JSON NIST envelope with a Base64 fileContent field. If the topic contains raw NIST bytes, change Message format to “Raw NIST bytes”.'); }
        files = normalizeNistSourcePayload(payload);
      }
      await addFiles(files, `Kafka · ${topic}`, { topic, partition, offset: message.offset });
    } catch (error) {
      const id = crypto.randomUUID(); inbox.set(id, { id, name: `${topic}-${partition}-${message.offset}`, size: 0, source: `Kafka · ${topic}`, kafka: { topic, partition, offset: message.offset }, error: error.message, createdAt: new Date().toISOString() }); await saveInbox();
    }
  } });
  return { listening: true, topic: config.topic, groupId: config.groupId };
}
async function stopKafka() { const consumer = kafkaConsumer; kafkaConsumer = null; kafkaInfo = null; if (consumer) { await consumer.stop().catch(() => {}); await consumer.disconnect().catch(() => {}); } }

async function readBody(request) {
  const chunks = []; let size = 0;
  for await (const chunk of request) { size += chunk.length; if (size > MAX_BODY) throw new Error('Request exceeds the worker payload limit.'); chunks.push(chunk); }
  return Buffer.concat(chunks);
}
function json(response, status, data) { response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); response.end(JSON.stringify(data)); }
function checkApiToken(request) {
  const received = Buffer.from(String(request.headers.authorization || '').replace(/^Bearer\s+/i, ''));
  const expected = Buffer.from(API_TOKEN);
  return Boolean(API_TOKEN && received.length === expected.length && crypto.timingSafeEqual(received, expected));
}
function requireApi(request, response) { if (checkApiToken(request)) return true; json(response, 401, { error: 'Worker authorization failed.' }); return false; }
function checkReceiverToken(request) {
  const received = Buffer.from(String(request.headers.authorization || '').replace(/^Bearer\s+/i, ''));
  const expected = Buffer.from(receiverToken);
  return Boolean(receiverToken && received.length === expected.length && crypto.timingSafeEqual(received, expected));
}

async function route(request, response) {
  const url = new URL(request.url, 'http://localhost');
  if (request.method === 'GET' && url.pathname === '/health') return json(response, 200, { ok: true, service: 'herta-worker', startedAt });
  if (url.pathname === '/api/v1/nist' && request.method === 'POST') {
    if (!checkReceiverToken(request)) return json(response, 401, { error: 'Invalid receiver token.' });
    try {
      const body = await readBody(request); let files;
      if (String(request.headers['content-type'] || '').includes('application/octet-stream')) {
        if (!body.length || body.length > MAX_FILE_BYTES) throw new Error('Raw NIST data must be 1 byte to 32 MiB.');
        files = [{ name: safeNistName(request.headers['x-file-name']), data: body }];
      } else files = normalizeNistSourcePayload(JSON.parse(body.toString('utf8')));
      const ids = await addFiles(files, 'REST receiver'); return json(response, 202, { accepted: ids.length, ids });
    } catch (error) { return json(response, 400, { error: error.message }); }
  }
  if (!requireApi(request, response)) return;
  try {
    if (request.method === 'GET' && url.pathname === '/api/status') return json(response, 200, {
      ok: true, service: 'herta-worker', startedAt, authenticated: Boolean(session?.token), backup: { enabled: backupSettings.enabled, cron: backupSettings.cron, retentionCount: backupSettings.retentionCount, nextRun: backupSettings.enabled ? nextCronDate(backupSettings.cron).toISOString() : null, running: backupBusy, lastRunAt: backupSettings.lastRunAt, lastRunStatus: backupSettings.lastRunStatus, lastRunMessage: backupSettings.lastRunMessage }, kafka: kafkaInfo || { listening: false }, receiver: { running: Boolean(receiverToken), port: 18127, token: receiverToken, url: 'http://127.0.0.1:18127/api/v1/nist' }, telegram: telegramStatus, inbox: [...inbox.values()].map(inboxSummary),
    });
    if (request.method === 'POST' && url.pathname === '/api/session') {
      const body = JSON.parse((await readBody(request)).toString('utf8'));
      const base = new URL(String(body.baseUrl || ''));
      if (!['http:', 'https:'].includes(base.protocol) || !body.token) throw new Error('A valid HERTA base URL and access token are required.');
      session = { baseUrl: base.toString().replace(/\/$/, ''), token: String(body.token), verifyTls: Boolean(body.verifyTls) };
      return json(response, 200, { authenticated: true });
    }
    if (request.method === 'DELETE' && url.pathname === '/api/session') { session = null; return json(response, 200, { authenticated: false }); }
    if (request.method === 'POST' && url.pathname === '/api/receiver/start') { if (!receiverToken) receiverToken = crypto.randomBytes(32).toString('base64url'); return json(response, 200, { running: true, port: 18127, token: receiverToken, url: 'http://127.0.0.1:18127/api/v1/nist' }); }
    if (request.method === 'POST' && url.pathname === '/api/receiver/stop') { receiverToken = ''; return json(response, 200, { running: false }); }
    if (request.method === 'GET' && url.pathname === '/api/telegram/status') return json(response, 200, telegramStatus);
    if (request.method === 'POST' && url.pathname === '/api/telegram/config') {
      const body = JSON.parse((await readBody(request)).toString('utf8'));
      telegramConfig = { botToken: String(body.botToken || '').trim(), chatId: String(body.chatId || '').trim() };
      if (!telegramConfig.botToken || !telegramConfig.chatId) { telegramConfig = { botToken: '', chatId: '' }; telegramStatus = { configured: false, connected: false, botName: '', checkedAt: new Date().toISOString(), lastError: '' }; return json(response, 200, telegramStatus); }
      return json(response, 200, await checkTelegram());
    }
    if (request.method === 'POST' && url.pathname === '/api/telegram/test') {
      await checkTelegram();
      await telegramRequest('sendMessage', { chat_id: telegramConfig.chatId, text: 'HERTA Operations Telegram connector test succeeded.' });
      telegramStatus = { ...telegramStatus, connected: true, checkedAt: new Date().toISOString(), lastError: '' };
      return json(response, 200, { ...telegramStatus, sent: true });
    }
    if (request.method === 'GET' && url.pathname === '/api/inbox') return json(response, 200, [...inbox.values()].map(inboxSummary));
    if (request.method === 'GET' && url.pathname === '/api/audit') return json(response, 200, await readJson(path.join(DATA_DIR, 'activity-log.json'), []));
    if (request.method === 'GET' && url.pathname.startsWith('/api/inbox/') && url.pathname.endsWith('/content')) {
      const id = url.pathname.split('/')[3]; const item = inbox.get(id); if (!item?.data) return json(response, 404, { error: 'Inbox file is unavailable.' });
      return json(response, 200, { id, name: item.name, source: item.source, fileContent: item.data.toString('base64'), kafka: item.kafka });
    }
    if (request.method === 'DELETE' && url.pathname.startsWith('/api/inbox/')) {
      const id = url.pathname.split('/').pop(); const item = inbox.get(id);
      if (item?.file) await fs.unlink(item.file).catch(() => {}); inbox.delete(id); await saveInbox();
      if (item?.kafka && kafkaConsumer) {
        const ref = item.kafka;
        const remaining = [...inbox.values()].some((candidate) => candidate.kafka && candidate.kafka.topic === ref.topic && candidate.kafka.partition === ref.partition && candidate.kafka.offset === ref.offset);
        if (!remaining) await kafkaConsumer.commitOffsets([{ topic: ref.topic, partition: ref.partition, offset: String(BigInt(ref.offset) + 1n) }]);
      }
      return json(response, 200, { ok: true });
    }
    if (request.method === 'POST' && url.pathname === '/api/rest/fetch') {
      const body = JSON.parse((await readBody(request)).toString('utf8'));
      const target = new URL(String(body.url || ''));
      if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password) throw new Error('Use a valid REST URL without embedded credentials.');
      const result = await requestJson(target.toString(), Boolean(body.verifyTls), target.toString(), { token: String(body.bearerToken || '') || undefined });
      const ids = await addFiles(normalizeNistSourcePayload(result), 'REST source'); return json(response, 200, { count: ids.length, files: ids.map((id) => inboxSummary(inbox.get(id))) });
    }
    if (request.method === 'POST' && url.pathname === '/api/kafka/start') { const result = await startKafka(JSON.parse((await readBody(request)).toString('utf8'))); return json(response, 200, result); }
    if (request.method === 'POST' && url.pathname === '/api/kafka/stop') { await stopKafka(); return json(response, 200, { listening: false }); }
    if (request.method === 'POST' && url.pathname === '/api/backup/settings') {
      const value = JSON.parse((await readBody(request)).toString('utf8'));
      const cron = String(value.cron || '').trim(); parseCron(cron);
      backupSettings = { ...backupSettings, enabled: Boolean(value.enabled), cron, retentionCount: Math.max(1, Math.min(365, Math.floor(Number(value.retentionCount) || 30))) };
      await persistSchedule(); return json(response, 200, backupSettings);
    }
    if (request.method === 'POST' && url.pathname === '/api/backup/run') {
      if (backupBusy) throw new Error('A backup is already running.'); backupBusy = true;
      try { return json(response, 200, await createBackup(false)); } finally { backupBusy = false; }
    }
    return json(response, 404, { error: 'Worker endpoint not found.' });
  } catch (error) { return json(response, 400, { error: error.message }); }
}

async function main() {
  if (!API_TOKEN) throw new Error('WORKER_API_TOKEN must be set.');
  await ensureFolders(); await loadInbox();
  backupSettings = { ...backupSettings, ...await readJson(path.join(DATA_DIR, 'backup-settings.json'), backupSettings) };
  backupSettings.retentionCount = Math.max(1, Math.min(365, Math.floor(Number(backupSettings.retentionCount) || 30)));
  try { parseCron(backupSettings.cron); } catch { backupSettings = { ...backupSettings, enabled: false, cron: '0 2 * * *' }; }
  const server = http.createServer((request, response) => { void route(request, response); });
  server.listen(PORT, '0.0.0.0');
  scheduler = setInterval(() => { void tickBackup(); }, 5000);
  const shutdown = async () => { clearInterval(scheduler); await stopKafka(); server.close(() => process.exit(0)); setTimeout(() => process.exit(1), 5000).unref(); };
  process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
  console.log(`HERTA worker listening on ${PORT}`);
}
main().catch((error) => { console.error(error); process.exit(1); });
