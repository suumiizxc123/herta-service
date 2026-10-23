const { createHertaAuth } = require('../../electron/herta-auth');
const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const { Kafka, logLevel } = require('kafkajs');
const { createZip } = require('../../electron/backup-archive');
const { MAX_FILE_BYTES, MAX_BATCH_BYTES, normalizeNistSourcePayload, safeNistName } = require('../../electron/source-files');
const { parseCron, matchesCron, nextCronDate } = require('../../electron/cron');
const { mergeHttpHeaders } = require('../../electron/http-headers');

const PORT = Number(process.env.PORT || 8127);
const DATA_DIR = process.env.DATA_DIR || '/data';
const BACKUP_DIR = process.env.BACKUP_DIR || '/backups';
const API_TOKEN = process.env.WORKER_API_TOKEN || '';
const INBOX_DIR = path.join(DATA_DIR, 'inbox');
const CONNECTORS_FILE = path.join(DATA_DIR, 'connectors.json');
const SESSION_FILE = path.join(DATA_DIR, 'herta-session.json');
const MAX_BODY = 350 * 1024 * 1024;
const inbox = new Map();
let queueHistory = [];
let connectorHistory = [];
let historyPersistQueue = Promise.resolve();
let session = null;
let kafkaConsumer = null;
let kafkaInfo = null;
let receiverToken = '';
let receiverConnectorId = '';
let telegramConfig = { botToken: '', chatId: '' };
let telegramStatus = { configured: false, connected: false, botName: '', checkedAt: null, lastError: '' };
let backupSettings = { enabled: false, cron: '0 2 * * *', timezone: 'UTC', retentionCount: 30, lastScheduledKey: '', lastRunAt: null, lastRunStatus: 'idle', lastRunMessage: '' };
let backupBusy = false;
let connectors = [];
const connectorRuntimes = new Map();
const restPollers = new Map();
let connectorPersistQueue = Promise.resolve();
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
function inboxSummary(entry) { return { id: entry.id, name: entry.name, size: entry.data?.length || 0, source: entry.source, sourceType: entry.sourceType || (entry.kafka || /^Kafka\b/i.test(entry.source || '') ? 'kafka' : /^REST\b/i.test(entry.source || '') ? 'rest' : 'unknown'), connectorId: entry.connectorId || '', error: entry.error || '', kafka: entry.kafka || null, createdAt: entry.createdAt || null }; }

async function recordQueueEvent(item, outcome, message = '') {
  queueHistory = [{ at: new Date().toISOString(), outcome, name: item?.name || '', source: item?.source || 'Unknown source', size: item?.data?.length || item?.size || 0, message: String(message).slice(0, 300) }, ...queueHistory].slice(0, 2000);
  const snapshot = queueHistory;
  historyPersistQueue = historyPersistQueue.catch(() => {}).then(() => writeJson(path.join(DATA_DIR, 'queue-history.json'), snapshot));
  await historyPersistQueue;
}

async function recordConnectorEvent(connector, event, status, message = '') {
  if (!connector) return;
  connectorHistory = [{ at: new Date().toISOString(), connectorId: connector.id, name: connector.name, type: connector.type, event, status, message: String(message).replace(/Bearer\s+\S+/gi, 'Bearer [redacted]').slice(0, 500) }, ...connectorHistory].slice(0, 2000);
  const snapshot = connectorHistory;
  historyPersistQueue = historyPersistQueue.catch(() => {}).then(() => writeJson(path.join(DATA_DIR, 'connector-history.json'), snapshot));
  await historyPersistQueue;
}

function connectorSummary(connector) {
  const runtime = connectorRuntimes.get(connector.id) || {};
  return { id: connector.id, name: connector.name, type: connector.type, mode: connector.mode || '', active: Boolean(connector.active), status: runtime.status || (connector.active ? 'starting' : 'inactive'), lastRunAt: runtime.lastRunAt || null, lastError: runtime.lastError || '', received: runtime.received || 0, nextRunAt: runtime.nextRunAt || null, lastBackupFile: runtime.lastBackupFile || '', lastBackupBytes: runtime.lastBackupBytes || 0 };
}

async function persistConnectors() {
  const snapshot = JSON.parse(JSON.stringify(connectors));
  connectorPersistQueue = connectorPersistQueue.catch(() => {}).then(() => writeJson(CONNECTORS_FILE, snapshot));
  await connectorPersistQueue;
}

async function stopConnectorRuntimes() {
  for (const timer of restPollers.values()) clearInterval(timer);
  restPollers.clear();
  if (kafkaConsumer) await stopKafka();
  receiverToken = ''; receiverConnectorId = '';
}

function saveRuntime(id, patch) { connectorRuntimes.set(id, { ...(connectorRuntimes.get(id) || {}), ...patch }); }

async function pollRestConnector(connector, { throwOnError = false } = {}) {
  const runtime = connectorRuntimes.get(connector.id) || {};
  try {
    const target = new URL(String(connector.config?.url || ''));
    if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password) throw new Error('Use a valid REST URL without embedded credentials.');
    const response = await requestJson(target.origin, connector.config.verifyTls !== false, target.href, { headers: Object.fromEntries(Object.entries(connector.config.headers || {}).filter(([key]) => !/^(authorization|proxy-authorization)$/i.test(key))) });
    const files = normalizeNistSourcePayload(response);
    const seen = new Set(Array.isArray(connector.seen) ? connector.seen : []);
    const fresh = files.filter((item) => {
      const fingerprint = crypto.createHash('sha256').update(item.name).update(item.data).digest('hex');
      if (seen.has(fingerprint)) return false;
      seen.add(fingerprint); return true;
    });
    connector.seen = [...seen].slice(-3000);
    const ids = fresh.length ? await addFiles(fresh, `REST · ${connector.name}`, null, connector.id) : [];
    const queued = ids.map(id => inboxSummary(inbox.get(id)));
    const results = await importRestFiles(ids, connector.config?.group || '0');
    await persistConnectors();
    const now = new Date(); const interval = Math.max(15, Math.min(86400, Number(connector.config.pollEverySeconds) || 60));
    saveRuntime(connector.id, { status: connector.active ? 'active' : 'inactive', lastRunAt: now.toISOString(), lastError: '', received: (runtime.received || 0) + ids.length, nextRunAt: connector.active ? new Date(now.getTime() + interval * 1000).toISOString() : null });
    if (ids.length) await recordConnectorEvent(connector, 'Files received', 'success', `${ids.length} file(s) from REST.`);
    return { count: ids.length, files: queued, imported: results.filter(r => r.imported).length, results };
  } catch (error) {
    const interval = Math.max(15, Math.min(86400, Number(connector.config?.pollEverySeconds) || 60));
    saveRuntime(connector.id, { status: 'error', lastRunAt: new Date().toISOString(), lastError: error.message, nextRunAt: new Date(Date.now() + interval * 1000).toISOString() });
    if (runtime.lastError !== error.message) await recordConnectorEvent(connector, 'Poll failed', 'error', error.message);
    if (throwOnError) throw error;
  }
}

async function startRestPolling(connector) {
  const seconds = Math.max(15, Math.min(86400, Number(connector.config?.pollEverySeconds) || 60));
  saveRuntime(connector.id, { status: 'starting', lastError: '' });
  await pollRestConnector(connector);
  const timer = setInterval(() => { void pollRestConnector(connector); }, seconds * 1000);
  timer.unref?.(); restPollers.set(connector.id, timer);
}

async function addFiles(files, source, kafka = null, connectorId = '') {
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
    const entry = { id, name, size: item.data.length, source, sourceType: kafka ? 'kafka' : /^REST\b/i.test(source) ? 'rest' : 'unknown', connectorId, kafka, file: filename, createdAt: new Date().toISOString() };
    inbox.set(id, { ...entry, data: item.data, file }); ids.push(id);
  }
  await saveInbox();
  for (const id of ids) await recordQueueEvent(inbox.get(id), 'received');
  void sendTelegramMessage(`HERTA Operations received ${ids.length} NIST file${ids.length === 1 ? '' : 's'} from ${source}.`);
  return ids;
}

async function telegramRequest(method, body = {}, botToken = telegramConfig.botToken) {
  const response = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(12000) });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || !result.ok) throw new Error(String(result.description || `Telegram API request failed (HTTP ${response.status}).`).slice(0, 240));
  return result.result;
}

async function findTelegramChats(botTokenInput) {
  const botToken = String(botTokenInput || '').trim();
  if (!/^\d+:[A-Za-z0-9_-]+$/.test(botToken) || botToken.length > 128) throw new Error('Enter a valid Telegram bot token from BotFather.');
  const bot = await telegramRequest('getMe', {}, botToken);
  const updates = await telegramRequest('getUpdates', { limit: 100, timeout: 0, allowed_updates: ['message', 'edited_message', 'channel_post'] }, botToken);
  const chats = new Map();
  for (const update of updates) {
    const message = update.message || update.edited_message || update.channel_post;
    const chat = message?.chat;
    if (!chat || message.from?.is_bot) continue;
    const id = String(chat.id);
    const name = chat.title || [chat.first_name, chat.last_name].filter(Boolean).join(' ') || chat.username || id;
    const preview = String(message.text || message.caption || '').trim().slice(0, 100);
    const entry = { id, name, type: String(chat.type || 'chat'), username: String(chat.username || ''), preview, lastSeen: message.date ? new Date(message.date * 1000).toISOString() : '' };
    const previous = chats.get(id);
    if (!previous || entry.lastSeen > previous.lastSeen) chats.set(id, entry);
  }
  return { bot: { username: String(bot.username || ''), name: String(bot.first_name || bot.username || 'Telegram bot') }, chats: [...chats.values()].sort((a, b) => b.lastSeen.localeCompare(a.lastSeen)) };
}

function friendlyTelegramError(error) {
  const message = String(error?.message || 'Telegram request failed.');
  if (/can't use getupdates.*webhook|webhook is active|webhook.*getupdates/i.test(message)) return 'This bot already uses a Telegram webhook, so Telegram will not return chats through Find chats. Keep the existing webhook and enter its destination chat ID manually, or ask its owner before switching it to polling.';
  if (/terminated by other getupdates request|another getupdates request/i.test(message)) return 'Another app is already polling updates for this bot. Stop that poller, then choose Find chats again.';
  if (/can't send messages to the bot|cannot send messages to the bot|chat id belongs to a bot|target chat is a bot|this bot account/i.test(message)) return 'This Chat ID belongs to a bot. Telegram bots cannot send private messages to other bots. Use a person chat ID: open your bot from your own Telegram account and send /start. For a group, add your bot and use the group chat ID.';
  if (/chat not found/i.test(message)) return 'Telegram could not find this chat. Open your bot and send /start first, or add it to the target group and send a message there; then use that chat ID.';
  if (/bot was blocked by the user/i.test(message)) return 'The bot is blocked by this user. Unblock the bot in Telegram and send /start before testing again.';
  if (/not enough rights|have no rights/i.test(message)) return 'The bot cannot send messages in this chat. Grant it permission to post messages, then test again.';
  return message;
}

async function checkTelegram() {
  if (!telegramConfig.botToken || !telegramConfig.chatId) throw new Error('Telegram bot token and chat ID are required.');
  try {
    const bot = await telegramRequest('getMe');
    if (/^\d+$/.test(telegramConfig.chatId) && String(bot.id) === telegramConfig.chatId) throw new Error('The Chat ID is this bot account.');
    const chat = await telegramRequest('getChat', { chat_id: telegramConfig.chatId });
    if (chat.is_bot === true) throw new Error('The target Chat ID belongs to a bot.');
    telegramStatus = { configured: true, connected: true, botName: bot.username || bot.first_name || 'Telegram bot', chatName: chat.title || chat.username || chat.first_name || String(telegramConfig.chatId), chatType: chat.type || '', checkedAt: new Date().toISOString(), lastError: '' };
    return telegramStatus;
  } catch (error) {
    const message = friendlyTelegramError(error);
    telegramStatus = { ...telegramStatus, configured: true, connected: false, checkedAt: new Date().toISOString(), lastError: message };
    throw new Error(message);
  }
}

async function sendTelegramMessage(text) {
  if (!telegramConfig.botToken || !telegramConfig.chatId) return;
  try {
    await telegramRequest('sendMessage', { chat_id: telegramConfig.chatId, text });
    telegramStatus = { ...telegramStatus, configured: true, connected: true, checkedAt: new Date().toISOString(), lastError: '' };
  } catch (error) {
    const message = friendlyTelegramError(error);
    telegramStatus = { ...telegramStatus, configured: true, connected: false, checkedAt: new Date().toISOString(), lastError: message };
    console.error('Telegram notification failed:', message);
  }
}

function requestJson(baseUrl, verifyTls, pathname, { method = 'GET', token, json, headers: customHeaders } = {}) {
  const target = new URL(pathname, `${baseUrl}/`);
  const payload = json === undefined ? null : Buffer.from(JSON.stringify(json));
  const headers = { Accept: 'application/json' };
  if (payload) { headers['Content-Type'] = 'application/json'; headers['Content-Length'] = payload.length; }
  if (token) headers.Authorization = `Bearer ${token}`;
  const requestHeaders = mergeHttpHeaders(headers, customHeaders || {});
  const transport = target.protocol === 'https:' ? https : http;
  return new Promise((resolve, reject) => {
    const request = transport.request(target, { method, headers: requestHeaders, rejectUnauthorized: target.protocol !== 'https:' || verifyTls, timeout: 45000 }, (response) => {
      const chunks = []; let size = 0;
      response.on('data', (chunk) => { size += chunk.length; if (size > 64 * 1024 * 1024) { request.destroy(new Error('HERTA response exceeded the 64 MiB limit.')); return; } chunks.push(chunk); });
      response.on('end', () => {
        let result = {};
        try { result = chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}; }
        catch { return reject(new Error(`HERTA returned unreadable JSON (HTTP ${response.statusCode}).`)); }
        if (response.statusCode < 200 || response.statusCode >= 300) return reject(Object.assign(new Error(`HERTA request failed (HTTP ${response.statusCode}). ${String(result.detail || result.message || '').slice(0, 300)}`), { status: response.statusCode }));
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
const credentialsFile = path.join(DATA_DIR, 'herta-credentials.enc');
const credentialKey = crypto.createHash('sha256').update(API_TOKEN).digest();
async function persistSession(value) {
 const { username, password, ...publicSession } = value;
 await writeJson(SESSION_FILE, publicSession);
 if (username && password) {
 const iv = crypto.randomBytes(12), cipher = crypto.createCipheriv('aes-256-gcm', credentialKey, iv);
 const data = Buffer.concat([cipher.update(JSON.stringify({ username, password })), cipher.final()]);
 await writeJson(credentialsFile, { iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), data: data.toString('base64') });
 } else await fs.rm(credentialsFile, { force: true });
}
const hertaAuth = createHertaAuth({ getSession: () => session, login: active => requestJson(active.baseUrl, active.verifyTls, '/api/v1/auth/login', { method: 'POST', json: { username: active.username, password: active.password } }), persist: persistSession });
async function hertaJson(pathname, options) { return hertaAuth.request(active => requestJson(active.baseUrl, active.verifyTls, pathname, { ...options, token: active.token })); }
async function importRestFiles(ids, group = '0') {
 const results = [];
 for (const id of ids) {
 const item = inbox.get(id);
 try {
 const data = await fs.readFile(item.file);
 await hertaJson('/api/v1/subjects/import', { method: 'POST', json: { data: data.toString('base64'), format: 'nist', group: String(group) } });
 await recordConnectorEvent(connectors.find(c => c.id === item.connectorId), 'REST import', 'success', item.name);
 await recordQueueEvent(item, 'imported', 'Automatically imported into HERTA via REST.'); await fs.rm(item.file, { force: true }); inbox.delete(id); await saveInbox();
 results.push({ id, imported: true });
 } catch(error) { item.error = error.message; await recordQueueEvent(item, 'failed', error.message); await saveInbox(); results.push({ id, imported: false, error: error.message }); }
 }
 return results;
}

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
  const hasDatabaseBackup = connectors.some((item) => item.active && item.type === 'postgres');
  if (backupBusy || !backupSettings.enabled || (!session?.token && !hasDatabaseBackup)) return;
  const now = new Date();
  if (!matchesCron(backupSettings.cron, now, backupSettings.timezone)) return;
  const key = now.toISOString().slice(0, 16);
  if (key === backupSettings.lastScheduledKey) return;
  backupBusy = true;
  backupSettings.lastScheduledKey = key;
  backupSettings.lastRunStatus = 'running';
  backupSettings.lastRunAt = new Date().toISOString();
  await persistSchedule();
  try {
    await runBackupTasks(true);
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
  const connectorId = String(input.connectorId || '');
  const config = parseKafkaConfig(input); const consumer = kafkaClient(config).consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
  await consumer.connect(); await consumer.subscribe({ topic: config.topic, fromBeginning: false });
  kafkaConsumer = consumer; kafkaInfo = { connectorId, name: String(input.connectorName || ''), topic: config.topic, groupId: config.groupId, listening: true, brokers: config.brokers, messageFormat: config.messageFormat };
  saveRuntime(connectorId, { status: 'active', lastError: '' });
  await recordConnectorEvent(connectors.find((item) => item.id === connectorId), 'Consumer started', 'active', `Topic ${config.topic} · group ${config.groupId}`);
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
      await addFiles(files, `Kafka · ${topic}`, { topic, partition, offset: message.offset }, connectorId);
      const runtime = connectorRuntimes.get(connectorId) || {};
      saveRuntime(connectorId, { status: 'active', lastRunAt: new Date().toISOString(), lastError: '', received: (runtime.received || 0) + files.length });
      await recordConnectorEvent(connectors.find((item) => item.id === connectorId), 'Messages received', 'success', `${files.length} file(s) from ${topic}.`);
    } catch (error) {
      const runtime = connectorRuntimes.get(connectorId) || {};
      saveRuntime(connectorId, { status: 'error', lastRunAt: new Date().toISOString(), lastError: error.message, received: runtime.received || 0 });
      const id = crypto.randomUUID(); inbox.set(id, { id, name: `${topic}-${partition}-${message.offset}`, size: 0, source: `Kafka · ${topic}`, sourceType: 'kafka', connectorId, kafka: { topic, partition, offset: message.offset }, error: error.message, createdAt: new Date().toISOString() }); await saveInbox();
      await recordQueueEvent(inbox.get(id), 'failed', error.message);
      await recordConnectorEvent(connectors.find((item) => item.id === connectorId), 'Message failed', 'error', error.message);
    }
  } });
  return { listening: true, topic: config.topic, groupId: config.groupId };
}
async function stopKafka() { const consumer = kafkaConsumer; kafkaConsumer = null; kafkaInfo = null; if (consumer) { await consumer.stop().catch(() => {}); await consumer.disconnect().catch(() => {}); } }

async function applyConnectors(nextConnectors) {
  if (!Array.isArray(nextConnectors) || nextConnectors.length > 100) throw new Error('Connector configuration must be a list of at most 100 items.');
  const normalized = nextConnectors.map((item) => {
    const type = String(item.type || ''); const id = String(item.id || '').trim();
    if (!id || !['rest', 'kafka', 'postgres'].includes(type)) throw new Error('Each connector requires an ID and a supported type.');
    return { ...item, id, type, name: String(item.name || type).slice(0, 120), active: Boolean(item.active), config: item.config && typeof item.config === 'object' ? item.config : {} };
  });
  const priorById = new Map(connectors.map((item) => [item.id, item]));
  if (normalized.filter((item) => item.active && item.type === 'kafka').length > 1) throw new Error('Only one Kafka consumer can be active at a time on this worker.');
  if (normalized.filter((item) => item.active && item.type === 'rest' && item.mode === 'receiver').length > 1) throw new Error('Only one REST receiver can be active at a time on this worker.');
  const oldById = new Map(connectors.map((item) => [item.id, item]));
  for (const item of normalized) if (item.type === 'rest' && item.mode === 'receiver' && item.active) item.receiverToken = oldById.get(item.id)?.receiverToken || item.receiverToken || crypto.randomBytes(32).toString('base64url');
  await stopConnectorRuntimes();
  connectors = normalized; await persistConnectors();
  connectorRuntimes.clear();
  for (const connector of connectors.filter((item) => item.active)) {
    try {
      if (connector.type === 'kafka') await startKafka({ ...connector.config, connectorId: connector.id, connectorName: connector.name });
      else if (connector.type === 'rest' && connector.mode === 'receiver') { receiverToken = connector.receiverToken; receiverConnectorId = connector.id; saveRuntime(connector.id, { status: 'active', lastError: '' }); }
      else if (connector.type === 'rest' && connector.mode === 'pull') void startRestPolling(connector);
      else if (connector.type === 'postgres') saveRuntime(connector.id, { status: 'ready', lastError: '' });
    } catch (error) { saveRuntime(connector.id, { status: 'error', lastRunAt: new Date().toISOString(), lastError: error.message }); await recordConnectorEvent(connector, 'Activation failed', 'error', error.message); }
  }
  await persistConnectors();
  for (const connector of normalized) {
    const previous = priorById.get(connector.id);
    if (!previous) await recordConnectorEvent(connector, 'Connector saved', connector.active ? 'active' : 'inactive');
    else if (previous.active !== connector.active) await recordConnectorEvent(connector, connector.active ? 'Activated' : 'Deactivated', connector.active ? 'active' : 'inactive');
  }
  for (const previous of priorById.values()) if (!normalized.some((item) => item.id === previous.id)) await recordConnectorEvent(previous, 'Connector removed', 'removed');
  return connectors.map(connectorSummary);
}

async function createPostgresBackup(connector, scheduled = false) {
  const config = connector.config || {};
  const host = String(config.host || '').trim(); const database = String(config.database || '').trim(); const username = String(config.username || '').trim();
  const port = Math.max(1, Math.min(65535, Number(config.port) || 5432));
  if (!host || !database || !username || !config.password) throw new Error(`PostgreSQL connector “${connector.name}” needs a host, database, username, and password.`);
  const filePart = `${connector.name}-${database}`.toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 70) || 'database';
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const destination = path.join(BACKUP_DIR, `postgres-${filePart}-${stamp}.dump`);
  const temporary = `${destination}.tmp`;
  const args = ['--no-password', '--format=custom', '--compress=6', '--file', temporary, '--host', host, '--port', String(port), '--username', username, '--dbname', database];
  const pgEnv = { ...process.env, PGPASSWORD: String(config.password), PGSSLMODE: config.sslMode === 'disable' ? 'disable' : config.sslMode === 'prefer' ? 'prefer' : 'require' };
  await new Promise((resolve, reject) => {
    const child = spawn('pg_dump', args, { env: pgEnv, stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = ''; const timer = setTimeout(() => child.kill('SIGTERM'), 2 * 60 * 60 * 1000); timer.unref?.();
    child.stderr.on('data', (chunk) => { stderr = `${stderr}${chunk.toString('utf8')}`.slice(-4000); });
    child.once('error', (error) => { clearTimeout(timer); reject(new Error(error.code === 'ENOENT' ? 'The worker image does not have pg_dump installed. Rebuild workers from Dashboard.' : error.message)); });
    child.once('close', (code) => { clearTimeout(timer); if (code === 0) resolve(); else reject(new Error(`pg_dump failed with exit code ${code}: ${stderr.trim().slice(-1200)}`)); });
  }).catch(async (error) => { await fs.rm(temporary, { force: true }); throw error; });
  const stat = await fs.stat(temporary); if (!stat.size) { await fs.rm(temporary, { force: true }); throw new Error('PostgreSQL produced an empty backup file.'); }
  await fs.rename(temporary, destination);
  const dumps = (await fs.readdir(BACKUP_DIR, { withFileTypes: true })).filter((entry) => entry.isFile() && /^postgres-.*\.dump$/i.test(entry.name)).map((entry) => path.join(BACKUP_DIR, entry.name));
  const dated = await Promise.all(dumps.map(async (file) => ({ file, stat: await fs.stat(file) }))); dated.sort((a, b) => b.stat.mtimeMs - a.stat.mtimeMs);
  await Promise.all(dated.slice(backupSettings.retentionCount).map(({ file }) => fs.rm(file, { force: true })));
  const event = { at: new Date().toISOString(), action: scheduled ? 'Automatic PostgreSQL backup created' : 'PostgreSQL backup created', summary: `${connector.name}: ${database} saved as ${path.basename(destination)} (${stat.size} bytes).` };
  const logPath = path.join(DATA_DIR, 'activity-log.json'); const log = await readJson(logPath, []); await writeJson(logPath, [event, ...log].slice(0, 500));
  saveRuntime(connector.id, { status: 'ready', lastRunAt: new Date().toISOString(), lastError: '', lastBackupFile: destination, lastBackupBytes: stat.size });
  await recordConnectorEvent(connector, scheduled ? 'Scheduled backup completed' : 'Backup completed', 'success', `${path.basename(destination)} · ${stat.size} bytes`);
  return { canceled: false, path: destination, bytes: stat.size, database, connectorId: connector.id };
}

async function runBackupTasks(scheduled = false) {
  const results = []; const failures = [];
  const runTask = async (label, task) => {
    const attempts = scheduled ? 3 : 1;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try { results.push(await task()); return; }
      catch (error) {
        if (attempt < attempts) await new Promise((resolve) => setTimeout(resolve, attempt * 2000));
        else {
          failures.push(`${label}: ${error.message}`);
          const connector = connectors.find((item) => item.active && item.type === 'postgres' && label === item.name);
          if (connector) { saveRuntime(connector.id, { status: 'error', lastRunAt: new Date().toISOString(), lastError: error.message }); await recordConnectorEvent(connector, scheduled ? 'Scheduled backup failed' : 'Backup failed', 'error', error.message); }
        }
      }
    }
  };
  if (session?.token) await runTask('HERTA NIST', () => createBackup(scheduled));
  const databases = connectors.filter((item) => item.active && item.type === 'postgres');
  for (const connector of databases) await runTask(connector.name, () => createPostgresBackup(connector, scheduled));
  if (!results.length) throw new Error('Sign in to HERTA or enable a PostgreSQL backup connector first.');
  if (failures.length) throw new Error(`Backup incomplete. ${failures.join(' | ')}`);
  return { ...results[0], paths: results.map((item) => item.path), databaseBackups: results.filter((item) => item.database) };
}

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
    if (!receiverToken) return json(response, 409, { error: 'Activate the REST receiver first.' });
    try {
      const body = await readBody(request); let files;
      if (String(request.headers['content-type'] || '').includes('application/octet-stream')) {
        if (!body.length || body.length > MAX_FILE_BYTES) throw new Error('Raw NIST data must be 1 byte to 32 MiB.');
        files = [{ name: safeNistName(request.headers['x-file-name']), data: body }];
      } else files = normalizeNistSourcePayload(JSON.parse(body.toString('utf8')));
    const ids = await addFiles(files, 'REST receiver', null, receiverConnectorId);
    if (receiverConnectorId) {
      const runtime = connectorRuntimes.get(receiverConnectorId) || {};
      saveRuntime(receiverConnectorId, { status: 'active', lastRunAt: new Date().toISOString(), lastError: '', received: (runtime.received || 0) + ids.length });
      await recordConnectorEvent(connectors.find((item) => item.id === receiverConnectorId), 'Files received', 'success', `${ids.length} file(s) via REST receiver.`);
    }
    const results = await importRestFiles(ids, connectors.find(c => c.id === receiverConnectorId)?.config?.group || '0'); return json(response, results.every(r => r.imported) ? 200 : 202, { accepted: ids.length, ids, imported: results.filter(r => r.imported).length, results });
    } catch (error) { return json(response, 400, { error: error.message }); }
  }
  if (!requireApi(request, response)) return;
  try {
    if (request.method === 'GET' && url.pathname === '/api/status') return json(response, 200, {
      ok: true, service: 'herta-worker', startedAt, authenticated: Boolean(session?.token), authentication: { automaticRenewal: Boolean(session?.username && session?.password), lastRenewedAt: session?.authenticatedAt || null, nextRenewalAt: session?.username ? new Date((session.authenticatedAt || 0) + 3600000).toISOString() : null }, backup: { enabled: backupSettings.enabled, cron: backupSettings.cron, timezone: backupSettings.timezone, retentionCount: backupSettings.retentionCount, nextRun: backupSettings.enabled ? nextCronDate(backupSettings.cron, new Date(), backupSettings.timezone).toISOString() : null, running: backupBusy, lastRunAt: backupSettings.lastRunAt, lastRunStatus: backupSettings.lastRunStatus, lastRunMessage: backupSettings.lastRunMessage }, kafka: kafkaInfo || { listening: false }, receiver: { running: Boolean(receiverToken), connectorId: receiverConnectorId, port: 18127, authenticationRequired: false, url: 'http://127.0.0.1:18127/api/v1/nist' }, telegram: telegramStatus, connectors: connectors.map(connectorSummary), inbox: [...inbox.values()].map(inboxSummary),
    });
    if (request.method === 'POST' && url.pathname === '/api/session') {
      const body = JSON.parse((await readBody(request)).toString('utf8'));
      const base = new URL(String(body.baseUrl || ''));
      if (!['http:', 'https:'].includes(base.protocol) || !body.token) throw new Error('A valid HERTA base URL and access token are required.');
      session = { baseUrl: base.toString().replace(/\/$/, ''), token: String(body.token), verifyTls: Boolean(body.verifyTls), username: String(body.username || ''), password: String(body.password || ''), authenticatedAt: Number(body.authenticatedAt) || Date.now() };
      await persistSession(session);
      return json(response, 200, { authenticated: true });
    }
    if (request.method === 'POST' && url.pathname === '/api/herta/request') { const body = JSON.parse((await readBody(request)).toString('utf8')); if (!/^\/api\/v1\/[a-z]/i.test(body.pathname) || body.pathname.includes('://') || body.pathname.includes('..')) throw new Error('Unsupported HERTA resource.'); return json(response, 200, await hertaJson(body.pathname, body.options)); }
    if (request.method === 'GET' && url.pathname === '/api/connectors') return json(response, 200, connectors.map(connectorSummary));
    if (request.method === 'PUT' && url.pathname === '/api/connectors') return json(response, 200, await applyConnectors(JSON.parse((await readBody(request)).toString('utf8'))));
    if (request.method === 'POST' && url.pathname === '/api/connectors/test') {
      const body = JSON.parse((await readBody(request)).toString('utf8') || '{}');
      const connector = connectors.find((item) => item.id === String(body.connectorId || ''));
      if (!connector || connector.type !== 'rest' || connector.mode !== 'pull') throw new Error('Choose a saved REST pull connector to test.');
      const target = new URL(String(connector.config?.url || ''));
      if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password) throw new Error('Use a valid REST URL without embedded credentials.');
      const payload = await requestJson(target.origin, connector.config.verifyTls !== false, target.href, { headers: Object.fromEntries(Object.entries(connector.config.headers || {}).filter(([key]) => !/^(authorization|proxy-authorization)$/i.test(key))) });
      const files = normalizeNistSourcePayload(payload);
      return json(response, 200, { ok: true, count: files.length, bytes: files.reduce((total, item) => total + item.data.length, 0), checkedAt: new Date().toISOString() });
    }
    if (request.method === 'DELETE' && url.pathname === '/api/session') { session = null; await fs.rm(SESSION_FILE, { force: true }); await fs.rm(credentialsFile, { force: true }); return json(response, 200, { authenticated: false }); }
    if (request.method === 'POST' && url.pathname === '/api/receiver/start') { const body = JSON.parse((await readBody(request)).toString('utf8') || '{}'); if (!receiverToken) receiverToken = crypto.randomBytes(32).toString('base64url'); receiverConnectorId = String(body.connectorId || ''); return json(response, 200, { running: true, connectorId: receiverConnectorId, port: 18127, authenticationRequired: false, url: 'http://127.0.0.1:18127/api/v1/nist' }); }
    if (request.method === 'POST' && url.pathname === '/api/receiver/stop') { receiverToken = ''; receiverConnectorId = ''; return json(response, 200, { running: false }); }
    if (request.method === 'GET' && url.pathname === '/api/telegram/status') return json(response, 200, telegramStatus);
    if (request.method === 'POST' && url.pathname === '/api/telegram/chats') {
      const body = JSON.parse((await readBody(request)).toString('utf8'));
      try { return json(response, 200, await findTelegramChats(body.botToken)); }
      catch (error) { throw new Error(friendlyTelegramError(error)); }
    }
    if (request.method === 'POST' && url.pathname === '/api/telegram/config') {
      const body = JSON.parse((await readBody(request)).toString('utf8'));
      telegramConfig = { botToken: String(body.botToken || '').trim(), chatId: String(body.chatId || '').trim() };
      if (!telegramConfig.botToken || !telegramConfig.chatId) { telegramConfig = { botToken: '', chatId: '' }; await fs.rm(path.join(DATA_DIR, 'telegram-settings.json'), { force: true }); telegramStatus = { configured: false, connected: false, botName: '', checkedAt: new Date().toISOString(), lastError: '' }; return json(response, 200, telegramStatus); }
      await writeJson(path.join(DATA_DIR, 'telegram-settings.json'), telegramConfig);
      return json(response, 200, await checkTelegram());
    }
    if (request.method === 'POST' && url.pathname === '/api/telegram/test') {
      await checkTelegram();
      try { await telegramRequest('sendMessage', { chat_id: telegramConfig.chatId, text: 'HERTA Operations Telegram connector test succeeded.' }); }
      catch (error) {
        const message = friendlyTelegramError(error);
        telegramStatus = { ...telegramStatus, connected: false, checkedAt: new Date().toISOString(), lastError: message };
        throw new Error(message);
      }
      telegramStatus = { ...telegramStatus, connected: true, checkedAt: new Date().toISOString(), lastError: '' };
      return json(response, 200, { ...telegramStatus, sent: true });
    }
    if (request.method === 'GET' && url.pathname === '/api/inbox') return json(response, 200, [...inbox.values()].map(inboxSummary));
    if (request.method === 'GET' && url.pathname === '/api/queue/history') return json(response, 200, queueHistory);
    if (request.method === 'GET' && url.pathname === '/api/connectors/logs') return json(response, 200, connectorHistory);
    if (request.method === 'GET' && url.pathname === '/api/audit') return json(response, 200, await readJson(path.join(DATA_DIR, 'activity-log.json'), []));
    if (request.method === 'POST' && url.pathname.startsWith('/api/inbox/') && url.pathname.endsWith('/complete')) {
      const id = url.pathname.split('/')[3]; const item = inbox.get(id);
      if (!item) return json(response, 404, { error: 'Inbox item not found.' });
      await recordQueueEvent(item, 'imported', 'Imported into HERTA.');
      if (item.file) await fs.unlink(item.file).catch(() => {}); inbox.delete(id); await saveInbox();
      if (item.kafka && kafkaConsumer) {
        const ref = item.kafka;
        const remaining = [...inbox.values()].some((candidate) => candidate.kafka && candidate.kafka.topic === ref.topic && candidate.kafka.partition === ref.partition && candidate.kafka.offset === ref.offset);
        if (!remaining) await kafkaConsumer.commitOffsets([{ topic: ref.topic, partition: ref.partition, offset: String(BigInt(ref.offset) + 1n) }]);
      }
      return json(response, 200, { ok: true, outcome: 'imported' });
    }
    if (request.method === 'GET' && url.pathname.startsWith('/api/inbox/') && url.pathname.endsWith('/content')) {
      const id = url.pathname.split('/')[3]; const item = inbox.get(id); if (!item?.data) return json(response, 404, { error: 'Inbox file is unavailable.' });
      return json(response, 200, { ...inboxSummary(item), fileContent: item.data.toString('base64') });
    }
    if (request.method === 'DELETE' && url.pathname.startsWith('/api/inbox/')) {
      const id = url.pathname.split('/').pop(); const item = inbox.get(id);
      if (item) await recordQueueEvent(item, 'discarded', 'Removed from the received inbox.');
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
      if (body.connectorId) {
        const connector = connectors.find((item) => item.id === String(body.connectorId) && item.type === 'rest' && item.mode !== 'receiver');
        if (!connector) throw new Error('Select a saved REST API pull source.');
        return json(response, 200, await pollRestConnector(connector, { throwOnError: true }));
      }
      const target = new URL(String(body.url || ''));
      if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password) throw new Error('Use a valid REST URL without embedded credentials.');
      const result = await requestJson(target.toString(), Boolean(body.verifyTls), target.toString(), {});
      const ids = await addFiles(normalizeNistSourcePayload(result), 'REST source'); const files = ids.map(id => inboxSummary(inbox.get(id))); const results = await importRestFiles(ids, body.group || '0'); return json(response, 200, { count: ids.length, files, results, imported: results.filter(r => r.imported).length });
    }
    if (request.method === 'POST' && url.pathname === '/api/kafka/start') { const result = await startKafka(JSON.parse((await readBody(request)).toString('utf8'))); return json(response, 200, result); }
    if (request.method === 'POST' && url.pathname === '/api/kafka/stop') { await stopKafka(); return json(response, 200, { listening: false }); }
    if (request.method === 'POST' && url.pathname === '/api/backup/settings') {
      const value = JSON.parse((await readBody(request)).toString('utf8'));
      const cron = String(value.cron || '').trim(); parseCron(cron);
      const timezone = String(value.timezone || backupSettings.timezone || 'UTC');
      try { new Intl.DateTimeFormat('en-US', { timeZone: timezone }).format(new Date()); } catch { throw new Error('Choose a valid local time zone before saving the backup schedule.'); }
      backupSettings = { ...backupSettings, enabled: Boolean(value.enabled), cron, timezone, retentionCount: Math.max(1, Math.min(365, Math.floor(Number(value.retentionCount) || 30))) };
      await persistSchedule(); return json(response, 200, backupSettings);
    }
    if (request.method === 'POST' && url.pathname === '/api/backup/run') {
      if (backupBusy) throw new Error('A backup is already running.'); backupBusy = true;
      try { return json(response, 200, await runBackupTasks(false)); } finally { backupBusy = false; }
    }
    return json(response, 404, { error: 'Worker endpoint not found.' });
  } catch (error) { return json(response, 400, { error: error.message }); }
}

async function main() {
  if (!API_TOKEN) throw new Error('WORKER_API_TOKEN must be set.');
  await ensureFolders(); await loadInbox();
  queueHistory = await readJson(path.join(DATA_DIR, 'queue-history.json'), []);
  connectorHistory = await readJson(path.join(DATA_DIR, 'connector-history.json'), []);
  session = await readJson(SESSION_FILE, null);
 if (session) { try { const saved = await readJson(credentialsFile, null); if (saved) { const decipher = crypto.createDecipheriv('aes-256-gcm', credentialKey, Buffer.from(saved.iv, 'base64')); decipher.setAuthTag(Buffer.from(saved.tag, 'base64')); Object.assign(session, JSON.parse(Buffer.concat([decipher.update(Buffer.from(saved.data, 'base64')), decipher.final()]).toString())); } } catch { console.error('Saved HERTA credentials cannot be decrypted; sign in again.'); } }
  if (session && (!session.token || !session.baseUrl)) { session = null; await fs.rm(SESSION_FILE, { force: true }); }
  connectors = await readJson(CONNECTORS_FILE, []);
  backupSettings = { ...backupSettings, ...await readJson(path.join(DATA_DIR, 'backup-settings.json'), backupSettings) };
  try { new Intl.DateTimeFormat('en-US', { timeZone: backupSettings.timezone || 'UTC' }).format(new Date()); } catch { backupSettings.timezone = 'UTC'; }
  backupSettings.retentionCount = Math.max(1, Math.min(365, Math.floor(Number(backupSettings.retentionCount) || 30)));
  try { parseCron(backupSettings.cron); } catch { backupSettings = { ...backupSettings, enabled: false, cron: '0 2 * * *' }; }
  telegramConfig = await readJson(path.join(DATA_DIR, 'telegram-settings.json'), telegramConfig);
  await applyConnectors(connectors);
  const server = http.createServer((request, response) => { void route(request, response); });
  server.listen(PORT, '0.0.0.0');
  if (telegramConfig.botToken && telegramConfig.chatId) void checkTelegram().catch(() => {});
  scheduler = setInterval(() => { void tickBackup(); if (session?.username) void hertaAuth.renew().catch(error => console.error("HERTA renewal:", error.message)); }, 5000);
  const shutdown = async () => { clearInterval(scheduler); await stopKafka(); server.close(() => process.exit(0)); setTimeout(() => process.exit(1), 5000).unref(); };
  process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
  console.log(`HERTA worker listening on ${PORT}`);
}
main().catch((error) => { console.error(error); process.exit(1); });
