const { createHertaAuth } = require('./herta-auth');
const { app, BrowserWindow, dialog, ipcMain, Menu, safeStorage } = require('electron');
const fs = require('node:fs/promises');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const http = require('node:http');
const https = require('node:https');
const crypto = require('node:crypto');
const { unzipSync } = require('fflate');
const { Kafka, logLevel } = require('kafkajs');
const { createZip, inspectBackupArchive } = require('./backup-archive');
const { MAX_FILES, safeNistName, normalizeNistSourcePayload } = require('./source-files');
const { mergeHttpHeaders } = require('./http-headers');
const { profileToText, subjectGroup, extractSubjectImage, extractImageFromNistBytes, summarizeSubject, makeSubjectDetails } = require('./subject-record');
const { parseCron, nextCronDate } = require('./cron');
const DockerPlatform = require('./docker-platform');

const APP_PAGE = path.resolve(__dirname, '../dist/renderer/index.html');
const DEV_SERVER_URL = process.env.VITE_DEV_SERVER_URL;
const APP_URL = DEV_SERVER_URL || pathToFileURL(APP_PAGE).href;
const MAX_FILE_BYTES = 32 * 1024 * 1024;
const MAX_BATCH_BYTES = 256 * 1024 * 1024;
const MAX_EXPORT_BYTES = 256 * 1024 * 1024;
const MAX_FILE_COUNT = 500;
const MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
const MAX_BACKUP_BYTES = 256 * 1024 * 1024;
const MAX_AUDIT_EVENTS = 500;
const SUBJECT_PAGE_SIZE = 50;
const sourceInbox = new Map();

let mainWindow;
let hertaSession = null;
const selectedFiles = new Map();
let auditCache;
let subjectCache = [];
const subjectImageCache = new Map();
let kafkaConsumer = null;
let kafkaCurrent = null;
let restReceiver = null;

const defaultBackupSettings = () => ({ enabled: false, directory: '', cron: '0 2 * * *', timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC', retentionCount: 30 });

function sessionFile() { return path.join(app.getPath('userData'), 'herta-session.enc'); }
function connectorSettingsFile() { return path.join(app.getPath('userData'), 'connector-settings.enc'); }
const defaultConnectorSettings = () => ({ rest: { url: '', bearerToken: '', verifyTls: true }, kafka: { brokers: 'localhost:9092', topic: 'herta.nist', groupId: 'herta-operations-desktop', tls: false, verifyTls: true, mechanism: 'none', username: '', password: '', messageFormat: 'json-base64', partitions: '1', replicationFactor: '1' }, telegram: { botToken: '', chatId: '' }, connectors: [] });

function telegramUserError(error) {
  const message = String(error?.message || error || 'Telegram request failed.');
  if (/can't use getupdates.*webhook|webhook is active|webhook.*getupdates/i.test(message)) return 'This bot already uses a Telegram webhook, so Telegram cannot discover chats through Find chats. Keep the existing webhook and enter the chat ID manually, or ask its owner before changing the bot setup.';
  if (/terminated by other getupdates request|another getupdates request/i.test(message)) return 'Another app is polling updates for this bot. Stop that poller, then click Find chats again.';
  if (/can't send messages to the bot|cannot send messages to the bot|bot can't message bots|chat id belongs to a bot|this bot account/i.test(message)) return 'Telegram cannot send a private message to another bot. The Chat ID must belong to a person or a group. Open your bot from your personal Telegram account, press Start or send /start, then use that person chat ID. For a group, add your bot and use the group chat ID.';
  if (/chat not found/i.test(message)) return 'Telegram cannot find this chat yet. Open the bot from your personal account and send /start first, or add the bot to the group and send a message there. Then enter that person or group chat ID.';
  return message.replace(/^Error invoking remote method '[^']+':\s*/i, '').replace(/^Error:\s*/i, '');
}

async function readConnectorSettings() {
  const fallback = defaultConnectorSettings();
  if (!await safeStorage.isAsyncEncryptionAvailable()) return fallback;
  try {
    const encrypted = await fs.readFile(connectorSettingsFile());
    const decrypted = await safeStorage.decryptStringAsync(encrypted);
    const saved = JSON.parse(decrypted.result);
    let connectors = Array.isArray(saved.connectors) ? saved.connectors : null;
    if (!connectors) {
      connectors = [];
      if (saved.rest?.url) connectors.push({ id: 'legacy-rest-source', name: 'REST source', type: 'rest', mode: 'pull', active: false, config: { ...fallback.rest, ...saved.rest }, createdAt: new Date().toISOString() });
      if (saved.kafka && (saved.kafka.brokers !== fallback.kafka.brokers || saved.kafka.topic !== fallback.kafka.topic)) connectors.push({ id: 'legacy-kafka-source', name: 'Kafka source', type: 'kafka', mode: 'consumer', active: false, config: { ...fallback.kafka, ...saved.kafka }, createdAt: new Date().toISOString() });
    }
    return { ...fallback, ...saved, rest: { ...fallback.rest, ...saved.rest }, kafka: { ...fallback.kafka, ...saved.kafka }, telegram: { ...fallback.telegram, ...saved.telegram }, connectors };
  } catch { return fallback; }
}

async function saveConnectorSettings(settings) {
  if (!await safeStorage.isAsyncEncryptionAvailable()) throw new Error('Secure credential storage is unavailable on this computer.');
  const defaults = defaultConnectorSettings();
  const next = { rest: { ...defaults.rest, ...settings?.rest }, kafka: { ...defaults.kafka, ...settings?.kafka }, telegram: { ...defaults.telegram, ...settings?.telegram }, connectors: Array.isArray(settings?.connectors) ? settings.connectors : [] };
  const file = connectorSettingsFile(); await fs.mkdir(path.dirname(file), { recursive: true });
  const encrypted = await safeStorage.encryptStringAsync(JSON.stringify(next));
  const temporary = `${file}.tmp`; await fs.writeFile(temporary, encrypted, { mode: 0o600 }); await fs.rename(temporary, file);
  return next;
}

async function persistHertaSession(session) {
  const destination = sessionFile();
  if (!session) { await fs.rm(destination, { force: true }); return false; }
  if (!await safeStorage.isAsyncEncryptionAvailable()) return false;
  await fs.mkdir(path.dirname(destination), { recursive: true });
  const encrypted = await safeStorage.encryptStringAsync(JSON.stringify(session));
  const temporary = `${destination}.tmp`;
  await fs.writeFile(temporary, encrypted, { mode: 0o600 });
  await fs.rename(temporary, destination);
  return true;
}

async function restoreHertaSession() {
  if (!await safeStorage.isAsyncEncryptionAvailable()) return;
  try {
    const encrypted = await fs.readFile(sessionFile());
    const decrypted = await safeStorage.decryptStringAsync(encrypted);
    const restored = JSON.parse(decrypted.result);
    if (!restored?.token || !restored?.baseUrl) throw new Error('Invalid saved session.');
    const baseUrl = normalizeBaseUrl(restored.baseUrl);
    const candidate = { ...restored, baseUrl, token: String(restored.token), verifyTls: Boolean(restored.verifyTls) };
    hertaSession = candidate;
    const response = await desktopAuth.request(active => requestJson(baseUrl, active.verifyTls, '/api/v1/subjects?offset=0&size=1', { token: active.token }));
    if (!Array.isArray(response.subjects)) throw new Error('Saved HERTA session could not be verified.');
    hertaSession = candidate;
    await DockerPlatform.setWorkerSession(app, hertaSession);
  } catch (error) {
    hertaSession = null;
    if (error.status === 401 || error.status === 403 || error instanceof SyntaxError) await fs.rm(sessionFile(), { force: true }).catch(() => {});
  }
}

function connectorsForWorker(connectors, status) {
  const localKafka = (status?.services || []).some((service) => service.Service === 'kafka' && /running|up/i.test(service.State || service.Status || ''));
  return connectors.map((connector) => {
    if (connector.type === 'rest' && connector.mode === 'pull') {
      try {
        const url = new URL(String(connector.config?.url || ''));
        if (url.hostname === 'localhost' || url.hostname === '127.0.0.1') url.hostname = 'host.docker.internal';
        return { ...connector, config: { ...connector.config, url: url.toString() } };
      } catch { return connector; }
    }
    if (connector.type !== 'kafka') return connector;
    const brokers = String(connector.config?.brokers || '').split(',').map((value) => {
      const broker = value.trim();
      if (localKafka && /^(localhost|127\.0\.0\.1):19092$/.test(broker)) return 'kafka:9092';
      return broker.replace(/^(localhost|127\.0\.0\.1):(\d+)$/, 'host.docker.internal:$2');
    }).join(',');
    return { ...connector, config: { ...connector.config, brokers } };
  });
}

async function readBackupSettings() {
  try {
    const value = JSON.parse(await fs.readFile(path.join(app.getPath('userData'), 'backup-settings.json'), 'utf8'));
    return { ...defaultBackupSettings(), ...value, enabled: Boolean(value.enabled), directory: typeof value.directory === 'string' ? value.directory : '', cron: typeof value.cron === 'string' ? value.cron : '0 2 * * *', timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC', retentionCount: Math.max(1, Math.min(365, Math.floor(Number(value.retentionCount) || 30))) };
  } catch { return defaultBackupSettings(); }
}

async function saveBackupSettings(settings) {
  const next = { enabled: Boolean(settings?.enabled), directory: String(settings?.directory || ''), cron: String(settings?.cron || '').trim(), timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC', retentionCount: Math.max(1, Math.min(365, Math.floor(Number(settings?.retentionCount) || 30))) };
  parseCron(next.cron);
  if (next.directory) {
    next.directory = path.resolve(next.directory);
    const stat = await fs.stat(next.directory).catch(() => null);
    if (!stat?.isDirectory()) throw new Error('Choose an existing backup destination folder.');
  }
  if (next.enabled && !next.directory) throw new Error('Choose a backup destination before enabling automatic backups.');
  const folder = app.getPath('userData');
  await fs.mkdir(folder, { recursive: true });
  const destination = path.join(folder, 'backup-settings.json');
  const temporary = `${destination}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(next, null, 2), 'utf8');
  await fs.rename(temporary, destination);
  return next;
}

async function readAuditEvents() {
  if (auditCache) return auditCache;
  try {
    const text = await fs.readFile(path.join(app.getPath('userData'), 'activity-log.json'), 'utf8');
    const parsed = JSON.parse(text);
    auditCache = Array.isArray(parsed) ? parsed.slice(0, MAX_AUDIT_EVENTS) : [];
  } catch { auditCache = []; }
  return auditCache;
}

async function recordActivity(action, summary) {
  try {
    const previous = await readAuditEvents();
    const next = [{ at: new Date().toISOString(), action, summary }, ...previous].slice(0, MAX_AUDIT_EVENTS);
    const folder = app.getPath('userData');
    const destination = path.join(folder, 'activity-log.json');
    const temporary = `${destination}.tmp`;
    await fs.mkdir(folder, { recursive: true });
    await fs.writeFile(temporary, JSON.stringify(next, null, 2), 'utf8');
    await fs.rename(temporary, destination);
    auditCache = next;
  } catch { /* An audit disk error must not interrupt HERTA operations. */ }
}

function assertTrusted(event) {
  if (!mainWindow || event.sender !== mainWindow.webContents || !event.senderFrame?.url.startsWith(APP_URL)) {
    throw new Error('This action is available only from the HERTA desktop window.');
  }
}

function assertSignedIn() {
  if (!hertaSession?.token) throw new Error('Sign in to HERTA to continue.');
  return hertaSession;
}

function normalizeBaseUrl(value) {
  let target;
  try { target = new URL(String(value || '').trim()); }
  catch { throw new Error('Enter a valid HERTA server URL.'); }
  if (!['http:', 'https:'].includes(target.protocol) || !target.hostname || target.username || target.password || target.search || target.hash) {
    throw new Error('Use an http:// or https:// HERTA URL without credentials, query, or fragment.');
  }
  return target.toString().replace(/\/$/, '');
}

function requestJson(baseUrl, verifyTls, pathname, { method = 'GET', token, json, headers: customHeaders } = {}) {
  const target = new URL(pathname, `${baseUrl}/`);
  const payload = json === undefined ? null : Buffer.from(JSON.stringify(json), 'utf8');
  const headers = { Accept: 'application/json', 'Accept-Language': 'en' };
  if (payload) {
    headers['Content-Type'] = 'application/json';
    headers['Content-Length'] = payload.length;
  }
  if (token) headers.Authorization = `Bearer ${token}`;
  const requestHeaders = mergeHttpHeaders(headers, customHeaders || {});
  const transport = target.protocol === 'https:' ? https : http;

  return new Promise((resolve, reject) => {
    const request = transport.request(target, {
      method,
      headers: requestHeaders,
      rejectUnauthorized: target.protocol !== 'https:' || verifyTls,
      timeout: 45_000,
    }, (response) => {
      const chunks = [];
      let size = 0;
      response.on('data', (chunk) => {
        size += chunk.length;
        if (size > MAX_RESPONSE_BYTES) {
          request.destroy(new Error('HERTA response exceeded the 64 MiB limit.'));
          return;
        }
        chunks.push(chunk);
      });
      response.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let data = {};
        try { data = text ? JSON.parse(text) : {}; }
        catch { return reject(new Error(`HERTA returned an unreadable response (HTTP ${response.statusCode}).`)); }
        if (response.statusCode < 200 || response.statusCode >= 300) {
          const detail = [data.detail, data.message, data.error_description, data.error]
            .find((value) => typeof value === 'string' && value.trim());
          const safeDetail = detail ? ` ${detail.trim().slice(0, 400)}` : '';
          const error = response.statusCode === 401 || response.statusCode === 403
            ? new Error(`HERTA rejected the sign-in (HTTP ${response.statusCode}). Check the username and password.${safeDetail}`)
            : new Error(`HERTA request failed (HTTP ${response.statusCode}).${safeDetail}`);
          error.status = response.statusCode;
          return reject(error);
        }
        resolve(data);
      });
    });
    request.on('timeout', () => request.destroy(new Error('The HERTA request timed out.')));
    request.on('error', (error) => {
      if (['CERT_HAS_EXPIRED', 'DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'UNABLE_TO_GET_ISSUER_CERT', 'UNABLE_TO_GET_ISSUER_CERT_LOCALLY', 'ERR_TLS_CERT_ALTNAME_INVALID'].includes(error.code)) {
        reject(new Error('HERTA TLS certificate could not be verified. Enable the self-signed certificate option only for a trusted HERTA server.'));
      } else {
        reject(new Error(`Could not reach HERTA: ${error.message}`));
      }
    });
    if (payload) request.write(payload);
    request.end();
  });
}

const desktopAuth = createHertaAuth({ getSession: () => hertaSession, login: active => requestJson(active.baseUrl, active.verifyTls, '/api/v1/auth/login', { method: 'POST', json: { username: active.username, password: active.password } }), persist: persistHertaSession });
async function authorizedJson(pathname, options = {}) {
 assertSignedIn();
 const status = await DockerPlatform.workerRequest(app, '/api/status').catch(() => null);
 if (status?.ok && status.authenticated) return DockerPlatform.workerRequest(app, '/api/herta/request', { method: 'POST', body: { pathname, options }, timeout: 150000 });
 return desktopAuth.request(active => requestJson(active.baseUrl, active.verifyTls, pathname, { ...options, token: active.token }));
}

function csvEscape(value) {
  const text = String(value ?? '');
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function toCsv(rows, columns) {
  return `\uFEFF${[columns.join(','), ...rows.map((row) => columns.map((column) => csvEscape(row[column])).join(','))].join('\r\n')}`;
}

function safeFilename(value) {
  return String(value).replace(/[^\p{L}\p{N}_.-]/gu, '_').slice(0, 100) || 'subject';
}

async function getAllSubjects() {
  const subjects = [];
  let offset = 0;
  let total = Infinity;
  while (offset < total) {
    const page = await authorizedJson(`/api/v1/subjects?offset=${offset}&size=${SUBJECT_PAGE_SIZE}`);
    if (!Array.isArray(page.subjects)) throw new Error('HERTA returned an unexpected subjects list.');
    subjects.push(...page.subjects.filter((subject) => subject && typeof subject === 'object'));
    const pageSize = page.subjects.length;
    total = Number.isFinite(Number(page.count)) ? Number(page.count) : offset + pageSize;
    offset += pageSize;
    if (!pageSize) break;
  }
  subjectCache = subjects;
  return subjects;
}

async function getSubjectDetails(code) {
  assertSignedIn();
  const subjects = subjectCache.length ? subjectCache : await getAllSubjects();
  const subject = subjects.find((item) => String(item.code || '') === String(code));
  if (!subject) throw new Error('Subject was not found. Refresh the HERTA subject list and try again.');
  const details = makeSubjectDetails(subject);
  if (details.imageDataUrl) details.imageSource = 'HERTA subject record';
  if (!details.imageDataUrl && subjectImageCache.has(details.code)) {
    details.imageDataUrl = subjectImageCache.get(details.code);
    details.imageSource = 'Previously exported NIST image';
  }
  if (!details.imageDataUrl) {
    try {
      const response = await authorizedJson(`/api/v1/subjects/${encodeURIComponent(details.code)}/export`, { method: 'POST', json: { format: 'nist' } });
      const encoded = String(response.fileContent || '').replace(/\s+/g, '');
      if (encoded && encoded.length <= Math.ceil(MAX_FILE_BYTES * 4 / 3) && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) {
        details.imageDataUrl = extractImageFromNistBytes(Buffer.from(encoded, 'base64'));
        if (details.imageDataUrl) {
          details.imageSource = 'HERTA NIST export';
          subjectImageCache.set(details.code, details.imageDataUrl);
        }
      }
    } catch { /* Keep the record view usable when this HERTA installation cannot export NIST files. */ }
  }
  return details;
}

async function profileCheck() {
  const subjects = await getAllSubjects();
  const results = subjects.map((subject) => {
    const profile = profileToText(subject.profile);
    let isJson = false;
    try { JSON.parse(profile); isJson = Boolean(profile.trim()); } catch { /* Return metadata only. */ }
    return {
      code: String(subject.code || ''),
      profile_length: profile.length,
      is_json: isJson,
      is_base64_like: Boolean(profile) && /^[A-Za-z0-9+/=\r\n]+$/.test(profile),
    };
  });
  await recordActivity('Profile check', `${results.length} subject profiles checked; metadata only.`);
  return results;
}

async function exportSubjects(codes, { backup = false, destinationDirectory = '', scheduled = false } = {}) {
  assertSignedIn();
  if (!Array.isArray(codes) || !codes.length) throw new Error('Select at least one subject to export.');
  if (codes.length > MAX_FILE_COUNT) throw new Error(`Export up to ${MAX_FILE_COUNT} subjects at a time.`);
  const rows = [];
  const files = [];
  const names = new Set();
  let totalBytes = 0;
  for (const code of [...new Set(codes.map((value) => String(value).trim()).filter(Boolean))]) {
    try {
      const response = await authorizedJson(`/api/v1/subjects/${encodeURIComponent(code)}/export`, { method: 'POST', json: { format: 'nist' } });
      const encoded = String(response.fileContent || '').replace(/\s+/g, '');
      if (!encoded || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) throw new Error('HERTA returned an invalid NIST payload.');
      const data = Buffer.from(encoded, 'base64');
      if (!data.length) throw new Error('HERTA returned an empty NIST file.');
      if (totalBytes + data.length > MAX_EXPORT_BYTES) throw new Error('Export package exceeds the 256 MiB limit.');
      const base = safeFilename(code);
      let filename = `${base}.nist`;
      let suffix = 2;
      while (names.has(filename.toLowerCase())) filename = `${base}-${suffix++}.nist`;
      names.add(filename.toLowerCase());
      files.push({ name: filename, data });
      totalBytes += data.length;
      rows.push({ Code: code, Status: 'Success', Bytes: data.length, Details: filename });
    } catch (error) {
      rows.push({ Code: code, Status: 'Failed', Bytes: 0, Details: error.message });
    }
  }
  files.push({ name: 'export-report.csv', data: toCsv(rows, ['Code', 'Status', 'Bytes', 'Details']) });
  if (backup) {
    files.push({ name: 'backup-manifest.json', data: Buffer.from(JSON.stringify({
      application: 'HERTA Operations Desktop',
      version: app.getVersion(),
      createdAt: new Date().toISOString(),
      subjectCount: codes.length,
      successfulExports: rows.filter((row) => row.Status === 'Success').length,
      format: 'NIST',
    }, null, 2), 'utf8') });
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  let filePath;
  if (destinationDirectory) {
    const directoryStat = await fs.stat(destinationDirectory).catch(() => null);
    if (!directoryStat?.isDirectory()) throw new Error('The configured backup destination is unavailable. Choose another folder.');
    filePath = path.join(destinationDirectory, `${backup ? 'herta-snapshot' : 'herta-export'}-${stamp}.zip`);
  } else {
    const result = await dialog.showSaveDialog(mainWindow, {
      title: backup ? 'Save HERTA snapshot backup' : 'Save HERTA NIST exports',
      defaultPath: path.join(app.getPath('downloads'), backup ? `herta-snapshot-${stamp}.zip` : 'herta-exports.zip'),
      filters: [{ name: 'ZIP archive', extensions: ['zip'] }],
    });
    if (result.canceled || !result.filePath) return { canceled: true, rows };
    filePath = result.filePath;
  }
  const archive = createZip(files);
  if (archive.length > MAX_BACKUP_BYTES) throw new Error('Snapshot ZIP exceeds the 256 MiB limit.');
  await fs.writeFile(filePath, archive);
  const succeeded = rows.filter((row) => row.Status === 'Success').length;
  const action = backup ? (scheduled ? 'Automatic snapshot created' : 'Snapshot created') : 'NIST export';
  await recordActivity(action, `${succeeded} of ${rows.length} subject files exported${destinationDirectory ? ` to ${filePath}` : ''}.`);
  return { canceled: false, path: filePath, rows };
}

async function createBackup({ directory = '', scheduled = false } = {}) {
  const subjects = await getAllSubjects();
  if (!subjects.length) throw new Error('HERTA has no subjects to include in a snapshot.');
  if (subjects.length > MAX_FILE_COUNT) {
    throw new Error(`This desktop snapshot supports up to ${MAX_FILE_COUNT} subjects per ZIP. HERTA currently has ${subjects.length}.`);
  }
  if (scheduled && !hertaSession?.token) throw new Error('Sign in to HERTA before the scheduled backup time.');
  return exportSubjects(subjects.map((subject) => String(subject.code || '')).filter(Boolean), { backup: true, destinationDirectory: directory, scheduled });
}

async function checkHertaHealth() {
  const started = Date.now();
  const page = await authorizedJson('/api/v1/subjects?offset=0&size=1');
  if (!Array.isArray(page.subjects)) throw new Error('HERTA health check returned an unexpected response.');
  return { ok: true, latencyMs: Date.now() - started, subjectCount: Number(page.count) || 0, checkedAt: new Date().toISOString() };
}

function normalizeSourceUrl(value) {
  let target;
  try { target = new URL(String(value || '').trim()); }
  catch { throw new Error('Enter a complete REST URL, including http:// or https://.'); }
  if (!['http:', 'https:'].includes(target.protocol) || !target.hostname || target.username || target.password) {
    throw new Error('REST source must use http:// or https:// and cannot include URL credentials.');
  }
  return target.toString();
}

function addSourceFiles(files, source, kafka = null) {
  if (files.length + sourceInbox.size > MAX_FILE_COUNT) throw new Error(`The desktop inbox supports at most ${MAX_FILE_COUNT} files.`);
  let total = [...sourceInbox.values()].reduce((sum, file) => sum + (file.data?.length || 0), 0);
  for (const file of files) {
    if (file.data.length > MAX_FILE_BYTES || total + file.data.length > MAX_BATCH_BYTES) {
      throw new Error('The desktop source inbox is limited to 32 MiB per file and 256 MiB total.');
    }
    total += file.data.length;
  }
  const ids = [];
  for (const file of files) {
    const id = crypto.randomUUID();
    sourceInbox.set(id, { id, name: file.name, data: file.data, source, kafka });
    ids.push(id);
  }
  return ids;
}

async function fetchRestSource({ url, bearerToken = '', verifyTls = true, headers = {} } = {}) {
  const target = normalizeSourceUrl(url);
  const response = await requestJson(target, verifyTls !== false, target, { token: String(bearerToken || '').trim() || undefined, headers });
  const files = normalizeNistSourcePayload(response);
  const ids = addSourceFiles(files, 'REST source');
  await recordActivity('REST source received', `${files.length} NIST files read from a configured REST endpoint.`);
  return { files: ids.map((id) => inboxSummary(sourceInbox.get(id))), count: files.length };
}

function normalizeKafkaConfig(config = {}) {
  const brokers = String(config.brokers || '').split(',').map((value) => value.trim()).filter(Boolean);
  const topic = String(config.topic || '').trim();
  const groupId = String(config.groupId || 'herta-operations-desktop').trim();
  if (!brokers.length || brokers.length > 20 || brokers.some((broker) => !/^[^\s,:]+:\d{1,5}$/.test(broker))) {
    throw new Error('Enter Kafka brokers as host:port, separated by commas.');
  }
  if (!topic || topic.length > 249 || /[\s,]/.test(topic)) throw new Error('Enter a valid Kafka topic name.');
  if (!groupId || groupId.length > 255) throw new Error('Enter a valid consumer group ID.');
  const mechanism = String(config.mechanism || 'none').toLowerCase();
  const sasl = mechanism === 'none' ? undefined : {
    mechanism,
    username: String(config.username || ''),
    password: String(config.password || ''),
  };
  if (sasl && (!sasl.username || !sasl.password || !['plain', 'scram-sha-256', 'scram-sha-512'].includes(mechanism))) {
    throw new Error('Enter valid Kafka SASL credentials and choose PLAIN or SCRAM-SHA-256/512.');
  }
  return { brokers, topic, groupId, ssl: Boolean(config.tls) ? { rejectUnauthorized: Boolean(config.verifyTls) } : undefined, sasl, messageFormat: config.messageFormat === 'raw-nist' ? 'raw-nist' : 'json-base64' };
}

function createKafkaClient(config) {
  return new Kafka({
    clientId: 'herta-operations-desktop', brokers: config.brokers, ssl: config.ssl, sasl: config.sasl,
    logLevel: logLevel.ERROR, connectionTimeout: 8000, requestTimeout: 15000,
    retry: { initialRetryTime: 300, retries: 2 },
  });
}

async function testKafkaConnection(input) {
  const config = normalizeKafkaConfig(input);
  const admin = createKafkaClient(config).admin();
  try {
    await admin.connect();
    const cluster = await admin.describeCluster();
    let topicExists = false;
    try { topicExists = (await admin.fetchTopicMetadata({ topics: [config.topic] })).topics.some((item) => item.name === config.topic); } catch { /* Broker may require topic creation before metadata lookup. */ }
    return { ok: true, brokers: cluster.brokers.length, controller: cluster.controller, topic: config.topic, topicExists };
  } catch (error) { throw new Error(`Kafka connection failed: ${error.message}`); }
  finally { await admin.disconnect().catch(() => {}); }
}

async function createKafkaTopic(input, { partitions = 1, replicationFactor = 1 } = {}) {
  const config = normalizeKafkaConfig(input);
  const admin = createKafkaClient(config).admin();
  try {
    await admin.connect();
    const created = await admin.createTopics({ waitForLeaders: true, topics: [{ topic: config.topic, numPartitions: Math.max(1, Math.min(64, Number(partitions) || 1)), replicationFactor: Math.max(1, Number(replicationFactor) || 1) }] });
    await recordActivity('Kafka topic configured', `Topic ${config.topic} ${created ? 'created' : 'already exists'}.`);
    return { created, topic: config.topic };
  } catch (error) { throw new Error(`Could not create Kafka topic: ${error.message}`); }
  finally { await admin.disconnect().catch(() => {}); }
}

function inboxSummary(file) {
  return { id: file.id, name: file.name, size: file.data?.length || 0, source: file.source, error: file.error || '' };
}

function getSourceInbox() { return [...sourceInbox.values()].map(inboxSummary); }

function loadInboxFiles(ids) {
  if (!Array.isArray(ids) || ids.length === 0 || ids.length > MAX_FILE_COUNT) throw new Error('Choose one or more received NIST files first.');
  const result = [];
  for (const id of ids) {
    const source = sourceInbox.get(String(id));
    if (!source || !source.data) continue;
    const fileId = crypto.randomUUID();
    const file = { id: fileId, name: source.name, data: source.data, sourceInboxId: source.id, source: source.source, kafka: source.kafka };
    selectedFiles.set(fileId, file);
    result.push({ id: fileId, name: file.name, size: file.data.length, lastModified: 0 });
  }
  if (!result.length) throw new Error('The selected inbox items do not contain valid NIST data.');
  return result;
}

async function commitKafkaAcks() {
  if (!kafkaCurrent?.consumer) return;
  const partitions = new Map();
  for (const state of kafkaCurrent.pending.values()) {
    if (!state.acknowledged) continue;
    const key = state.partition;
    const current = partitions.get(key);
    if (!current || BigInt(state.offset) > BigInt(current.offset)) partitions.set(key, state);
  }
  const offsets = [];
  for (const [partition, latest] of partitions) {
    const blocked = [...kafkaCurrent.pending.values()].some((state) => state.partition === partition && !state.acknowledged && BigInt(state.offset) < BigInt(latest.offset));
    if (!blocked) offsets.push({ topic: kafkaCurrent.topic, partition, offset: (BigInt(latest.offset) + 1n).toString() });
  }
  if (offsets.length) await kafkaCurrent.consumer.commitOffsets(offsets);
}

async function startKafkaSource(input) {
  const config = normalizeKafkaConfig(input);
  if (kafkaCurrent && [...kafkaCurrent.pending.values()].some((state) => !state.acknowledged)) {
    throw new Error('Finish importing or explicitly discard the pending Kafka messages before changing the Kafka connection.');
  }
  if (kafkaConsumer) {
    await kafkaConsumer.stop().catch(() => {});
    await kafkaConsumer.disconnect().catch(() => {});
  }
  const consumer = createKafkaClient(config).consumer({ groupId: config.groupId, allowAutoTopicCreation: false });
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: config.topic, fromBeginning: false });
    kafkaConsumer = consumer;
    kafkaCurrent = { consumer, topic: config.topic, pending: new Map(), listening: true };
    await consumer.run({ autoCommit: false, eachMessage: async ({ topic, partition, message }) => {
      const key = `${topic}:${partition}:${message.offset}`;
      const state = { topic, partition, offset: message.offset, expected: 0, successful: 0, acknowledged: false };
      kafkaCurrent.pending.set(key, state);
      try {
        let files;
        if (config.messageFormat === 'raw-nist') {
          const data = Buffer.from(message.value || []);
          if (!data.length || data.length > MAX_FILE_BYTES) throw new Error('Kafka NIST message must contain 1 byte to 32 MiB.');
          const name = message.key?.toString() || `${topic}-${partition}-${message.offset}`;
          files = [{ name: safeNistName(name), data }];
        } else {
          let payload;
          try { payload = JSON.parse(Buffer.from(message.value || []).toString('utf8')); }
          catch { throw new Error('This message is not valid JSON. Expected a JSON NIST envelope with a Base64 fileContent field. If the topic contains raw NIST bytes, change Message format to “Raw NIST bytes”.'); }
          files = normalizeNistSourcePayload(payload);
        }
        const ids = addSourceFiles(files, `Kafka · ${topic}`, { topic, partition, offset: message.offset });
        state.expected = ids.length;
      } catch (error) {
        const id = crypto.randomUUID();
        sourceInbox.set(id, { id, name: `${topic}-${partition}-${message.offset}`, data: null, source: `Kafka · ${topic}`, error: error.message, kafka: { topic, partition, offset: message.offset } });
      }
    } });
    await recordActivity('Kafka source connected', `Listening to topic ${config.topic}.`);
    return { connected: true, topic: config.topic, groupId: config.groupId };
  } catch (error) {
    await consumer.disconnect().catch(() => {});
    kafkaConsumer = null; kafkaCurrent = null;
    throw new Error(`Could not start Kafka consumer: ${error.message}`);
  }
}

async function pauseKafkaSource() {
  if (!kafkaCurrent) return { listening: false };
  kafkaCurrent.consumer.pause([{ topic: kafkaCurrent.topic }]);
  kafkaCurrent.listening = false;
  return { listening: false };
}

async function acknowledgeInboxItem(id) {
  const item = sourceInbox.get(String(id));
  if (!item) return { ok: true };
  if (item.kafka) {
    const key = `${item.kafka.topic}:${item.kafka.partition}:${item.kafka.offset}`;
    const state = kafkaCurrent?.pending.get(key);
    if (state) state.acknowledged = true;
  }
  sourceInbox.delete(item.id);
  await commitKafkaAcks().catch(() => {});
  return { ok: true };
}

async function startRestReceiver({ port = 8006 } = {}) {
  if (restReceiver) return { running: true, port: restReceiver.port, token: restReceiver.token };
  const selectedPort = Number(port);
  if (!Number.isInteger(selectedPort) || selectedPort < 1024 || selectedPort > 65535) throw new Error('Choose a local REST port between 1024 and 65535.');
  const token = crypto.randomBytes(32).toString('base64url');
  const server = http.createServer(async (request, response) => {
    response.setHeader('Content-Type', 'application/json');
    if (request.method === 'GET' && request.url === '/health') { response.writeHead(200); response.end(JSON.stringify({ ok: true, service: 'HERTA local NIST receiver' })); return; }
    if (request.method !== 'POST' || request.url !== '/api/v1/nist') { response.writeHead(404); response.end(JSON.stringify({ error: 'Use POST /api/v1/nist.' })); return; }
    const givenToken = String(request.headers.authorization || '').replace(/^Bearer\s+/i, '');
    const supplied = Buffer.from(givenToken); const expected = Buffer.from(token);
    if (supplied.length !== expected.length || !crypto.timingSafeEqual(supplied, expected)) { response.writeHead(401); response.end(JSON.stringify({ error: 'Invalid bearer token.' })); return; }
    const chunks = []; let bytes = 0; let tooLarge = false;
    request.on('data', (chunk) => {
      if (tooLarge) return;
      bytes += chunk.length;
      if (bytes > Math.ceil(MAX_BATCH_BYTES * 4 / 3) + 2 * 1024 * 1024) {
        tooLarge = true; response.writeHead(413); response.end(JSON.stringify({ error: 'Request body is too large.' })); request.resume();
      } else chunks.push(chunk);
    });
    request.on('end', () => {
      if (tooLarge) return;
      try {
        const contentType = String(request.headers['content-type'] || '');
        let files;
        if (contentType.includes('application/octet-stream')) {
          const data = Buffer.concat(chunks);
          if (!data.length || data.length > MAX_FILE_BYTES) throw new Error('Raw NIST body must be between 1 byte and 32 MiB.');
          files = [{ name: safeNistName(request.headers['x-file-name'] || 'rest-received'), data }];
        } else {
          files = normalizeNistSourcePayload(JSON.parse(Buffer.concat(chunks).toString('utf8')));
        }
        const ids = addSourceFiles(files, 'Local REST receiver');
        response.writeHead(202); response.end(JSON.stringify({ accepted: files.length, ids }));
      } catch (error) { response.writeHead(400); response.end(JSON.stringify({ error: error.message })); }
    });
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(selectedPort, '127.0.0.1', resolve); });
  restReceiver = { server, port: selectedPort, token };
  await recordActivity('Local REST receiver started', `Listening on 127.0.0.1:${selectedPort}.`);
  return { running: true, port: selectedPort, token };
}

async function stopRestReceiver() {
  if (!restReceiver) return { running: false };
  const current = restReceiver; restReceiver = null;
  await new Promise((resolve) => current.server.close(() => resolve()));
  return { running: false };
}

function getPickedFiles(ids) {
  if (!Array.isArray(ids) || ids.length === 0 || ids.length > MAX_FILE_COUNT) throw new Error(`Choose between 1 and ${MAX_FILE_COUNT} NIST files.`);
  return ids.map((id) => {
    const file = selectedFiles.get(String(id));
    if (!file) throw new Error('The file selection expired. Choose the files again.');
    return file;
  });
}

async function prepareImport(ids) {
  const files = getPickedFiles(ids);
  const rows = [];
  const accepted = [];
  let total = 0;
  for (const file of files) {
    const size = file.data !== undefined ? file.data.length : (await fs.stat(file.path)).size;
    const code = path.parse(file.name).name || file.name;
    if (size === 0) {
      rows.push({ File: file.name, Code: code, Status: 'Rejected', Bytes: size, Message: 'File is empty.' });
    } else if (size > MAX_FILE_BYTES) {
      rows.push({ File: file.name, Code: code, Status: 'Rejected', Bytes: size, Message: 'File exceeds 32 MiB.' });
    } else if (total + size > MAX_BATCH_BYTES) {
      rows.push({ File: file.name, Code: code, Status: 'Rejected', Bytes: size, Message: 'Batch exceeds 256 MiB.' });
    } else {
      accepted.push({ ...file, size });
      total += size;
    }
  }
  return { accepted, rows };
}

async function runImport({ ids, group = '0', confirmation = '' }) {
  const session = assertSignedIn();
  if (confirmation !== 'IMPORT') throw new Error('Type IMPORT to confirm sending biometric data to HERTA.');
  const { accepted, rows } = await prepareImport(ids);
  for (const file of accepted) {
    const row = { File: file.name, Code: path.parse(file.name).name || file.name, Status: 'Failed', HttpStatus: '', Bytes: file.size, Message: '' };
    try {
      const data = file.data !== undefined ? file.data : await fs.readFile(file.path);
      const response = await authorizedJson('/api/v1/subjects/import', {
        method: 'POST',
        token: session.token,
        json: { data: data.toString('base64'), format: 'nist', group: String(group) },
      });
      row.Status = 'HTTP_SUCCESS';
      row.Message = 'Server returned success; verify subject in HERTA.';
      row.HttpStatus = 200;
    } catch (error) {
      row.Status = 'HTTP_ERROR';
      row.HttpStatus = error.status || '';
      row.Message = error.message;
    }
    rows.push(row);
  }
  const succeeded = rows.filter((row) => row.Status === 'HTTP_SUCCESS').length;
  for (const file of accepted) {
    const result = rows.find((row) => row.File === file.name && row.Status === 'HTTP_SUCCESS');
    if (!result || !file.sourceInboxId) continue;
    const inboxItem = sourceInbox.get(file.sourceInboxId);
    sourceInbox.delete(file.sourceInboxId);
    if (inboxItem?.kafka && kafkaCurrent) {
      const ref = inboxItem.kafka;
      const state = kafkaCurrent.pending.get(`${ref.topic}:${ref.partition}:${ref.offset}`);
      if (state) { state.successful += 1; if (state.successful >= state.expected) state.acknowledged = true; }
    }
  }
  for (const file of accepted) {
    const succeededFile = rows.find((row) => row.File === file.name && row.Status === 'HTTP_SUCCESS');
    if (!succeededFile || !file.workerInboxId) continue;
    await DockerPlatform.workerRequest(app, `/api/inbox/${encodeURIComponent(file.workerInboxId)}/complete`, { method: 'POST', body: {} });
  }
  await commitKafkaAcks().catch(() => {});
  await recordActivity('NIST import', `${succeeded} of ${rows.length} file imports succeeded.`);
  return { results: rows, executed: true };
}

async function saveReport({ rows, kind = 'import' }) {
  if (!Array.isArray(rows)) throw new Error('There is no report to save.');
  const exportReport = kind === 'export';
  const columns = exportReport ? ['Code', 'Status', 'Bytes', 'Details'] : ['File', 'Code', 'Status', 'HttpStatus', 'Bytes', 'Message'];
  const result = await dialog.showSaveDialog(mainWindow, {
    title: 'Save HERTA report',
    defaultPath: path.join(app.getPath('downloads'), `herta-${kind}-report.csv`),
    filters: [{ name: 'CSV file', extensions: ['csv'] }],
  });
  if (result.canceled || !result.filePath) return { canceled: true };
  await fs.writeFile(result.filePath, toCsv(rows, columns), 'utf8');
  return { canceled: false, path: result.filePath };
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1320,
    height: 900,
    minWidth: 840,
    minHeight: 620,
    title: `HERTA Operations v${app.getVersion()}`,
    backgroundColor: '#f7f8fa',
    show: false,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      webSecurity: true,
    },
  });
  mainWindow.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  mainWindow.webContents.on('will-navigate', (event, target) => {
    if (!target.startsWith(APP_URL)) event.preventDefault();
  });
  if (DEV_SERVER_URL) await mainWindow.loadURL(DEV_SERVER_URL);
  else await mainWindow.loadFile(APP_PAGE);
  mainWindow.once('ready-to-show', () => mainWindow.show());
  mainWindow.on('closed', () => { mainWindow = null; hertaSession = null; selectedFiles.clear(); });
}

function registerIpc() {
  const handle = (channel, callback) => ipcMain.handle(channel, (event, ...args) => {
    assertTrusted(event);
    return callback(...args);
  });
  handle('app:config', () => ({
    baseUrl: process.env.HERTA_BASE_URL || 'https://localhost:8005',
    version: app.getVersion(),
    // HERTA deployments often use a private or self-signed certificate.
    // Verification is an explicit sign-in choice in the desktop UI.
    verifyTls: false,
  }));
  handle('platform:status', () => DockerPlatform.platformStatus(app));
  handle('platform:start', async (options) => {
    const status = await DockerPlatform.startPlatform(app, options || {});
    if (hertaSession) await DockerPlatform.setWorkerSession(app, hertaSession);
    const settings = await readBackupSettings();
    const connectors = await readConnectorSettings();
    if (status.worker?.ok) {
      await DockerPlatform.workerRequest(app, '/api/connectors', { method: 'PUT', body: connectorsForWorker(connectors.connectors, status) }).catch(() => {});
      await DockerPlatform.workerRequest(app, '/api/backup/settings', { method: 'POST', body: { enabled: settings.enabled, cron: settings.cron, timezone: settings.timezone, retentionCount: settings.retentionCount } }).catch(() => {});
    }
    if (status.worker?.ok && connectors.telegram.botToken && connectors.telegram.chatId) await DockerPlatform.workerRequest(app, '/api/telegram/config', { method: 'POST', body: connectors.telegram }).catch(() => {});
    return DockerPlatform.platformStatus(app);
  });
  handle('platform:stop', () => DockerPlatform.stopPlatform(app));
  handle('platform:logs', (service) => DockerPlatform.platformLogs(app, service));
  handle('platform:worker-call', async (operation, payload) => {
    const endpoints = {
      status: ['GET', '/api/status'], restFetch: ['POST', '/api/rest/fetch'], receiverStart: ['POST', '/api/receiver/start'],
      receiverStop: ['POST', '/api/receiver/stop'], inbox: ['GET', '/api/inbox'], kafkaStart: ['POST', '/api/kafka/start'],
      kafkaStop: ['POST', '/api/kafka/stop'], backupSettings: ['POST', '/api/backup/settings'], backupRun: ['POST', '/api/backup/run'],
      queueHistory: ['GET', '/api/queue/history'], connectorLogs: ['GET', '/api/connectors/logs'],
      restTest: ['POST', '/api/connectors/test'],
    };
    const endpoint = endpoints[operation];
    if (!endpoint) throw new Error('Unknown worker operation.');
    return DockerPlatform.workerRequest(app, endpoint[1], { method: endpoint[0], body: payload, timeout: operation === 'backupRun' ? 2 * 60 * 60 * 1000 : 30000 });
  });
  handle('herta:session', () => ({ authenticated: Boolean(hertaSession?.token), baseUrl: hertaSession?.baseUrl || '' }));
  handle('connectors:settings', () => readConnectorSettings());
  handle('connectors:settings-save', async (settings) => {
    const previous = await readConnectorSettings();
    const saved = await saveConnectorSettings(settings);
    const status = await DockerPlatform.platformStatus(app).catch(() => null);
    if (status?.worker?.ok) await DockerPlatform.workerRequest(app, '/api/connectors', { method: 'PUT', body: connectorsForWorker(saved.connectors, status) });
    const telegramChanged = previous.telegram.botToken !== saved.telegram.botToken || previous.telegram.chatId !== saved.telegram.chatId;
    if (status?.worker?.ok && (settings?.forceTelegram || telegramChanged || (saved.telegram.botToken && !status.worker.telegram?.configured))) {
      try { await DockerPlatform.workerRequest(app, '/api/telegram/config', { method: 'POST', body: saved.telegram }); }
      catch (error) { throw new Error(telegramUserError(error)); }
    }
    return saved;
  });
  handle('connectors:telegram-status', async () => {
    const status = await DockerPlatform.platformStatus(app);
    return status.worker?.ok ? DockerPlatform.workerRequest(app, '/api/telegram/status') : { configured: false, connected: false, error: 'Start the Docker worker first.' };
  });
  handle('connectors:telegram-chats', async (botToken) => {
    const status = await DockerPlatform.platformStatus(app);
    if (!status.worker?.ok) throw new Error('Start the Docker worker before finding Telegram chats.');
    try { return await DockerPlatform.workerRequest(app, '/api/telegram/chats', { method: 'POST', body: { botToken: String(botToken || '') } }); }
    catch (error) { throw new Error(telegramUserError(error)); }
  });
  handle('connectors:telegram-test', async () => {
    const settings = await readConnectorSettings();
    if (!settings.telegram.botToken || !settings.telegram.chatId) throw new Error('Enter and save the Telegram bot token and chat ID first.');
    try { return await DockerPlatform.workerRequest(app, '/api/telegram/test', { method: 'POST', body: {} }); }
    catch (error) { throw new Error(telegramUserError(error)); }
  });
  handle('herta:login', async ({ baseUrl, username, password, verifyTls }) => {
    if (!String(username || '').trim() || !String(password || '')) throw new Error('Enter your HERTA username and password.');
    const normalized = normalizeBaseUrl(baseUrl);
    const verified = Boolean(verifyTls);
    const response = await requestJson(normalized, verified, '/api/v1/auth/login', {
      method: 'POST',
      json: { username: String(username).trim(), password: String(password) },
    });
    const token = response.access_token || response.accessToken;
    if (!token) throw new Error('HERTA sign-in did not return an access token.');
    subjectImageCache.clear();
    hertaSession = { baseUrl: normalized, verifyTls: verified, token: String(token), username: String(username).trim(), password: String(password), authenticatedAt: Date.now() };
    await persistHertaSession(hertaSession).catch(() => false);
    await DockerPlatform.setWorkerSession(app, hertaSession);
    await recordActivity('Signed in', 'A HERTA desktop session started.');
    return { authenticated: true, baseUrl: normalized };
  });
  handle('herta:logout', () => {
    if (hertaSession) void recordActivity('Signed out', 'The HERTA desktop session ended.');
    void DockerPlatform.setWorkerSession(app, null);
    void persistHertaSession(null);
    hertaSession = null; subjectImageCache.clear(); selectedFiles.clear(); void pauseKafkaSource(); return { ok: true };
  });
  handle('herta:subjects', async ({ query = '', group = '', offset = 0, limit = 40, includeCodes = false } = {}) => {
    const subjects = await getAllSubjects();
    const normalized = String(query).trim().toLocaleLowerCase();
    const normalizedGroup = String(group || '');
    const matching = subjects.filter((item) => {
      const searchable = Object.entries(item).filter(([key, value]) => key !== 'profile' && value != null && typeof value !== 'object').map(([, value]) => String(value)).join(' ');
      const groupValue = subjectGroup(item);
      const groupMatches = !normalizedGroup || (normalizedGroup === '__unassigned__' ? !groupValue : groupValue === normalizedGroup);
      return groupMatches && (!normalized || searchable.toLocaleLowerCase().includes(normalized));
    });
    const start = Math.max(0, Number(offset) || 0);
    const pageSize = Math.max(1, Math.min(100, Number(limit) || 40));
    const groups = [...new Set(subjects.map(subjectGroup).filter(Boolean))].sort((a, b) => a.localeCompare(b));
    return {
      subjects: matching.slice(start, start + pageSize).map((subject) => ({ ...summarizeSubject(subject), imageDataUrl: subjectImageCache.get(String(subject.code || '')) || extractSubjectImage(subject, 48 * 1024) })),
      count: matching.length,
      server_count: subjects.length,
      groups,
      ...(includeCodes ? { matchingCodes: matching.slice(0, MAX_FILE_COUNT).map((subject) => String(subject.code || '')).filter(Boolean) } : {}),
    };
  });
  handle('herta:subject-detail', getSubjectDetails);
  handle('herta:profiles', profileCheck);
  handle('herta:export', exportSubjects);
  handle('herta:backup-settings', () => readBackupSettings());
  handle('herta:backup-preview', (cron) => nextCronDate(cron).toISOString());
  handle('herta:backup-settings-save', async (settings) => {
    const saved = await saveBackupSettings(settings);
    if (saved.directory) await DockerPlatform.configureBackupMount(app, saved.directory);
    let status = await DockerPlatform.platformStatus(app).catch(() => null);
    if (status?.worker?.ok && saved.directory) {
      status = await DockerPlatform.recreateWorker(app);
      if (hertaSession) await DockerPlatform.setWorkerSession(app, hertaSession);
      const connectors = await readConnectorSettings();
      if (status.worker?.ok) await DockerPlatform.workerRequest(app, '/api/connectors', { method: 'PUT', body: connectorsForWorker(connectors.connectors, status) }).catch(() => {});
      if (status.worker?.ok && connectors.telegram.botToken && connectors.telegram.chatId) await DockerPlatform.workerRequest(app, '/api/telegram/config', { method: 'POST', body: connectors.telegram }).catch(() => {});
    }
      if (status?.worker?.ok) await DockerPlatform.workerRequest(app, '/api/backup/settings', { method: 'POST', body: { enabled: saved.enabled, cron: saved.cron, timezone: saved.timezone, retentionCount: saved.retentionCount } });
    return saved;
  });
  handle('files:pick-backup-folder', async () => {
    const result = await dialog.showOpenDialog(mainWindow, { title: 'Choose automatic HERTA backup folder', properties: ['openDirectory', 'createDirectory'] });
    return result.canceled ? '' : (result.filePaths[0] || '');
  });
  handle('herta:backup', async (options) => {
    const status = await DockerPlatform.platformStatus(app).catch(() => null);
    if (!status?.worker?.ok) {
      const settings = await readConnectorSettings();
      if (settings.connectors.some((connector) => connector.type === 'postgres' && connector.active)) throw new Error('Start the Docker worker before running the configured PostgreSQL full backup.');
      return createBackup(options);
    }
    const result = await DockerPlatform.workerRequest(app, '/api/backup/run', { method: 'POST', body: {}, timeout: 2 * 60 * 60 * 1000 });
    const destination = status.backupPath || options?.directory || '';
    const localPath = (file) => destination && file ? path.join(destination, path.basename(file)) : file;
    return { ...result, path: localPath(result.path), paths: (result.paths || []).map(localPath), databaseBackups: (result.databaseBackups || []).map((item) => ({ ...item, path: localPath(item.path) })) };
  });
  handle('herta:health', checkHertaHealth);
  handle('herta:audit', async () => {
    const status = await DockerPlatform.platformStatus(app).catch(() => null);
    return status?.worker?.ok ? DockerPlatform.workerRequest(app, '/api/audit') : readAuditEvents();
  });
  handle('source:rest-fetch', (config) => {
    let url = String(config?.url || '');
    try { const target = new URL(url); if (target.hostname === 'localhost' || target.hostname === '127.0.0.1') { target.hostname = 'host.docker.internal'; url = target.toString(); } } catch { /* Worker returns the user-facing URL validation error. */ }
    return DockerPlatform.workerRequest(app, '/api/rest/fetch', { method: 'POST', body: { ...config, url } });
  });
  handle('source:kafka-test', testKafkaConnection);
  handle('source:kafka-create-topic', (config, topicOptions) => createKafkaTopic(config, topicOptions));
  handle('source:kafka-start', async (config) => {
    const status = await DockerPlatform.platformStatus(app);
    const localKafka = status.services.some((service) => service.Service === 'kafka' && /running|up/i.test(service.State || service.Status || ''));
    const workerConfig = { ...config, brokers: String(config.brokers || '').split(',').map((broker) => {
      const value = broker.trim();
      if (localKafka && /^(localhost|127\.0\.0\.1):19092$/.test(value)) return 'kafka:9092';
      return value.replace(/^(localhost|127\.0\.0\.1):(\d+)$/, 'host.docker.internal:$2');
    }).join(',') };
    return DockerPlatform.workerRequest(app, '/api/kafka/start', { method: 'POST', body: workerConfig });
  });
  handle('source:kafka-pause', () => DockerPlatform.workerRequest(app, '/api/kafka/stop', { method: 'POST', body: {} }));
  handle('source:inbox', () => DockerPlatform.workerRequest(app, '/api/inbox'));
  handle('source:inbox-load', async (ids) => {
    if (!Array.isArray(ids) || !ids.length) throw new Error('Choose one or more received NIST files first.');
    const output = [];
    for (const id of ids) {
      const item = await DockerPlatform.workerRequest(app, `/api/inbox/${encodeURIComponent(id)}/content`);
      const data = Buffer.from(item.fileContent, 'base64');
      const fileId = crypto.randomUUID();
      selectedFiles.set(fileId, { id: fileId, name: item.name, data, workerInboxId: item.id, kafka: item.kafka });
      output.push({ id: fileId, name: item.name, size: data.length, source: item.source, sourceType: item.sourceType, connectorId: item.connectorId, lastModified: 0 });
    }
    if (!output.length) throw new Error('No valid worker inbox files were selected.');
    return output;
  });
  handle('source:inbox-discard', (id) => DockerPlatform.workerRequest(app, `/api/inbox/${encodeURIComponent(id)}`, { method: 'DELETE' }));
  handle('source:rest-receiver-start', (config = {}) => DockerPlatform.workerRequest(app, '/api/receiver/start', { method: 'POST', body: config }));
  handle('source:rest-receiver-stop', () => DockerPlatform.workerRequest(app, '/api/receiver/stop', { method: 'POST', body: {} }));
  handle('files:pick-import', async () => {
    const result = await dialog.showOpenDialog(mainWindow, {
      title: 'Choose NIST / ANSI-NIST files from any source',
      properties: ['openFile', 'multiSelections'],
      filters: [
        { name: 'NIST / ANSI-NIST files', extensions: ['nist', 'an2', 'anf', 'eft', 'dat', 'xml', 'txt'] },
        { name: 'All files', extensions: ['*'] },
      ],
    });
    if (result.canceled) return [];
    selectedFiles.clear();
    return Promise.all(result.filePaths.slice(0, MAX_FILE_COUNT).map(async (filePath, index) => {
      const stat = await fs.stat(filePath);
      const id = `${Date.now()}-${index}-${Math.random().toString(36).slice(2)}`;
      const file = { id, path: filePath, name: path.basename(filePath) };
      selectedFiles.set(id, file);
      return { id, name: file.name, size: stat.size, lastModified: stat.mtimeMs };
    }));
  });
  handle('files:pick-backup', async () => {
    const result = await dialog.showOpenDialog(mainWindow, {
      title: 'Choose a HERTA snapshot ZIP to restore',
      properties: ['openFile'],
      filters: [{ name: 'HERTA backup ZIP', extensions: ['zip'] }],
    });
    if (result.canceled || !result.filePaths[0]) return [];
    const stat = await fs.stat(result.filePaths[0]);
    if (stat.size > MAX_BACKUP_BYTES) throw new Error('Backup ZIP exceeds the 256 MiB limit.');
    const archive = await fs.readFile(result.filePaths[0]);
    const entries = inspectBackupArchive(archive);
    const nistNames = [...entries].filter(([, entry]) => entry.isNist);
    if (!nistNames.length) throw new Error('Backup ZIP does not contain any .nist files.');
    selectedFiles.clear();
    const unzipped = unzipSync(archive);
    let total = 0;
    return nistNames.map(([name, entry], index) => {
      const bytes = unzipped[name];
      if (!bytes || bytes.length !== entry.uncompressedSize) throw new Error(`Backup entry ${name} is damaged.`);
      if (bytes.length > MAX_FILE_BYTES || total + bytes.length > MAX_BATCH_BYTES) {
        throw new Error('Restored NIST files exceed the 32 MiB per-file or 256 MiB batch limit.');
      }
      total += bytes.length;
      const id = `${Date.now()}-${index}-${Math.random().toString(36).slice(2)}`;
      selectedFiles.set(id, { id, name, data: Buffer.from(bytes) });
      return { id, name, size: bytes.length, lastModified: 0 };
    });
  });
  handle('herta:import-preview', async ({ ids, group = '0' }) => {
    const { accepted, rows } = await prepareImport(ids);
    rows.push(...accepted.map((file) => ({
      File: file.name,
      Code: path.parse(file.name).name || file.name,
      Status: 'DryRun',
      Bytes: file.size,
      Message: `Not sent to HERTA. Group: ${String(group)}.`,
    })));
    await recordActivity('Import preview', `${accepted.length} NIST files inspected; none were sent.`);
    return { results: rows, executed: false };
  });
  handle('herta:import', runImport);
  handle('files:save-report', saveReport);
}

app.whenReady().then(async () => {
  Menu.setApplicationMenu(null);
  registerIpc();
  await restoreHertaSession();
  let initialStatus = await DockerPlatform.platformStatus(app).catch(() => null);
  let rebuildWorker = false;
  if (initialStatus?.worker?.ok) {
    try { await DockerPlatform.workerRequest(app, '/api/queue/history'); }
    catch { rebuildWorker = true; }
  }
  if (rebuildWorker) {
    const running = (name) => (initialStatus.services || []).some((service) => service.Service === name && /running|up/i.test(service.State || service.Status || ''));
    initialStatus = await DockerPlatform.startPlatform(app, { localKafka: running('kafka'), search: running('elasticsearch') }).catch((error) => ({ ...initialStatus, worker: { ok: false, error: error.message } }));
    if (initialStatus.worker?.ok && hertaSession) await DockerPlatform.setWorkerSession(app, hertaSession).catch(() => {});
  }
  const initialConnectors = await readConnectorSettings();
  if (initialStatus?.worker?.ok) {
    await DockerPlatform.workerRequest(app, '/api/connectors', { method: 'PUT', body: connectorsForWorker(initialConnectors.connectors, initialStatus) }).catch(() => {});
    if (initialConnectors.telegram.botToken && initialConnectors.telegram.chatId) await DockerPlatform.workerRequest(app, '/api/telegram/config', { method: 'POST', body: initialConnectors.telegram }).catch(() => {});
  }
  await createWindow();
  app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
});

app.on('before-quit', () => {
  hertaSession = null; selectedFiles.clear();
  if (kafkaConsumer) void kafkaConsumer.disconnect().catch(() => {});
  if (restReceiver) void stopRestReceiver();
});
app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
