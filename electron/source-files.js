const MAX_FILE_BYTES = 32 * 1024 * 1024;
const MAX_BATCH_BYTES = 256 * 1024 * 1024;
const MAX_FILES = 500;

function safeNistName(value, fallback = 'received') {
  let name = String(value || fallback).split(/[\\/]/).pop().replace(/[^\p{L}\p{N}_.-]/gu, '_').slice(0, 100);
  if (!name) name = fallback;
  return name.toLowerCase().endsWith('.nist') ? name : `${name}.nist`;
}

function decodeBase64(value) {
  const encoded = String(value || '').replace(/\s+/g, '');
  if (!encoded || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) {
    throw new Error('Each NIST item must contain valid Base64 in fileContent or dataBase64.');
  }
  const data = Buffer.from(encoded, 'base64');
  if (!data.length || data.length > MAX_FILE_BYTES) throw new Error('Each NIST item must be between 1 byte and 32 MiB.');
  return data;
}

function normalizeNistSourcePayload(payload) {
  const candidates = Array.isArray(payload) ? payload
    : Array.isArray(payload?.files) ? payload.files
      : Array.isArray(payload?.items) ? payload.items
        : [payload];
  if (!candidates.length || candidates.length > MAX_FILES) throw new Error(`Source must return 1 to ${MAX_FILES} NIST files.`);
  const files = [];
  const names = new Set();
  let total = 0;
  for (const [index, item] of candidates.entries()) {
    if (!item || typeof item !== 'object') throw new Error(`Source item ${index + 1} is not a NIST file object.`);
    const encoded = item.fileContent ?? item.dataBase64;
    if (typeof encoded !== 'string') throw new Error(`Source item ${index + 1} must include fileContent or dataBase64.`);
    const data = decodeBase64(encoded);
    total += data.length;
    if (total > MAX_BATCH_BYTES) throw new Error('NIST source batch exceeds the 256 MiB limit.');
    const base = safeNistName(item.fileName || item.filename || item.code || item.name, `received-${index + 1}`);
    let name = base;
    let suffix = 2;
    while (names.has(name.toLowerCase())) {
      const stem = base.slice(0, -5);
      name = `${stem}-${suffix++}.nist`;
    }
    names.add(name.toLowerCase());
    files.push({ name, data });
  }
  return files;
}

module.exports = { MAX_FILE_BYTES, MAX_BATCH_BYTES, MAX_FILES, safeNistName, normalizeNistSourcePayload };
