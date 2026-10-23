const FORBIDDEN_HEADERS = new Set(['connection', 'content-length', 'host', 'transfer-encoding', 'upgrade']);

function normalizeHttpHeaders(input = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Headers must be a name and value map.');
  const entries = Object.entries(input);
  if (entries.length > 100) throw new Error('REST endpoints support up to 100 custom headers.');
  const result = {};
  const seen = new Set();
  for (const [rawName, rawValue] of entries) {
    const name = String(rawName).trim();
    const value = String(rawValue ?? '');
    if (!name || !/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name)) throw new Error(`Invalid HTTP header name: ${name || '(empty)'}.`);
    const lowerName = name.toLowerCase();
    if (FORBIDDEN_HEADERS.has(lowerName)) throw new Error(`The ${name} header is managed by the HTTP client.`);
    if (seen.has(lowerName)) throw new Error(`Header ${name} is listed more than once.`);
    if (/[\r\n]/.test(value) || value.length > 8192) throw new Error(`Invalid value for header ${name}.`);
    seen.add(lowerName);
    result[name] = value;
  }
  return result;
}

function mergeHttpHeaders(defaults, input = {}) {
  const result = { ...defaults };
  for (const [name, value] of Object.entries(normalizeHttpHeaders(input))) {
    for (const existing of Object.keys(result)) if (existing.toLowerCase() === name.toLowerCase()) delete result[existing];
    result[name] = value;
  }
  return result;
}

module.exports = { normalizeHttpHeaders, mergeHttpHeaders };
