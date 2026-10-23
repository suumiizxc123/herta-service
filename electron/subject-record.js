function profileToText(profile) {
  if (profile == null) return '';
  if (typeof profile === 'string') return profile;
  try { return JSON.stringify(profile); } catch { return String(profile); }
}

function subjectGroup(subject) {
  const keys = new Set(['group', 'groupid', 'groupname', 'subjectgroup', 'subjectgroupid', 'subjectgroupname']);
  for (const [key, raw] of Object.entries(subject)) {
    if (!keys.has(key.toLowerCase().replace(/[^a-z0-9]/g, '')) || raw == null) continue;
    const value = typeof raw === 'object'
      ? (raw.name ?? raw.label ?? raw.title ?? raw.code ?? raw.id)
      : raw;
    if (value != null && String(value).trim()) return String(value).trim().slice(0, 100);
  }
  return '';
}

function imageDataUrl(bytes) {
  if (bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return `data:image/jpeg;base64,${bytes.toString('base64')}`;
  if (bytes.length >= 8 && bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) return `data:image/png;base64,${bytes.toString('base64')}`;
  if (bytes.length >= 12 && bytes.toString('ascii', 0, 4) === 'RIFF' && bytes.toString('ascii', 8, 12) === 'WEBP') return `data:image/webp;base64,${bytes.toString('base64')}`;
  if (bytes.length >= 6 && /^GIF8[79]a$/.test(bytes.toString('ascii', 0, 6))) return `data:image/gif;base64,${bytes.toString('base64')}`;
  return '';
}

function findEmbeddedJpeg(bytes) {
  const start = bytes.indexOf(Buffer.from([0xff, 0xd8, 0xff]));
  if (start < 0) return Buffer.alloc(0);
  const end = bytes.indexOf(Buffer.from([0xff, 0xd9]), start + 3);
  return end < 0 ? Buffer.alloc(0) : bytes.subarray(start, end + 2);
}

function extractSubjectImage(subject, maxBytes = 1024 * 1024) {
  const parsed = {};
  for (const [key, value] of Object.entries(subject)) parsed[key] = value;
  if (typeof parsed.profile === 'string') {
    try { parsed.profile = JSON.parse(parsed.profile); } catch { /* It may be a base64 image or NIST payload. */ }
  }
  const candidates = [];
  const visited = new Set();
  const walk = (value, keyHint = '', depth = 0) => {
    if (depth > 6 || value == null) return;
    if (typeof value === 'string') {
      if (/^data:image\/(?:jpeg|jpg|png|webp|gif);base64,/i.test(value)) {
        const encoded = value.slice(value.indexOf(',') + 1).replace(/\s+/g, '');
        if (encoded.length <= Math.ceil(maxBytes * 4 / 3)) {
          const bytes = Buffer.from(encoded, 'base64');
          if (bytes.length <= maxBytes) candidates.push(bytes);
        }
      } else if (/image|photo|portrait|face|picture/i.test(keyHint) || keyHint === 'profile') {
        const encoded = value.replace(/\s+/g, '');
        if (encoded.length >= 64 && encoded.length <= Math.ceil(maxBytes * 4 / 3) && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) {
          const bytes = Buffer.from(encoded, 'base64');
          if (bytes.length <= maxBytes) candidates.push(bytes);
        }
      }
      return;
    }
    if (typeof value !== 'object' || visited.has(value)) return;
    visited.add(value);
    if (Array.isArray(value)) { for (const item of value.slice(0, 100)) walk(item, keyHint, depth + 1); return; }
    for (const [key, nested] of Object.entries(value).slice(0, 200)) walk(nested, /image|photo|portrait|face|picture/i.test(key) ? key : keyHint, depth + 1);
  };
  walk(parsed);
  for (const bytes of candidates) {
    const detected = imageDataUrl(bytes) || imageDataUrl(findEmbeddedJpeg(bytes));
    if (detected) return detected;
  }
  return '';
}

function summarizeSubject(subject) {
  const metadata = {};
  for (const [key, value] of Object.entries(subject)) {
    if (key === 'profile' || value == null || typeof value === 'object') continue;
    metadata[key] = String(value).slice(0, 160);
  }
  return {
    code: String(subject.code || ''),
    group: subjectGroup(subject),
    profile_length: profileToText(subject.profile).length,
    field_count: Object.keys(subject).length,
    metadata,
  };
}

function makeSubjectDetails(subject, maxProfileChars = 512 * 1024) {
  const profile = profileToText(subject.profile);
  let profileJson = null;
  if (profile.length <= maxProfileChars) {
    try { profileJson = profile ? JSON.parse(profile) : null; } catch { /* Keep unstructured profile text available to the user. */ }
  }
  const fields = {};
  let totalFieldChars = 0;
  let fieldsTruncated = false;
  for (const [key, value] of Object.entries(subject)) {
    if (key === 'profile') continue;
    let serialized;
    try { serialized = typeof value === 'string' ? value : JSON.stringify(value); } catch { serialized = String(value); }
    serialized ??= 'null';
    if (serialized.length > 32 * 1024 || totalFieldChars + serialized.length > 256 * 1024) {
      fields[key] = `${serialized.slice(0, Math.max(0, 32 * 1024 - 40))} … [field truncated]`;
      fieldsTruncated = true;
    } else {
      fields[key] = value;
      totalFieldChars += serialized.length;
    }
  }
  return {
    code: String(subject.code || ''),
    group: subjectGroup(subject),
    imageDataUrl: extractSubjectImage(subject),
    fields,
    fieldsTruncated,
    profileText: profile.slice(0, maxProfileChars),
    profileJson,
    profileLength: profile.length,
    profileTruncated: profile.length > maxProfileChars,
  };
}

module.exports = { profileToText, subjectGroup, extractSubjectImage, summarizeSubject, makeSubjectDetails };
