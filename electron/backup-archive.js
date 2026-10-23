const zlib = require('node:zlib');

function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function dosTimestamp(date = new Date()) {
  const time = (date.getHours() << 11) | (date.getMinutes() << 5) | Math.floor(date.getSeconds() / 2);
  const day = Math.max(date.getDate(), 1);
  return { time, date: ((Math.max(date.getFullYear(), 1980) - 1980) << 9) | ((date.getMonth() + 1) << 5) | day };
}

function createZip(files) {
  const localParts = [];
  const centralParts = [];
  let offset = 0;
  const stamp = dosTimestamp();
  for (const file of files) {
    const name = Buffer.from(file.name, 'utf8');
    const raw = Buffer.isBuffer(file.data) ? file.data : Buffer.from(file.data, 'utf8');
    const compressed = zlib.deflateRawSync(raw);
    const crc = crc32(raw);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(0x0800, 6);
    local.writeUInt16LE(8, 8);
    local.writeUInt16LE(stamp.time, 10);
    local.writeUInt16LE(stamp.date, 12);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(raw.length, 22);
    local.writeUInt16LE(name.length, 26);
    local.writeUInt16LE(0, 28);
    localParts.push(local, name, compressed);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(0x0800, 8);
    central.writeUInt16LE(8, 10);
    central.writeUInt16LE(stamp.time, 12);
    central.writeUInt16LE(stamp.date, 14);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(compressed.length, 20);
    central.writeUInt32LE(raw.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt16LE(0, 34);
    central.writeUInt16LE(0, 36);
    central.writeUInt32LE(0, 38);
    central.writeUInt32LE(offset, 42);
    centralParts.push(central, name);
    offset += local.length + name.length + compressed.length;
  }
  const directory = Buffer.concat(centralParts);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(files.length, 8);
  end.writeUInt16LE(files.length, 10);
  end.writeUInt32LE(directory.length, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(0, 20);
  return Buffer.concat([...localParts, directory, end]);
}

function inspectBackupArchive(buffer, {
  maxArchiveBytes = 256 * 1024 * 1024,
  maxEntries = 502,
  maxExpandedBytes = 256 * 1024 * 1024,
} = {}) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 22 || buffer.length > maxArchiveBytes) {
    throw new Error(`Backup ZIP must be between 22 bytes and ${Math.round(maxArchiveBytes / (1024 * 1024))} MiB.`);
  }
  const start = Math.max(0, buffer.length - 65_557);
  let end = -1;
  for (let offset = buffer.length - 22; offset >= start; offset -= 1) {
    if (buffer.readUInt32LE(offset) === 0x06054b50) { end = offset; break; }
  }
  if (end < 0) throw new Error('This is not a supported HERTA backup ZIP.');
  const count = buffer.readUInt16LE(end + 10);
  const directorySize = buffer.readUInt32LE(end + 12);
  const directoryOffset = buffer.readUInt32LE(end + 16);
  if (count > maxEntries || directoryOffset + directorySize > end) {
    throw new Error('Backup archive has an invalid file count or directory.');
  }
  const entries = new Map();
  const names = new Set();
  let offset = directoryOffset;
  let totalBytes = 0;
  for (let index = 0; index < count; index += 1) {
    if (offset + 46 > buffer.length || buffer.readUInt32LE(offset) !== 0x02014b50) {
      throw new Error('Backup archive directory is damaged.');
    }
    const flags = buffer.readUInt16LE(offset + 8);
    const compressedSize = buffer.readUInt32LE(offset + 20);
    const uncompressedSize = buffer.readUInt32LE(offset + 24);
    const nameLength = buffer.readUInt16LE(offset + 28);
    const extraLength = buffer.readUInt16LE(offset + 30);
    const commentLength = buffer.readUInt16LE(offset + 32);
    const nameStart = offset + 46;
    const nameEnd = nameStart + nameLength;
    if (nameEnd + extraLength + commentLength > buffer.length || flags & 1) {
      throw new Error('Encrypted or malformed backup ZIP entries are not supported.');
    }
    const name = buffer.toString('utf8', nameStart, nameEnd);
    const normalizedName = name.toLowerCase();
    const isNist = normalizedName.endsWith('.nist');
    const isReport = normalizedName === 'export-report.csv';
    const isManifest = normalizedName === 'backup-manifest.json';
    if (!name || name === '.' || name === '..' || name.includes('/') || name.includes('\\') || !(isNist || isReport || isManifest)) {
      throw new Error('Only flat HERTA NIST backup archives can be restored.');
    }
    if (names.has(normalizedName)) throw new Error('Backup ZIP contains duplicate filenames.');
    names.add(normalizedName);
    totalBytes += uncompressedSize;
    if (totalBytes > maxExpandedBytes || compressedSize > buffer.length) {
      throw new Error(`Backup archive expands beyond the ${Math.round(maxExpandedBytes / (1024 * 1024))} MiB restore limit.`);
    }
    entries.set(name, { uncompressedSize, isNist });
    offset = nameEnd + extraLength + commentLength;
  }
  if (offset !== directoryOffset + directorySize) throw new Error('Backup archive directory length is invalid.');
  return entries;
}

module.exports = { createZip, inspectBackupArchive };
