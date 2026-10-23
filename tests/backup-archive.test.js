const assert = require('node:assert/strict');
const test = require('node:test');
const { unzipSync } = require('fflate');
const { createZip, inspectBackupArchive } = require('../electron/backup-archive');

const archive = (files) => createZip(Object.entries(files).map(([name, data]) => ({ name, data: Buffer.from(data) })));

test('accepts a flat HERTA NIST snapshot and metadata', () => {
  const zip = archive({ 'SUB001.nist': [1, 2, 3], 'export-report.csv': [65], 'backup-manifest.json': [123, 125] });
  const entries = inspectBackupArchive(zip);
  assert.equal(entries.get('SUB001.nist').isNist, true);
  assert.equal(entries.get('SUB001.nist').uncompressedSize, 3);
  assert.equal(entries.get('export-report.csv').isNist, false);
  assert.deepEqual([...unzipSync(zip)['SUB001.nist']], [1, 2, 3]);
  assert.equal(inspectBackupArchive(archive({ 'SUB..001.nist': [1] })).get('SUB..001.nist').isNist, true);
});

test('rejects nested and traversal paths', () => {
  assert.throws(() => inspectBackupArchive(archive({ '../escape.nist': [1] })), /flat HERTA/);
  assert.throws(() => inspectBackupArchive(archive({ 'nested/SUB001.nist': [1] })), /flat HERTA/);
});

test('rejects non-NIST payloads and oversized expanded archives', () => {
  assert.throws(() => inspectBackupArchive(archive({ 'payload.exe': [1] })), /flat HERTA/);
  assert.throws(() => inspectBackupArchive(archive({ 'SUB001.nist': [1, 2, 3] }), { maxExpandedBytes: 2 }), /expands beyond/);
});
