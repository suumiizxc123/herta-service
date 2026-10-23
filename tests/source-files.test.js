const assert = require('node:assert/strict');
const test = require('node:test');
const { normalizeNistSourcePayload } = require('../electron/source-files');

test('normalizes a REST JSON file envelope into safe NIST filenames', () => {
  const files = normalizeNistSourcePayload({ files: [
    { code: 'SUB 001', fileContent: Buffer.from([1, 2, 3]).toString('base64') },
    { filename: 'sub 001.nist', dataBase64: Buffer.from([4, 5]).toString('base64') },
  ] });
  assert.deepEqual(files.map(({ name }) => name), ['SUB_001.nist', 'sub_001-2.nist']);
  assert.deepEqual([...files[0].data], [1, 2, 3]);
});

test('rejects malformed or oversized source payloads', () => {
  assert.throws(() => normalizeNistSourcePayload({ fileContent: 'not base64!' }), /valid Base64/);
  assert.throws(() => normalizeNistSourcePayload({ files: [] }), /1 to 500/);
});
