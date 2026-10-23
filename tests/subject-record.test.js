const assert = require('node:assert/strict');
const test = require('node:test');
const { makeSubjectDetails, summarizeSubject } = require('../electron/subject-record');

test('summarizes subject fields without returning the profile payload', () => {
  const subject = { code: 'SUB001', group: '1', profile: { fingers: 10 } };
  const summary = summarizeSubject(subject);
  assert.equal(summary.profile_length, JSON.stringify(subject.profile).length);
  assert.equal(summary.field_count, 3);
  assert.deepEqual(summary.metadata, { code: 'SUB001', group: '1' });
  assert.equal(Object.hasOwn(summary, 'profile'), false);
});

test('prepares readable JSON profile details and applies a display limit', () => {
  const details = makeSubjectDetails({ code: 'SUB001', profile: '0123456789abcdefghij' }, 8);
  assert.equal(details.profileJson, null);
  assert.equal(details.profileLength > details.profileText.length, true);
  assert.equal(details.profileTruncated, true);
  assert.deepEqual(details.fields, { code: 'SUB001' });
  assert.deepEqual(makeSubjectDetails({ code: 'SUB002', profile: { fingerprints: [{ type: 4 }] } }).profileJson, { fingerprints: [{ type: 4 }] });
});
