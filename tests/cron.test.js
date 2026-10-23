const assert = require('node:assert/strict');
const test = require('node:test');
const { parseCron, matchesCron, nextCronDate } = require('../electron/cron');

test('matches a daily cron schedule using local time', () => {
  assert.equal(matchesCron('0 2 * * *', new Date(2026, 9, 23, 2, 0)), true);
  assert.equal(matchesCron('0 2 * * *', new Date(2026, 9, 23, 2, 1)), false);
});

test('supports ranges, lists and step values', () => {
  assert.equal(matchesCron('*/15 8-10 * * 1-5', new Date(2026, 9, 23, 9, 30)), true);
  assert.equal(matchesCron('*/15 8-10 * * 1-5', new Date(2026, 9, 24, 9, 30)), false);
});

test('rejects invalid cron expressions and finds the next matching minute', () => {
  assert.throws(() => parseCron('0 25 * * *'), /between/);
  const next = nextCronDate('0 2 * * *', new Date(2026, 9, 23, 2, 1));
  assert.equal(next.getDate(), 24);
  assert.equal(next.getHours(), 2);
  assert.equal(next.getMinutes(), 0);
});
