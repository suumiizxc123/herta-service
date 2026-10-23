const test = require('node:test');
const assert = require('node:assert/strict');

test('keeps REST and Kafka queues distinct, including previously saved inbox entries', async () => {
  const { inboxSourceType, sourceInbox } = await import('../src/lib/import-sources.mjs');
  const entries = [
    { id: 'rest-new', sourceType: 'rest', connectorId: 'api-a' },
    { id: 'rest-old', source: 'REST receiver' },
    { id: 'kafka-old', source: 'Kafka · herta.nist', kafka: { partition: 0, offset: '22' } },
    { id: 'kafka-error', sourceType: 'kafka', connectorId: 'topic-b', error: 'Invalid message' },
    { id: 'unknown', source: 'Other system' },
  ];
  assert.deepEqual(sourceInbox(entries, 'rest').map((item) => item.id), ['rest-new', 'rest-old']);
  assert.deepEqual(sourceInbox(entries, 'kafka').map((item) => item.id), ['kafka-old', 'kafka-error']);
  assert.deepEqual(sourceInbox(entries, 'rest', 'api-a').map((item) => item.id), ['rest-new']);
  assert.deepEqual(sourceInbox(entries, 'kafka', 'api-a'), []);
  assert.equal(inboxSourceType(entries[4]), 'unknown');
});
