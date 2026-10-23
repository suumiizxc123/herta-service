import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { envelope, fileArguments, loadFiles } from '../lib/files.mjs';
import { publishRest } from '../rest-publisher.mjs';
import { kafkaMessages, kafkaOptions, publishKafka } from '../kafka-publisher.mjs';
import { createSourceServer } from '../rest-source-server.mjs';

const sample = { name: 'subject-001.nist', data: Buffer.from([0, 1, 128, 255, 10]) }; // Transport fixture only.

async function listen(t, server) {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }));
  return `http://127.0.0.1:${server.address().port}`;
}

test('CLI and file input preserve binary bytes, reject empty files and bad arguments', async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'herta-publisher-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = path.join(dir, sample.name);
  await writeFile(file, sample.data);
  assert.deepEqual(await loadFiles(fileArguments(['--file', file])), [sample]);
  assert.deepEqual(Buffer.from(envelope([sample]).files[0].fileContent, 'base64'), sample.data);
  await writeFile(file, '');
  await assert.rejects(loadFiles([file]), /non-empty/);
  assert.throws(() => fileArguments(['--file']), /Use --file/);
  assert.throws(() => fileArguments([]), /1 to 500/);
});

test('REST publisher posts exact bytes and bearer token, reports queued response', async t => {
  const url = await listen(t, http.createServer(async (req, res) => {
    assert.equal(req.url, '/api/v1/nist');
    assert.equal(req.method, 'POST');
    assert.equal(req.headers.authorization, 'Bearer test-receiver-token');
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    assert.deepEqual(JSON.parse(Buffer.concat(chunks)), envelope([sample]));
    res.writeHead(202, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ accepted: 1, ids: ['test-id'] }));
  }));
  const result = await publishRest([sample], { REST_URL: `${url}/api/v1/nist`, REST_TOKEN: 'test-receiver-token' });
  assert.equal(result.httpStatus, 202);
  assert.equal(result.accepted, 1);
  assert.equal(result.imported, undefined);
});

test('REST publisher rejects HTTP errors and wrong acknowledgement, supports auto import response', async t => {
  let mode = 401;
  const url = await listen(t, http.createServer(async (req, res) => {
    for await (const _chunk of req) { /* drain body */ }
    res.writeHead(mode === 401 ? 401 : 200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(mode === 401 ? { error: 'secret must not be echoed' } : mode === 200 ? { ok: true } : { accepted: 1, ids: ['id'], imported: 1, results: [{ imported: true }] }));
  }));
  await assert.rejects(publishRest([sample], { REST_URL: url }), /401.*token/);
  mode = 200;
  await assert.rejects(publishRest([sample], { REST_URL: url }), /Unexpected.*acknowledgement/);
  mode = 201;
  assert.equal((await publishRest([sample], { REST_URL: url })).imported, 1);
});

test('REST source reloads the configured file and serves the worker envelope', async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'herta-source-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = path.join(dir, sample.name);
  await writeFile(file, sample.data);
  const url = await listen(t, createSourceServer([file]));
  assert.deepEqual(await (await fetch(`${url}/nist`)).json(), envelope([sample]));
  await writeFile(file, 'changed');
  assert.equal(Buffer.from((await (await fetch(`${url}/nist`)).json()).files[0].fileContent, 'base64').toString(), 'changed');
  assert.equal((await fetch(`${url}/unknown`)).status, 404);
  assert.equal((await fetch(`${url}/nist`, { method: 'POST' })).status, 404);
});

test('Kafka JSON and raw formats match worker contract, preflight rejects oversized batch members', () => {
  assert.deepEqual(JSON.parse(kafkaMessages([sample], {})[0].value), envelope([sample]));
  assert.deepEqual(kafkaMessages([sample], { KAFKA_FORMAT: 'raw-nist' })[0], { key: sample.name, value: sample.data });
  assert.throws(() => kafkaMessages([sample], { KAFKA_MAX_MESSAGE_BYTES: '2' }), /exceeds/);
  assert.throws(() => kafkaMessages([sample], { KAFKA_FORMAT: 'csv' }), /KAFKA_FORMAT/);
});

test('Kafka validates TLS/SASL and broker configuration without exposing credentials', async () => {
  const options = await kafkaOptions({ KAFKA_TLS: 'true', KAFKA_SASL_MECHANISM: 'scram-sha-512', KAFKA_USERNAME: 'user', KAFKA_PASSWORD: 'test-secret', KAFKA_BROKERS: 'host:9093,host2:9093' });
  assert.equal(options.ssl.rejectUnauthorized, true);
  assert.equal(options.sasl.mechanism, 'scram-sha-512');
  assert.equal(options.brokers.length, 2);
  await assert.rejects(kafkaOptions({ KAFKA_BROKERS: 'http://host:9092' }), /host:port/);
  await assert.rejects(kafkaOptions({ KAFKA_TLS: 'invalid' }), /true or false/);
  await assert.rejects(kafkaOptions({ KAFKA_SASL_MECHANISM: 'plain' }), /KAFKA_USERNAME/);
});

test('Kafka sends acks=-1, returns metadata and disconnects on success/failure', async () => {
  let disconnected = 0;
  let fail = false;
  const producer = {
    async connect() {},
    async send(input) {
      assert.equal(input.acks, -1);
      assert.equal(input.topic, 'herta.nist');
      assert.deepEqual(JSON.parse(input.messages[0].value), envelope([sample]));
      if (fail) throw new Error('secret test-password');
      return [{ partition: 2, baseOffset: '42' }];
    },
    async disconnect() { disconnected++; },
  };
  const result = await publishKafka([sample], {}, () => producer);
  assert.deepEqual(result[0].records, [{ partition: 2, offset: '42' }]);
  fail = true;
  await assert.rejects(publishKafka([sample], {}, () => producer), /0\/1.*acknowledged/);
  assert.equal(disconnected, 2);
});
