import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { envelope, fileArguments, loadFiles, positiveInteger } from './lib/files.mjs';

export async function kafkaOptions(env = process.env) {
  const brokers = (env.KAFKA_BROKERS || 'localhost:19092').split(',').map(v => v.trim()).filter(Boolean);
  if (!brokers.length || brokers.some(v => !/^(\[[\da-f:]+\]|[^\s:/]+):\d+$/i.test(v))) {
    throw new Error('KAFKA_BROKERS must contain host:port entries separated by commas (no URL scheme).');
  }
  const tls = env.KAFKA_TLS || 'false';
  if (!['true', 'false'].includes(tls)) throw new Error('KAFKA_TLS must be true or false.');
  if (env.KAFKA_CA_FILE && tls !== 'true') throw new Error('KAFKA_CA_FILE requires KAFKA_TLS=true.');
  let sasl;
  if (env.KAFKA_SASL_MECHANISM) {
    if (!['plain', 'scram-sha-256', 'scram-sha-512'].includes(env.KAFKA_SASL_MECHANISM) || !env.KAFKA_USERNAME || !env.KAFKA_PASSWORD) {
      throw new Error('Set a supported KAFKA_SASL_MECHANISM, KAFKA_USERNAME and KAFKA_PASSWORD.');
    }
    sasl = { mechanism: env.KAFKA_SASL_MECHANISM, username: env.KAFKA_USERNAME, password: env.KAFKA_PASSWORD };
  } else if (env.KAFKA_USERNAME || env.KAFKA_PASSWORD) {
    throw new Error('Credentials require KAFKA_SASL_MECHANISM.');
  }
  return {
    clientId: env.KAFKA_CLIENT_ID || 'herta-external-publisher', brokers, sasl,
    ssl: tls === 'true' ? { rejectUnauthorized: true, ...(env.KAFKA_CA_FILE ? { ca: [await readFile(env.KAFKA_CA_FILE, 'utf8')] } : {}) } : undefined,
    connectionTimeout: 10000, requestTimeout: 30000, retry: { retries: 3 },
  };
}

export function kafkaMessages(files, env = process.env) {
  const format = env.KAFKA_FORMAT || 'json-base64';
  if (!['json-base64', 'raw-nist'].includes(format)) throw new Error('KAFKA_FORMAT must be json-base64 or raw-nist.');
  const limit = positiveInteger(env.KAFKA_MAX_MESSAGE_BYTES, 900000, 'KAFKA_MAX_MESSAGE_BYTES');
  return files.map(file => {
    const value = format === 'raw-nist' ? file.data : Buffer.from(JSON.stringify(envelope([file])));
    if (Buffer.byteLength(file.name) + value.length > limit) {
      throw new Error(`${file.name}: Kafka message exceeds ${limit} bytes. Configure broker/topic/consumer limits before raising KAFKA_MAX_MESSAGE_BYTES, or use REST.`);
    }
    return { key: file.name, value };
  });
}

export async function publishKafka(files, env = process.env, createProducer) {
  const topic = env.KAFKA_TOPIC || 'herta.nist';
  if (!/^[\w.-]{1,249}$/.test(topic) || ['.', '..'].includes(topic)) throw new Error('Invalid KAFKA_TOPIC.');
  const messages = kafkaMessages(files, env); // Validate every file before connecting/sending any.
  const options = await kafkaOptions(env);
  let producer;
  if (createProducer) producer = createProducer(options);
  else {
    const { Kafka, logLevel } = await import('kafkajs');
    producer = new Kafka({ ...options, logLevel: logLevel.NOTHING }).producer({ allowAutoTopicCreation: false, idempotent: true, maxInFlightRequests: 1 });
  }
  const published = [];
  try {
    await producer.connect();
    for (const message of messages) {
      const records = await producer.send({ topic, acks: -1, messages: [message] });
      const receipt = { fileName: message.key, topic, records: records.map(r => ({ partition: r.partition, offset: r.baseOffset })) };
      published.push(receipt);
    }
    return published;
  } catch {
    throw new Error(`Kafka publish failed; ${published.length}/${messages.length} message(s) acknowledged. Check broker connectivity, topic, TLS/SASL and receiver history before retrying. A failed acknowledgement can still mean delivery.`);
  } finally {
    try { await producer.disconnect(); }
    catch { console.error('Kafka disconnect did not complete; check broker connections.'); }
  }
}

async function main() {
  const paths = fileArguments(process.argv.slice(2));
  if (!paths) return console.log('Usage: npm run kafka -- --file "C:/data/subject.nist" [--file "C:/data/other.nist"]');
  console.log(JSON.stringify(await publishKafka(await loadFiles(paths)), null, 2));
  console.log('Broker acknowledged. Check the Kafka receiver inbox and import into HERTA.');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
