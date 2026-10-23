import { pathToFileURL } from 'node:url';
import { envelope, fileArguments, httpUrl, loadFiles, positiveInteger } from './lib/files.mjs';

export async function publishRest(files, env = process.env) {
  const url = httpUrl(env.REST_URL || 'http://127.0.0.1:18127/api/v1/nist');
  const headers = { 'Content-Type': 'application/json' };
  if (env.REST_TOKEN?.trim()) headers.Authorization = `Bearer ${env.REST_TOKEN.trim()}`;
  let response;
  try {
    response = await fetch(url, {
      method: 'POST', headers, body: JSON.stringify(envelope(files)),
      signal: AbortSignal.timeout(positiveInteger(env.REQUEST_TIMEOUT_MS, 60000, 'REQUEST_TIMEOUT_MS')),
      redirect: 'error',
    });
  } catch (error) {
    throw new Error(`REST connection failed (${error.name}). Delivery may be uncertain; check receiver history before retrying.`);
  }
  let result;
  try { result = await response.json(); }
  catch { throw new Error(`REST HTTP ${response.status}: expected a JSON response. Check REST_URL.`); }
  if (!response.ok) {
    const hint = response.status === 401 ? ' Check the receiver token for this installed app version.'
      : response.status === 409 ? ' Activate the REST receiver in Connectors first.' : '';
    throw new Error(`REST HTTP ${response.status}.${hint}`);
  }
  if (!Number.isInteger(result.accepted) || result.accepted !== files.length || !Array.isArray(result.ids) || result.ids.length !== files.length) {
    throw new Error('Unexpected receiver acknowledgement; check REST_URL and receiver history before retrying.');
  }
  return { httpStatus: response.status, ...result };
}

async function main() {
  const paths = fileArguments(process.argv.slice(2));
  if (!paths) return console.log('Usage: npm run rest -- --file "C:/data/subject.nist" [--file "C:/data/other.nist"]');
  const result = await publishRest(await loadFiles(paths));
  console.log(JSON.stringify(result, null, 2));
  console.log(Number.isInteger(result.imported)
    ? `Received: ${result.accepted}; imported into HERTA: ${result.imported}. Check results/history for pending files.`
    : `Received: ${result.accepted}. Open the app receiver inbox to review and import into HERTA.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
