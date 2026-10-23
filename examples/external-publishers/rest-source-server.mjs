import http from 'node:http';
import { pathToFileURL } from 'node:url';
import { envelope, fileArguments, loadFiles, positiveInteger } from './lib/files.mjs';

export function createSourceServer(paths) {
  return http.createServer(async (request, response) => {
    const pathname = new URL(request.url, 'http://localhost').pathname;
    if (request.method !== 'GET' || pathname !== '/nist') {
      response.writeHead(404, { 'Content-Type': 'application/json' });
      response.end(JSON.stringify({ error: 'Use GET /nist.' })); return;
    }
    try {
      const payload = envelope(await loadFiles(paths));
      response.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      response.end(JSON.stringify(payload));
    } catch {
      response.writeHead(500, { 'Content-Type': 'application/json' });
      response.end(JSON.stringify({ error: 'Cannot read the configured NIST files. Check file paths and size limits on the source machine.' }));
    }
  });
}

async function main() {
  const paths = fileArguments(process.argv.slice(2));
  if (!paths) return console.log('Usage: npm run source -- --file "C:/data/subject.nist"');
  await loadFiles(paths); // Fail before binding if files are unavailable.
  const port = positiveInteger(process.env.SOURCE_PORT, 18200, 'SOURCE_PORT');
  if (port > 65535) throw new Error('SOURCE_PORT must be between 1 and 65535.');
  const host = process.env.SOURCE_HOST || '127.0.0.1';
  const server = createSourceServer(paths);
  server.on('error', () => { console.error('Source server could not listen. Check SOURCE_HOST, SOURCE_PORT and port conflicts.'); process.exitCode = 1; });
  server.listen(port, host, () => console.log(`REST source listening on ${host}:${port}, GET /nist. Ctrl+C to stop.`));
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close());
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
