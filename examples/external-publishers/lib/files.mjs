import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';

export const MAX_FILE_BYTES = 32 * 1024 * 1024;
export const MAX_BATCH_BYTES = 256 * 1024 * 1024;

export function fileArguments(args) {
  if (args.includes('--help') || args.includes('-h')) return null;
  const files = [];
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] !== '--file' || !args[i + 1] || args[i + 1].startsWith('--')) {
      throw new Error('Use --file "path/to/file.nist" (repeat --file for multiple files).');
    }
    files.push(args[++i]);
  }
  if (!files.length || files.length > 500) throw new Error('Choose 1 to 500 files with --file.');
  return files;
}

export async function loadFiles(paths) {
  if (!paths.length || paths.length > 500) throw new Error('Choose 1 to 500 files.');
  const files = [];
  let total = 0;
  for (const filePath of paths) {
    const info = await stat(filePath);
    if (!info.isFile() || info.size < 1 || info.size > MAX_FILE_BYTES) {
      throw new Error(`${path.basename(filePath)}: expected a non-empty file of at most 32 MiB.`);
    }
    const data = await readFile(filePath);
    total += data.length;
    if (!data.length || data.length > MAX_FILE_BYTES || total > MAX_BATCH_BYTES) {
      throw new Error('Limits: 32 MiB per file and 256 MiB per batch.');
    }
    files.push({ name: path.basename(filePath), data });
  }
  return files;
}

export function envelope(files) {
  return { files: files.map(({ name, data }) => ({ fileName: name, fileContent: data.toString('base64') })) };
}

export function positiveInteger(value, fallback, name) {
  const number = value === undefined || value === '' ? fallback : Number(value);
  if (!Number.isSafeInteger(number) || number < 1) throw new Error(`${name} must be a positive integer.`);
  return number;
}

export function httpUrl(value) {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
    throw new Error('Use an HTTP(S) URL without embedded credentials.');
  }
  return url;
}
