import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { writeFileSync, renameSync, existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
const [cliPath, stateDir, cwd, conversationID, messagesJSON, fileOut] = process.argv.slice(2);
let root = resolve(cliPath);
while (true) {
  const manifest = resolve(root, 'package.json');
  if (existsSync(manifest) && JSON.parse(await readFile(manifest, 'utf8')).name === '@earendil-works/pi-coding-agent') break;
  const parent = resolve(root, '..');
  if (parent === root) throw new Error('could not locate Pi package root from CLI');
  root = parent;
}
const pkg = JSON.parse(await readFile(resolve(root, 'package.json'), 'utf8'));
const { SessionManager } = await import(pathToFileURL(resolve(root, pkg.main || 'dist/index.js')).href);
const pointer = resolve(stateDir, 'session.json');
let sessionFile;
if (existsSync(pointer)) {
  const value = JSON.parse(await (await import('node:fs/promises')).readFile(pointer, 'utf8'));
  if (value.conversationID !== conversationID || typeof value.sessionFile !== 'string') throw new Error('session pointer does not match conversation');
  sessionFile = resolve(value.sessionFile);
  if (!existsSync(sessionFile)) throw new Error('mapped Pi session is missing');
} else {
  const manager = SessionManager.create(cwd, resolve(stateDir, 'pi-sessions'));
  const messages = JSON.parse(messagesJSON);
  const lastUser = messages.reduce((n, item, i) => item.role === 'user' ? i : n, -1);
  for (let i=0; i<messages.length; i++) {
    const item = messages[i];
    if (i === lastUser || item.role === 'system' || item.role === 'developer') continue;
    if (item.role === 'user') manager.appendMessage({ role: 'user', content: item.content, timestamp: Date.now() });
    else if (item.role === 'assistant') manager.appendMessage({ role: 'assistant', content: [{ type: 'text', text: item.content }], api: 'openai-completions', provider: 'openai', model: 'imported-history', usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: 'stop', timestamp: Date.now() });
    else throw new Error(`unsupported imported role: ${item.role}`);
  }
  sessionFile = manager.getSessionFile();
  if (!sessionFile) throw new Error('Pi SessionManager did not persist session');
  const temp = `${pointer}.${process.pid}.tmp`;
  writeFileSync(temp, JSON.stringify({ conversationID, sessionFile }), { flag: 'wx', mode: 0o600 });
  renameSync(temp, pointer);
}
writeFileSync(fileOut, sessionFile);
