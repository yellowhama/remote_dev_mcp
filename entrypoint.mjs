import fs from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
try {
  execFileSync('git', ['config', '--global', '--add', 'safe.directory', '*']);
  execFileSync('git', ['config', '--global', 'core.autocrlf', 'false']);
  execFileSync('git', ['config', '--global', 'core.preloadindex', 'true']);
} catch {}
const settings = JSON.parse(await fs.readFile('/state/runtime.json', 'utf8'));
process.env.MCP_PUBLIC_URL = settings.publicUrl;
process.env.MCP_ALLOWED_HOSTS = `${new URL(settings.publicUrl).hostname},localhost,127.0.0.1,mcp`;
process.env.MCP_OAUTH_APPROVAL_KEY = (await fs.readFile('/state/approval-key.txt', 'utf8')).trim();
delete process.env.MCP_AUTH_TOKEN;
await import('./guard.mjs');
await import('./dist/src/server.js');
