import { test as base } from 'playwright/test';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const web = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const source = resolve(web, '..');

export const test = base.extend({
  serviceURL: [async ({}, use) => {
    const dir = await mkdtemp(join(tmpdir(), 'agent-chat-e2e-'));
    const binary = join(dir, process.platform === 'win32' ? 'agentchat.exe' : 'agentchat');
    let server;
    try {
      const build = spawnSync('go', ['build', '-o', binary, './cmd/agentchat'], { cwd: source, encoding: 'utf8' });
      if (build.status !== 0) throw new Error(`Go build failed: ${build.error || build.stderr}`);
      server = spawn(binary, ['-data', join(dir, 'data'), '-static', join(web, 'dist'), '-listen', '127.0.0.1:0'], { stdio: ['ignore', 'ignore', 'pipe'] });
      const url = await new Promise((resolveURL, reject) => {
        let output = '';
        const timeout = setTimeout(() => reject(new Error(`Fixture startup timed out: ${output}`)), 15000);
        server.stderr.on('data', chunk => {
          output += chunk.toString();
          const match = output.match(/runtime on (http:\/\/127\.0\.0\.1:\d+)/);
          if (match) { clearTimeout(timeout); resolveURL(match[1]); }
        });
        server.once('error', error => { clearTimeout(timeout); reject(error); });
        server.once('exit', code => { clearTimeout(timeout); reject(new Error(`Fixture exited (${code}): ${output}`)); });
      });
      await use(url);
    } finally {
      if (server && server.exitCode === null) {
        await new Promise(resolveExit => { server.once('exit', resolveExit); server.kill('SIGTERM'); });
      }
      await rm(dir, { recursive: true, force: true });
    }
  }, { scope: 'worker' }],
});
export { expect } from 'playwright/test';
