import { spawnSync } from 'node:child_process';
import { test, expect } from './fixture.js';

function jq(input, filter) {
  const result = spawnSync('jq', ['-er', filter], {input, encoding:'utf8'});
  if (result.status !== 0) throw new Error(`jq failed: ${result.stderr}`);
  return result.stdout.trim();
}

test('routine polls merge only changes while earlier conversation stays available', async ({page, serviceURL}) => {
  const old = {id:'7916a600-6c3a-471e-bce9-08aa0749c087', recipient:'root', text:'Earlier chat remains available'};
  const newMessage = {id:'91ac34b1-1b44-4a31-8e89-c4b284ebcd14', recipient:'root', text:'A new message arrives in a state update', reply_to:old.id};
  expect((await page.request.post(serviceURL+'/api/messages', {data:old})).status()).toBe(201);

  let snapshots = 0;
  const deltas = [];
  page.on('response', response => {
    const path = new URL(response.url()).pathname;
    if (path === '/api/state') snapshots++;
    if (path === '/api/state/changes' && response.ok()) deltas.push(response);
  });
  await page.goto(serviceURL);
  await expect(page.locator(`[data-message-id="${old.id}"]`)).toBeVisible();
  await expect.poll(() => deltas.length, {timeout:15000}).toBeGreaterThan(0);
  expect((await page.request.post(serviceURL+'/api/messages', {data:newMessage})).status()).toBe(201);
  await expect(page.locator(`[data-message-id="${newMessage.id}"]`)).toContainText(newMessage.text, {timeout:15000});
  await expect(page.locator(`[data-message-id="${old.id}"]`)).toBeVisible();
  expect(snapshots).toBe(1);

  const messages = [];
  for (const delta of deltas) {
    const ids = jq(await delta.text(), '[(.messages // [])[].id] | join(" ")');
    if (ids) messages.push(...ids.split(' '));
  }
  expect(messages).toContain(newMessage.id);
  expect(messages).not.toContain(old.id);
});
