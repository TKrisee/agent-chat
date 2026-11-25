import { spawnSync } from 'node:child_process';
import { test, expect } from './fixture.js';

function jq(input, filter) {
  const result = spawnSync('jq', ['-r', filter], {input, encoding:'utf8'});
  if (result.status !== 0) throw new Error(`jq failed: ${result.stderr}`);
  return result.stdout.trim().split('\n').filter(Boolean);
}

test('group unread boundary and explicit room-only mark as read', async ({page, serviceURL}) => {
  let explicit = false;
  const requests = [];
  await page.route('**/api/messages/read', async route => {
    if (!explicit) return route.fulfill({status:503, body:''});
    requests.push(...jq(route.request().postData() || '', '.message_ids[]'));
    await route.continue();
  });
  await page.goto(serviceURL);
  const root = page.getByLabel('Message Root');
  await root.fill('Start project atlas: inspect unread boundary');
  await root.press('Enter');
  await expect(page.locator('[data-project="atlas"]')).toBeVisible({timeout:20000});
  await expect.poll(async () => jq(await (await page.request.get(`${serviceURL}/api/state`)).text(), '[.tasks[] | select(.project_id=="atlas" and .status=="completed")] | length')[0], {timeout:30000}).toBe('2');
  await root.fill('Start project birch: inspect other room');
  await root.press('Enter');
  await expect(page.locator('[data-project="birch"]')).toBeVisible({timeout:20000});
  await expect.poll(async () => jq(await (await page.request.get(`${serviceURL}/api/state`)).text(), '[.tasks[] | select(.project_id=="birch" and .status=="completed")] | length')[0], {timeout:30000}).toBe('2');
  await page.locator('[data-project="atlas"]').click();
  const banner = page.locator('.group-unread-banner');
  await expect(banner).toBeVisible();
  await expect(page.locator('.unread-divider')).toHaveCount(1);
  const before = await (await page.request.get(`${serviceURL}/api/state`)).text();
  const atlas = jq(before, '.messages[] | select(.room_id=="group:atlas" and .sender!="operator" and .operator_read_at==null) | .id');
  const birch = jq(before, '.messages[] | select(.room_id=="group:birch" and .sender!="operator" and .operator_read_at==null) | .id');
  expect(atlas.length).toBeGreaterThan(0);
  expect(birch.length).toBeGreaterThan(0);
  await expect(banner).toContainText(`${atlas.length} unread`);
  await expect(page.locator('.unread-divider + [data-message-id]')).toHaveAttribute('data-message-id', atlas[0]);
  await page.locator('#chat-scroll').evaluate(el => { el.scrollTop = 0; });
  expect(requests).toEqual([]);
  explicit = true;
  await banner.getByRole('button', {name:'Mark as read'}).click();
  await expect(banner).toHaveCount(0);
  expect([...new Set(requests)].sort()).toEqual([...atlas].sort());
  const after = await (await page.request.get(`${serviceURL}/api/state`)).text();
  expect(jq(after, '.messages[] | select(.room_id=="group:birch" and .sender!="operator" and .operator_read_at==null) | .id').sort()).toEqual(birch.sort());
});

test('fast group scrolling batches messages visible during the gesture', async ({page, serviceURL}) => {
  const state = `{"projects":[{"id":"atlas","name":"Atlas","status":"running","po_id":"po"}],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle"},{"id":"po","name":"PO","role":"po","project_id":"atlas","status":"idle"}],"tasks":[],"jobs":[],"messages":[{"id":"a1","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"one","at":"2026-01-01T00:00:01Z"},{"id":"a2","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"two","at":"2026-01-01T00:00:02Z"},{"id":"a3","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"three","at":"2026-01-01T00:00:03Z"},{"id":"a4","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"four","at":"2026-01-01T00:00:04Z"},{"id":"a5","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"five","at":"2026-01-01T00:00:05Z"},{"id":"a6","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"six","at":"2026-01-01T00:00:06Z"},{"id":"a7","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"seven","at":"2026-01-01T00:00:07Z"},{"id":"a8","room_id":"group:atlas","sender":"po","recipient":"group:atlas","role":"po","text":"eight","at":"2026-01-01T00:00:08Z"}]}`;
  const requests = [];
  await page.route('**/api/state', route => route.fulfill({status:200,contentType:'application/json',body:state}));
  await page.route('**/api/messages/read', route => { requests.push(jq(route.request().postData() || '', '.message_ids[]')); return route.fulfill({status:204}); });
  await page.goto(serviceURL);
  await page.addStyleTag({content:'.message { min-height: 190px !important; }'});
  await page.locator('[data-project="atlas"]').click();
  const scroll = page.locator('#chat-scroll');
  await expect(scroll.locator('[data-message-id="a1"]')).toBeVisible();
  await page.waitForTimeout(300);
  requests.length = 0;
  const box = await scroll.boundingBox();
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  for (let i = 0; i < 5; i++) { await page.mouse.wheel(0, 170); await page.waitForTimeout(35); }
  expect(requests).toHaveLength(0);
  await expect.poll(() => requests.length).toBeGreaterThan(0);
  const ids = requests.flat();
  expect(ids).toContain('a4');
  expect(ids).toContain('a6');
});
