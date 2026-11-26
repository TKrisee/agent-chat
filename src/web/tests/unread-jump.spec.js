import { test, expect } from './fixture.js';

test('unread banner jumps to latest read without marking messages read', async ({page, serviceURL}) => {
  const messages = Array.from({length:12}, (_, i) => ({id:`m${i+1}`, room_id:'group:atlas', sender:'po', recipient:'group:atlas', role:'po', text:`Message ${i+1}`, at:`2026-01-01T00:00:${String(i+1).padStart(2,'0')}Z`, ...([1,5].includes(i+1)?{operator_read_at:'2026-01-01T01:00:00Z'}:{})}));
  const state = {projects:[{id:'atlas',name:'Atlas',status:'running',po_id:'po'}], agents:[{id:'root',name:'Root',role:'root',project_id:'',status:'idle'},{id:'po',name:'PO',role:'po',project_id:'atlas',status:'idle'}], tasks:[], jobs:[], messages};
  // Playwright serializes the fixture object for the mocked response; no JSON parsing is used.
  await page.route('**/api/state', route => route.fulfill({status:200, json:state}));
  let reads=0;
  await page.route('**/api/messages/read', route => { reads++; return route.fulfill({status:204}); });
  await page.goto(serviceURL);
  await page.addStyleTag({content:'.message { min-height: 190px !important; }'});
  await page.locator('[data-project="atlas"]').click();
  await page.getByRole('button',{name:'Jump to latest read message'}).click();
  await expect(page.locator('[data-message-id="m5"]')).toBeInViewport();
  expect(reads).toBe(0);
  await expect(page.locator('.group-unread-banner')).toBeVisible();
  await page.getByRole('button',{name:'Mark as read'}).click();
  await expect.poll(()=>reads).toBeGreaterThan(0);
});
