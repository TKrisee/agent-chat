import { test, expect } from './fixture.js';

test('resizing preserves the visible message or the bottom', async ({page, serviceURL}) => {
  const messages = Array.from({length:30}, (_,i) => `{"id":"m${i}","room_id":"dm:operator:root","sender":"root","recipient":"operator","role":"root","text":"Message ${i}: ${'A longer line of text that wraps with the window width. '.repeat(4)}","at":"2026-01-01T00:00:00Z"}`).join(',');
  const state = `{"projects":[],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle"}],"tasks":[],"jobs":[],"messages":[${messages}]}`;
  await page.route('**/api/state', route => route.fulfill({status:200,contentType:'application/json',body:state}));
  await page.setViewportSize({width:1200,height:800});
  await page.goto(serviceURL);
  const scroll=page.locator('#chat-scroll');
  await page.locator('[data-message-id="m12"]').scrollIntoViewIfNeeded();
  await scroll.evaluate(el => { const item=el.querySelector('[data-message-id="m12"]'); el.scrollTop += item.getBoundingClientRect().top-el.getBoundingClientRect().top+8; });
  await page.waitForTimeout(100);
  expect(await scroll.evaluate(el => Array.from(el.querySelectorAll('[data-message-id]')).find(item => item.getBoundingClientRect().bottom > el.getBoundingClientRect().top)?.getAttribute('data-message-id'))).toBe('m12');
  const offset=()=>page.locator('[data-message-id="m12"]').evaluate(el=>el.getBoundingClientRect().top-el.closest('#chat-scroll').getBoundingClientRect().top);
  const before=await offset();
  await page.setViewportSize({width:930,height:650});
  await expect.poll(async()=>Math.abs(await offset()-before)).toBeLessThan(3);
  await scroll.evaluate(el=>{el.scrollTop=el.scrollHeight;});
  await page.waitForTimeout(100);
  await page.setViewportSize({width:1100,height:750});
  await expect.poll(()=>scroll.evaluate(el=>el.scrollHeight-el.clientHeight-el.scrollTop)).toBeLessThan(3);
});
