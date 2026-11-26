import { test, expect } from './fixture.js';

test('highlights recognized mentions only in ordinary message prose', async ({page, serviceURL}) => {
 const state = `{"projects":[{"id":"rot","name":"Rot","po_id":"po"}],"agents":[{"id":"root","name":"Root","role":"root","status":"idle"},{"id":"po","name":"into-the-rot-PO","role":"po","project_id":"rot","status":"idle"}],"tasks":[],"jobs":[],"messages":[{"id":"m1","room_id":"group:rot","project_id":"rot","sender":"po","recipient":"group:rot","role":"po","text":"Hello @into-the-rot-PO and @operator. mail@operator.com https://example.com/@operator \`@operator\` [@operator](https://example.com) @unknown @operatoré é@operator","at":"2026-01-01T00:00:00Z"}]}`;
 await page.route('**/api/state', route => route.fulfill({status:200,contentType:'application/json',body:state}));
 await page.goto(serviceURL);
 await page.locator('[data-project="rot"]').click();
 const message=page.locator('[data-message-id="m1"]');
 await expect(message.locator('.message-mention')).toHaveText(['@into-the-rot-PO','@operator']);
 await expect(message.locator('code .message-mention, a .message-mention')).toHaveCount(0);
 await expect(message.locator('code')).toHaveText('@operator');
});
