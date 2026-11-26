import { test, expect } from './fixture.js';

test('dependency-waiting work is visible without claiming the worker is queued', async ({page, serviceURL}) => {
  const state = `{"projects":[{"id":"atlas","name":"Atlas","status":"running","po_id":"po"}],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle"},{"id":"po","name":"PO","role":"po","project_id":"atlas","status":"idle"},{"id":"worker","name":"Worker","role":"worker","project_id":"atlas","status":"idle"}],"tasks":[{"id":"task","project_id":"atlas","title":"Dependent work","status":"queued","owner_id":"worker","waiting_on_dependencies":true}],"jobs":[{"id":"job","agent_id":"worker","project_id":"atlas","task_id":"task","kind":"work","status":"pending"}],"messages":[],"approvals":[]}`;
  await page.route('**/api/state', route => route.fulfill({status:200,contentType:'application/json',body:state}));
  await page.goto(serviceURL);
  await page.locator('[data-project="atlas"]').click();
  await expect(page.locator('[data-agent="worker"]')).toHaveAttribute('aria-label', /Idle/);
  await page.locator('.project-activity details').filter({hasText:'Work'}).locator('summary').click();
  await expect(page.locator('.project-activity li').filter({hasText:'Dependent work'})).toContainText('Waiting on dependencies');
  await page.locator('[data-agent="worker"]').click();
  await expect(page.locator('.channel-header')).not.toContainText('Queued');
});
