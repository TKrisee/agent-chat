import { test, expect } from './fixture.js';

test('retired one-shot workers stay discoverable without crowding active team', async ({page, serviceURL}) => {
  const state = `{"projects":[{"id":"atlas","name":"Atlas","status":"running","po_id":"po"}],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle"},{"id":"po","name":"PO","role":"po","project_id":"atlas","status":"idle"},{"id":"active","name":"Active","role":"worker","project_id":"atlas","status":"idle","one_shot":true},{"id":"past","name":"Past","role":"worker","project_id":"atlas","status":"idle","model":"terra","one_shot":true,"one_shot_retired":true}],"tasks":[],"jobs":[],"messages":[{"id":"m1","room_id":"dm:operator:past","sender":"past","recipient":"operator","role":"worker","text":"Saved result","at":"2026-01-01T00:00:01Z"}]}`;
  await page.route('**/api/state', route => route.fulfill({status:200,contentType:'application/json',body:state}));
  await page.goto(serviceURL);
  await page.locator('[data-project="atlas"]').click();
  await expect(page.locator('[data-agent="active"]')).toBeVisible();
  const history=page.locator('.project-agents details').filter({hasText:'Past one-shot workers'});
  await expect(history).not.toHaveAttribute('open', '');
  await history.locator('summary').click();
  await history.locator('[data-agent="past"]').click();
  await expect(page.getByRole('region', {name:'Conversation with Past'}).getByText('Saved result')).toBeVisible();
  const team=page.locator('.project-activity details').filter({hasText:'Past one-shot workers'});
  await team.locator('summary').click();
  await expect(team.getByText('terra')).toBeVisible();
});
