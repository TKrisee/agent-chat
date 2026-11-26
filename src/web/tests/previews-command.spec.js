import {test,expect} from './fixture.js';

test('worker DM previews command explicitly controls that worker only',async({page,serviceURL})=>{
 const state=`{"projects":[{"id":"alpha","name":"Alpha","status":"running","po_id":"po"}],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle"},{"id":"po","name":"Owner","role":"po","project_id":"alpha","status":"idle"},{"id":"worker","name":"Artist","role":"worker","project_id":"alpha","status":"idle"}],"tasks":[],"jobs":[],"messages":[]}`;
 const calls=[];
 await page.route('**/api/state',route=>route.fulfill({status:200,contentType:'application/json',body:state}));
 await page.route('**/api/agents/worker/operator-previews',route=>{calls.push(route.request().postData());return route.fulfill({status:200,body:''});});
 await page.goto(serviceURL);await page.locator('[data-project="alpha"]').click();await page.locator('[data-agent="worker"]').click();
 const composer=page.getByLabel('Message Artist');
 await composer.fill('/pre');await expect(page.getByRole('listbox',{name:'Slash commands'})).toContainText('/previews');
 await composer.press('Enter');await composer.fill('/previews on');await composer.press('Enter');
 await expect.poll(()=>calls.length).toBe(1);expect(calls[0]).toBe('enabled=true');
 await expect(page.getByRole('status').filter({hasText:'Direct image previews enabled.'})).toBeVisible();
 await composer.fill('/previews off');await composer.press('Enter');await expect.poll(()=>calls.length).toBe(2);expect(calls[1]).toBe('enabled=false');
});
