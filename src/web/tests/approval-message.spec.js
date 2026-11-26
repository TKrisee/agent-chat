import {test, expect} from './fixture.js';

test('Root notification allows only an explicit linked approval decision', async ({page,serviceURL}) => {
 const pending=`{"projects":[],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle","model":"test"}],"tasks":[],"jobs":[],"approvals":[{"id":"approval-1","status":"pending"}],"messages":[{"id":"plain","room_id":"dm:operator:root","sender":"root","recipient":"operator","role":"root","text":"Ordinary update","at":"2026-01-01T00:00:00Z"},{"id":"question","room_id":"dm:operator:root","sender":"root","recipient":"operator","role":"root","text":"Choose whether to approve the requested roster change.","at":"2026-01-01T00:00:01Z","approval_id":"approval-1"}]}`;
 const resolved=`{"projects":[],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle","model":"test"}],"tasks":[],"jobs":[],"approvals":[{"id":"approval-1","status":"approved"}],"messages":[{"id":"plain","room_id":"dm:operator:root","sender":"root","recipient":"operator","role":"root","text":"Ordinary update","at":"2026-01-01T00:00:00Z"},{"id":"question","room_id":"dm:operator:root","sender":"root","recipient":"operator","role":"root","text":"Choose whether to approve the requested roster change.","at":"2026-01-01T00:00:01Z","approval_id":"approval-1"}]}`;
 let decided=false, posts=0, body='';let release;
 const wait=new Promise(resolve=>{release=resolve;});
 await page.route('**/api/state',route=>route.fulfill({status:200,contentType:'application/json',body:decided?resolved:pending}));
 await page.route('**/api/approvals/approval-1',async route=>{posts++;body=route.request().postData()||'';await wait;decided=true;await route.fulfill({status:200,body:''});});
 await page.goto(serviceURL);
 const ordinary=page.locator('[data-message-id="plain"]');const question=page.locator('[data-message-id="question"]');
 await expect(ordinary.getByRole('button',{name:'Approve'})).toHaveCount(0);
 await expect(question).toContainText('Choose whether to approve the requested roster change.');
 await question.getByRole('button',{name:'Approve'}).click();
 await expect(question.getByRole('button',{name:'Deny'})).toBeDisabled();
 expect(posts).toBe(1);expect(body).toBe('{"decision":"approved"}');
 release();
 await expect(question.getByRole('status')).toHaveText('Approved');
 await expect(question.getByRole('button',{name:'Approve'})).toHaveCount(0);
});
