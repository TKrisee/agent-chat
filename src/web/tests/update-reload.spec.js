import { test, expect } from './fixture.js';

test('reloads once only after the expected update is committed and healthy', async ({page, serviceURL}) => {
  test.setTimeout(120000);
  let phase='available', polls=0, navigations=0, healthFails=0;
  await page.route('**/api/update**', async route => {
    const path=new URL(route.request().url()).pathname;
    if(path.endsWith('/download')) {phase='ready';return route.fulfill({status:202,headers:{'X-AgentChat-Update-Available':'true','X-AgentChat-Update-Current':'1.0.0','X-AgentChat-Update-Version':'2.0.0','X-AgentChat-Update-State':'downloading'}});}
    if(path.endsWith('/apply')) {phase='draining';return route.fulfill({status:202,headers:{'X-AgentChat-Update-Available':'true','X-AgentChat-Update-Current':'1.0.0','X-AgentChat-Update-Version':'2.0.0','X-AgentChat-Update-State':'draining'}});}
    polls++;
    if(phase==='offline')return route.abort();
    const current=phase==='trial'||phase==='unhealthy'||phase==='healthy'?'2.0.0':'1.0.0';
    const state=phase==='available'?'available':phase==='ready'?'ready':phase==='draining'?'draining':'idle';
    const committed=phase==='trial'?'false':'true';
    return route.fulfill({status:200,headers:{'X-AgentChat-Update-Available':'true','X-AgentChat-Update-Current':current,'X-AgentChat-Update-Version':'2.0.0','X-AgentChat-Update-State':state,'X-AgentChat-Update-Committed':committed}});
  });
  await page.route('**/api/state', route => {
    if(phase==='unhealthy'){healthFails++;return route.fulfill({status:503,body:'not ready'});}
    return route.continue();
  });
  const first=await page.goto(serviceURL);
  expect(first.headers()['cache-control']).toBe('no-store');
  page.on('framenavigated',frame=>{if(frame===page.mainFrame())navigations++;});
  await page.getByRole('button',{name:'Settings',exact:true}).click();
  await page.getByRole('button',{name:'Download',exact:true}).click();
  await page.getByRole('button',{name:'Restart to update'}).waitFor({timeout:15000});
  expect(navigations).toBe(0);
  await page.getByRole('button',{name:'Restart to update'}).click();
  const pollOnce=async()=>{const before=polls;await expect.poll(()=>polls,{timeout:12000}).toBeGreaterThan(before);};
  await expect(page.locator('#update-panel')).toContainText('Waiting for active work');
  await page.evaluate(()=>{window.__realDateNow=Date.now;Date.now=()=>window.__realDateNow()+180000;});
  await pollOnce(); await expect(page.locator('#update-panel')).toContainText('Waiting for active work'); expect(navigations).toBe(0);
  await page.evaluate(()=>{Date.now=window.__realDateNow;});
  phase='old';
  await pollOnce(); expect(navigations).toBe(0); // Old server remains.
  phase='offline'; await pollOnce(); expect(navigations).toBe(0);
  phase='trial'; await pollOnce(); expect(navigations).toBe(0); // New version before launcher commit.
  phase='old'; await pollOnce(); expect(navigations).toBe(0); // Rollback.
  phase='unhealthy'; await pollOnce(); await expect.poll(()=>healthFails,{timeout:12000}).toBeGreaterThan(0); expect(navigations).toBe(0);
  await page.getByRole('button',{name:/Root conversation/}).click();
  const composer=page.getByLabel('Message Root');
  await composer.fill('Unsent draft');
  await page.locator('#file-input').setInputFiles({name:'draft.txt',mimeType:'text/plain',buffer:Buffer.from('unsent')});
  await expect(page.getByLabel('Attachments').getByText('draft.txt')).toBeVisible();
  phase='healthy';
  await expect(page.locator('#update-panel')).toContainText('Update ready. Finish or clear drafts to reload.',{timeout:15000});
  // Simulate returning after the normal wait budget; a committed target must still win.
  await page.evaluate(()=>{const realNow=Date.now;Date.now=()=>realNow()+180000;});
  expect(navigations).toBe(0);
  await expect(composer).toHaveValue('Unsent draft');
  await composer.fill('');
  await pollOnce();
  expect(navigations).toBe(0); // The in-memory attachment alone still defers reload.
  await page.getByRole('button',{name:'Remove draft.txt'}).click();
  await expect.poll(()=>navigations,{timeout:15000}).toBe(1);
  await pollOnce();
  expect(navigations).toBe(1);
});
