import { test, expect } from './fixture.js';

test.use({deviceScaleFactor:1});

test('channel header retains panel controls on desktop and mobile', async ({ page, serviceURL }) => {
  let saved = '';
  // The fixture service is shared between specs; keep this empty-project layout deterministic.
  const state = `{"projects":[],"agents":[{"id":"root","name":"Root","role":"root","project_id":"","status":"idle","model":"test"}],"tasks":[],"jobs":[],"messages":[],"approvals":[]}`;
  await page.route('**/api/state', route => route.fulfill({status:200,contentType:'application/json',body:state}));
  await page.route('**/api/usage**', async route => {
    if (route.request().method() === 'POST') saved = route.request().postData() || '';
    await route.fulfill({status:200, headers:{
      'X-AgentChat-Usage-State':'known', 'X-AgentChat-Weekly-Remaining':'53',
      'X-AgentChat-Guard-Enabled':saved ? 'true' : 'false',
      'X-AgentChat-Guard-Warn':'40', 'X-AgentChat-Guard-Reserve':'30', 'X-AgentChat-Guard-Paused':'false',
    }});
  });
  await page.route('**/api/update', route => route.fulfill({status:200,headers:{
    'X-AgentChat-Update-Available':'true', 'X-AgentChat-Update-Current':'0.5.0', 'X-AgentChat-Update-State':'idle',
  }}));
  await page.goto(serviceURL);
  const weekly=page.getByRole('button', {name:'Weekly · 53% left, open settings'});
  await expect(weekly).toBeVisible();
  for (const button of [weekly, page.getByRole('button', {name:'Settings', exact:true})]) {
    await expect.poll(() => button.evaluateAll(elements => {
      const target = elements[0]?.getBoundingClientRect();
      const glyph = elements[0]?.querySelector('span')?.getBoundingClientRect();
      return !!target && !!glyph && glyph.width === 22 && glyph.height === 22 &&
        Math.abs((glyph.x + glyph.width/2) - (target.x + target.width/2)) < 1;
    })).toBe(true);
  }
  // Compare the painted pixels, not just the equal CSS boxes.
  const paintedBounds = async locator => {
    const png = await locator.screenshot({animations:'disabled'});
    return page.evaluate(async bytes => {
      const image = await createImageBitmap(new Blob([Uint8Array.from(bytes)], {type:'image/png'}));
      const canvas = document.createElement('canvas');
      canvas.width = image.width; canvas.height = image.height;
      const context = canvas.getContext('2d');
      context.drawImage(image,0,0); image.close();
      const pixels = context.getImageData(0,0,canvas.width,canvas.height).data;
      const bg = [pixels[0],pixels[1],pixels[2]];
      let left=canvas.width,top=canvas.height,right=-1,bottom=-1;
      for(let y=0;y<canvas.height;y++)for(let x=0;x<canvas.width;x++){
        const i=(y*canvas.width+x)*4;
        if(Math.max(...bg.map((v,n)=>Math.abs(v-pixels[i+n])))>20){left=Math.min(left,x);right=Math.max(right,x);top=Math.min(top,y);bottom=Math.max(bottom,y);}
      }
      return {left,top,right,bottom};
    }, [...png]);
  };
  await page.mouse.move(0,0);
  const pie = await paintedBounds(weekly.locator('span'));
  const cog = await paintedBounds(page.getByRole('button', {name:'Settings', exact:true}).locator('span'));
  console.log('Painted icon bounds', {pie, cog});
  expect(pie.right).toBeGreaterThan(pie.left);
  for(const edge of ['left','top','right','bottom']) expect(Math.abs(pie[edge]-cog[edge])).toBeLessThanOrEqual(1);
  await weekly.focus();
  await expect(page.getByRole('tooltip')).toHaveText('Weekly · 53% left');
  await weekly.click();
  await expect(page.getByRole('heading', {name:'Settings'})).toBeVisible();
  await expect(page.getByRole('button', {name:'Back to chat'})).toHaveCount(0);
  await expect(page.getByRole('tab', {name:'Usage limits'})).toHaveAttribute('aria-selected','true');
  await expect(page.getByRole('heading', {name:'Weekly allowance'})).toBeVisible();
  await expect(page.locator('.settings-page #update-panel')).toBeHidden();
  await expect(page.locator('#work-panel #update-panel')).toHaveCount(0);
  await page.getByLabel('Pause new work when the weekly reserve is reached').check();
  await page.getByRole('tab',{name:'Updates'}).click();
  await expect(page.locator('.settings-page #update-panel').getByRole('button', {name:'Check for updates'})).toBeVisible();
  await weekly.click();
  await expect(page.getByLabel('Pause new work when the weekly reserve is reached')).toBeChecked();
  await page.getByRole('button', {name:'Save settings'}).click();
  await expect.poll(() => saved).toContain('enabled=true');
  await expect.poll(() => saved).toContain('warn_percent=40');
  await expect.poll(() => saved).toContain('reserve_percent=30');
  await page.getByRole('button', {name:/Root conversation/}).click();
  const header = page.locator('.channel-header');
  const left = header.locator('#rail-toggle');
  const right = header.locator('#work-toggle');
  await expect(header.getByRole('heading', { name: 'Root' })).toBeVisible();
  await expect(page.getByText('Projects will appear here.')).toBeVisible();
  await expect(left).toHaveAttribute('aria-expanded', 'true');
  await expect(right).toHaveAttribute('aria-expanded', 'true');
  await left.click();
  await expect(left).toHaveAttribute('aria-expanded', 'false');
  await expect(page.locator('#conversation-panel')).toHaveClass(/rail-collapsed/);
  await expect(page.getByText('Projects will appear here.')).toBeHidden();
  await right.click();
  await expect(right).toHaveAttribute('aria-expanded', 'false');
  await expect(page.locator('#work-panel')).toBeHidden();
  await right.click();
  await expect(page.locator('#work-panel')).toBeVisible();
  await expect.poll(() => page.locator('#work-panel').evaluate(el => Math.abs(el.getBoundingClientRect().bottom - window.innerHeight))).toBeLessThan(2);
  await left.click();
  await expect(left).toHaveAttribute('aria-expanded', 'true');
  await expect(page.getByText('Projects will appear here.')).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(left).toHaveAttribute('aria-expanded', 'false');
  await left.click();
  await expect(page.locator('#conversation-panel')).toBeVisible();
  await expect(page.getByText('Projects will appear here.')).toBeVisible();
  await left.click();
  await expect(page.locator('#conversation-panel')).toBeHidden();
  await right.click();
  await expect(page.locator('#work-panel')).toBeHidden();
  await right.click();
  await expect(page.locator('#work-panel')).toBeVisible();
  await left.click();
  await page.getByRole('button', {name:'Settings', exact:true}).click();
  await expect(header.getByRole('heading', {name:'Settings'})).toBeVisible();
  await left.click();
  await page.getByRole('button', {name:/Root conversation/}).click();
  await expect(header.getByRole('heading', {name:'Root'})).toBeVisible();
});

test('setup keeps chat unavailable until a retry is ready', async ({page, serviceURL}) => {
  let phase='downloading', source='managed', retries=0, switches=0, navigations=0;
  page.on('framenavigated', frame=>{if(frame===page.mainFrame())navigations++;});
  const headers=()=>({'X-AgentChat-Codex-State':phase,'X-AgentChat-Codex-Source':source,'X-AgentChat-Codex-Managed-Version':'0.172.0','X-AgentChat-Codex-Can-Select-Managed':'true','X-AgentChat-Codex-Can-Select-Override':'true'});
  await page.route('**/api/codex-setup**', route => {
    if(route.request().url().endsWith('/source')){switches++;source=route.request().postData()==='source=override'?'override':'managed';if(source==='managed')phase='downloading';return route.fulfill({status:202,headers:headers()});}
    if(route.request().method()==='POST'){retries++;phase='ready';return route.fulfill({status:202,headers:{...headers(),'X-AgentChat-Codex-State':'downloading'}});}
    return route.fulfill({status:200,headers:headers(),body:phase==='error'?'Download failed.':''});
  });
  await page.goto(serviceURL);
  await expect(page.getByRole('status').filter({hasText:'Downloading Codex…'})).toContainText('Managed version 0.172.0');
  await expect(page.getByRole('button',{name:/Root conversation/})).toBeHidden();
  await expect(page.getByLabel('Message Root')).toBeHidden();
  phase='error';
  await expect(page.getByRole('status').filter({hasText:'Codex setup could not finish'})).toContainText('Managed version 0.172.0', {timeout:10000});
  await page.getByRole('button',{name:'Retry setup'}).click();
  await expect.poll(()=>retries).toBe(1);
  await expect(page.getByRole('status').filter({hasText:'Codex is ready'})).toContainText('Managed version 0.172.0', {timeout:10000});
  await page.getByRole('button',{name:'Use external CLI'}).click();
  await expect.poll(()=>navigations).toBe(2);
  await page.getByRole('button',{name:'Settings',exact:true}).click();
  await page.getByRole('tab',{name:'Codex setup'}).click();
  await expect(page.getByText('External CLI selected')).toBeVisible();
  await page.getByRole('button',{name:'Use managed CLI'}).click();
  await expect(page.getByRole('status').filter({hasText:'Downloading Codex…'})).toBeVisible();
  expect(navigations).toBe(2);
  phase='ready';
  await expect.poll(()=>navigations,{timeout:10000}).toBe(3);
  await page.getByRole('button',{name:'Settings',exact:true}).click();
  await page.getByRole('tab',{name:'Codex setup'}).click();
  await expect(page.getByText('Managed CLI selected')).toBeVisible();
  expect(switches).toBe(2);
  await page.getByRole('button',{name:/Root conversation/}).click();
  await expect(page.getByLabel('Message Root')).toBeVisible();
});
