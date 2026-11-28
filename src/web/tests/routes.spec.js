import {test, expect} from './fixture.js';

const projects=[
  {id:'into-the-rot',name:'Into the Rot',status:'active',po_id:'lead'},
  {id:'rot-archive',name:'Into the Rot',status:'active'},
  {id:'settings',name:'Settings',status:'active'},
];
const agents=[
  {id:'root',name:'Root',role:'root',project_id:''},
  {id:'lead',name:'Project lead',role:'po',project_id:'into-the-rot'},
];
const message=(id,room_id,text,options={})=>({
  id,room_id,text,sender:'root',recipient:'operator',role:'root',at:'2026-01-01T00:00:00Z',
  operator_read_at:'2026-01-01T00:00:00Z',...options,
});
const messages=[
  message('root-message','dm:operator:root','Root transcript'),
  ...Array.from({length:24},(_,index)=>message('rot-'+index,'group:into-the-rot',
    `Group transcript ${index} ${'Text for scrolling. '.repeat(12)}`,{project_id:'into-the-rot'})),
  message('archive-message','group:rot-archive','Archive transcript',{project_id:'rot-archive'}),
  message('settings-message','group:settings','Reserved group transcript',{project_id:'settings'}),
  message('dm-message','dm:lead:operator','Private lead transcript',{sender:'lead',project_id:'into-the-rot'}),
];
const state={projects,agents,messages,tasks:[],jobs:[]};

test('a direct group link survives state startup errors and resolves by stable ID',async ({page,serviceURL})=>{
  let failing=true;
  await page.context().route('**/api/state',route=>failing?route.fulfill({status:503}):route.fulfill({json:state}));
  await page.addInitScript(()=>sessionStorage.setItem('agent-chat.selection.recipient','root'));
  const direct=await page.goto(serviceURL+'/into-the-rot');
  expect(direct.status()).toBe(200);
  expect(direct.headers()['cache-control']).toBe('no-store');
  await expect(page.getByRole('alert')).toContainText('Could not load conversations');
  await expect(page.locator('#message')).toBeHidden();
  await expect(page).toHaveURL(serviceURL+'/into-the-rot');

  failing=false;
  await page.getByRole('button',{name:'Try again',exact:true}).click();
  await expect(page.locator('.channel-header h1')).toHaveText('Into the Rot');
  await expect(page.locator('#chat-scroll')).toContainText('Group transcript 23');
  await expect(page.locator('#chat-scroll')).not.toContainText('Root transcript');
  await expect(page).toHaveURL(serviceURL+'/into-the-rot');

  const archive=page.locator('[data-project="rot-archive"]');
  await expect(archive).toHaveAttribute('href','/groups/rot-archive');
  const [newTab]=await Promise.all([page.context().waitForEvent('page'),archive.click({button:'middle'})]);
  await expect(newTab).toHaveURL(serviceURL+'/groups/rot-archive');
  await expect(newTab.locator('#chat-scroll')).toContainText('Archive transcript');
  await expect(page).toHaveURL(serviceURL+'/into-the-rot');
  await newTab.close();
  await archive.click();
  await expect(page.locator('#chat-scroll')).toContainText('Archive transcript');
  await expect(page.locator('#chat-scroll')).not.toContainText('Group transcript');
  await expect(page).toHaveURL(serviceURL+'/groups/rot-archive');
  const reserved=page.locator('[data-project="settings"]');
  await expect(reserved).toHaveAttribute('href','/groups/settings');
  await reserved.click();
  await expect(page.locator('#chat-scroll')).toContainText('Reserved group transcript');
  await page.reload();
  await expect(page).toHaveURL(serviceURL+'/groups/settings');
  await expect(page.locator('#chat-scroll')).toContainText('Reserved group transcript');

  await page.goto(serviceURL+'/missing-conversation');
  await expect(page).toHaveURL(serviceURL+'/root');
  await expect(page.getByRole('status').filter({hasText:'This conversation link is unavailable'})).toBeVisible();
  await expect(page.locator('#chat-scroll')).toContainText('Root transcript');
});

test('history and reload restore the destination and keep per-conversation drafts',async ({page,serviceURL})=>{
  await page.route('**/api/state',route=>route.fulfill({json:state}));
  await page.goto(serviceURL);
  const composer=page.locator('#message');
  await expect(composer).toBeVisible();
  await expect(page).toHaveURL(serviceURL+'/root');
  await composer.fill('Root draft');

  const group=page.locator('[data-project="into-the-rot"]');
  await expect(group).toHaveAttribute('href','/into-the-rot');
  await group.click();
  await composer.fill('Group draft');
  const scroll=page.locator('#chat-scroll');
  await scroll.evaluate(el=>{el.scrollTop=300;});
  const lead=page.locator('[data-agent="lead"]');
  await expect(lead).toHaveAttribute('href','/agents/lead');
  await lead.click();
  await expect(page).toHaveURL(serviceURL+'/agents/lead');
  await expect(scroll).toContainText('Private lead transcript');
  await composer.fill('Private draft');

  await page.goBack();
  await expect(page).toHaveURL(serviceURL+'/into-the-rot');
  await expect(composer).toHaveValue('Group draft');
  expect(await scroll.evaluate(el=>el.scrollTop)).toBe(300);
  await page.goBack();
  await expect(composer).toHaveValue('Root draft');
  await page.goForward();
  await expect(composer).toHaveValue('Group draft');
  await page.goForward();
  await expect(composer).toHaveValue('Private draft');

  await page.getByRole('link',{name:'Settings',exact:true}).click();
  await expect(page).toHaveURL(serviceURL+'/settings');
  await expect(page.getByRole('tab',{name:'Models'})).toHaveAttribute('aria-selected','true');
  await page.getByRole('tab',{name:'Usage limits'}).click();
  await expect(page).toHaveURL(serviceURL+'/settings/usage');
  await expect(page.getByRole('heading',{name:'Weekly allowance'})).toBeVisible();
  await page.reload();
  await expect(page.getByRole('tab',{name:'Usage limits'})).toHaveAttribute('aria-selected','true');
  await page.goBack();
  await expect(page.getByRole('tab',{name:'Models'})).toHaveAttribute('aria-selected','true');
  await page.goBack();
  await expect(page).toHaveURL(serviceURL+'/agents/lead');
  await expect(composer).toHaveValue('Private draft');
  await page.reload();
  await expect(scroll).toContainText('Private lead transcript');
  await expect(composer).toHaveValue('Private draft');
});
