/* Project creation must explain a rejected name and switch on success. */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const fixtureCode = `
import signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-project-browser-') as directory:
    db = Path(directory) / 'state.sqlite3'
    sessions_root = Path(directory) / 'synthetic-sessions'
    sessions_root.mkdir()
    c = Coordinator(db, 'alpha'); c.register('alpha'); c.close()
    server = create_server(db, port=0, sessions_root=sessions_root)
    def stop(*args): threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print('http://127.0.0.1:' + str(server.server_port), flush=True)
    try: server.serve_forever(poll_interval=.1)
    finally: server.stop_event.set(); server.server_close()
`;

async function main() {
  const fixture = spawn(process.env.PYTHON || 'python3', ['-u', '-c', fixtureCode], { cwd: root, stdio: ['ignore', 'pipe', 'inherit'] });
  let browser;
  try {
    const lines = readline.createInterface({ input: fixture.stdout });
    const [url] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited: ${code}`); }),
    ]);
    browser = await chromium.launch({ headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH ? { executablePath: process.env.AGENT_CHAT_CHROME_PATH } : {}) });
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url);
    await page.waitForFunction(() => !document.querySelector('#message-input').disabled);
    await page.getByRole('button', { name: 'New project' }).click();
    assert.equal(await page.locator('#project-dialog').evaluate(dialog => dialog.open), true);
    await page.locator('#project-name').fill('Default project');
    await page.locator('#save-project').click();
    await page.locator('#project-error').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#project-error').textContent(), 'a project with this name already exists');
    assert.equal(await page.locator('#project-dialog').evaluate(dialog => dialog.open), true);
    const statuses = [];
    page.on('response', response => {
      if (new URL(response.url()).pathname === '/api/projects' && response.request().method() === 'POST') statuses.push(response.status());
    });
    await page.evaluate(() => { state.config.csrf_token = 'expired'; });
    const workspaces = [
      '/projects/teams/calendar/' + 'quarterly-planning-and-delivery/'.repeat(5) + 'application',
      '/projects/teams/calendar/' + 'quarterly-planning-and-delivery/'.repeat(5) + 'shared-tests',
    ];
    await page.locator('#project-name').fill('New workspace');
    await page.locator('#project-workspaces').fill(workspaces.join('\n'));
    await page.locator('#project-instructions').fill('Build a calendar.\nKeep literal $(data).');
    if (process.env.AGENT_CHAT_PROJECT_SCREENSHOT) {
      await page.screenshot({path:process.env.AGENT_CHAT_PROJECT_SCREENSHOT, fullPage:true});
      await page.setViewportSize({width:390,height:844});
      await page.screenshot({path:process.env.AGENT_CHAT_PROJECT_SCREENSHOT.replace('.png','-mobile.png'), fullPage:true});
      await page.setViewportSize({width:1280,height:720});
    }
    const brief = 'Build a calendar.\nKeep literal $(data).\n' + '"'.repeat(35000);
    await page.locator('#project-instructions').fill(brief);
    const creationBodies = [];
    page.on('request', request => {
      if (new URL(request.url()).pathname === '/api/projects' && request.method() === 'POST') creationBodies.push(request.postData());
    });
    await page.locator('#save-project').click();
    await page.waitForFunction(() => document.querySelector('#project-select').value !== 'default');
    assert.equal(await page.locator('#project-dialog').evaluate(dialog => dialog.open), false);
    assert.equal(await page.locator('#project-select option:checked').textContent(), 'New workspace');
    assert.deepEqual(statuses, [403, 200]);
    assert.equal(creationBodies.length, 2);
    assert.equal(creationBodies[1], JSON.stringify({name:'New workspace',workspaces,instructions:brief}));
    assert.ok(Buffer.byteLength(creationBodies[1]) > 65536);
    await page.locator('#project-directory').waitFor({state:'visible'});
    assert.equal(await page.locator('#project-directory-title').textContent(), 'Project workspaces');
    assert.deepEqual(await page.locator('#project-workspace-path li').allTextContents(), workspaces);
    await page.evaluate(() => renderProjects({...state.config, project:{...state.config.project,cwd:null,workspace:null,workspaces:[],creation_workspace:null}}));
    assert.equal(await page.locator('#project-workspace-path').textContent(), 'No workspace is configured for this project.');
    await page.evaluate(() => renderProjects({...state.config, project:{...state.config.project,cwd:'/authoritative/root-projects-cwd',workspace:null,workspaces:null,creation_workspace:'/creation-fallback'}}));
    assert.deepEqual(await page.locator('#project-workspace-path li').allTextContents(), ['/authoritative/root-projects-cwd']);
    await page.evaluate(() => renderProjects({...state.config, project:{...state.config.project,cwd:null,workspace:null,workspaces:null,creation_workspace:'/creation-fallback'}}));
    assert.deepEqual(await page.locator('#project-workspace-path li').allTextContents(), ['/creation-fallback']);
    await page.evaluate(workspaces => renderProjects({...state.config, project:{...state.config.project,cwd:null,workspace:null,workspaces,creation_workspace:'/creation-fallback'}}), workspaces);
    assert.deepEqual(await page.locator('#project-workspace-path li').allTextContents(), workspaces);
    await page.setViewportSize({width:390,height:844});
    await page.locator('#resources-toggle').click();
    await page.locator('#project-directory').waitFor({state:'visible'});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth), true, 'Long workspace path fits the narrow layout');
    await page.locator('#resources-toggle').click();
    await page.setViewportSize({width:1280,height:720});
    await page.getByRole('button', {name:'New project'}).click();
    await page.locator('#project-name').fill('Second group');
    await page.locator('#project-workspaces').fill('/teams/lmx/api\n/teams/lmx/web');
    await page.locator('#project-instructions').fill('Keep the existing LMX task scope.');
    await page.locator('#save-project').click();
    await page.waitForFunction(() => document.querySelector('#project-select option:checked').textContent === 'Second group');
    const projectIds = await page.evaluate(() => state.projects.filter(project => project.id !== 'default').map(project => project.id));
    await page.locator('#manage-project-workspaces').click();
    const groupAreas = page.locator('#workspace-groups-list textarea[data-project-id]');
    assert.equal(await groupAreas.count(), 2);
    const assignments = [
      {id: projectIds[0], workspaces: ['/teams/inlst/core', '/teams/inlst/web']},
      {id: projectIds[1], workspaces: ['/teams/lmx/api', '/teams/lmx/web']},
    ];
    for (const assignment of assignments) {
      await page.locator(`#workspace-groups-list textarea[data-project-id="${assignment.id}"]`).fill(assignment.workspaces.join('\n'));
    }
    const batchStatuses = [];
    const batchBodies = [];
    page.on('response', response => {
      if (new URL(response.url()).pathname === '/api/projects/workspaces-batch' && response.request().method() === 'POST') batchStatuses.push(response.status());
    });
    page.on('request', request => {
      if (new URL(request.url()).pathname === '/api/projects/workspaces-batch' && request.method() === 'POST') batchBodies.push(request.postData());
    });
    const overlapping = [
      {id: projectIds[0], workspaces: ['/teams/shared']},
      {id: projectIds[1], workspaces: ['/teams/shared/nested']},
    ];
    for (const assignment of overlapping) {
      await page.locator(`#workspace-groups-list textarea[data-project-id="${assignment.id}"]`).fill(assignment.workspaces.join('\n'));
    }
    await page.locator('#save-workspace-groups').click();
    await page.locator('#workspace-groups-error').waitFor({state:'visible'});
    assert.equal(await page.locator('#workspace-groups-dialog').evaluate(dialog => dialog.open), true);
    assert.deepEqual(batchStatuses, [400]);
    for (const assignment of assignments) {
      await page.locator(`#workspace-groups-list textarea[data-project-id="${assignment.id}"]`).fill(assignment.workspaces.join('\n'));
    }
    await page.evaluate(() => { state.config.csrf_token = 'expired'; });
    await page.locator('#save-workspace-groups').click();
    await page.waitForFunction(() => !document.querySelector('#workspace-groups-dialog').open);
    assert.deepEqual(batchStatuses, [400, 403, 200]);
    assert.equal(batchBodies[0], JSON.stringify({assignments:overlapping}));
    assert.equal(batchBodies[1], JSON.stringify({assignments}));
    assert.equal(batchBodies[2], batchBodies[1]);

    const rootPostBodies = [];
    const updateResponse = {reconfiguration:{id:'stable-request-id',old_thread_id:'existing-thread',requested_workspaces:workspaces,status:'pending'}};
    await page.route('**/api/root/workspaces', async route => {
      if (route.request().method() === 'POST') {
        rootPostBodies.push(route.request().postData());
        if (rootPostBodies.length === 1) return route.abort();
        await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(updateResponse)});
      } else await route.continue();
    });
    page.on('dialog', dialog => dialog.accept());
    await page.evaluate((projectId, workspaces) => {
      state.rootView = true;
      state.project = 'default';
      state.root = {home_project:'default',home_session:'alpha'};
      renderProjects({...state.config, root_orchestrator:state.root, root_overview:{projects:[{
        project_id:projectId,name:'New workspace',setup:'ready',workspaces:['/teams/current'],creation_workspaces:workspaces,
        orchestrator:{thread_id:'existing-thread'},bridge:{recent:true,project_scope_worker:true,project_workspaces_worker:true},
      }]},projects:state.projects});
      Object.defineProperty(crypto, 'randomUUID', {value:()=> 'stable-request-id', configurable:true});
    }, projectIds[0], workspaces);
    const rootCard = page.locator('.root-project').filter({hasText:'New workspace'});
    assert.deepEqual(await rootCard.locator('.root-project-directory').nth(0).locator('li').allTextContents(), ['/teams/current']);
    assert.deepEqual(await rootCard.locator('.root-project-directory').nth(1).locator('li').allTextContents(), workspaces);
    await rootCard.getByRole('button', {name:'Apply saved workspaces'}).click();
    await rootCard.getByRole('button', {name:'Apply saved workspaces'}).click();
    assert.equal(rootPostBodies.length, 2);
    assert.equal(rootPostBodies[0], JSON.stringify({
      project:projectIds[0],expected_thread:'existing-thread',workspaces,
        prompt:'Continue the existing project work with this saved workspace assignment. Read the current project checkpoint before proceeding. Keep the existing project identity, inbox, queue, and ownership intact.',
        request_id:'stable-request-id',confirm:true,
    }));
    assert.equal(rootPostBodies[1], rootPostBodies[0]);
    assert.match(await rootCard.textContent(), /Workspace update: pending/);
    assert.deepEqual(await rootCard.locator('.root-project-directory').nth(0).locator('li').allTextContents(), ['/teams/current']);
    await rootCard.getByRole('button', {name:'Continue workspace update'}).click();
    assert.equal(rootPostBodies.length, 3);
    assert.equal(rootPostBodies[2], rootPostBodies[0]);
    await page.evaluate((projectId, workspaces) => {
      state.rootOverviewKey = null;
      renderProjects({...state.config, root_orchestrator:state.root, root_overview:{projects:[{
        project_id:projectId,name:'New workspace',setup:'ready',workspaces,creation_workspaces:workspaces,
        orchestrator:{thread_id:'new-thread'},reconfiguration:{id:'stable-request-id',status:'completed',new_thread_id:'new-thread'},
        bridge:{recent:true,project_scope_worker:true,project_workspaces_worker:true},
      }]},projects:state.projects});
    }, projectIds[0], workspaces);
    const completedCard = page.locator('.root-project').filter({hasText:'New workspace'});
    assert.deepEqual(await completedCard.locator('.root-project-directory').nth(0).locator('li').allTextContents(), workspaces);
    assert.match(await completedCard.textContent(), /Workspace update: completed/);
    let provisionBody = null;
    await page.route('**/api/root/provision', async route => {
      provisionBody = route.request().postData();
      await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({status:'pending'})});
    });
    await page.evaluate((projectId, workspaces) => {
      state.rootOverviewKey = null;
      renderProjects({...state.config, root_orchestrator:state.root, root_overview:{projects:[{
        project_id:projectId,name:'New workspace',setup:'ready_to_provision',workspaces:[],creation_workspaces:workspaces,
        bridge:{recent:true,project_scope_worker:true,project_workspaces_worker:true},
      }]},projects:state.projects});
    }, projectIds[0], workspaces);
    page.once('dialog', dialog => dialog.accept());
    await page.getByRole('button', {name:'Provision root orchestrator'}).click();
    assert.equal(provisionBody, JSON.stringify({project:projectIds[0],agent:'orchestrator',request_id:'stable-request-id',confirm:true,model:null,effort:null}));
    await page.getByRole('button', {name:'Rename project'}).click();
    assert.equal(await page.locator('#project-creation-scope').isVisible(), false);
    await page.locator('#cancel-project').click();
    assert.deepEqual(errors, []);
    console.log('PASS: ordered project workspaces, CSRF-retried atomic group update, current/saved root workspace display, explicit root provision, lost-response retry, and fresh-thread request');
  } finally {
    if (browser) await browser.close();
    const exited = once(fixture, 'exit');
    fixture.kill('SIGTERM');
    await exited;
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
