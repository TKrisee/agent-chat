const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { readFileSync } = require('node:fs');
const assert = require('node:assert/strict');

(async () => {
  const source = readFileSync('src/agent_chat/web/app.js', 'utf8');
  const start = source.indexOf('const actionReview = ');
  const end = source.indexOf('async function loadActionReview()', start);
  assert(start >= 0 && end > start);
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.setContent('<button id="action-review-button"></button><span id="action-review-count"></span><div id="action-review-list"></div><div id="action-review-empty"></div>');
    await page.addScriptTag({ content: `const state = {epoch: 1, config: {operator_ack_enabled: true}};
      const $ = id => document.getElementById(id);
      function node(tag, cls, text) { const n = document.createElement(tag); n.className = cls; if (text) n.textContent = text; return n; }
      function decideAction(handle, decision) { window.recordedDecision = {handle, decision}; }
      ${source.slice(start, end)}
      actionReview.epoch = 1; actionReview.stale = false;
      actionReview.rows.set('fixture', {handle:'fixture', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'completed', decision:'decline', remaining:1, observedAt:performance.now()});
      actionReview.rows.set('failed', {handle:'failed', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'completed', decision:'accept', item_outcome:{type:'commandExecution',status:'failed',exitCode:7}, remaining:1, observedAt:performance.now()});
      actionReview.rows.set('cancelled', {handle:'cancelled', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'completed', decision:'cancel', remaining:1, observedAt:performance.now()});
      actionReview.rows.set('success-unknown', {handle:'success-unknown', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'completed', decision:'accept', item_outcome:{type:'commandExecution',status:'completed',exitCode:0}, remaining:1, observedAt:performance.now()});
      actionReview.rows.set('nonzero', {handle:'nonzero', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'completed', decision:'accept', item_outcome:{type:'commandExecution',status:'completed',exitCode:3}, remaining:1, observedAt:performance.now()});
      actionReview.rows.set('declined-native', {handle:'declined-native', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'completed', decision:'accept', item_outcome:{type:'commandExecution',status:'declined'}, remaining:1, observedAt:performance.now()});
      actionReview.rows.set('resolved', {handle:'resolved', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'resolved', decision:null, remaining:1, observedAt:performance.now()});
      actionReview.rows.set('uncertain', {handle:'uncertain', agent:'worker', preview:{kind:'command'}, choices:['accept','decline'], status:'pending', decision:null, remaining:10, observedAt:performance.now()});
      actionReview.rows.set('permission', {handle:'permission', agent:'worker', preview:{kind:'permissions',cwd:'/tmp',environment:'local',permissions:'Network access\\nread: /tmp/read'}, permission_items:[{id:'network',label:'Network access'},{id:'fs:0',label:'read: /tmp/read'}], choices:['deny','custom','network_turn','network_session','fileSystem_turn','fileSystem_session','all_turn','all_session'], status:'pending', decision:null, remaining:10, observedAt:performance.now()});
      actionReview.uncertain.add('uncertain');
      renderActionReview();` });
    const outcomes = page.locator('.action-review-outcome');
    assert.equal(await outcomes.nth(0).innerText(), 'Item ended · outcome not verified');
    assert.equal(await outcomes.nth(1).innerText(), 'Action failed');
    assert.equal(await outcomes.nth(2).innerText(), 'Item ended · outcome not verified');
    assert.equal(await outcomes.nth(3).innerText(), 'Command completed · exit 0');
    assert.equal(await outcomes.nth(4).innerText(), 'Command failed · exit 3');
    assert.equal(await outcomes.nth(5).innerText(), 'Action declined by provider');
    assert.equal(await outcomes.nth(6).innerText(), 'Request resolved');
    assert.match(await outcomes.nth(7).innerText(), /uncertain.*not be resent/i);
    const cards = page.locator('.action-review-card');
    assert.equal(await cards.nth(7).locator('button:enabled').count(), 0);
    assert.equal(await cards.nth(8).locator('button:enabled').count(), 9);
    assert.match(await cards.nth(8).innerText(), /Network access/);
    assert.match(await cards.nth(8).innerText(), /read: \/tmp\/read/);
    assert.match(await cards.nth(8).innerText(), /Allow network for session/);
    await cards.nth(8).locator('input[value="fs:0"]').check();
    await cards.nth(8).getByText('Allow selected for turn').click();
    assert.deepEqual(await page.evaluate(() => window.recordedDecision), {handle:'permission', decision:{scope:'turn',selection:['fs:0']}});
    console.log('approval outcome projection passed');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
