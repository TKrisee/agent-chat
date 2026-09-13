/* Optional browser integration check. Runtime app has no Node dependencies.
 * Set PLAYWRIGHT_MODULE to an installed playwright module when it is not local.
 */
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const readline = require('node:readline');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const python = process.env.PYTHON || 'python3';
const fixtureCode = `
import json, os, signal, sys, tempfile, threading
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import create_server
with tempfile.TemporaryDirectory(prefix='agent-chat-web-browser-') as directory:
    db = str(Path(directory) / 'state.sqlite3')
    for sid, label in [('fixture-alpha','alpha'), ('fixture-nested','alpha/nested'), ('fixture-beta','beta')]:
        c = Coordinator(db, sid)
        c.register(label)
        c.close()
    c = Coordinator(db, 'fixture-alpha')
    c.send('fixture-beta', 'The work item is ready for review.')
    c.close()
    server = create_server(db, port=0, web_root=os.environ.get('AGENT_CHAT_TEST_WEB_ROOT'))
    def stop(*args):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    print(json.dumps({'url': 'http://127.0.0.1:' + str(server.server_port), 'db': db}), flush=True)
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.stop_event.set()
        server.server_close()
`;

async function run() {
  const fixture = spawn(python, ['-u', '-c', fixtureCode], { cwd: root, stdio: ['ignore', 'pipe', 'inherit'] });
  let browser;
  try {
    const lines = readline.createInterface({ input: fixture.stdout });
    const [line] = await Promise.race([
      once(lines, 'line'),
      once(fixture, 'exit').then(([code]) => { throw Error(`Fixture exited early: ${code}`); }),
      new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Fixture startup timed out')), 15000); timer.unref(); }),
    ]);
    const fixtureInfo = JSON.parse(line);
    browser = await chromium.launch({ headless: true, ...(process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN ? { executablePath: process.env.AGENT_CHAT_CHROME_PATH || process.env.CHROME_BIN } : {}) });
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    let postCount = 0;
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => {
      if (request.url().endsWith('/api/messages') && request.method() === 'POST') postCount += 1;
    });
    await page.goto(fixtureInfo.url);
    await page.waitForFunction(() => document.querySelectorAll('.message').length > 0 && !document.querySelector('#message-input').disabled);
    assert.equal(await page.locator('#feed').evaluate(feed => feed.clientHeight >= 600), true);
    for (const selector of ['#connection-label', '#recipient', '.topbar', '.resources-heading', '.feed-footer', '.composer-recipient']) {
      assert.equal(await page.locator(selector).count(), 0, `${selector} should be removed`);
    }

    const input = page.locator('#message-input');
    await input.fill('Draft body stays intact');
    await page.getByRole('button', { name: 'Show conversations with alpha/nested', exact: true }).click();
    assert.match(await input.inputValue(), /^@alpha\/nested\s+Draft body stays intact$/);
    await page.locator('#all-conversations').click();

    await input.fill('@al draft completion');
    await input.evaluate(element => { element.setSelectionRange(3, 3); element.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.locator('#mention-list').waitFor();
    assert.equal(await page.getByRole('option', { name: /@alpha$/ }).count(), 1);
    assert.equal(await page.getByRole('option', { name: /@alpha\/nested$/ }).count(), 1);
    await input.press('ArrowDown');
    await input.press('Enter');
    assert.match(await input.inputValue(), /^@alpha\/nested\s+draft completion$/);
    await input.fill('@al tab completion');
    await input.evaluate(element => { element.setSelectionRange(3, 3); element.dispatchEvent(new Event('input', { bubbles: true })); });
    await input.press('ArrowDown');
    await input.press('ArrowUp');
    await input.press('Tab');
    assert.match(await input.inputValue(), /^@alpha\s+tab completion$/);

    await input.fill('@nobody should not post');
    await input.press('Control+Enter');
    await page.waitForTimeout(100);
    assert.equal(postCount, 0);
    await input.fill('   ');
    await input.press('Control+Enter');
    await page.waitForTimeout(100);
    assert.equal(postCount, 0);

    const message = 'Browser delivery check <script>window.injected = true</script>';
    await input.fill('@alpha ' + message);
    const posted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const postResponse = await posted;
    assert.equal(postResponse.status(), 200);
    const sent = await postResponse.json();
    await page.locator('.message-body').filter({ hasText: message }).waitFor();
    assert.equal(await input.inputValue(), '');
    assert.equal(await page.evaluate(() => window.injected), undefined);

    const action = `
import base64, json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
inbox = c.inbox()['messages']
m = next(m for m in inbox if m['id'] == sys.argv[2])
assert m['sender_session'].startswith('web_operator_')
c.acknowledge(m['id'])
picture = Path(sys.argv[1]).parent / 'review & proof.png'
picture.write_bytes(base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1sAAAAASUVORK5CYII='))
reply = c.send(m['sender_session'], 'Received. The next validation window is queued.', attachments=[picture], reply_to=m['id'])
print(json.dumps(reply))
picture.unlink()
c.close()
`;
    const agentReply = JSON.parse(execFileSync(python, ['-c', action, fixtureInfo.db, sent.id], { cwd: root, encoding: 'utf8' }));
    await page.locator(`[data-message-id="${sent.id}"] .acknowledged`).waitFor();
    await page.locator('.message-body').filter({ hasText: 'Received. The next validation window is queued.' }).waitFor();
    const preview = page.locator('.attachment-preview');
    await preview.scrollIntoViewIfNeeded();
    await page.waitForFunction(() => document.querySelector('.attachment-preview')?.naturalWidth === 1);
    const fullSize = page.waitForEvent('popup');
    await page.getByRole('link', { name: 'Open review & proof.png at full size' }).click();
    const imagePage = await fullSize;
    await imagePage.waitForLoadState();
    assert.match(imagePage.url(), /\/api\/attachments\/attachment_[A-Za-z0-9_-]+$/);
    await imagePage.close();
    const agentReplyCard = page.locator(`[data-message-id="${agentReply.id}"]`);
    assert.equal(await agentReplyCard.locator('.reply-quote').textContent(), '↩ You' + message);
    await page.locator('#search').fill('next validation window');
    await agentReplyCard.locator('.reply-quote').click();
    assert.equal(await page.locator('#search').inputValue(), '');
    await page.locator(`[data-message-id="${sent.id}"].highlighted`).waitFor();
    const colors = await page.locator(`[data-message-id="${sent.id}"]`).evaluate(article => ({
      background: getComputedStyle(article.querySelector('.message-card')).backgroundColor,
      foreground: getComputedStyle(article.querySelector('.message-body')).color,
    }));
    function luminance(color) {
      const channels = color.match(/[\d.]+/g).slice(0, 3).map(value => {
        const channel = Number(value) / 255;
        return channel <= .04045 ? channel / 12.92 : ((channel + .055) / 1.055) ** 2.4;
      });
      return channels[0] * .2126 + channels[1] * .7152 + channels[2] * .0722;
    }
    const dark = luminance(colors.background), light = luminance(colors.foreground);
    assert.ok(dark < .12 && (light + .05) / (dark + .05) >= 7, 'Operator message should be dark with readable light text');
    await input.fill('Keep this draft');
    await agentReplyCard.getByRole('button', { name: 'Reply to alpha', exact: true }).click();
    assert.equal(await input.inputValue(), '@alpha Keep this draft');
    assert.equal(await page.locator('#reply-label').textContent(), 'Replying to alpha');
    await input.fill('@beta Different recipient');
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'differs from this reply' }).waitFor();
    assert.equal(postCount, 1);
    await page.locator('#cancel-reply').click();
    assert.equal(await input.inputValue(), '@beta Different recipient');
    assert.equal(await page.locator('#composer-reply').isHidden(), true);
    await input.fill('Which checks remain?');
    await agentReplyCard.getByRole('button', { name: 'Reply to alpha', exact: true }).click();
    const replyPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const replyResponse = await replyPosted;
    assert.equal(replyResponse.status(), 200);
    const userReply = await replyResponse.json();
    assert.equal(userReply.reply_to, agentReply.id);
    await page.locator(`[data-message-id="${userReply.id}"] .reply-quote`).waitFor();
    assert.equal(await page.locator('#composer-reply').isHidden(), true);
    const desktopFeedHeight = await page.locator('#feed').evaluate(feed => feed.clientHeight);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'desktop.png'), animations: 'disabled' });
    await page.locator('#search').fill('nonexistent message query');
    await page.locator('#empty-title').filter({ hasText: 'No matching messages' }).waitFor();
    await page.locator('#search').fill('');
    await page.locator('#pause-button').click();
    assert.equal(await page.locator('#pause-button').getAttribute('aria-pressed'), 'true');
    await page.locator('#pause-button').click();
    assert.equal(await page.locator('#pause-button').getAttribute('aria-pressed'), 'false');
    await page.setViewportSize({ width: 390, height: 844 });
    await page.locator('#agents-toggle').click();
    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    assert.equal(await page.locator('#conversation-title').textContent(), 'beta');
    await page.locator('#resources-toggle').click();
    assert.equal(await page.locator('#resources-toggle').getAttribute('aria-expanded'), 'true');
    await page.keyboard.press('Escape');
    await page.locator('#agents-toggle').click();
    await page.locator('#all-conversations').click();
    await page.locator('.attachment-preview').scrollIntoViewIfNeeded();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    const postsBeforeMulti = postCount;
    await input.fill('@alpha @missing Multi should not send');
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'Unknown or ambiguous agent' }).waitFor();
    assert.equal(postCount, postsBeforeMulti);
    await input.fill('@alpha @be Shared review request');
    await input.evaluate(element => {
      element.setSelectionRange('@alpha @be'.length, '@alpha @be'.length);
      element.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await page.getByRole('option', { name: '@beta', exact: true }).waitFor();
    assert.equal(await page.getByRole('option', { name: '@alpha', exact: true }).count(), 0);
    await input.press('Enter');
    assert.equal(await input.inputValue(), '@alpha @beta Shared review request');
    await input.fill('@alpha @beta @alpha/nested @alpha Shared review request');
    const multiPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const multiResponse = await multiPosted;
    assert.equal(multiResponse.status(), 200);
    const multi = await multiResponse.json();
    assert.equal(multi.messages.length, 3);
    const batchId = multi.messages[0].batch_id;
    assert.ok(batchId);
    const batchCard = page.locator(`[data-batch-id="${batchId}"]`);
    await batchCard.waitFor();
    assert.equal(await batchCard.count(), 1);
    assert.equal(await batchCard.locator('.message-body').textContent(), 'Shared review request');
    assert.equal(await batchCard.locator('.delivery-statuses .pending').count(), 3);
    const alphaDelivery = multi.messages.find(message => message.recipient_session === 'fixture-alpha');
    const batchAck = `
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
m = next(m for m in c.inbox()['messages'] if m['id'] == sys.argv[2])
c.acknowledge(m['id'])
print(json.dumps(c.send(m['sender_session'], 'I will review my part.', reply_to=m['id'])))
c.close()
`;
    const batchReply = JSON.parse(execFileSync(python, ['-c', batchAck, fixtureInfo.db, alphaDelivery.id], { cwd: root, encoding: 'utf8' }));
    await batchCard.locator('.delivery-statuses .acknowledged').waitFor();
    assert.equal(await batchCard.locator('.delivery-statuses .pending').count(), 2);
    await page.locator(`[data-message-id="${batchReply.id}"] .reply-quote`).click();
    await page.locator(`[data-batch-id="${batchId}"].highlighted`).waitFor();
    await batchCard.getByRole('button', { name: 'Reply to You', exact: true }).click();
    const recipientDraft = await input.inputValue();
    for (const tag of ['@alpha ', '@beta ', '@alpha/nested ']) assert.ok(recipientDraft.includes(tag));
    await input.fill('@alpha @beta @alpha/nested Follow up to everyone');
    const groupReplyPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const groupReplyResponse = await groupReplyPosted;
    assert.equal(groupReplyResponse.status(), 200);
    const groupReply = await groupReplyResponse.json();
    for (const delivery of groupReply.messages) {
      assert.equal(delivery.reply_to, multi.messages.find(parent => parent.recipient_session === delivery.recipient_session).id);
    }
    await page.locator(`[data-batch-id="${groupReply.messages[0].batch_id}"]`).waitFor();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'multi.png'), animations: 'disabled' });

    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    await page.locator('#ack-filter').selectOption('acknowledged');
    assert.equal(await batchCard.count(), 0, 'Optimization must not inherit alpha acknowledgement');
    await page.getByRole('button', { name: 'Show conversations with alpha', exact: true }).click();
    assert.equal(await batchCard.count(), 1);
    await page.locator('#ack-filter').selectOption('all');
    await page.locator('#all-conversations').click();
    await page.setViewportSize({ width: 390, height: 844 });
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'multi-mobile.png'), animations: 'disabled' });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);

    // Existing CLI bodies and newly composed messages use the same safe Markdown renderer.
    const markdown = [
      '## Validation status', '',
      '| Item | Current status |', '| :--- | --- |',
      '| Targeting pressure | **Implemented**, ordinary multiplayer acceptance remains open. |',
      '| File edits | Review `file:Assets/a_b.cs` before requesting ownership. |',
      '| Escape \\| pipe | `a|b` stays in one cell. |', '',
      '- **First check**', '  - Nested *detail*', '- Second check', '',
      '3. Run checks', '4. Close processes', '',
      '> Read the inbox before shared mutations.', '',
      '```html', '<img src=x onerror="window.markdownInjected=true">', '**literal code**', '```', '',
      '[Reference](https://example.com/review) and [mail](mailto:agent@example.com).',
      '[Unsafe](javascript:alert%281%29) [Data](data:text/html,bad) [File](file:///etc/passwd)',
      '<script>window.markdownInjected=true</script>',
      'Keep snake_case_file and \\*literal stars\\*.',
    ].join('\n');
    const sendMarkdown = `
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
print(json.dumps(c.send(sys.argv[2], sys.argv[3])))
c.close()
`;
    const markdownMessage = JSON.parse(execFileSync(python, ['-c', sendMarkdown, fixtureInfo.db, sent.sender_session, markdown], { cwd: root, encoding: 'utf8' }));
    await page.reload();
    const markdownCard = page.locator(`[data-message-id="${markdownMessage.id}"]`);
    await markdownCard.locator('table').waitFor();
    await markdownCard.locator('.expand-message').click();
    assert.equal(await markdownCard.locator('h2').textContent(), 'Validation status');
    assert.deepEqual(await markdownCard.locator('th').allTextContents(), ['Item', 'Current status']);
    assert.equal(await markdownCard.locator('tbody tr').count(), 3);
    assert.equal(await markdownCard.locator('tbody tr').nth(2).locator('td').first().textContent(), 'Escape | pipe');
    assert.equal(await markdownCard.locator('tbody tr').nth(2).locator('code').textContent(), 'a|b');
    assert.equal(await markdownCard.locator('td strong').textContent(), 'Implemented');
    assert.equal(await markdownCard.locator('ul ul em').textContent(), 'detail');
    assert.equal(await markdownCard.locator('ol').getAttribute('start'), '3');
    assert.equal(await markdownCard.locator('blockquote').textContent(), 'Read the inbox before shared mutations.');
    assert.equal(await markdownCard.locator('pre code').textContent(), '<img src=x onerror="window.markdownInjected=true">\n**literal code**');
    assert.equal(await markdownCard.locator('.message-body a').count(), 2);
    assert.equal(await markdownCard.locator('.message-body a').first().getAttribute('rel'), 'noopener noreferrer');
    assert.equal(await markdownCard.locator('.message-body img, .message-body script').count(), 0);
    assert.equal(await page.evaluate(() => window.markdownInjected), undefined);
    assert.ok((await markdownCard.locator('.message-body').textContent()).includes('Keep snake_case_file and *literal stars*.'));
    const stored = await (await page.request.get(fixtureInfo.url + '/api/messages/' + markdownMessage.id)).json();
    assert.equal(stored.body, markdown, 'Formatting must not rewrite stored messages');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    await markdownCard.locator('h2').scrollIntoViewIfNeeded();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'markdown-mobile.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await markdownCard.locator('h2').scrollIntoViewIfNeeded();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'markdown.png'), animations: 'disabled' });
    const ownMarkdown = '| Decision | Next step |\n| --- | --- |\n| **Approved for review** | Run `checks` and report. |';
    await input.fill('@alpha ' + ownMarkdown);
    const markdownPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const ownMarkdownResponse = await markdownPosted;
    assert.equal(ownMarkdownResponse.status(), 200);
    const ownMarkdownMessage = await ownMarkdownResponse.json();
    const ownMarkdownCard = page.locator(`[data-message-id="${ownMarkdownMessage.id}"]`);
    await ownMarkdownCard.locator('th').first().waitFor();
    assert.equal(await ownMarkdownCard.evaluate(card => card.classList.contains('own')), true);
    assert.equal(await ownMarkdownCard.locator('td strong').textContent(), 'Approved for review');
    const markdownColors = await ownMarkdownCard.locator('td').first().evaluate(cell => ({
      foreground: getComputedStyle(cell).color,
      background: getComputedStyle(cell.closest('.message-card')).backgroundColor,
    }));
    assert.ok((luminance(markdownColors.foreground) + .05) / (luminance(markdownColors.background) + .05) >= 7);
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'markdown-own.png'), animations: 'disabled' });
    await page.setViewportSize({ width: 390, height: 844 });
    const wideMarkdown = [
      '| Left | Center | Right | Extra |', '| :--- | :---: | ---: | --- |',
      '| ' + 'longidentifier'.repeat(30) + ' | middle | 42 | more |', '',
      '```', 'long code line '.repeat(80), '```',
    ].join('\n');
    const wideMessage = JSON.parse(execFileSync(python, ['-c', sendMarkdown, fixtureInfo.db, sent.sender_session, wideMarkdown], { cwd: root, encoding: 'utf8' }));
    const wideCard = page.locator(`[data-message-id="${wideMessage.id}"]`);
    await wideCard.locator('table').waitFor();
    await wideCard.locator('.expand-message').click();
    assert.equal(await wideCard.locator('td').nth(1).evaluate(cell => getComputedStyle(cell).textAlign), 'center');
    assert.equal(await wideCard.locator('td').nth(2).evaluate(cell => getComputedStyle(cell).textAlign), 'right');
    assert.equal(await wideCard.locator('.markdown-table').evaluate(table => table.scrollWidth > table.clientWidth), true);
    assert.equal(await wideCard.locator('pre').evaluate(pre => pre.scrollWidth > pre.clientWidth), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    const delimiterCheck = await page.evaluate(() => {
      const source = 'prefix ' + '`'.repeat(59993);
      const started = performance.now();
      const rendered = CoordMarkdown.render(source);
      return { elapsed: performance.now() - started, intact: rendered.textContent === source };
    });
    assert.equal(delimiterCheck.intact, true, 'An unmatched delimiter run must stay literal');
    assert.ok(delimiterCheck.elapsed < 500, 'An unmatched delimiter run must not stall rendering');

    // Plain room updates reach every listed main agent and subagent once.
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.getByRole('button', { name: 'Show conversations with beta', exact: true }).click();
    const notice = 'Heads up, weekly usage limit just dipped below 10%.';
    await input.fill(notice);
    const broadcastPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const broadcastResponse = await broadcastPosted;
    assert.equal(broadcastResponse.status(), 200);
    const broadcast = await broadcastResponse.json();
    assert.deepEqual(broadcast.messages.map(message => message.recipient_session).sort(), ['fixture-alpha', 'fixture-beta', 'fixture-nested']);
    assert.equal(new Set(broadcast.messages.map(message => message.batch_id)).size, 1);
    await page.locator('#all-conversations').click();
    const broadcastCard = page.locator(`[data-batch-id="${broadcast.messages[0].batch_id}"]`);
    await broadcastCard.waitFor();
    assert.equal(await broadcastCard.count(), 1);
    assert.equal(await broadcastCard.locator('.message-body').textContent(), notice);
    assert.equal(await broadcastCard.locator('.delivery-statuses .pending').count(), 3);
    assert.equal(await input.inputValue(), '');
    const verifyBroadcast = `
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
for sid in ['fixture-alpha', 'fixture-beta', 'fixture-nested']:
    c = Coordinator(sys.argv[1], sid)
    received = [m for m in c.inbox()['messages'] if m['body'] == sys.argv[2]]
    assert len(received) == 1 and received[0]['acked_at'] is None
    c.close()
`;
    execFileSync(python, ['-c', verifyBroadcast, fixtureInfo.db, notice], { cwd: root });
    const postsBeforeMalformed = postCount;
    await input.fill('@[alpha Incomplete tag stays a draft');
    await input.press('Control+Enter');
    await page.locator('#composer-status').filter({ hasText: 'Complete the @agent tag' }).waitFor();
    assert.equal(postCount, postsBeforeMalformed, 'Malformed tags must not become broadcasts');
    await agentReplyCard.getByRole('button', { name: 'Reply to alpha', exact: true }).click();
    await input.fill('Thanks, keep this reply in its original conversation.');
    const plainReplyPosted = page.waitForResponse(response => response.url().endsWith('/api/messages') && response.request().method() === 'POST');
    await input.press('Control+Enter');
    const plainReplyResponse = await plainReplyPosted;
    assert.equal(plainReplyResponse.status(), 200);
    const plainReply = await plainReplyResponse.json();
    assert.equal(plainReply.recipient_session, 'fixture-alpha');
    assert.equal(plainReply.reply_to, agentReply.id);
    await page.locator(`[data-message-id="${plainReply.id}"]`).waitFor();
    if (process.env.SCREENSHOT_DIR) await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, 'broadcast.png'), animations: 'disabled' });

    // Quoted originals outside the latest500 must not corrupt the history cursor.
    const backlog = `
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from agent_chat.core import Coordinator
c = Coordinator(sys.argv[1], 'fixture-alpha')
for index in range(505):
    c.send('fixture-beta', 'History filler ' + str(index))
parent = c.db.execute('SELECT sender_session FROM messages WHERE id=?', (sys.argv[2],)).fetchone()
c.send(parent['sender_session'], 'Reply to the older operator request.', reply_to=sys.argv[2])
c.close()
`;
    execFileSync(python, ['-c', backlog, fixtureInfo.db, sent.id], { cwd: root });
    await page.reload();
    const oldReply = page.locator('.message').filter({ has: page.locator('.message-body').filter({ hasText: 'Reply to the older operator request.' }) });
    await oldReply.waitFor();
    assert.equal(await page.locator(`[data-message-id="${sent.id}"]`).count(), 0);
    const originalFetched = page.waitForResponse(response => response.url().endsWith('/api/messages/' + sent.id));
    await oldReply.locator('.reply-quote').click();
    assert.equal((await originalFetched).status(), 200);
    await page.locator(`[data-message-id="${sent.id}"].highlighted`).waitFor();
    const olderFetched = page.waitForRequest(request => request.url().includes('/api/messages?before='));
    await page.locator('#load-older').click();
    assert.ok(Number(new URL((await olderFetched).url()).searchParams.get('before')) > 2, 'Original lookup must not skip the remaining history');
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ passed: true, desktopFeedHeight, checks: ['compact layout', 'mention composer', 'operator send via keyboard', 'literal unsafe text', 'live reply', 'live acknowledgement', 'persistent image preview and full-size link', 'dark operator contrast', 'quoted original jump', 'reply draft/cancel/recipient validation', 'user reply metadata', 'older original and history cursor', 'successive mention completion', 'atomic multi-recipient send and deduplication', 'single grouped card and individual acknowledgements', 'batch follow-up reply links', 'per-recipient acknowledgement filtering', 'mobile multi-tag layout', 'Markdown table and escaped/code pipes', 'Markdown headings, emphasis, nested lists, quotes and fences', 'safe links and literal raw HTML', 'existing message Markdown without rewriting storage', 'operator Markdown compose and contrast', 'mobile Markdown layout', 'untagged room broadcast to main agents and subagents', 'broadcast grouped delivery and unread inboxes', 'malformed tags cannot broadcast', 'untagged reply preserves original recipients', 'search', 'pause/resume', 'mobile agent filter', 'resources drawer', 'no page errors or horizontal overflow'] }));
  } finally {
    if (browser) await browser.close();
    fixture.kill('SIGTERM');
    await Promise.race([
      fixture.exitCode === null ? once(fixture, 'exit') : Promise.resolve(),
      new Promise(resolve => { const timer = setTimeout(() => { fixture.kill('SIGKILL'); resolve(); }, 5000); timer.unref(); }),
    ]);
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
