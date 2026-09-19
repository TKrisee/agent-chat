'use strict';

const $ = (id) => document.getElementById(id);
const state = {
  snapshot: null, messages: new Map(), selected: null, query: '', ack: 'all',
  expanded: new Set(), paused: false, pending: null, connected: false,
  hasOlder: false, source: null, newCount: 0, config: null, sending: false,
  mentionOptions: [], mentionIndex: 0, reply: null, originals: new Map(), highlighted: null,
};
const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const dateFormat = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric' });

function node(tag, className, text) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (text !== undefined) el.textContent = text;
  return el;
}

function agentLabel(id, fallback) {
  if (id === state.config?.sender.id) return 'You';
  return state.snapshot?.sessions.find((item) => item.id === id)?.agent || fallback || 'Unknown agent';
}

function tone(label) {
  return [...(label || '?').split('/')[0]].reduce((sum, char) => sum + char.charCodeAt(0), 0) % 6;
}

function avatar(label) {
  const letters = label.split('/')[0].slice(0, 2).toUpperCase();
  return node('span', `avatar tone-${tone(label)}`, letters);
}

function setConnection(connected) {
  state.connected = connected;
  $('connection-notice').hidden = connected || !state.snapshot;
  if (!connected && !state.snapshot) {
    $('empty-title').textContent = 'Waiting for the connection';
    $('empty-description').textContent = 'The conversation will appear when the local server is available.';
  }
}

function closePanels() {
  document.body.classList.remove('show-sidebar', 'show-resources');
  $('scrim').hidden = true;
  $('agents-toggle').setAttribute('aria-expanded', 'false');
  $('resources-toggle').setAttribute('aria-expanded', 'false');
}

function selectAgent(id) {
  state.selected = id;
  if (id && id !== state.config?.sender.id) {
    const session = mentionSessions().find((item) => item.id === id);
    if (session) insertMention(session, false);
  }
  closePanels();
  renderAgents();
  renderMessages(true);
}

function renderAgents() {
  if (!state.snapshot) return;
  const sessions = state.snapshot.sessions.filter((item) => item.id !== state.config?.sender.id)
    .sort((a, b) => a.agent.localeCompare(b.agent) || a.registered_at - b.registered_at);
  $('agent-count').textContent = sessions.length;
  $('all-count').textContent = state.snapshot.total_messages.toLocaleString();
  $('all-conversations').classList.toggle('selected', !state.selected);
  $('all-conversations').setAttribute('aria-pressed', String(!state.selected));
  const fragment = document.createDocumentFragment();
  for (const session of sessions) {
    const isSubagent = session.agent.includes('/');
    const controls = node('div', 'agent-controls');
    const button = node('button', `agent-button${isSubagent ? ' subagent' : ''}${state.selected === session.id ? ' selected' : ''}`);
    button.type = 'button';
    button.title = `${session.agent}\n${session.id}`;
    button.setAttribute('aria-label', `Show conversations with ${session.agent}`);
    button.setAttribute('aria-pressed', String(state.selected === session.id));
    button.append(avatar(session.agent), node('span', 'agent-name', isSubagent ? session.agent.slice(session.agent.indexOf('/') + 1) : session.agent));
    if ([...state.messages.values()].some((message) => message.recipient_session === session.id && message.acked_at == null)) {
      const pending = node('span', 'agent-pending');
      pending.title = 'Has messages awaiting acknowledgement';
      button.append(pending);
    }
    const removeBtn = node('button', 'agent-remove', '×');
    removeBtn.type = 'button';
    removeBtn.title = `Remove inactive session ${session.agent}`;
    removeBtn.setAttribute('aria-label', `Remove inactive session ${session.agent}`);
    removeBtn.addEventListener('click', async () => {
      if (!confirm(`Remove inactive session ${session.agent}? Its history will be retained. Any held reservations must first be closed or released.`)) return;
      try {
        await fetchJSON('/api/sessions/remove', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
          body: JSON.stringify({ id: session.id }),
        });
        if (state.selected === session.id) state.selected = null;
        if (state.reply && (state.reply.sender_session === session.id || messageDeliveries(state.reply).some((delivery) => delivery.recipient_session === session.id))) clearReply();
        applySnapshot(await fetchJSON('/api/snapshot'));
        composerStatus(`Removed ${session.agent}. Its history is retained.`);
      } catch (error) {
        composerStatus(error.message, true);
      }
    });
    button.addEventListener('click', () => selectAgent(session.id));
    controls.append(button, removeBtn);
    fragment.append(controls);
  }
  $('agent-list').replaceChildren(fragment);
}

function mentionSessions() {
  const sessions = (state.snapshot?.sessions || []).filter((item) => item.id !== state.config?.sender.id);
  return sessions.map((session) => {
    let key = session.agent;
    if (sessions.filter((item) => item.agent === key).length > 1) key += `#${session.id.slice(-6)}`;
    if (/[\]\n]/.test(key)) key = session.id;
    return { ...session, key, tag: /\s/.test(key) ? `@[${key}]` : `@${key}` };
  }).sort((a, b) => a.agent.localeCompare(b.agent));
}

function addressTags(value) {
  const tags = [];
  let cursor = value.match(/^\s*/)[0].length;
  const pattern = /@(?:\[([^\]\n]+)\]|([^\s[\]]+))(?=\s|$)/y;
  while (value[cursor] === '@') {
    pattern.lastIndex = cursor;
    const match = pattern.exec(value);
    if (!match) return { tags, bodyStart: cursor, invalid: true };
    tags.push({ key: match[1] || match[2], start: cursor, end: pattern.lastIndex });
    cursor = pattern.lastIndex;
    cursor += value.slice(cursor).match(/^\s*/)[0].length;
  }
  return { tags, bodyStart: cursor, invalid: false };
}

function activeMention() {
  const input = $('message-input'), caret = input.selectionStart;
  if (caret !== input.selectionEnd) return null;
  const prefix = input.value.slice(0, caret), parsed = addressTags(prefix);
  const last = parsed.tags.at(-1);
  const start = last?.end === caret ? last.start : parsed.bodyStart;
  const match = prefix.slice(start).match(/^@(?:\[([^\]\n]*)\]?|([^\s[\]]*))$/);
  if (!match) return null;
  const tail = input.value.slice(start);
  const token = tail.match(/^@(?:\[[^\]\n]*\]|[^\s[\]]*)/);
  return { start, end: token ? start + token[0].length : caret, query: (match[1] ?? match[2]).toLowerCase() };
}

function hideMentions() {
  $('mention-list').hidden = true;
  $('message-input').setAttribute('aria-expanded', 'false');
  $('message-input').removeAttribute('aria-activedescendant');
  state.mentionOptions = [];
}

function insertMention(session, focus = true, complete = false) {
  const input = $('message-input');
  const active = complete && activeMention();
  if (active) {
    const suffix = input.value.slice(active.end).replace(/^\s*/, '');
    input.value = input.value.slice(0, active.start) + session.tag + ' ' + suffix;
    const caret = active.start + session.tag.length + 1;
    input.setSelectionRange(caret, caret);
    if (focus) input.focus();
    hideMentions();
    composerStatus('');
  } else insertRecipients([session], focus);
}

function insertRecipients(sessions, focus = true) {
  if (state.reply && sessions.some((session) => !replyRecipients(state.reply).includes(session.id))) clearReply();
  const input = $('message-input'), parsed = addressTags(input.value);
  const body = parsed.invalid ? input.value.slice(parsed.bodyStart).replace(/^@[^\s]*/, '').trimStart() : input.value.slice(parsed.bodyStart);
  const tags = sessions.map((session) => session.tag).join(' ');
  input.value = tags + ' ' + body;
  input.setSelectionRange(tags.length + 1, tags.length + 1);
  if (focus) input.focus();
  hideMentions();
  composerStatus('');
}

function renderMentionSelection() {
  const options = [...$('mention-list').children];
  options.forEach((option, index) => option.setAttribute('aria-selected', String(index === state.mentionIndex)));
  const active = options[state.mentionIndex];
  if (active && state.mentionOptions.length) {
    $('message-input').setAttribute('aria-activedescendant', active.id);
    active.scrollIntoView({ block: 'nearest' });
  }
}

function showMentions() {
  const input = $('message-input');
  const active = activeMention();
  if (!active) { hideMentions(); return; }
  const existing = addressTags(input.value).tags.filter((tag) => tag.start !== active.start).map((tag) => tag.key);
  state.mentionOptions = mentionSessions().filter((session) => session.key.toLowerCase().includes(active.query) && !existing.includes(session.key) && !existing.includes(session.id)).slice(0, 12);
  state.mentionIndex = 0;
  const fragment = document.createDocumentFragment();
  state.mentionOptions.forEach((session, index) => {
    const option = node('button', 'mention-option', session.tag);
    option.type = 'button';
    option.id = `mention-option-${index}`;
    option.setAttribute('role', 'option');
    option.addEventListener('mousedown', (event) => event.preventDefault());
    option.addEventListener('click', () => insertMention(session, true, true));
    fragment.append(option);
  });
  if (!state.mentionOptions.length) fragment.append(node('div', 'mention-empty', 'No matching agent'));
  $('mention-list').replaceChildren(fragment);
  $('mention-list').hidden = false;
  input.setAttribute('aria-expanded', 'true');
  renderMentionSelection();
}

function composerStatus(message, error = false) {
  const status = $('composer-status');
  status.textContent = message;
  status.hidden = !message;
  status.className = error ? 'composer-status error' : 'sr-only';
}

function dateLabel(timestamp) {
  const date = new Date(timestamp * 1000);
  return date.toDateString() === new Date().toDateString() ? 'Today' : dateFormat.format(date);
}

function messageDeliveries(message) {
  return message.deliveries?.length ? message.deliveries : [{ id: message.id, recipient_session: message.recipient_session, recipient_agent: message.recipient_agent, acked_at: message.acked_at }];
}

function replyRecipients(message) {
  return message.sender_session === state.config?.sender.id ? messageDeliveries(message).map((delivery) => delivery.recipient_session) : [message.sender_session];
}

function clearReply() {
  state.reply = null;
  $('composer-reply').hidden = true;
}

function startReply(message) {
  if (state.sending) return;
  const recipients = replyRecipients(message);
  const sessions = mentionSessions().filter((item) => recipients.includes(item.id));
  if (!sessions.length) return;
  insertRecipients(sessions);
  state.reply = message;
  $('reply-label').textContent = `Replying to ${agentLabel(message.sender_session, message.sender_agent)}`;
  $('reply-excerpt').textContent = message.body.slice(0, 240) || 'Image attachment';
  $('composer-reply').hidden = false;
}

async function jumpToMessage(id) {
  try {
    if (!state.messages.has(id) && !state.originals.has(id)) {
      const original = await fetchJSON(`/api/messages/${encodeURIComponent(id)}`);
      // Keep fetched originals separate so loading one never skips intervening history.
      state.originals.set(original.id, original);
    }
    state.selected = null;
    state.query = '';
    state.ack = 'all';
    state.highlighted = id;
    $('search').value = '';
    $('ack-filter').value = 'all';
    closePanels();
    renderAgents();
    renderMessages();
    const batch = (state.messages.get(id) || state.originals.get(id))?.batch_id;
    const original = $(`chat-${id}`) || (batch && [...document.querySelectorAll('.message')].find((article) => article.dataset.batchId === batch));
    original?.focus({ preventScroll: true });
    original?.scrollIntoView({ block: 'center' });
  } catch (error) {
    composerStatus(`Could not open the original message: ${error.message}`, true);
  }
}

function messageCard(message) {
  const sender = agentLabel(message.sender_session, message.sender_agent);
  const deliveries = messageDeliveries(message);
  const recipient = deliveries.map((delivery) => agentLabel(delivery.recipient_session, delivery.recipient_agent)).join(', ');
  const own = message.sender_session === state.config?.sender.id;
  const article = node('article', `message${own ? ' own' : ''}`);
  article.id = `chat-${message.id}`;
  article.tabIndex = -1;
  article.classList.toggle('highlighted', deliveries.some((delivery) => delivery.id === state.highlighted));
  if (message.batch_id) article.dataset.batchId = message.batch_id;
  article.dataset.messageId = message.id;
  article.append(avatar(sender));
  const content = node('div', 'message-content');
  const meta = node('div', 'message-meta');
  const name = node('button', 'sender-button', sender);
  name.type = 'button';
  name.addEventListener('click', () => selectAgent(message.sender_session));
  const target = node('span', 'recipient', deliveries.length > 3 ? `${deliveries.length} agents` : recipient);
  target.title = `To ${recipient}`;
  const time = node('time', 'message-time', timeFormat.format(new Date(message.created_at * 1000)));
  time.dateTime = new Date(message.created_at * 1000).toISOString();
  time.title = new Date(message.created_at * 1000).toLocaleString();
  meta.append(name, target, time);
  const card = node('div', 'message-card');
  if (message.reply_to) {
    const original = message.reply_preview || state.messages.get(message.reply_to) || state.originals.get(message.reply_to);
    const quote = node('button', 'reply-quote');
    quote.type = 'button';
    const author = original ? agentLabel(original.sender_session, original.sender_agent) : 'original message';
    quote.setAttribute('aria-label', `Show original message from ${author}`);
    quote.append(node('strong', '', `↩ ${author}`), node('span', '', original?.body || 'Image attachment'));
    quote.addEventListener('click', () => jumpToMessage(message.reply_to));
    card.append(quote);
  }
  const long = message.body.length > 900 || message.body.split('\n').length > 8;
  const expanded = state.expanded.has(message.id);
  const body = node('div', `message-body markdown${long && !expanded ? ' collapsed' : ''}`);
  body.append(CoordMarkdown.render(message.body));
  card.append(body);
  if (long) {
    const toggle = node('button', 'expand-message', expanded ? 'Show less' : 'Read full message');
    toggle.type = 'button';
    toggle.setAttribute('aria-expanded', String(expanded));
    toggle.addEventListener('click', () => {
      const open = !state.expanded.has(message.id);
      if (open) state.expanded.add(message.id); else state.expanded.delete(message.id);
      body.classList.toggle('collapsed', !open);
      toggle.textContent = open ? 'Show less' : 'Read full message';
      toggle.setAttribute('aria-expanded', String(open));
    });
    card.append(toggle);
  }
  if (message.attachments?.length) {
    const images = node('div', 'message-attachments');
    for (const attachment of message.attachments) {
      const link = node('a', 'message-attachment');
      link.href = `/api/attachments/${encodeURIComponent(attachment.id)}`;
      link.target = '_blank';
      link.rel = 'noopener';
      link.setAttribute('aria-label', `Open ${attachment.name} at full size`);
      const preview = node('img', 'attachment-preview');
      preview.src = link.href;
      preview.alt = attachment.name;
      preview.loading = 'lazy';
      link.append(preview, node('span', 'attachment-name', attachment.name));
      images.append(link);
    }
    card.append(images);
  }
  const bottom = node('div', 'message-bottom');
  const acknowledged = message.acked_at != null;
  const receipt = node('span', acknowledged ? 'acknowledged' : 'pending',
    acknowledged ? '✓ Acknowledged' : '· Awaiting acknowledgement');
  if (acknowledged) receipt.title = new Date(message.acked_at * 1000).toLocaleString();
  const actions = node('span', 'message-actions');
  if (own || message.recipient_session === state.config?.sender.id) {
    const reply = node('button', 'reply-message', 'Reply');
    reply.type = 'button';
    reply.setAttribute('aria-label', `Reply to ${sender}`);
    reply.addEventListener('click', () => startReply(message));
    actions.append(reply);
  }
  actions.append(node('span', 'message-id', message.id.slice(-8)));
  const receipts = node('span', 'delivery-statuses');
  if (deliveries.length > 1) {
    for (const delivery of deliveries) {
      const done = delivery.acked_at != null;
      const status = node('span', done ? 'acknowledged' : 'pending', `${done ? '✓' : '·'} ${agentLabel(delivery.recipient_session, delivery.recipient_agent)}`);
      status.title = done ? 'Acknowledged' : 'Awaiting acknowledgement';
      receipts.append(status);
    }
  } else receipts.append(receipt);
  bottom.append(receipts, actions);
  content.append(meta, card, bottom);
  article.append(content);
  return article;
}

function renderMessages(forceBottom = false) {
  const feed = $('feed');
  const oldTop = feed.scrollTop;
  const nearBottom = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 90;
  const label = state.selected ? agentLabel(state.selected) : 'All conversations';
  $('conversation-title').textContent = label;
  const batches = new Set();
  const rows = [...new Map([...state.originals, ...state.messages]).values()].sort((a, b) => a.seq - b.seq).filter((message) => {
    let deliveries = messageDeliveries(message);
    if (state.selected && message.sender_session !== state.selected) deliveries = deliveries.filter((delivery) => delivery.recipient_session === state.selected);
    if (!deliveries.length) return false;
    if (state.ack === 'pending' && !deliveries.some((delivery) => delivery.acked_at == null)) return false;
    if (state.ack === 'acknowledged' && !deliveries.some((delivery) => delivery.acked_at != null)) return false;
    return !state.query || `${message.body} ${agentLabel(message.sender_session, message.sender_agent)} ${messageDeliveries(message).map((delivery) => agentLabel(delivery.recipient_session, delivery.recipient_agent)).join(' ')}`.toLowerCase().includes(state.query);
  });
  const fragment = document.createDocumentFragment();
  let previousDate = '';
  for (const message of rows) {
    if (message.batch_id && batches.has(message.batch_id)) continue;
    if (message.batch_id) batches.add(message.batch_id);
    const date = new Date(message.created_at * 1000).toDateString();
    if (date !== previousDate) fragment.append(node('div', 'date-divider', dateLabel(message.created_at)));
    previousDate = date;
    fragment.append(messageCard(message));
  }
  $('messages').replaceChildren(fragment);
  $('empty-state').hidden = rows.length > 0;
  $('empty-title').textContent = state.query || state.ack !== 'all' ? 'No matching messages' : 'The room is quiet';
  $('empty-description').textContent = state.query || state.ack !== 'all' ? 'Try a different search or message filter.' : 'New agent messages will appear here automatically. You can start a conversation below.';
  $('load-older').hidden = !state.hasOlder;
  if (forceBottom || nearBottom) {
    feed.scrollTop = feed.scrollHeight;
    state.newCount = 0;
    $('new-messages').hidden = true;
  } else {
    feed.scrollTop = oldTop;
    $('new-messages').hidden = state.newCount === 0;
    $('new-messages').textContent = `${state.newCount} new ${state.newCount === 1 ? 'message' : 'messages'} ↓`;
  }
}

function prettyResource(value) {
  if (value.startsWith('file:')) return value.split('/').pop();
  return value.charAt(0).toUpperCase() + value.slice(1).replaceAll('-', ' ');
}

function renderResources() {
  if (!state.snapshot) return;
  const priorities = { 'validation-clone': 0, 'main-inputs': 1, 'git-index': 2 };
  const held = state.snapshot.resources.filter((item) => item.state !== 'free').sort((a, b) =>
    (priorities[a.resource] ?? 3) - (priorities[b.resource] ?? 3) || a.resource.localeCompare(b.resource));
  const available = state.snapshot.resources.filter((item) => item.state === 'free');
  const waiting = held.reduce((sum, item) => sum + item.queue.length, 0);
  $('held-count').textContent = held.length;
  $('mobile-resource-count').textContent = held.length;
  $('waiting-count').textContent = waiting ? `${waiting} queued ${waiting === 1 ? 'request' : 'requests'}` : 'No agents waiting';
  const fragment = document.createDocumentFragment();
  for (const resource of held) {
    const card = node('section', 'resource-card');
    const top = node('div', 'resource-top');
    const name = node('span', 'resource-name', prettyResource(resource.resource));
    name.title = resource.resource;
    top.append(name, node('span', `resource-status ${resource.state}`, resource.state === 'stale' ? 'Stale hold' : 'Reserved'));
    card.append(top, node('div', 'resource-owner', agentLabel(resource.owner_session, resource.owner_agent)));
    const remaining = Math.max(0, Math.ceil(resource.deadline - Date.now() / 1000));
    card.append(node('div', 'resource-time', resource.state === 'stale' || !remaining ? 'Waiting for verified closure' : `${Math.floor(remaining / 60)}m ${String(remaining % 60).padStart(2, '0')}s remaining`));
    if (resource.reason) card.append(node('p', 'resource-reason', resource.reason));
    if (resource.queue.length) {
      const queue = node('div', 'queue');
      queue.append(node('div', 'queue-label', 'Up next'));
      for (const entry of resource.queue) {
        const item = node('div', 'queue-item');
        item.append(node('span', 'queue-position', entry.position), node('span', '', agentLabel(entry.session, entry.agent)));
        queue.append(item);
      }
      card.append(queue);
    }
    fragment.append(card);
  }
  if (!held.length) fragment.append(node('p', 'resource-reason', 'No shared resources are reserved right now.'));
  $('resource-list').replaceChildren(fragment);
  $('available-summary').textContent = `${available.length} available ${available.length === 1 ? 'resource' : 'resources'}`;
  $('available-resources').hidden = !available.length;
  $('available-list').replaceChildren(...available.map((item) => {
    const row = node('div', 'available-item', `○ ${prettyResource(item.resource)}`);
    row.title = item.resource;
    return row;
  }));
}

function applySnapshot(data) {
  if (state.paused) { state.pending = data; return; }
  const initial = !state.snapshot;
  const oldMax = Math.max(0, ...[...state.messages.values()].map((item) => item.seq));
  state.snapshot = data;
  const seen = new Set([...state.messages.values()].map((message) => message.batch_id || message.id));
  for (const message of data.messages) {
    const key = message.batch_id || message.id;
    if (!initial && message.seq > oldMax && !seen.has(key)) state.newCount += 1;
    seen.add(key);
    state.messages.set(message.id, message);
  }
  state.hasOlder = data.total_messages > state.messages.size;
  renderAgents();
  renderMessages(initial);
  renderResources();
  if (state.newCount) $('announcement').textContent = `${state.newCount} new messages received`;
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, { cache: 'no-store', ...options });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

async function loadConfig() {
  try {
    state.config = await fetchJSON('/api/config');
    $('message-input').disabled = false;
    $('send-button').disabled = false;
    composerStatus('');
    renderAgents();
    if (state.snapshot) renderMessages();
  } catch (error) {
    composerStatus('Messaging unavailable. Reconnect to try again.', true);
  }
}

function connect() {
  state.source?.close();
  loadConfig();
  fetchJSON('/api/snapshot').then(applySnapshot).catch(() => setConnection(false));
  const source = new EventSource('/api/events');
  state.source = source;
  source.onopen = () => { setConnection(true); loadConfig(); };
  source.addEventListener('snapshot', (event) => {
    try { applySnapshot(JSON.parse(event.data)); setConnection(true); }
    catch { setConnection(false); }
  });
  source.onerror = () => setConnection(false);
}

$('all-conversations').addEventListener('click', () => selectAgent(null));
$('search').addEventListener('input', (event) => { state.query = event.target.value.toLowerCase(); renderMessages(true); });
$('ack-filter').addEventListener('change', (event) => { state.ack = event.target.value; renderMessages(true); });
$('retry-button').addEventListener('click', connect);
$('pause-button').addEventListener('click', () => {
  state.paused = !state.paused;
  $('pause-button').setAttribute('aria-pressed', String(state.paused));
  $('pause-label').textContent = state.paused ? 'Resume feed' : 'Pause feed';
  $('pause-icon').textContent = state.paused ? '▷' : 'Ⅱ';
  if (!state.paused && state.pending) { applySnapshot(state.pending); state.pending = null; }
  setConnection(state.connected);
});
$('new-messages').addEventListener('click', () => { $('feed').scrollTop = $('feed').scrollHeight; state.newCount = 0; $('new-messages').hidden = true; });
$('load-older').addEventListener('click', async () => {
  const button = $('load-older');
  button.disabled = true;
  const feed = $('feed'), oldHeight = feed.scrollHeight, oldTop = feed.scrollTop;
  const before = Math.min(...[...state.messages.values()].map((message) => message.seq));
  try {
    const result = await fetchJSON(`/api/messages?before=${before}&limit=200`);
    for (const message of result.messages) state.messages.set(message.id, message);
    state.hasOlder = result.has_more;
    renderMessages();
    feed.scrollTop = oldTop + feed.scrollHeight - oldHeight;
  } catch { button.textContent = 'Could not load history · try again'; }
  finally { button.disabled = false; }
});
for (const [id, panel] of [['agents-toggle', 'sidebar'], ['resources-toggle', 'resources']]) {
  $(id).addEventListener('click', () => {
    const open = !document.body.classList.contains(`show-${panel}`);
    closePanels();
    document.body.classList.toggle(`show-${panel}`, open);
    $(id).setAttribute('aria-expanded', String(open));
    $('scrim').hidden = !open;
  });
}
$('scrim').addEventListener('click', closePanels);
$('cancel-reply').addEventListener('click', () => { clearReply(); $('message-input').focus(); });
$('reply-context').addEventListener('click', () => { if (state.reply) jumpToMessage(state.reply.id); });
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') closePanels();
  if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) {
    event.preventDefault(); $('search').focus();
  }
});
$('message-input').addEventListener('keydown', (event) => {
  if (!$('mention-list').hidden && !(event.metaKey || event.ctrlKey)) {
    if (event.key === 'Escape') { event.preventDefault(); hideMentions(); return; }
    if (state.mentionOptions.length && ['ArrowDown', 'ArrowUp', 'Enter', 'Tab'].includes(event.key)) {
      event.preventDefault();
      if (event.key === 'Enter' || event.key === 'Tab') insertMention(state.mentionOptions[state.mentionIndex], true, true);
      else {
        state.mentionIndex = (state.mentionIndex + (event.key === 'ArrowDown' ? 1 : -1) + state.mentionOptions.length) % state.mentionOptions.length;
        renderMentionSelection();
      }
      return;
    }
  }
  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) { event.preventDefault(); $('composer').requestSubmit(); }
});
$('message-input').addEventListener('input', () => { composerStatus(''); showMentions(); });
$('message-input').addEventListener('click', showMentions);
document.addEventListener('click', (event) => { if (!$('composer').contains(event.target)) hideMentions(); });
$('composer').addEventListener('submit', async (event) => {
  event.preventDefault();
  if (state.sending || !state.config) return;
  const draft = $('message-input').value.trimStart();
  const parsed = addressTags(draft), sessions = mentionSessions();
  const recipients = [];
  for (const tag of parsed.tags) {
    const session = sessions.find((item) => item.key === tag.key || item.id === tag.key);
    if (!session) { composerStatus(`Unknown or ambiguous agent: @${tag.key}. Choose a suggestion.`, true); return; }
    if (!recipients.includes(session.id)) recipients.push(session.id);
  }
  if (parsed.invalid) { composerStatus('Complete the @agent tag or remove it to message all agents.', true); return; }
  const broadcast = !parsed.tags.length && !state.reply;
  if (!parsed.tags.length) recipients.push(...(state.reply ? replyRecipients(state.reply) : sessions.map((session) => session.id)));
  const body = draft.slice(parsed.bodyStart).trim();
  if (!body) { composerStatus('Write a message.', true); return; }
  if (!recipients.length) { composerStatus('No agents are registered yet.', true); return; }
  if (state.reply && recipients.some((id) => !replyRecipients(state.reply).includes(id))) {
    composerStatus('An @agent differs from this reply. Cancel the reply to start a new conversation.', true);
    return;
  }
  const to = recipients.length === 1 && !state.reply?.batch_id ? recipients[0] : recipients;
  hideMentions();
  state.sending = true;
  $('send-button').disabled = true;
  $('message-input').disabled = true;
  composerStatus('Sending…');
  try {
    await fetchJSON('/api/messages', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token }, body: JSON.stringify({ to, body, ...(state.reply ? { reply_to: state.reply.id } : {}) }) });
    $('message-input').value = '';
    clearReply();
    composerStatus(broadcast ? `Sent to all ${recipients.length} agents` : `Sent to ${recipients.map((id) => agentLabel(id)).join(', ')}`);
    try {
      applySnapshot(await fetchJSON('/api/snapshot'));
      $('feed').scrollTop = $('feed').scrollHeight;
    } catch { setConnection(false); }
  } catch (error) {
    composerStatus(error.message, true);
  } finally {
    state.sending = false;
    $('send-button').disabled = false;
    $('message-input').disabled = false;
  }
});
setInterval(() => { if (!state.paused) renderResources(); }, 1000);
window.addEventListener('beforeunload', () => state.source?.close());
connect();
