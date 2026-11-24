'use strict';

const $ = (id) => document.getElementById(id);
const MESSAGE_LIMIT = 50;
const MESSAGE_WINDOW_LIMIT = 150;
const MESSAGE_PREVIEW_LENGTH = 2000;
const RESOURCE_PAGE_LIMIT = 50;
const MAX_FILES = 50;
const MAX_FILE_BYTES = 10 * 1024 * 1024;
const MAX_MODEL_FILE_BYTES = 64 * 1024 * 1024;
const IMAGE_EXTENSIONS = new Set(['png', 'jpg', 'jpeg', 'gif', 'webp']);
const VIDEO_EXTENSIONS = new Set(['mp4', 'm4v', 'mov', 'webm']);
const AUDIO_EXTENSIONS = new Set(['wav']);
const MODEL_EXTENSIONS = new Set(['glb', 'fbx']);
const DOCUMENT_EXTENSIONS = new Set(['eml', 'pdf', 'txt', 'md', 'markdown', 'json', 'xml', 'csv', 'tsv', 'log', 'yaml', 'yml', 'toml']);
const SETUP_STORAGE_KEY = 'agent-chat.setup-guide.v1';
const IMAGE_MIMES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
const VIDEO_MIMES = new Set(['video/mp4', 'video/quicktime', 'video/webm']);
const AUDIO_MIMES = new Set(['audio/wav']);
let mediaViewer = null;
const state = {
  rootView: location.pathname === '/root',
  snapshot: null, messages: new Map(), selected: null, query: '', ack: 'all', toMe: false,
  expanded: new Set(), paused: false, pending: null, connected: false,
  hasOlder: false, source: null, newCount: 0, config: null, sending: false,
  mentionOptions: [], mentionIndex: 0, reply: null, originals: new Map(), highlighted: null,
  project: new URL(location.href).searchParams.get('project') || 'default', epoch: 0,
  activeProjectsOnly: false, projects: [], drafts: new Map(), busy: false, rootOverviewData: null, rootWorkspaceRequests: new Map(),
  hasNewer: false, historyLoaded: false, loadingHistory: false,
  historyDirection: null, historyError: null, scrollTop: 0,
  viewGeneration: 0, viewLoading: false,
  attachments: [],
  resourceOffset: 0, resourceBusy: false, resourceRenderKey: null,
  usage: null, usageDialogDirty: false, usageSaving: false,
  measurement: null, measurementLoading: false, measurementSaving: false, measurementRequest: 0, measurementPoll: null,
};
const rootAck = {scope:null,pending:null,loading:false,running:false,error:'',done:0,total:0,request:0};
const decisionAlerts = { scope: null, pending: [], total: 0 };
const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const dateFormat = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric' });

function node(tag, className, text) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (text !== undefined) el.textContent = text;
  return el;
}

function workspaceLines(value) {
  return typeof value === 'string'
    ? value.split('\n').map(path => path.trim()).filter(Boolean)
    : [];
}

function workspaceMembers(item, keys = ['workspaces', 'workspace', 'cwd', 'creation_workspace']) {
  for (const key of keys) {
    const value = item?.[key];
    if (Array.isArray(value)) return value.filter(path => typeof path === 'string' && path);
    if (typeof value === 'string' && value) return [value];
  }
  return [];
}

function sameWorkspaceMembers(left, right) {
  return left.length === right.length && left.every((path, index) => path === right[index]);
}

function fillWorkspaceList(list, members, emptyLabel) {
  list.replaceChildren();
  if (!members.length) {
    list.append(node('li', '', emptyLabel));
    list.classList.add('unavailable');
    return;
  }
  list.classList.remove('unavailable');
  for (const member of members) list.append(node('li', '', member));
}

let setupStep = 0;
function renderSetupGuide() {
  const steps = [...$('setup-steps').children];
  steps.forEach((step, index) => {
    step.hidden = index !== setupStep;
    step.toggleAttribute('aria-current', index === setupStep);
  });
  $('setup-progress').textContent = `Step ${setupStep + 1} of ${steps.length}`;
  $('setup-back').disabled = setupStep === 0;
  $('setup-next').textContent = setupStep === steps.length - 1 ? 'Finish' : 'Next';
}
function openSetupGuide() {
  const dialog = $('setup-dialog');
  if (dialog.open) return;
  setupStep = 0;
  renderSetupGuide();
  dialog.showModal();
}
function dismissSetupGuide() {
  localStorage.setItem(SETUP_STORAGE_KEY, 'complete');
  if ($('setup-dialog').open) $('setup-dialog').close();
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

function usageStatus(usage = state.usage) {
  if (!usage) return 'Usage unavailable';
  if (!usage?.enabled) return 'Weekly guard off';
  if (usage.paused) return usage.remaining_percent == null ? 'Usage unknown · work paused' : 'Weekly reserve reached · work paused';
  if (usage.blocked || usage.remaining_percent == null) return 'Usage unknown · work paused';
  return `${Math.round(usage.remaining_percent)}% weekly left`;
}

function usageReset(usage = state.usage) {
  if (!usage?.resets_at) return 'Reset time unavailable';
  const date = new Date(usage.resets_at * 1000);
  if (Number.isNaN(date.getTime())) return 'Reset time unavailable';
  return `Resets ${new Intl.DateTimeFormat(undefined, { weekday: 'short', hour: 'numeric', minute: '2-digit' }).format(date)}`;
}

function renderUsage(usage) {
  state.usage = usage || null;
  const label = usageStatus();
  $('usage-label').textContent = label;
  $('usage-button').classList.toggle('paused', Boolean(usage?.enabled && (usage.paused || usage.blocked || usage.remaining_percent == null)));
  $('usage-button').classList.toggle('enabled', Boolean(usage?.enabled && !(usage.paused || usage.blocked || usage.remaining_percent == null)));
  $('usage-button').title = usage?.reason || (usage?.enabled ? usageReset() : 'Configure a global weekly usage reserve');
  const remaining = usage?.remaining_percent == null ? 'Remaining usage is unknown' : `${Math.round(usage.remaining_percent)}% remaining this week`;
  $('usage-current').textContent = usage?.enabled
    ? `${remaining}. ${label}. ${usageReset()}${usage.reason ? ` ${usage.reason}` : ''}`
    : `${remaining}. ${usageReset()}. The weekly usage reserve is off.`;
  const failures = (usage?.hosts || []).filter(host => host.recent && host.enforcement_error);
  $('usage-enforcement').textContent = failures.map(host => `${host.host_id}: ${host.enforcement_error}`).join('\n');
  $('usage-enforcement').hidden = !failures.length;
  $('resume-usage').hidden = !(usage?.enabled && usage.paused);
  if (!$('usage-dialog').open || !state.usageDialogDirty) syncUsageForm();
}

function syncUsageForm() {
  const usage = state.usage;
  $('usage-enabled').checked = Boolean(usage?.enabled);
  $('usage-threshold').value = String(usage?.threshold_percent ?? 30);
  usageFormControls();
}

function usageFormControls() {
  $('usage-enabled').disabled = state.usageSaving;
  $('usage-threshold').disabled = !$('usage-enabled').checked || state.usageSaving;
  $('save-usage').disabled = state.usageSaving;
  $('resume-usage').disabled = state.usageSaving;
}

function usageError(message = '') {
  $('usage-error').textContent = message;
  $('usage-error').hidden = !message;
}

const measurementValue = value => value == null ? '—' : Number(value).toLocaleString();

function measurementTime(value) {
  const date = new Date(Number(value) * 1000);
  return value == null || Number.isNaN(date.getTime()) ? '—' : date.toLocaleString();
}

function measurementDuration(report) {
  if (report?.ended_at != null && report?.started_at != null) {
    const seconds = Math.max(0, Math.round(Number(report.ended_at) - Number(report.started_at)));
    if (!seconds) return '<1s';
    return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  }
  return report?.duration_seconds == null ? 'duration unavailable' : `${Math.round(report.duration_seconds / 60)} min`;
}

const measurementActive = report => ['starting', 'running'].includes(report?.state);
function measurementError(message = '') {
  $('measurement-error').textContent = message;
  $('measurement-error').hidden = !message;
}
function measurementSummary(metrics) {
  if (!metrics) return '—';
  return `responses ${measurementValue(metrics.response_count)} · fresh ${measurementValue(metrics.fresh_input_tokens)} · cached ${measurementValue(metrics.cached_input_tokens)} · output ${measurementValue(metrics.output_tokens)}`;
}
function clearMeasurementPolling() {
  if (state.measurementPoll) clearInterval(state.measurementPoll);
  state.measurementPoll = null;
}
function closeMeasurement() {
  clearMeasurementPolling();
  state.measurementRequest++;
  state.measurementLoading = false;
  if ($('measurement-dialog').open) $('measurement-dialog').close();
}
function syncMeasurementPolling() {
  clearMeasurementPolling();
  if ($('measurement-dialog').open && measurementActive(state.measurement?.active)) {
    state.measurementPoll = setInterval(loadMeasurement, 12000);
  }
}
function measurementControls() {
  const active = measurementActive(state.measurement?.active);
  const unavailable = state.measurement?.available === false;
  $('measurement-duration').disabled = state.measurementSaving || active || unavailable;
  $('measurement-pause-at-end').disabled = !state.measurement || state.measurementSaving || state.measurementLoading || active || unavailable;
  $('start-measurement').disabled = !state.measurement || state.measurementSaving || state.measurementLoading || active || unavailable;
  $('stop-measurement').hidden = !active;
  $('stop-measurement').disabled = state.measurementSaving || state.measurementLoading;
}
function renderMeasurement(data) {
  state.measurement = data || null;
  const report = data?.active || data?.latest;
  const active = measurementActive(data?.active);
  if (data?.active?.pause_at_end != null) $('measurement-pause-at-end').checked = Boolean(data.active.pause_at_end);
  $('measurement-status').textContent = data?.available === false ? (data.error || 'Measurements are unavailable for this project.')
    : active ? `${data.active.state === 'starting' ? 'Starting' : 'Running'} · deadline ${measurementTime(data.active.deadline)} · last sample ${measurementTime(data.active.sampled_at)}`
      : report ? `${report.state} · ${measurementDuration(report)}`
        : 'No measurement has been recorded for this project.';
  $('measurement-report').hidden = !report;
  if (report) {
    $('measurement-started').textContent = measurementTime(report.started_at);
    $('measurement-deadline').textContent = report.ended_at == null ? measurementTime(report.deadline) : `ended ${measurementTime(report.ended_at)}`;
    $('measurement-sampled').textContent = measurementTime(report.sampled_at);
    const agents = Array.isArray(report.agents) && report.agents.length ? report.agents : [null];
    $('measurement-agents').replaceChildren(...agents.map(agent => {
      const row = document.createElement('tr');
      const values = agent
        ? [agent.agent || 'Unnamed agent', measurementValue(agent.response_count), measurementValue(agent.fresh_input_tokens), measurementValue(agent.cached_input_tokens), measurementValue(agent.output_tokens), `${measurementValue(agent.tool_result_text_characters)} / ${measurementValue(agent.wake_message_count)}`]
        : ['No agent samples', '—', '—', '—', '—', '—'];
      row.replaceChildren(...values.map(value => node('td', '', value)));
      if (agent) {
        // Existing servers already expose current models in their agent snapshot.
        // Prefer report-thread metadata when the server supplies the new fields.
        const metadata = 'model' in agent ? agent
          : state.snapshot?.sessions.find(session => session.id === agent.session_id) || {};
        const details = `${metadata.model || 'Model unavailable'} · ${metadata.reasoning_effort ? `${metadata.reasoning_effort} reasoning` : 'reasoning unavailable'}`;
        row.firstElementChild.classList.add('measurement-agent-name');
        const model = node('span', 'measurement-agent-model', details);
        model.title = 'model' in agent ? 'Latest known model and reasoning for the recorded thread'
          : 'Current model and reasoning for this connected agent';
        row.firstElementChild.append(model);
      }
      return row;
    }));
    $('measurement-agent-total').textContent = measurementSummary(report.totals);
    $('measurement-guardian-total').textContent = measurementSummary(report.guardian);
    const start = report.quota_start?.remaining_percent;
    const end = report.quota_end?.remaining_percent;
    $('measurement-quota').textContent = start == null && end == null ? '—' : `start ${start == null ? '—' : `${Math.round(start)}%`} · end ${end == null ? '—' : `${Math.round(end)}%`}`;
    $('measurement-coverage').textContent = report.coverage || 'Coverage unavailable.';
    const errors = Array.isArray(report.errors) ? report.errors : [];
    $('measurement-errors').replaceChildren(...errors.map(error => node('li', '', error)));
    $('measurement-errors').hidden = !errors.length;
  }
  const pauseReport = data?.active || report;
  const pauseStatus = pauseReport?.pause_error
    ? `Safe-pause delivery error: ${pauseReport.pause_error}`
    : pauseReport?.pause_requested_at != null
      ? `Safe-pause requested ${measurementTime(pauseReport.pause_requested_at)}. Delivery is not confirmation that agents are paused.`
      : '';
  $('measurement-pause-status').textContent = pauseStatus;
  $('measurement-pause-status').hidden = !pauseStatus;
  measurementError(data?.error || '');
  measurementControls();
  syncMeasurementPolling();
}
async function loadMeasurement() {
  if (state.measurementLoading || state.measurementSaving || !state.config) return;
  const request = ++state.measurementRequest;
  const project = state.project;
  state.measurementLoading = true;
  measurementControls();
  try {
    const data = await fetchJSON('/api/measurements');
    if (request === state.measurementRequest && project === state.project && $('measurement-dialog').open) renderMeasurement(data);
  } catch (error) {
    if (error.name !== 'AbortError' && request === state.measurementRequest && project === state.project && $('measurement-dialog').open) measurementError(error.message);
  } finally {
    if (request === state.measurementRequest) {
      state.measurementLoading = false;
      measurementControls();
    }
  }
}
async function saveMeasurement(operation) {
  if (state.measurementSaving || state.measurementLoading || !state.config || (operation === 'start' && measurementActive(state.measurement?.active))) return;
  const request = ++state.measurementRequest;
  const project = state.project;
  state.measurementSaving = true;
  measurementError();
  measurementControls();
  try {
    const duration = Number($('measurement-duration').value);
    const data = await fetchJSON('/api/measurements', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify(operation === 'start'
        ? { op: 'start', duration_seconds: duration, pause_at_end: $('measurement-pause-at-end').checked }
        : { op: 'stop' }),
    });
    if (request === state.measurementRequest && project === state.project && $('measurement-dialog').open) renderMeasurement(data);
  } catch (error) {
    if (error.name !== 'AbortError' && request === state.measurementRequest && project === state.project && $('measurement-dialog').open) measurementError(error.message);
  } finally {
    if (request === state.measurementRequest) {
      state.measurementSaving = false;
      measurementControls();
    }
  }
}

async function saveUsage(operation) {
  if (!state.config || state.usageSaving) return;
  const enabled = $('usage-enabled').checked;
  const threshold = Number($('usage-threshold').value);
  if (operation === 'configure' && (!Number.isInteger(threshold) || threshold < 0 || threshold > 100)) {
    usageError('Choose a whole percentage from 0 to 100.');
    return;
  }
  state.usageSaving = true;
  usageError();
  usageFormControls();
  try {
    const usage = await fetchUsage('/api/usage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify(operation === 'resume' ? { op: 'resume' } : { op: 'configure', enabled, threshold_percent: threshold }),
    });
    state.usageDialogDirty = false;
    renderUsage(usage);
  } catch (error) {
    usageError(error.message);
  } finally {
    state.usageSaving = false;
    usageFormControls();
  }
}

function closePanels() {
  document.body.classList.remove('show-sidebar', 'show-resources');
  $('scrim').hidden = true;
  $('agents-toggle').setAttribute('aria-expanded', 'false');
  $('resources-toggle').setAttribute('aria-expanded', 'false');
}

async function selectAgent(id) {
  if (state.rootView) await switchProject(state.project, false);
  if (communication.active) {
    hideCommunication();
    const url = new URL(location.href); url.pathname = '/'; history.pushState(null, '', url);
  }
  state.selected = id;
  if (id && id !== state.config?.sender.id) {
    const session = mentionSessions().find((item) => item.id === id);
    if (session) insertMention(session, false);
  }
  closePanels();
  renderAgents();
  loadConversationView();
}

function renderAgents() {
  if (!state.snapshot) return;
  const sessions = state.snapshot.sessions.filter((item) => item.id !== state.config?.sender.id)
    .sort((a, b) => Number(!!b.is_root_orchestrator) - Number(!!a.is_root_orchestrator) || a.agent.localeCompare(b.agent) || a.registered_at - b.registered_at);
  $('agent-count').textContent = sessions.length;
  $('all-count').textContent = state.snapshot.total_messages.toLocaleString();
  $('all-conversations').classList.toggle('selected', !state.selected && !communication.active);
  $('all-conversations').setAttribute('aria-pressed', String(!state.selected && !communication.active));
  const visible = new Set(sessions.map(session => session.id));
  // Existing coordinators expose recent creation jobs but lack the role flag.
  const mains = new Set(sessions.filter(session => session.thread_id && !session.parent_session).map(session => session.id));
  const creations = (state.snapshot.session_resets || []).filter(job => job.kind === 'create'
    && job.status === 'completed' && mains.has(job.requester_session) && mains.has(job.target_session));
  const created = new Set(creations.map(job => job.target_session));
  const orchestrators = new Set(creations.map(job => job.requester_session).filter(id => !created.has(id)));
  const children = new Map();
  const roots = [];
  for (const session of sessions) {
    const parent = session.parent_session || session.orchestration_parent;
    if (parent && visible.has(parent)) {
      if (!children.has(parent)) children.set(parent, []);
      children.get(parent).push(session);
    } else roots.push(session);
  }
  const ordered = [];
  const visited = new Set();
  function appendBranch(session, depth) {
    if (visited.has(session.id)) return;
    visited.add(session.id);
    ordered.push({ session, depth });
    for (const child of children.get(session.id) || []) appendBranch(child, depth + 1);
  }
  for (const session of roots) appendBranch(session, 0);
  // Keep every registration visible even if a stale route has no visible root.
  for (const session of sessions) appendBranch(session, 0);
  const fragment = document.createDocumentFragment();
  for (const { session, depth } of ordered) {
    const isSubagent = Boolean(session.parent_session);
    const controls = node('div', 'agent-controls');
    controls.style.marginLeft = `${Math.min(depth, 4) * 14}px`;
    const button = node('button', `agent-button${isSubagent ? ' subagent' : ''}${state.selected === session.id && !state.rootView && !communication.active ? ' selected' : ''}`);
    button.type = 'button';
    button.title = `${session.agent}\n${session.id}`;
    button.setAttribute('aria-label', `Show conversations with ${session.agent}`);
    button.setAttribute('aria-pressed', String(state.selected === session.id && !state.rootView && !communication.active));
    const label = node('span', 'agent-label');
    const nameRow = node('span', 'agent-name-row');
    nameRow.append(node('span', 'agent-name', session.agent));
    if (session.is_orchestrator ?? orchestrators.has(session.id)) {
      const crown = node('span', 'agent-orchestrator', '👑');
      crown.id = `orchestrator-${encodeURIComponent(session.id)}`;
      crown.title = session.is_root_orchestrator ? 'Root orchestrator' : 'Orchestrator';
      crown.setAttribute('role', 'img');
      crown.setAttribute('aria-label', crown.title);
      button.setAttribute('aria-describedby', crown.id);
      nameRow.append(crown);
    }
    label.append(nameRow);
    const details = [session.model, session.reasoning_effort && `${session.reasoning_effort} reasoning`].filter(Boolean).join(' · ');
    const metadata = node('span', 'agent-model', details || 'Model details unavailable');
    metadata.title = details ? `Latest observed: ${details}` : 'No model or reasoning level has been observed for this session';
    label.append(metadata);
    button.append(avatar(session.agent), label);
    if ([...state.messages.values()].some((message) => message.recipient_session === session.id && message.acked_at == null)) {
      const pending = node('span', 'agent-pending');
      pending.title = 'Has messages awaiting acknowledgement';
      button.append(pending);
    }
    const removeBtn = node('button', 'agent-remove', '×');
    removeBtn.type = 'button';
    removeBtn.disabled = !!session.is_root_orchestrator;
    removeBtn.title = `Remove inactive session ${session.agent}`;
    removeBtn.setAttribute('aria-label', `Remove inactive session ${session.agent}`);
    removeBtn.addEventListener('click', async () => {
      if (state.busy || state.sending) return;
      if (!confirm(`Remove inactive session ${session.agent}? Its history will be retained. Any held reservations must first be closed or released.`)) return;
      state.busy = true; projectControls();
      try {
        await fetchJSON('/api/sessions/remove', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
          body: JSON.stringify({ id: session.id }),
        });
        if (state.selected === session.id) state.selected = null;
        if (state.reply && (state.reply.sender_session === session.id || messageDeliveries(state.reply).some((delivery) => delivery.recipient_session === session.id))) clearReply();
        applySnapshot(await fetchJSON(resourceSnapshotURL()));
        composerStatus(`Removed ${session.agent}. Its history is retained.`);
      } catch (error) {
        composerStatus(error.message, true);
      } finally {
        state.busy = false; projectControls();
      }
    });
    button.addEventListener('click', () => selectAgent(session.id));
    const resetBtn = node('button', 'agent-remove agent-reset', '↻');
    resetBtn.type = 'button';
    resetBtn.dataset.session = session.id;
    resetBtn.title = session.thread_id ? `Start fresh conversation for ${session.agent}` : 'Fresh conversations require a bound main agent';
    if (session.is_root_orchestrator) resetBtn.title = 'Detach global root cleanly before resetting its shared conversation';
    resetBtn.setAttribute('aria-label', `Start fresh conversation for ${session.agent}`);
    resetBtn.disabled = !session.thread_id || session.is_root_orchestrator;
    resetBtn.addEventListener('click', () => openResetDialog(session));
    controls.append(button, removeBtn, resetBtn);
    fragment.append(controls);
  }
  $('agent-list').replaceChildren(fragment);
  renderResetStatus();
}

let resetTarget = null;
let resetRequestId = null;
let resetSending = false;
let creatingAgent = false;
let createdAgentSession = null;

function openResetDialog(session, create = false) {
  creatingAgent = create;
  createdAgentSession = null;
  resetTarget = { ...session, project: state.project };
  resetRequestId = crypto.randomUUID();
  $('reset-title').textContent = create ? 'Create independent agent' : `Fresh conversation · ${session.agent}`;
  $('create-agent-fields').hidden = !create;
  $('new-agent-name').required = create;
  $('new-agent-name').value = '';
  $('reset-description').textContent = create
    ? 'Create a distinct chat identity and fresh Codex conversation on the selected agent’s existing host. Its model, permissions and workspace are copied; its conversation history is not.'
    : 'Start a fresh Codex conversation with a new prompt. Keep this agent’s chat identity, inbox and queue position. The old transcript remains available.';
  $('reset-conditions').hidden = create;
  $('submit-reset').textContent = create ? 'Create agent' : 'Start fresh conversation';
  $('reset-prompt').value = '';
  $('reset-error').hidden = true;
  renderResetStatus();
  $('reset-dialog').showModal();
  $('reset-prompt').focus();
}

function resetJob() {
  return state.snapshot?.session_resets?.find(job => job.target_session === (creatingAgent ? createdAgentSession : resetTarget?.id));
}

$('create-agent').addEventListener('click', () => {
  const templates = (state.snapshot?.sessions || []).filter(session => session.thread_id);
  if (!templates.length) return;
  $('new-agent-template').replaceChildren(...templates.map(session => {
    const option = node('option', '', session.agent); option.value = session.id; return option;
  }));
  openResetDialog(templates[0], true);
});
$('new-agent-template').addEventListener('change', () => {
  const session = state.snapshot.sessions.find(item => item.id === $('new-agent-template').value);
  if (session && creatingAgent) resetTarget = { ...session, project: state.project };
});

function renderResetStatus() {
  $('create-agent').disabled = !(state.snapshot?.sessions || []).some(session => session.thread_id);
  if (!resetTarget) return;
  const job = resetJob();
  const active = job && !['completed', 'cancelled', 'failed'].includes(job.status);
  $('reset-progress').textContent = job
    ? `${job.status === 'completed' ? 'Fresh prompt dispatched' : job.status} · ${job.id}${job.error ? `\n${job.error}` : ''}`
    : 'No reset requested. The existing bridge will wait for this agent’s current turn to finish.';
  $('submit-reset').disabled = resetSending || Boolean(active) || (creatingAgent && Boolean(job) && job.status !== 'cancelled');
  $('cancel-reset-request').hidden = !job || !['pending', 'created'].includes(job.status);
  $('cancel-reset-request').disabled = resetSending;
}

function resetError(error) {
  $('reset-error').textContent = error.message;
  $('reset-error').hidden = false;
}

$('reset-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (!resetTarget || resetSending || resetTarget.project !== state.project) return;
  const target = resetTarget;
  const current = state.snapshot.sessions.find(session => session.id === target.id);
  if (!current || current.thread_id !== target.thread_id) {
    resetError(new Error('Agent binding changed. Close and reopen this dialog before requesting a reset.'));
    return;
  }
  const prompt = $('reset-prompt').value;
  if (!prompt.trim() || new TextEncoder().encode(prompt).length > 65536) {
    resetError(new Error('Enter a new prompt of at most 64 KiB.'));
    return;
  }
  resetSending = true; renderResetStatus();
  try {
    const result = await fetchJSON('/api/sessions/reset', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify(creatingAgent
        ? { op:'create', agent:$('new-agent-name').value, template_session:target.id,
            expected_thread:target.thread_id, prompt, request_id:resetRequestId, confirm:true }
        : { op: 'request', to: target.id, expected_thread: target.thread_id,
            prompt, request_id: resetRequestId, confirm: true }),
    });
    if (creatingAgent) createdAgentSession = result.target_session;
    if (target.project === state.project) {
      $('reset-error').hidden = true;
      const snapshot = await fetchJSON(resourceSnapshotURL());
      if (target.project === state.project) applySnapshot(snapshot);
    }
  } catch (error) { if (target.project === state.project) resetError(error); }
  finally { resetSending = false; renderResetStatus(); }
});

$('cancel-reset-request').addEventListener('click', async () => {
  const job = resetJob();
  if (!job || resetSending || resetTarget?.project !== state.project) return;
  const project = state.project;
  resetSending = true; renderResetStatus();
  try {
    await fetchJSON('/api/sessions/reset', { method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify({ op: 'cancel', id: job.id }) });
    resetRequestId = crypto.randomUUID();
    const snapshot = await fetchJSON(resourceSnapshotURL());
    if (project === state.project) applySnapshot(snapshot);
  } catch (error) { if (project === state.project) resetError(error); }
  finally { resetSending = false; renderResetStatus(); }
});
$('close-reset').addEventListener('click', () => $('reset-dialog').close());
$('reset-dialog').addEventListener('close', () => {
  const button = creatingAgent ? $('create-agent') : [...document.querySelectorAll('.agent-reset')].find(item => item.dataset.session === resetTarget?.id);
  requestAnimationFrame(() => button?.focus());
  resetTarget = null;
});

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
  if (state.rootView) { hideMentions(); return; }
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

function directDelivery(message) {
  return !message.batch_id;
}

function matchesConversation(message) {
  if (!state.selected && !state.toMe) return true;
  const operator = state.config?.sender.id;
  return messageDeliveries(message).some((delivery) => directDelivery(message) && (
    (delivery.recipient_session === operator && (!state.selected || message.sender_session === state.selected)) ||
    (!state.toMe && state.selected && message.sender_session === operator && delivery.recipient_session === state.selected)
  ));
}

function conversationQuery() {
  const params = new URLSearchParams();
  if (state.selected) params.set('agent', state.selected);
  if (state.toMe) params.set('to_me', '1');
  return params;
}

async function loadConversationView() {
  const generation = ++state.viewGeneration;
  const params = conversationQuery();
  state.originals.clear(); state.highlighted = null;
  state.hasNewer = false; state.hasOlder = false; state.historyError = null;
  state.historyLoaded = false; state.newCount = 0;
  state.loadingHistory = false; state.historyDirection = null;
  if (!params.size) {
    state.viewLoading = false;
    showLatest();
    return;
  }
  state.viewLoading = true;
  renderMessages(true);
  try {
    params.set('limit', String(MESSAGE_LIMIT));
    const result = await fetchJSON(`/api/messages?${params}`);
    if (generation !== state.viewGeneration) return;
    state.messages = new Map(result.messages.map((message) => [message.id, message]));
    state.hasOlder = result.has_more;
    state.historyLoaded = true;
    pruneExpanded();
    renderMessages(true);
  } catch (error) {
    if (generation === state.viewGeneration && error.name !== 'AbortError') composerStatus(`Could not load conversation: ${error.message}`, true);
  } finally {
    if (generation === state.viewGeneration) state.viewLoading = false;
  }
}

function replyRecipients(message) {
  return message.sender_session === state.config?.sender.id ? messageDeliveries(message).map((delivery) => delivery.recipient_session) : [message.sender_session];
}

function clearReply() {
  state.reply = null;
  $('composer-reply').hidden = true;
}

function fileExtension(name) {
  const match = /\.([^.]+)$/.exec(name);
  return match ? match[1].toLowerCase() : '';
}

function isImageFile(file) {
  return IMAGE_EXTENSIONS.has(fileExtension(file.name));
}

function attachmentType(attachment) {
  return fileExtension(attachment.name).toUpperCase() || 'FILE';
}

function renderDraftImages() {
  const fragment = document.createDocumentFragment();
  for (const attachment of state.attachments) {
    const item = node('div', 'draft-image');
    item.setAttribute('role', 'listitem');
    let preview;
    if (attachment.url) {
      const video = VIDEO_EXTENSIONS.has(fileExtension(attachment.file.name));
      preview = node(video ? 'video' : 'img');
      preview.src = attachment.url;
      if (video) {
        preview.muted = true;
        preview.preload = 'metadata';
        preview.playsInline = true;
        preview.setAttribute('aria-label', `Video preview: ${attachment.file.name}`);
      } else {
        preview.alt = attachment.file.name;
        preview.decoding = 'async';
      }
    } else {
      preview = node('span', 'draft-document-type', fileExtension(attachment.file.name).toUpperCase() || 'FILE');
      preview.setAttribute('aria-hidden', 'true');
    }
    const name = node('span', 'draft-image-name', attachment.file.name);
    name.title = attachment.file.name;
    const remove = node('button', 'remove-image', '×');
    remove.type = 'button';
    remove.setAttribute('aria-label', `Remove ${attachment.file.name}`);
    remove.disabled = state.sending || state.busy;
    remove.addEventListener('click', () => {
      if (state.sending || state.busy) return;
      if (attachment.url) URL.revokeObjectURL(attachment.url);
      state.attachments = state.attachments.filter(item => item !== attachment);
      renderDraftImages();
    });
    item.append(preview, name, remove);
    fragment.append(item);
  }
  $('composer-images').replaceChildren(fragment);
  $('composer-images').hidden = !state.attachments.length;
  $('message-input').required = !state.attachments.length;
}

function addImages(files) {
  if (state.sending || state.busy || !state.config) return;
  if (state.attachments.length + files.length > MAX_FILES) {
    composerStatus('Attach up to 50 files per message.', true); return;
  }
  const totalBytes = [...state.attachments, ...files].reduce((sum, item) => sum + (item.file || item).size, 0);
  if (totalBytes > MAX_FILES * MAX_FILE_BYTES) {
    composerStatus('Attachments may total up to 500 MiB before metadata.', true); return;
  }
  for (const file of files) {
    const extension = fileExtension(file.name);
    if (!IMAGE_EXTENSIONS.has(extension) && !VIDEO_EXTENSIONS.has(extension) && !AUDIO_EXTENSIONS.has(extension) && !MODEL_EXTENSIONS.has(extension) && !DOCUMENT_EXTENSIONS.has(extension)) {
      composerStatus('Choose a supported image, video, PDF or text file, WAV audio, EML mail, GLB or FBX model.', true); return;
    }
    const sizeLimit = MODEL_EXTENSIONS.has(extension) ? MAX_MODEL_FILE_BYTES : MAX_FILE_BYTES;
    if (!file.size || file.size > sizeLimit) {
      composerStatus(MODEL_EXTENSIONS.has(extension)
        ? 'GLB v2 and binary FBX 7400 models may be up to 64 MiB; other files may be up to 10 MiB.'
        : 'Each non-model file must be nonempty and no larger than 10 MiB.', true); return;
    }
  }
  state.attachments.push(...files.map(file => ({ file, url: isImageFile(file) || VIDEO_EXTENSIONS.has(fileExtension(file.name)) ? URL.createObjectURL(file) : null })));
  renderDraftImages();
  composerStatus('');
  $('message-input').focus();
}

function clearImages() {
  for (const attachment of state.attachments) if (attachment.url) URL.revokeObjectURL(attachment.url);
  state.attachments = [];
  renderDraftImages();
}

function messageAuthor(message) {
  return message.service_notice ? 'Service notice' : agentLabel(message.sender_session, message.sender_agent);
}

function startReply(message) {
  if (state.sending) return;
  const recipients = replyRecipients(message);
  const sessions = mentionSessions().filter((item) => recipients.includes(item.id));
  if (!sessions.length) return;
  if (state.rootView) $('message-input').focus();
  else insertRecipients(sessions);
  state.reply = message;
  $('reply-label').textContent = `Replying to ${messageAuthor(message)}`;
  $('reply-excerpt').textContent = message.body.slice(0, 240) || 'File attachment';
  $('composer-reply').hidden = false;
}

async function jumpToMessage(id) {
  try {
    if (!state.messages.has(id) && !state.originals.has(id)) {
      const original = await fetchJSON(`/api/messages/${encodeURIComponent(id)}`);
      // Keep fetched originals separate so loading one never skips intervening history.
      state.originals.clear();
      state.originals.set(original.id, original);
      pruneExpanded();
    }
    state.selected = null;
    state.query = '';
    state.ack = 'all';
    state.toMe = false;
    state.viewGeneration++; state.viewLoading = false;
    state.loadingHistory = false; state.historyDirection = null;
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
    state.scrollTop = $('feed').scrollTop;
  } catch (error) {
    if (error.name === 'AbortError') return;
    composerStatus(`Could not open the original message: ${error.message}`, true);
  }
}

async function copyMessageText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  // HTTP hosts may not expose the Clipboard API. Keep copying in the click event.
  const active = document.activeElement;
  const selection = window.getSelection();
  const ranges = selection ? Array.from({ length: selection.rangeCount }, (_, index) => selection.getRangeAt(index).cloneRange()) : [];
  const input = node('textarea', 'clipboard-source');
  input.value = text;
  input.readOnly = true;
  document.body.append(input);
  try {
    input.select();
    if (!document.execCommand('copy')) throw new Error('Copy unavailable');
  } finally {
    input.remove();
    active?.focus({ preventScroll: true });
    if (selection) {
      selection.removeAllRanges();
      ranges.forEach(range => selection.addRange(range));
    }
  }
}

function renderMediaViewer() {
  const attachment = mediaViewer.items[mediaViewer.index];
  for (const video of $('media-stage').querySelectorAll('video')) video.pause();
  const video = VIDEO_MIMES.has(attachment.mime);
  const preview = node(video ? 'video' : 'img', 'media-full-size');
  preview.src = attachment.url;
  if (video) {
    preview.controls = true;
    preview.playsInline = true;
    preview.preload = 'metadata';
    preview.setAttribute('aria-label', `Play ${attachment.name}`);
  } else preview.alt = attachment.name;
  $('media-error').hidden = true;
  preview.addEventListener('error', () => {
    if (!preview.isConnected) return;
    $('media-error').textContent = 'Preview unavailable. Try opening this attachment in a new tab.';
    $('media-error').hidden = false;
  });
  $('media-stage').replaceChildren(preview);
  $('media-title').textContent = attachment.name;
  $('media-position').textContent = `${mediaViewer.index + 1} of ${mediaViewer.items.length}`;
  $('previous-media').disabled = mediaViewer.index === 0;
  $('next-media').disabled = mediaViewer.index === mediaViewer.items.length - 1;
  $('open-media-tab').href = attachment.url;
}

function openMediaViewer(message, attachment, opener) {
  const items = message.attachments.filter(item => IMAGE_MIMES.has(item.mime) || VIDEO_MIMES.has(item.mime))
    .map(item => ({ ...item, url: item.galleryURL || projectURL(`/api/attachments/${encodeURIComponent(item.id)}`) }));
  const index = items.findIndex(item => item.id === attachment.id);
  if (index < 0) return;
  for (const video of $('messages').querySelectorAll('video')) video.pause();
  mediaViewer = { items, index, opener };
  renderMediaViewer();
  $('media-dialog').showModal();
}

function stepMedia(direction) {
  if (!mediaViewer) return;
  const next = mediaViewer.index + direction;
  if (next < 0 || next >= mediaViewer.items.length) return;
  mediaViewer.index = next;
  renderMediaViewer();
}

function positionDeliveryTimeline(details, panel) {
  if (!details.open || !panel.matches(':popover-open')) return;
  const trigger = details.querySelector('summary');
  if (!trigger) return;
  const rect = trigger.getBoundingClientRect();
  const edge = 12, gap = 6;
  const maxHeight = Math.min(360, window.innerHeight * 0.65, window.innerHeight - edge * 2);
  const aboveSpace = Math.max(0, rect.top - gap - edge);
  const belowSpace = Math.max(0, window.innerHeight - rect.bottom - gap - edge);
  const needed = Math.min(maxHeight, panel.scrollHeight);
  const above = aboveSpace >= needed || (belowSpace < needed && aboveSpace >= belowSpace);
  const available = above ? aboveSpace : belowSpace;
  panel.style.maxHeight = `${Math.max(1, Math.min(maxHeight, available))}px`;
  const panelRect = panel.getBoundingClientRect();
  const left = Math.max(edge, Math.min(window.innerWidth - edge - panelRect.width, rect.right - panelRect.width));
  const top = above
    ? Math.max(edge, rect.top - gap - panelRect.height)
    : Math.min(window.innerHeight - edge - panelRect.height, rect.bottom + gap);
  panel.style.left = `${left}px`;
  panel.style.top = `${top}px`;
}

let deliveryTimelineGlobalHandlersInstalled = false;
function installDeliveryTimelineGlobalHandlers() {
  if (deliveryTimelineGlobalHandlersInstalled) return;
  deliveryTimelineGlobalHandlersInstalled = true;
  const openTimelines = () => document.querySelectorAll('.delivery-timeline[open]').forEach(details => {
    const panel = details.querySelector('.delivery-timeline-panel');
    if (panel) positionDeliveryTimeline(details, panel);
  });
  document.addEventListener('pointerdown', event => {
    document.querySelectorAll('.delivery-timeline[open]').forEach(details => {
      if (!details.contains(event.target)) details.open = false;
    });
  }, true);
  document.addEventListener('keydown', event => {
    if (event.key !== 'Escape') return;
    const details = [...document.querySelectorAll('.delivery-timeline[open]')].at(-1);
    if (!details) return;
    event.preventDefault();
    details.open = false;
    details.querySelector('summary')?.focus();
  }, true);
  document.addEventListener('scroll', openTimelines, true);
  window.addEventListener('resize', openTimelines);
}

function deliveryTimeline(receipt, observedAt) {
  if (!receipt) return null;
  installDeliveryTimelineGlobalHandlers();
  const details = node('details', 'delivery-timeline');
  const acked = receipt.agent_ack_at != null;
  const job = receipt.native_job;
  const summaryText = acked ? 'Delivery status · agent acknowledgement recorded'
    : job?.status === 'queued' ? 'Delivery status · native queue entry recorded'
    : job?.status === 'dispatched' ? 'Delivery status · input found in conversation history'
    : receipt.wake_requested ? 'Delivery status · saved; native delivery pending'
    : 'Delivery status · saved; no wake requested';
  const summary = node('summary', 'delivery-menu-trigger');
  summary.setAttribute('aria-label', summaryText);
  summary.title = summaryText;
  summary.append(node('span', 'delivery-menu-icon', '···'));
  summary.append(node('span', 'sr-only', 'Show delivery status details'));
  details.append(summary);
  const panel = node('div', 'delivery-timeline-panel');
  panel.setAttribute('popover', 'manual');
  details.addEventListener('toggle', () => {
    if (details.open) {
      if (typeof panel.showPopover === 'function') {
        if (!panel.matches(':popover-open')) panel.showPopover();
        positionDeliveryTimeline(details, panel);
      }
    } else if (panel.matches(':popover-open')) panel.hidePopover();
  });
  panel.append(node('p', 'delivery-timeline-status', summaryText));
  const list = node('ol', 'delivery-timeline-events');
  const add = (label, at = null, note = '') => {
    const item = node('li', '', label);
    if (at != null) {
      const time = node('time', '', timeFormat.format(new Date(at * 1000)));
      time.dateTime = new Date(at * 1000).toISOString();
      time.title = new Date(at * 1000).toLocaleString();
      item.append(' · ', time);
    }
    if (note) item.append(node('span', 'delivery-event-note', ` — ${note}`));
    list.append(item);
  };
  add('Saved by Agent Chat', receipt.saved_at);
  if (job) {
    const labels = {
      prepared: ['Native wake prepared', 'not yet confirmed in the native queue'],
      adding: ['Adding to native queue', 'submission is in progress'],
      queued: ['Native queue entry recorded', 'this confirms queueing, not that a turn started'],
      starting: ['Native start requested', 'a running turn is not confirmed'],
      dispatched: ['Input found in conversation history', 'this confirms input presence, not task completion'],
      uncertain: ['Delivery outcome uncertain', 'automatic retry was withheld'],
      failed: ['Native dispatch failed'],
      cancelled: ['Native dispatch cancelled'],
    };
    const [label, note] = labels[job.status] || ['Native dispatch status unavailable'];
    add(label, null, note);
    if (job.updated_at != null) add('Native dispatch record last updated', job.updated_at,
      'latest saved job update; not necessarily the queue or start time');
  } else if (receipt.wake_requested) add('Wake requested', null, 'request time is not stored; no native dispatch record is available yet');
  else add('No wake requested', null, 'the message is stored in the recipient inbox');
  if (acked) add('Agent acknowledgement recorded', receipt.agent_ack_at,
    'acknowledgement alone does not confirm that work started or finished');
  else add('No agent acknowledgement recorded yet');
  for (const reply of receipt.linked_replies || []) add('Explicit reply linked to this message', reply.created_at,
    'the reply itself does not imply task completion');
  add('Task progress', null, 'no exact per-message running or completion status is recorded');
  const note = node('p', 'delivery-timeline-note', 'Status last observed by Agent Chat at ');
  const observed = node('time', '', timeFormat.format(new Date(observedAt * 1000)));
  observed.dateTime = new Date(observedAt * 1000).toISOString();
  observed.title = new Date(observedAt * 1000).toLocaleString();
  note.append(observed, '. Agent Chat records delivery evidence; it does not infer task progress from message text or an idle agent.');
  panel.append(list, note);
  details.append(panel);
  return details;
}

// A typed, user-supplied message convention; this is display, not verification.
function isObservationReport(text) {
  return text === 'OBSERVATION_REPORT_V1' || text.startsWith('OBSERVATION_REPORT_V1\n') || text.startsWith('OBSERVATION_REPORT_V1\r\n');
}

function renderObservationReport(text) {
  if (!isObservationReport(text)) return null;
  const lines = text.replaceAll('\r\n', '\n').split('\n');
  if (lines.at(-1) === '') lines.pop();
  const root = node('section', 'observation-report');
  const owner = lines[1]?.split('\t');
  const action = lines[2]?.split('\t');
  const rows = lines.slice(3).map(line => line.split('\t'));
  const valid = owner?.length === 2 && owner[0] === 'owner' && owner[1].trim() &&
    action?.length === 2 && action[0] === 'action' && action[1].trim() && rows.length > 0 &&
    rows.every(row => row.length === 7 && row[0] === 'observation' && row.slice(1).every(value => value.trim()));
  if (!valid) {
    root.append(node('strong', '', 'Invalid observation report — no launch clearance.'), node('pre', '', text));
    return root;
  }
  root.append(node('strong', '', 'Unresolved observations — No launch clearance.'),
    node('p', '', 'Reported by the message author; not independently verified.'));
  const list = node('ol');
  const labels = ['Source', 'Component', 'Version', 'Observed at', 'Provenance', 'Finding'];
  for (const row of rows) {
    const item = node('li');
    labels.forEach((label, index) => item.append(node('p', '', `${label}: ${row[index + 1]}`)));
    list.append(item);
  }
  root.append(list, node('p', '', `Reconciliation owner: ${owner[1]}`), node('p', '', `Next diagnostic: ${action[1]}`));
  return root;
}

function messageCard(message, videos) {
  const sender = messageAuthor(message);
  const deliveries = messageDeliveries(message);
  const recipient = deliveries.map((delivery) => agentLabel(delivery.recipient_session, delivery.recipient_agent)).join(', ');
  const own = message.sender_session === state.config?.sender.id;
  const operatorAuthored = own && !message.service_notice;
  const article = node('article', `message${operatorAuthored ? ' own' : ''}`);
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
  if (message.service_notice) {
    name.disabled = true;
    name.title = 'Automatically generated by agent-chat; not written by the operator';
  } else name.addEventListener('click', () => selectAgent(message.sender_session));
  const quietGroup = message.batch_id && !deliveries.some((delivery) => delivery.wake_requested);
  const target = node('span', 'recipient', quietGroup ? 'Group · info' : (deliveries.length > 3 ? `${deliveries.length} agents` : recipient));
  target.title = quietGroup ? `Shared information for ${recipient}; no wake requested` : `Addressed to ${recipient}; wake requested`;
  const time = node('time', 'message-time', timeFormat.format(new Date(message.created_at * 1000)));
  time.dateTime = new Date(message.created_at * 1000).toISOString();
  time.title = new Date(message.created_at * 1000).toLocaleString();
  meta.append(name, target, time);
  if (message.decision) {
    const stateLabel = message.decision.status === 'answer_recorded'
      ? `Author-recorded classification · ${message.decision.answer_outcome}`
      : message.decision.status === 'reply_received' ? 'Decision reply received'
      : message.decision.answer_outcome === 'clarification' ? 'Author-recorded clarification; final answer pending' : 'Decision requested';
    meta.append(node('span', 'decision-message-state', stateLabel));
  }
  const card = node('div', 'message-card');
  if (message.retained_child_diagnostic) {
    const diagnostic = message.retained_child_diagnostic;
    const details = node('details', 'decision-dependency');
    details.append(node('summary', '', 'Inspect retained child-review dependency'));
    details.append(node('p', '', `Author-recorded classification: ${diagnostic.author_classification}`));
    details.append(node('p', '', `Operator reply provenance: ${diagnostic.provenance}`));
    details.append(node('p', '', `Expiry: ${diagnostic.expiration}`));
    details.append(node('p', '', `Independently verified consent: ${diagnostic.verified_consent}`));
    details.append(node('p', '', `Selected capability: ${diagnostic.capability} · ${diagnostic.eligibility}. ${diagnostic.reason}`));
    details.append(node('p', '', `Native review outcome: ${diagnostic.review_outcome}. No action is granted or retried. Preserve any existing refusal.`));
    card.append(details);
  }
  const timeline = deliveryTimeline(message.delivery_receipt, state.snapshot?.server_time ?? message.created_at);
  if (timeline) card.append(timeline);
  if (message.reply_to) {
    const original = message.reply_preview || state.messages.get(message.reply_to) || state.originals.get(message.reply_to);
    const quote = node('button', 'reply-quote');
    quote.type = 'button';
    const author = original ? messageAuthor(original) : 'original message';
    quote.setAttribute('aria-label', `Show original message from ${author}`);
    quote.append(node('strong', '', `↩ ${author}`), node('span', '', original?.body || 'File attachment'));
    quote.addEventListener('click', () => jumpToMessage(message.reply_to));
    card.append(quote);
  }
  const publicationPlan = message.body.startsWith('Publication plan v1\n');
  const long = !publicationPlan && (message.body.length > 900 || message.body.split('\n').length > 8);
  const expanded = publicationPlan || state.expanded.has(message.id);
  const body = node('div', `message-body markdown${long && !expanded ? ' collapsed' : ''}`);
  if (publicationPlan) body.style.whiteSpace = 'pre-wrap';
  const renderBody = (open) => {
    const text = !open && message.body.length > MESSAGE_PREVIEW_LENGTH
      ? message.body.slice(0, MESSAGE_PREVIEW_LENGTH) + '\n…' : message.body;
    if (publicationPlan) body.replaceChildren(document.createTextNode(text));
    else if (!open && message.body.length > MESSAGE_PREVIEW_LENGTH && isObservationReport(message.body)) {
      const preview = node('section', 'observation-report');
      preview.append(node('strong', '', 'Unverified observation report preview — No launch clearance.'), node('pre', '', text));
      body.replaceChildren(preview);
    } else body.replaceChildren(renderObservationReport(message.body) || CoordMarkdown.render(text));
  };
  renderBody(expanded);
  card.append(body);
  if (state.config?.operator_ack_enabled && Array.isArray(message.artifact_decisions)) {
    for (const item of message.artifact_decisions) {
      const section = node('section', 'artifact-decision');
      section.append(node('strong', '', 'Artifact decision'),
        node('p', '', `Request ${item.request_id}; artifact ${item.artifact_name}; SHA-256 ${item.after_sha256}`),
        node('p', '', 'This records a decision on these delivered bytes only; it does not complete the task or grant publication authority.'));
      if (item.outcome) section.append(node('p', '', `Recorded: ${item.outcome}`));
      else {
        const status = node('span', 'artifact-decision-status');
        const buttons = [];
        const epoch = state.epoch;
        for (const [outcome, label] of [['accepted', 'Accept artifact'], ['declined', 'Decline artifact']]) {
          const button = node('button', 'artifact-decision-action', label);
          button.type = 'button'; buttons.push(button);
          button.addEventListener('click', async () => {
            if (epoch !== state.epoch) return;
            buttons.forEach(control => { control.disabled = true; });
            try {
              const body = new URLSearchParams({work_id:item.work_id, request_id:item.request_id, delivery_id:item.id, outcome});
              const response = await fetch(projectURL('/api/artifact-acceptance', 'POST'), {method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded', 'X-Agent-Chat-CSRF':state.config.csrf_token}, body});
              if (!response.ok) throw new Error('action rejected');
              status.textContent = `Recorded: ${outcome}`;
            } catch {
              status.textContent = 'Action not confirmed. Reload before taking any further action.';
            }
          });
          section.append(button);
        }
        section.append(status);
      }
      card.append(section);
    }
  }
  if (long) {
    const toggle = node('button', 'expand-message', expanded ? 'Show less' : 'Read full message');
    toggle.type = 'button';
    toggle.setAttribute('aria-expanded', String(expanded));
    toggle.addEventListener('click', () => {
      const open = !state.expanded.has(message.id);
      if (open) state.expanded.add(message.id); else state.expanded.delete(message.id);
      renderBody(open);
      body.classList.toggle('collapsed', !open);
      toggle.textContent = open ? 'Show less' : 'Read full message';
      toggle.setAttribute('aria-expanded', String(open));
    });
    card.append(toggle);
  }
  if (message.attachments?.length) {
    const images = node('div', 'message-attachments');
    for (const attachment of message.attachments) {
      if (VIDEO_MIMES.has(attachment.mime)) {
        const tile = node('div', 'message-attachment');
        const source = new URL(projectURL(`/api/attachments/${encodeURIComponent(attachment.id)}`), location.href).href;
        const preview = videos.get(source) || node('video', 'attachment-preview');
        if (!preview.src) {
          preview.src = source;
          preview.controls = true;
          preview.playsInline = true;
          preview.preload = 'metadata';
          preview.setAttribute('aria-label', `Play ${attachment.name}`);
        }
        const download = node('a', 'attachment-name', `Download ${attachment.name}`);
        download.href = preview.src;
        download.download = attachment.name;
        const expand = node('button', 'view-media', 'View larger');
        expand.type = 'button';
        expand.dataset.attachmentId = attachment.id;
        expand.setAttribute('aria-label', `View ${attachment.name} in attachment viewer`);
        expand.addEventListener('click', () => openMediaViewer(message, attachment, expand));
        tile.append(preview, expand, download);
        images.append(tile);
        continue;
      }
      if (AUDIO_MIMES.has(attachment.mime)) {
        const tile = node('div', 'message-attachment');
        const source = projectURL(`/api/attachments/${encodeURIComponent(attachment.id)}`);
        const player = node('audio', 'message-attachment-audio');
        player.controls = true;
        player.preload = 'metadata';
        player.src = source;
        player.setAttribute('aria-label', `Play ${attachment.name}`);
        player.addEventListener('error', () => {
          if (player.isConnected) player.replaceWith(node('span', 'attachment-document-type', attachmentType(attachment)));
        }, {once: true});
        const download = node('a', 'attachment-name', `Download ${attachment.name}`);
        download.href = source;
        download.download = attachment.name;
        tile.append(player, download);
        images.append(tile);
        continue;
      }
      const link = node('a', 'message-attachment');
      link.href = projectURL(`/api/attachments/${encodeURIComponent(attachment.id)}`);
      link.target = '_blank';
      link.rel = 'noopener';
      const image = IMAGE_MIMES.has(attachment.mime);
      link.setAttribute('aria-label', image ? `Open ${attachment.name} at full size` : `Download ${attachment.name}`);
      if (image) {
        link.dataset.attachmentId = attachment.id;
        link.addEventListener('click', event => {
          if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
          event.preventDefault();
          openMediaViewer(message, attachment, link);
        });
        const preview = node('img', 'attachment-preview');
        preview.src = link.href;
        preview.alt = attachment.name;
        preview.loading = 'lazy';
        link.append(preview, node('span', 'attachment-name', attachment.name));
      } else {
        link.download = attachment.name;
        const tile = node('span', 'attachment-document-type', attachmentType(attachment));
        tile.setAttribute('aria-hidden', 'true');
        link.append(tile, node('span', 'attachment-name', attachment.name));
      }
      images.append(link);
    }
    card.append(images);
  }
  const bottom = node('div', 'message-bottom');
  const acknowledged = message.acked_at != null;
  const receipt = node('span', acknowledged ? 'acknowledged' : 'pending',
    acknowledged ? '✓ Acknowledged' : '· Awaiting acknowledgement');
  if (acknowledged) receipt.title = new Date(message.acked_at * 1000).toLocaleString();
  const ackAction = operatorAckButton(message);
  if (ackAction) meta.append(ackAction);
  const actions = node('span', 'message-actions');
  if (message.body) {
    const copy = node('button', 'copy-message', 'Copy');
    copy.type = 'button';
    copy.setAttribute('aria-label', `Copy message from ${sender}`);
    copy.title = 'Copy full message text';
    let resetCopy;
    copy.addEventListener('click', async () => {
      if (copy.getAttribute('aria-disabled') === 'true') return;
      clearTimeout(resetCopy);
      copy.setAttribute('aria-disabled', 'true');
      $('copy-status').textContent = '';
      try {
        await copyMessageText(message.body);
        copy.textContent = 'Copied!';
        copy.title = 'Message copied.';
        $('copy-status').textContent = 'Message copied.';
      } catch {
        copy.textContent = 'Copy failed';
        copy.title = 'Could not copy. Select the message text and copy it manually.';
        $('copy-status').textContent = copy.title;
      } finally {
        copy.removeAttribute('aria-disabled');
        resetCopy = setTimeout(() => {
          copy.textContent = 'Copy';
          copy.title = 'Copy full message text';
        }, 2000);
      }
    });
    actions.append(copy);
  }
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

function feedAnchor(feed = $('feed'), messages = $('messages'), visible = card => state.messages.has(card.dataset.messageId)) {
  const top = feed.getBoundingClientRect().top;
  const card = [...messages.querySelectorAll('.message')].find(card =>
    visible(card) && card.getBoundingClientRect().bottom > top
    && card.getBoundingClientRect().top < top + feed.clientHeight);
  return { id: card?.id, batch: card?.dataset.batchId,
    offset: card ? card.getBoundingClientRect().top - top : 0, top: feed.scrollTop };
}

function restoreFeedAnchor(feed, messages, anchor) {
  const card = (anchor.id && document.getElementById(anchor.id))
    || (anchor.batch && [...messages.querySelectorAll('.message')].find(card => card.dataset.batchId === anchor.batch));
  feed.scrollTop = card ? feed.scrollTop + card.getBoundingClientRect().top - feed.getBoundingClientRect().top - anchor.offset : anchor.top;
}

function renderFeedHistory(view, prefix = '') {
  const elements = prefix ? {feed:'communication-feed',latest:'communication-show-latest',window:'communication-status',newMessages:'communication-new-messages'}
    : {feed:'feed',latest:'show-latest',window:'message-window',newMessages:'new-messages'};
  $(elements.feed).setAttribute('aria-busy', String(view.loadingHistory));
  $(elements.latest).disabled = view.loadingHistory;
  $(elements.latest).hidden = !view.historyLoaded && !view.originals?.size && !view.hasNewer;
  $(elements.window).textContent = view.historyLoaded ? `${view.messages.size} messages loaded` : view.hasOlder ? `Latest ${view.messages.size} messages` : '';
  for (const direction of ['older', 'newer']) {
    const edge = $(prefix + 'history-' + direction);
    const available = direction === 'older' ? view.hasOlder : view.hasNewer;
    edge.hidden = !available;
    edge.textContent = view.loadingHistory && view.historyDirection === direction
      ? `Loading ${direction} messages…`
      : view.historyError === direction ? `Could not load ${direction} messages. Scroll to retry.`
        : `Scroll ${direction === 'older' ? 'up' : 'down'} for ${direction} messages`;
  }
  $(elements.newMessages).hidden = view.newCount === 0;
  $(elements.newMessages).textContent = `${view.newCount} new · Back to latest`;
}

function renderHistoryStatus() { renderFeedHistory(state); }

function historyWindow(messages, compare, newer) {
  const rows = [...messages.values()].sort(compare);
  return {trimmed:rows.length > MESSAGE_WINDOW_LIMIT, rows: newer ? rows.slice(-MESSAGE_WINDOW_LIMIT) : rows.slice(0,MESSAGE_WINDOW_LIMIT)};
}

function applyLiveFeed(view, incoming, {initial, following, hasOlder, key = item => item.id, compare = (a,b) => a.seq-b.seq, merge = (_old,item) => item}) {
  const overlap = incoming.some(item => view.messages.has(key(item)));
  const latest = [...view.messages.values()].sort(compare).at(-1);
  const combined = new Map(view.messages);
  for (const item of incoming) {
    const id = key(item), updated = merge(view.messages.get(id),item);
    combined.set(id,updated);
    if (view.messages.has(id)) view.messages.set(id,updated);
  }
  if (initial || (!view.historyLoaded && following && (overlap || !view.messages.size))) {
    view.messages = new Map(incoming.map(item => [key(item),combined.get(key(item))]));
    view.hasOlder = hasOlder; view.hasNewer = false;
  } else if (overlap && (following || (!view.historyLoaded && !view.loadingHistory && !view.hasNewer))) {
    const window = historyWindow(combined,compare,true);
    if (!following && window.trimmed) { view.historyLoaded = true; view.hasNewer = true; }
    else {
      if (window.trimmed) view.hasOlder = true;
      view.messages = new Map(window.rows.map(item => [key(item),item]));
    }
  } else if (incoming.some(item => !latest || compare(item,latest)>0)) {
    view.historyLoaded = true; view.hasNewer = true;
  }
}

function renderMessages(forceBottom = false, anchor = feedAnchor()) {
  const feed = $('feed');
  const videos = new Map([...$('messages').querySelectorAll('video')].map(video => [video.src, video]));
  const playing = [...videos.values()].filter(video => !video.paused && !video.ended);
  const label = state.toMe
    ? (state.selected ? `${agentLabel(state.selected)} → You` : 'Messages to you')
    : (state.selected ? agentLabel(state.selected) : 'All conversations');
  $('conversation-title').textContent = state.rootView ? 'Root orchestrator' : label;
  $('to-me-filter').setAttribute('aria-pressed', String(state.toMe));
  const batches = new Set();
  const rows = [...new Map([...state.originals, ...state.messages]).values()].sort((a, b) => a.seq - b.seq).filter((message) => {
    if (!matchesConversation(message)) return false;
    let deliveries = messageDeliveries(message);
    if (state.selected || state.toMe) deliveries = deliveries.filter((delivery) => directDelivery(message) && (
      (delivery.recipient_session === state.config?.sender.id && (!state.selected || message.sender_session === state.selected)) ||
      (!state.toMe && state.selected && message.sender_session === state.config?.sender.id && delivery.recipient_session === state.selected)
    ));
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
    fragment.append(messageCard(message, videos));
  }
  $('messages').replaceChildren(fragment);
  // Moving media nodes can pause playback; keep live-message updates continuous.
  for (const video of playing) if (video.isConnected) video.play().catch(() => {});
  $('empty-state').hidden = rows.length > 0;
  const filtered = state.query || state.ack !== 'all' || state.toMe;
  $('empty-title').textContent = filtered ? 'No matching messages' : 'The room is quiet';
  $('empty-description').textContent = filtered ? 'Try a different search or message filter.' : 'New agent messages will appear here automatically. You can start a conversation below.';
  if (state.rootView && !state.root) {
    $('empty-title').textContent = 'Connect your root orchestrator';
    $('empty-description').textContent = 'Enroll one main conversation from a neutral workspace using the root setup guide.';
  }
  renderHistoryStatus();
  if (forceBottom) {
    feed.scrollTop = feed.scrollHeight;
    if (!state.hasNewer) {
      state.newCount = 0;
      $('new-messages').hidden = true;
    }
  } else {
    restoreFeedAnchor(feed, $('messages'), anchor);
  }
  state.scrollTop = feed.scrollTop;
}

function prettyResource(value) {
  if (value.startsWith('file:')) return value.split('/').pop();
  return value.charAt(0).toUpperCase() + value.slice(1).replaceAll('-', ' ');
}

function renderResources() {
  if (!state.snapshot) return;
  const paging = state.snapshot.resource_page;
  const priorities = { 'validation-clone': 0, 'main-inputs': 1, 'git-index': 2 };
  // Older servers return the entire roster. Bound their DOM too, until activation.
  const resources = paging ? state.snapshot.resources : [...state.snapshot.resources].sort((a, b) =>
    ({owned: 0, stale: 1, free: 2}[a.state] - {owned: 0, stale: 1, free: 2}[b.state]) ||
    (priorities[a.resource] ?? 3) - (priorities[b.resource] ?? 3) || a.resource.localeCompare(b.resource));
  const total = paging?.total ?? resources.length;
  state.resourceOffset = paging?.offset ?? Math.min(state.resourceOffset, Math.max(0, Math.ceil(total / RESOURCE_PAGE_LIMIT) - 1) * RESOURCE_PAGE_LIMIT);
  const visible = paging ? resources : resources.slice(state.resourceOffset, state.resourceOffset + RESOURCE_PAGE_LIMIT);
  const held = visible.filter(item => item.state !== 'free');
  const available = visible.filter(item => item.state === 'free');
  const heldCount = paging?.held ?? resources.filter(item => item.state !== 'free').length;
  const freeCount = paging?.free ?? resources.filter(item => item.state === 'free').length;
  const waiting = paging?.waiting ?? resources.reduce((sum, item) => sum + item.queue.length, 0);
  $('held-count').textContent = heldCount;
  $('mobile-resource-count').textContent = heldCount;
  $('waiting-count').textContent = waiting ? `${waiting} queued ${waiting === 1 ? 'request' : 'requests'}` : 'No agents waiting';
  $('resource-page-label').textContent = total ? `${state.resourceOffset + 1}–${state.resourceOffset + visible.length} of ${total.toLocaleString()}` : 'No resources';
  $('resource-previous').disabled = state.resourceBusy || state.paused || !state.resourceOffset;
  $('resource-next').disabled = state.resourceBusy || state.paused || state.resourceOffset + visible.length >= total;
  $('available-summary').textContent = `${freeCount} available ${freeCount === 1 ? 'resource' : 'resources'}`;
  $('available-resources').hidden = !freeCount;
  if ($('browse-available')) $('browse-available').disabled = state.resourceBusy || state.paused;
  const key = JSON.stringify([visible, heldCount, freeCount, state.snapshot.sessions.map(({id, agent}) => [id, agent])]);
  if (key === state.resourceRenderKey) { updateResourceCountdowns(); return; }
  state.resourceRenderKey = key;
  const fragment = document.createDocumentFragment();
  for (const resource of held) {
    const card = node('section', 'resource-card');
    const top = node('div', 'resource-top');
    const name = node('span', 'resource-name', prettyResource(resource.resource));
    name.title = resource.resource;
    top.append(name, node('span', `resource-status ${resource.state}`, resource.state === 'stale' ? 'Stale hold' : 'Reserved'));
    card.append(top, node('div', 'resource-owner', agentLabel(resource.owner_session, resource.owner_agent)));
    const countdown = node('div', 'resource-time');
    countdown.dataset.deadline = resource.deadline;
    countdown.dataset.state = resource.state;
    card.append(countdown);
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
  if (!held.length) fragment.append(node('p', 'resource-reason', heldCount ? 'No held resources on this page.' : 'No shared resources are reserved right now.'));
  $('resource-list').replaceChildren(fragment);
  $('available-list').replaceChildren(...available.map((item) => {
    const row = node('div', 'available-item', `○ ${prettyResource(item.resource)}`);
    row.title = item.resource;
    return row;
  }));
  if (!available.length && freeCount) {
    const browse = node('button', 'quiet-button', 'Browse available resources');
    browse.id = 'browse-available';
    browse.type = 'button';
    browse.disabled = state.resourceBusy || state.paused;
    browse.addEventListener('click', () => changeResourcePage(0, Math.floor(heldCount / RESOURCE_PAGE_LIMIT) * RESOURCE_PAGE_LIMIT));
    $('available-list').append(browse);
  }
  updateResourceCountdowns();
}

function updateResourceCountdowns() {
  for (const countdown of $('resource-list').querySelectorAll('.resource-time')) {
    const remaining = Math.max(0, Math.ceil(Number(countdown.dataset.deadline) - Date.now() / 1000));
    const text = countdown.dataset.state === 'stale' || !remaining ? 'Waiting for verified closure'
      : `${Math.floor(remaining / 60)}m ${String(remaining % 60).padStart(2, '0')}s remaining`;
    if (countdown.textContent !== text) countdown.textContent = text;
  }
}

function resourceSnapshotURL(path = '/api/snapshot') {
  return `${path}?resource_limit=${RESOURCE_PAGE_LIMIT}&resource_offset=${state.resourceOffset}`;
}

function startSnapshotSource(epoch) {
  state.source?.close();
  const source = new EventSource(projectURL(resourceSnapshotURL('/api/events')));
  state.source = source;
  source.onopen = () => { if (epoch === state.epoch && state.source === source) setConnection(true); };
  source.addEventListener('snapshot', event => {
    if (epoch !== state.epoch || state.source !== source) return;
    try { applySnapshot(JSON.parse(event.data)); setConnection(true); }
    catch { setConnection(false); }
  });
  source.onerror = () => { if (epoch === state.epoch && state.source === source) setConnection(false); };
}

async function changeResourcePage(direction, offset = null) {
  if (state.resourceBusy || state.paused) return;
  const epoch = state.epoch;
  const previous = state.resourceOffset;
  state.resourceOffset = offset ?? Math.max(0, previous + direction * RESOURCE_PAGE_LIMIT);
  state.resourceBusy = true;
  state.source?.close();
  $('resource-previous').disabled = true;
  $('resource-next').disabled = true;
  try {
    const data = await fetchJSON(resourceSnapshotURL());
    if (epoch !== state.epoch) return;
    applySnapshot(data);
  } catch (error) {
    if (epoch !== state.epoch) return;
    state.resourceOffset = previous;
    composerStatus(`Could not load resources: ${error.message}`, true);
  } finally {
    if (epoch === state.epoch) {
      state.resourceBusy = false;
      renderResources();
      startSnapshotSource(epoch);
    }
  }
}

function applySnapshot(data) {
  if (data.project && data.project.id !== state.project) return;
  // Keep a bounded latest window, even while connected to an older server.
  data = { ...data, messages: data.messages.slice(-MESSAGE_LIMIT),
    history_truncated: data.history_truncated || data.messages.length > MESSAGE_LIMIT };
  renderProjects(data);
  const bridge = data.bridge;
  const clients = bridge?.clients || [];
  const connected = clients.filter(client => client.recent && !client.error).length;
  $('bridge-caption').textContent = clients.length > 1
    ? `Wake bridges: ${connected}/${clients.length} connected`
    : bridge?.recent && !bridge.error ? 'Wake bridge connected' : 'Wake bridge offline';
  $('bridge-caption').title = clients.length
    ? clients.map(client => `${client.host_id}: ${client.error || (client.recent ? 'connected' : 'offline')}`).join('\n')
    : bridge?.error || 'Only loaded, idle, bound Codex agents can be woken.';
  const outdated = clients.filter(client => client.recent && !client.error && client.running_version && client.installed_version && client.running_version !== client.installed_version);
  if (outdated.length) {
    $('bridge-caption').textContent += ' · Codex update: restart service';
    $('bridge-caption').title += '\n' + outdated.map(client => `${client.host_id}: running ${client.running_version}, installed ${client.installed_version}. Restart the service manually to use the installed version.`).join('\n');
  }
  renderUsage(data.usage);
  if (state.paused) { state.pending = data; return; }
  const initial = !state.snapshot;
  const previous = state.snapshot?.messages || [];
  const oldMax = Math.max(0, ...previous.map((item) => item.seq));
  const visibleMax = Math.max(0, ...[...state.messages.values()].map(message => message.seq));
  const feed = $('feed');
  const following = !state.loadingHistory && !state.hasNewer && !state.originals.size
    && feed.scrollHeight - feed.scrollTop - feed.clientHeight < 90;
  const overlap = data.messages.some(message => state.messages.has(message.id));
  const oldVisible = JSON.stringify([...state.messages.values()]);
  const labels = (sessions) => JSON.stringify((sessions || []).map(({ id, agent, model, reasoning_effort }) => [id, agent, model, reasoning_effort]));
  const labelsChanged = labels(data.sessions) !== labels(state.snapshot?.sessions);
  state.snapshot = data;
  renderDecisionNotifications(data.pending_decisions || [], data.pending_decision_count);
  if (state.selected || state.toMe) {
    if (initial && state.rootView) state.hasOlder = data.history_truncated;
    const scoped = data.messages.filter(matchesConversation);
    const seen = new Set(previous.map((message) => message.batch_id || message.id));
    for (const message of scoped) {
      const key = message.batch_id || message.id;
      if (!initial && message.seq > oldMax && !seen.has(key)) state.newCount++;
      seen.add(key);
    }
    if (!state.viewLoading) {
      if (!state.messages.size || scoped.some((message) => state.messages.has(message.id))) {
        const combined = new Map([...state.messages, ...scoped.map((message) => [message.id, message])]);
        const rows = [...combined.values()].sort((a, b) => a.seq - b.seq);
        if (state.rootView && (!following || communication.active) && rows.length > MESSAGE_WINDOW_LIMIT) {
          // Keep the bounded window the operator is reading; newer pages remain available below.
          state.historyLoaded = true; state.hasNewer = true;
        } else {
          if (rows.length > MESSAGE_WINDOW_LIMIT) state.hasOlder = true;
          state.messages = new Map(rows.slice(-MESSAGE_WINDOW_LIMIT).map((message) => [message.id, message]));
        }
      } else if (scoped.some((message) => message.seq > visibleMax)) {
        state.hasNewer = true;
      }
    }
    pruneExpanded();
    renderAgents();
    if (initial || labelsChanged || oldVisible !== JSON.stringify([...state.messages.values()])) renderMessages(following && !state.hasNewer);
    else renderHistoryStatus();
    renderResources();
    if (following && state.hasNewer) loadMessagePage(true);
    return;
  }
  const seen = new Set(previous.map((message) => message.batch_id || message.id));
  for (const message of data.messages) {
    const key = message.batch_id || message.id;
    if (!initial && message.seq > oldMax && !seen.has(key)) state.newCount += 1;
    seen.add(key);
    if (state.messages.has(message.id)) state.messages.set(message.id, message);
  }
  applyLiveFeed(state, data.messages, {initial, following, hasOlder:data.history_truncated});
  pruneExpanded();
  renderAgents();
  if (initial || labelsChanged || oldVisible !== JSON.stringify([...state.messages.values()])) renderMessages(initial || (following && !state.hasNewer));
  else renderHistoryStatus();
  renderResources();
  if (state.newCount) $('announcement').textContent = `${state.newCount} new messages received`;
}

function pruneExpanded() {
  for (const id of state.expanded) {
    if (!state.messages.has(id) && !state.originals.has(id)) state.expanded.delete(id);
  }
}

function showLatest() {
  if (state.loadingHistory || !state.snapshot) return;
  if (state.selected || state.toMe) { loadConversationView(); return; }
  state.historyLoaded = false; state.hasNewer = false; state.historyError = null;
  state.originals.clear(); state.highlighted = null;
  state.messages = new Map(state.snapshot.messages.map((message) => [message.id, message]));
  state.hasOlder = state.snapshot.history_truncated;
  pruneExpanded();
  renderMessages(true);
}

async function loadMessagePage(newer = false) {
  if (state.loadingHistory || !state.messages.size || (newer ? !state.hasNewer : !state.hasOlder)) return;
  const sequences = [...state.messages.values()].map(message => message.seq);
  const cursor = newer ? Math.max(...sequences) : Math.min(...sequences);
  const epoch = state.epoch;
  const viewGeneration = state.viewGeneration;
  state.loadingHistory = true;
  state.historyDirection = newer ? 'newer' : 'older';
  state.historyError = null;
  renderHistoryStatus();
  try {
    const params = conversationQuery();
    params.set(newer ? 'after' : 'before', String(cursor));
    params.set('limit', String(MESSAGE_LIMIT));
    const result = await fetchJSON(`/api/messages?${params}`);
    if (viewGeneration !== state.viewGeneration) return;
    const anchor = feedAnchor();
    const messages = new Map([...state.messages, ...result.messages.slice(0, MESSAGE_LIMIT).map(message => [message.id, message])]);
    const {trimmed,rows:retained} = historyWindow(messages,(a,b) => a.seq-b.seq,newer);
    // If the user moved to the opposite end during the request, avoid evicting
    // the message they are now reading. The next edge scroll can retry.
    if (anchor.id && !retained.some(message => `chat-${message.id}` === anchor.id || (anchor.batch && message.batch_id === anchor.batch))) return;
    state.messages = new Map(retained.map(message => [message.id, message]));
    state.historyLoaded = true;
    state.originals.clear(); state.highlighted = null;
    if (newer) {
      state.hasNewer = result.has_more || state.snapshot.messages.some(message => message.seq > (retained.at(-1)?.seq || 0));
      if (trimmed) state.hasOlder = true;
      if (!state.hasNewer) state.newCount = 0;
    } else {
      state.hasOlder = result.has_more;
      if (trimmed) state.hasNewer = true;
    }
    pruneExpanded();
    renderMessages(false, anchor);
  } catch (error) {
    if (error.name !== 'AbortError') state.historyError = newer ? 'newer' : 'older';
  } finally {
    if (epoch === state.epoch && viewGeneration === state.viewGeneration) {
      state.loadingHistory = false;
      state.historyDirection = null;
      renderHistoryStatus();
      state.scrollTop = $('feed').scrollTop;
    }
  }
}

function loadAtScrollEdge(direction, feed = $('feed'), view = state, loadPage = loadMessagePage) {
  if (view.loadingHistory || (view === state && !state.config)) return;
  if (direction < 0 && feed.scrollTop < 180 && view.hasOlder) loadPage(false);
  else if (direction > 0 && feed.scrollHeight - feed.scrollTop - feed.clientHeight < 180 && view.hasNewer) loadPage(true);
}

function bindFeedEdges(feed, view, loadPage) {
  const edge = direction => loadAtScrollEdge(direction,feed,view,loadPage);
  feed.addEventListener('scroll', () => {
    const top = feed.scrollTop, direction = top - view.scrollTop;
    view.scrollTop = top; edge(direction);
  }, {passive:true});
  feed.addEventListener('wheel', event => edge(event.deltaY), {passive:true});
  feed.addEventListener('keydown', event => {
    if (event.target !== feed) return;
    if (['ArrowUp','PageUp','Home'].includes(event.key)) edge(-1);
    if (['ArrowDown','PageDown','End'].includes(event.key)) edge(1);
  });
  let touchY = null;
  feed.addEventListener('touchstart', event => { touchY = event.touches[0]?.clientY; }, {passive:true});
  feed.addEventListener('touchmove', event => {
    const y = event.touches[0]?.clientY;
    if (touchY != null && y != null && Math.abs(touchY-y)>30) { edge(touchY-y); touchY = y; }
  }, {passive:true});
}

async function fetchJSON(url, options = {}, { projectScoped = true } = {}) {
  const epoch = state.epoch;
  const response = await fetch(projectScoped ? projectURL(url, options.method || 'GET') : url, { cache: 'no-store', ...options });
  const data = await response.json();
  if (epoch !== state.epoch) throw new DOMException('Project changed', 'AbortError');
  if (!response.ok) {
    const error = new Error(data.error || `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return data;
}

async function fetchUsage(url, options = {}) {
  const response = await fetch(url, { cache: 'no-store', ...options });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

function projectURL(path, method = 'GET') {
  const url = new URL(path, location.origin);
  if (state.rootView && method === 'GET' && /^\/api\/(config|snapshot|events|messages)(\/|$)/.test(url.pathname)) {
    url.pathname = url.pathname.replace('/api/', '/api/root/');
  }
  if (state.project !== 'default') url.searchParams.set('project', state.project);
  return url.pathname + url.search;
}

function projectControls() {
  const disabled = state.busy || state.sending;
  if ($('root-conversation')) $('root-conversation').disabled = disabled || !state.config;
  $('measurement-button').disabled = disabled || !state.config;
  for (const button of $('project-list').querySelectorAll('button')) button.disabled = disabled;
  $('new-project').disabled = disabled || !state.config;
  $('rename-project').disabled = disabled || !state.config;
  for (const id of ['manage-project-workspaces', 'manage-root-workspaces']) {
    if ($(id)) $(id).disabled = disabled || !state.config;
  }
  $('attach-button').disabled = disabled || !state.config || (state.rootView && !state.root);
  $('image-input').disabled = disabled || !state.config || (state.rootView && !state.root);
  for (const button of $('composer-images').querySelectorAll('button')) button.disabled = disabled;
}

function openWorkspaceGroups() {
  const projects = state.projects.filter(project => project.id !== 'default');
  const list = $('workspace-groups-list');
  list.replaceChildren();
  if (!projects.length) {
    list.append(node('p', 'project-scope-help', 'Create a project before configuring its workspaces.'));
  }
  for (const project of projects) {
    const section = node('section', 'workspace-group-entry');
    const label = node('label', '', `${project.name} workspaces`);
    const area = node('textarea', '', undefined);
    area.rows = 3;
    area.maxLength = 4096 * 32;
    area.dataset.projectId = project.id;
    area.setAttribute('aria-label', `${project.name} workspaces, one directory per line`);
    area.placeholder = '/absolute/canonical/primary-directory\n/absolute/canonical/related-directory';
    area.value = workspaceMembers(project).join('\n');
    section.append(label, area);
    list.append(section);
  }
  $('workspace-groups-error').textContent = '';
  $('workspace-groups-error').hidden = true;
  $('workspace-groups-dialog').showModal();
}

async function postProjectJSON(path, payload) {
  const body = JSON.stringify(payload);
  const submit = () => fetchJSON(path, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token }, body,
  });
  try { return await submit(); }
  catch (error) {
    if (error.status !== 403) throw error;
    state.config = await fetchJSON('/api/config');
    renderProjects(state.config);
    return submit();
  }
}

async function reconfigureProjectWorkspaces(project) {
  const previous = project.reconfiguration;
  const continuing = previous && !['completed', 'failed', 'cancelled'].includes(previous.status);
  const workspaces = continuing ? previous.requested_workspaces : workspaceMembers(project, ['creation_workspaces', 'creation_workspace', 'workspace']);
  const expectedThread = continuing ? (previous.old_thread_id || project.orchestrator?.thread_id) : project.orchestrator?.thread_id;
  if (!state.root || !workspaces.length || !expectedThread) return;
  if (continuing && previous.prompt && previous.prompt !== 'Continue the existing project work with this saved workspace assignment. Read the current project checkpoint before proceeding. Keep the existing project identity, inbox, queue, and ownership intact.') {
    composerStatus('This workspace update uses a different saved prompt. Reconcile it through the root controls before continuing.', true); return;
  }
  const paths = workspaces.map(path => `• ${path}`).join('\n');
  const action = continuing ? 'Continue' : 'Apply';
  if (!confirm(`${action} these saved workspaces for ${project.name} using a fresh conversation?\n\n${paths}\n\nThe current workspace assignment remains active until the new settings are verified. The project identity, inbox, and queue stay with the same orchestrator.`)) {
    state.rootWorkspaceRequests.delete(project.project_id); return;
  }
  const prompt = 'Continue the existing project work with this saved workspace assignment. Read the current project checkpoint before proceeding. Keep the existing project identity, inbox, queue, and ownership intact.';
  let payload = state.rootWorkspaceRequests.get(project.project_id);
  if (!payload) {
    payload = { project: project.project_id, expected_thread: expectedThread, workspaces, prompt, request_id: continuing ? previous.id : crypto.randomUUID(), confirm: true };
    state.rootWorkspaceRequests.set(project.project_id, payload);
  }
  state.busy = true; projectControls();
  try {
    const result = await postProjectJSON('/api/root/workspaces', payload);
    project.reconfiguration = result.reconfiguration || result;
    if (['completed', 'failed', 'cancelled'].includes(project.reconfiguration.status)) state.rootWorkspaceRequests.delete(project.project_id);
    if (state.rootOverviewData) {
      state.rootOverviewData.projects = state.rootOverviewData.projects.map(item => item.project_id === project.project_id ? project : item);
      state.rootOverviewKey = null;
      renderProjects({ root_orchestrator: state.root, root_overview: state.rootOverviewData, projects: state.projects, project: state.config?.project });
    }
  } catch (error) { composerStatus(error.message, true); }
  finally { state.busy = false; projectControls(); }
}

async function provisionRootProject(project) {
  if (!confirm(`Provision the root orchestrator for ${project.name}? It will use the saved ordered workspace group.`)) {
    state.rootWorkspaceRequests.delete(`provision:${project.project_id}`); return;
  }
  let payload = state.rootWorkspaceRequests.get(`provision:${project.project_id}`);
  if (!payload) {
    payload = { project: project.project_id, agent: 'orchestrator', request_id: crypto.randomUUID(), confirm: true, model: null, effort: null };
    state.rootWorkspaceRequests.set(`provision:${project.project_id}`, payload);
  }
  state.busy = true; projectControls();
  try {
    const result = await postProjectJSON('/api/root/provision', payload);
    if (result?.status && ['completed', 'failed', 'cancelled'].includes(result.status)) state.rootWorkspaceRequests.delete(`provision:${project.project_id}`);
    state.config = await fetchJSON('/api/config');
    renderProjects(state.config);
  } catch (error) { composerStatus(error.message, true); }
  finally { state.busy = false; projectControls(); }
}

function activityLabel(project) {
  const activity = project.activity;
  if (!activity) return 'Tracked work: unavailable';
  if (!activity.active) return 'Tracked work: none';
  return activity.blocked ? `Tracked work: ${activity.blocked} blocked` : 'Tracked work: unfinished';
}

function nativeGoalLabel(project) {
  const labels = { active: 'active', blocked: 'blocked', paused: 'paused',
    none: 'none', usageLimited: 'usage limited', budgetLimited: 'budget limited',
    complete: 'complete' };
  const status = project?.native_goal?.status;
  return `Native goal: ${Object.hasOwn(labels, status) ? labels[status] : 'unavailable'}`;
}

function renderProjects(data) {
  if ('root_orchestrator' in data) state.root = data.root_orchestrator;
  if ('root_overview' in data) {
    state.rootOverviewData = data.root_overview;
    for (const project of data.root_overview?.projects || []) {
      if (['completed', 'failed', 'cancelled'].includes(project.reconfiguration?.status)) state.rootWorkspaceRequests.delete(project.project_id);
    }
  }
  if (state.rootView) state.selected = state.root?.home_session || null;
  document.body.classList.toggle('root-screen', state.rootView && !communication.active);
  if ($('resources-button-label')) $('resources-button-label').textContent = state.rootView ? 'Projects' : 'Resources';
  $('resources-panel').setAttribute('aria-label', state.rootView ? 'Project overview' : 'Shared resources');
  document.title = communication.active ? 'All communication · Agent chat' : state.rootView ? 'Root orchestrator · Agent chat' : 'Agent room · Agent chat';
  $('message-input').placeholder = state.rootView ? 'Message your root orchestrator…' : 'Message all agents, @tag recipients, or drop files…';
  if ($('root-directory')) $('root-directory').hidden = !state.rootView;
  if ($('project-directory')) {
    $('project-directory').hidden = state.rootView;
    const workspaces = workspaceMembers(data.project);
    fillWorkspaceList($('project-workspace-path'), workspaces, 'No workspace is configured for this project.');
  }
  if (state.rootView && data.root_overview && $('root-projects')) {
    const key = JSON.stringify([data.root_overview, state.activeProjectsOnly]);
    if (state.rootOverviewKey !== key) {
      state.rootOverviewKey = key;
      const cards = (data.root_overview.projects || []).filter(project => !state.activeProjectsOnly || project.activity?.active).map(project => {
        const card = node('article', 'root-project');
        card.append(node('h3', '', project.name), node('p', 'root-project-state', project.setup.replaceAll('_', ' ')));
        card.dataset.projectId = project.project_id;
        const goalBadge = node('p', 'project-native-goal-badge', nativeGoalLabel(project));
        card.append(node('p', 'project-activity-badge', activityLabel(project)), goalBadge);
        const goalCheck = node('button', 'quiet-button', 'Check native goal');
        goalCheck.type = 'button';
        goalCheck.addEventListener('click', async () => {
          goalCheck.disabled = true; goalBadge.textContent = 'Native goal: unavailable';
          try {
            const result = await fetchJSON('/api/root/goal-status?project=' + encodeURIComponent(project.project_id), {}, {projectScoped: false});
            if (typeof result.observed_at === 'number' && Number.isFinite(result.observed_at) &&
                Date.now() / 1000 >= result.observed_at && Date.now() / 1000 - result.observed_at <= 5) {
              goalBadge.textContent = nativeGoalLabel({native_goal: result});
              setTimeout(() => { goalBadge.textContent = 'Native goal: unavailable'; }, Math.max(0, 5000 - (Date.now() - result.observed_at * 1000)));
            }
          } catch (_) { goalBadge.textContent = 'Native goal: unavailable'; }
          finally { goalCheck.disabled = false; }
        });
        card.append(goalCheck);
        const objectives = node('section', 'project-objectives');
        objectives.append(node('h4', '', 'PO backlog objectives'));
        if (!Array.isArray(project.objectives)) {
          objectives.append(node('p', '', 'Backlog objectives: unavailable'));
        } else if (!project.objectives.length) {
          objectives.append(node('p', '', 'No recorded backlog objectives.'));
        } else {
          for (const item of project.objectives) {
            const entry = node('article', 'project-objective');
            entry.append(node('h5', '', item.title),
              node('p', '', `Source: ${item.source_request_id} · Owner: ${item.owner_session} · State: ${item.state}`),
              node('p', '', `Dependencies: ${Array.isArray(item.dependencies) ? (item.dependencies.join(', ') || 'none') : 'unknown'}`),
              node('p', '', `Next action: ${item.next_action ?? 'unknown'}`));
            objectives.append(entry);
          }
        }
        card.append(objectives);
        const currentWorkspaces = workspaceMembers(project, ['workspaces', 'cwd']);
        const savedWorkspaces = workspaceMembers(project, ['creation_workspaces', 'creation_workspace', 'workspace']);
        const directory = node('div', 'root-project-directory');
        directory.append(node('span', 'root-project-directory-label', 'Currently in use'));
        const currentList = node('ol', 'root-project-directory-value');
        fillWorkspaceList(currentList, currentWorkspaces, 'Not configured');
        directory.append(currentList);
        const savedDirectory = node('div', 'root-project-directory');
        savedDirectory.append(node('span', 'root-project-directory-label', 'Saved for setup'));
        const savedList = node('ol', 'root-project-directory-value');
        fillWorkspaceList(savedList, savedWorkspaces, 'Not configured');
        savedDirectory.append(savedList);
        card.append(directory);
        card.append(savedDirectory);
        if (project.agent) card.append(node('p', '', `${project.agent}${project.model ? ` · ${project.model}` : ''}`));
        if (project.error) card.append(node('p', 'root-project-error', project.error));
        if (project.reconfiguration) {
          const update = project.reconfiguration;
          card.append(node('p', 'root-project-state', `Workspace update: ${String(update.status || 'pending').replaceAll('_', ' ')}`));
          if (update.error) card.append(node('p', 'root-project-error', update.error));
        }
        card.append(node('p', 'root-project-note', 'Completed workspace updates become current when this overview refreshes.'));
        const bridge = project.bridge;
        if (!bridge?.recent || !bridge.project_scope_worker) card.append(node('p', 'root-project-error', !bridge?.recent ? 'Wake bridge unavailable' : 'Project creation awaits bridge activation'));
        const update = project.reconfiguration;
        if (project.setup === 'ready_to_provision' && bridge?.recent && bridge.project_scope_worker && bridge.project_workspaces_worker) {
          const provision = node('button', 'quiet-button', 'Provision root orchestrator');
          provision.type = 'button'; provision.disabled = state.busy || state.sending;
          provision.addEventListener('click', () => provisionRootProject(project));
          card.append(provision);
        }
        const updatePending = update && !['completed', 'failed', 'cancelled'].includes(update.status);
        const savedDiffers = savedWorkspaces.length && !sameWorkspaceMembers(currentWorkspaces, savedWorkspaces);
        if (project.orchestrator?.thread_id && (savedDiffers || updatePending)) {
          const apply = node('button', 'quiet-button', updatePending ? 'Continue workspace update' : 'Apply saved workspaces');
          apply.type = 'button';
          apply.disabled = state.busy || state.sending || !bridge?.recent || !bridge.project_workspaces_worker;
          apply.addEventListener('click', () => reconfigureProjectWorkspaces(project));
          card.append(apply);
        }
        const open = node('button', 'quiet-button', 'Open project');
        open.type = 'button'; open.setAttribute('aria-label', `Open project ${project.name}`);
        open.addEventListener('click', () => switchProject(project.project_id));
        card.append(open); return card;
      });
      $('root-projects').replaceChildren(...(cards.length ? cards : [node('p', 'empty-state', 'No projects have unfinished tracked work.')]));
    }
  }
  if ($('root-conversation')) {
    $('root-conversation').hidden = !state.root && !state.rootView;
    $('root-conversation').classList.toggle('selected', state.rootView && !communication.active);
    $('root-conversation').disabled = state.busy || state.sending || !state.config;
  }
  if (!data.projects) { if (communication.active) communicationContext(); return; }
  state.projects = data.projects;
  const projectButtons = data.projects.filter(project => !state.activeProjectsOnly || project.activity?.active).map(project => {
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'room-button project-nav-button';
    button.dataset.projectId = project.id;
    button.append(node('span', '', project.name), node('span', 'project-activity-badge', activityLabel(project)), node('span', 'project-native-goal-badge', nativeGoalLabel(project)));
    const selected = !communication.active && !state.rootView && project.id === state.project;
    button.classList.toggle('selected', selected);
    button.setAttribute('aria-current', selected ? 'page' : 'false');
    button.addEventListener('click', () => switchProject(project.id));
    return button;
  });
  $('project-list').replaceChildren(...projectButtons);
  const measuring = !!data.projects.find(p => p.id === state.project)?.measurement_running;
  $('measurement-indicator').hidden = !measuring;
  $('measurement-button').setAttribute('aria-label', measuring ? 'Measure usage (active)' : 'Measure usage');
  $('measurement-button').title = measuring ? 'Usage measurement is active' : 'Measure usage for this project';
  projectControls();
  if (communication.active) communicationContext();
  refreshRootAckPlan();
}

// Native action review is separate from chat-message decisions and acknowledgements.
const actionReview = { epoch: -1, rows: new Map(), cards: new Map(), sending: new Set(),
  uncertain: new Set(), submitted: new Set(), loading: false, stale: true, lastPoll: 0, sequence: 0 };
const actionLabels = { accept: 'Allow once', decline: 'Decline', cancel: 'Cancel action', deny: 'Deny',
  network_turn: 'Allow network for turn', network_session: 'Allow network for session',
  fileSystem_turn: 'Allow files for turn', fileSystem_session: 'Allow files for session',
  all_turn: 'Allow all for turn', all_session: 'Allow all for session' };
const actionStates = { pending: 'Awaiting your decision', proposed: 'Decision submitted · awaiting agent response',
  response_sent: 'Response sent to the requesting agent', response_uncertain: 'Response delivery is uncertain',
  resolved: 'Request resolved', completed: 'Item ended · outcome not verified', expired: 'Request expired', disconnected: 'Request disconnected' };

function actionItemOutcome(row) {
  const item = row.item_outcome;
  if (row.status !== 'completed' || !item || !['commandExecution', 'fileChange'].includes(item.type)) return actionStates[row.status];
  if (item.status === 'declined') return 'Action declined by provider';
  if (item.status === 'failed') return 'Action failed';
  if (item.status === 'completed') {
    if (item.type === 'fileChange') return 'File change completed';
    if (Number.isInteger(item.exitCode)) return item.exitCode === 0 ? 'Command completed · exit 0' : `Command failed · exit ${item.exitCode}`;
  }
  return actionStates.completed;
}

function resetActionReview() {
  actionReview.epoch = state.epoch; actionReview.sequence++;
  actionReview.rows.clear(); actionReview.cards.clear(); actionReview.sending.clear(); actionReview.uncertain.clear(); actionReview.submitted.clear();
  actionReview.loading = false; actionReview.stale = true; actionReview.lastPoll = 0;
  $('action-review-list').replaceChildren();
  $('action-review-status').textContent = 'Loading current project requests…';
  if ($('action-review-dialog').open) $('action-review-dialog').close();
  renderActionReview();
}

function actionRemaining(row) {
  return Math.max(0, row.remaining - (performance.now() - row.observedAt) / 1000);
}

function renderActionReview() {
  const ready = state.config?.operator_ack_enabled === true && actionReview.epoch === state.epoch;
  $('action-review-button').disabled = !ready;
  let pending = 0;
  for (const row of actionReview.rows.values()) {
    const remaining = actionRemaining(row);
    if (row.status === 'pending' && remaining > 0) pending++;
    let card = actionReview.cards.get(row.handle);
    if (!card) {
      card = node('article', 'action-review-card');
      const heading = document.createElement('h3');
      heading.textContent = `${row.agent} · ${row.preview.kind}`;
      card.append(heading);
      const fields = [['reason','Reason'],['command','Command'],['cwd','Directory'],['environment','Environment'],['grantRoot','Requested file scope'],['host','Host'],['protocol','Protocol'],['permissions','Requested permissions']];
      for (const [key, label] of fields) {
        if (typeof row.preview[key] !== 'string' || !row.preview[key]) continue;
        const field = node('div', 'action-review-field');
        const caption = document.createElement('strong');
        caption.textContent = label;
        const preview = document.createElement('pre');
        preview.textContent = row.preview[key];
        field.append(caption, preview);
        card.append(field);
      }
      const status = node('p', 'action-review-outcome'); status.setAttribute('role', 'status');
      const actions = node('div', 'action-review-actions');
      if (row.choices.includes('custom') && Array.isArray(row.permission_items)) {
        const selections = node('div', 'action-review-selections');
        const epoch = state.epoch;
        for (const item of row.permission_items) {
          const label = document.createElement('label');
          const checkbox = document.createElement('input'); checkbox.type = 'checkbox'; checkbox.value = item.id;
          label.append(checkbox, document.createTextNode(item.label)); selections.append(label);
        }
        card.append(selections);
        for (const scope of ['turn', 'session']) {
          const button = node('button', 'quiet-button', `Allow selected for ${scope}`);
          button.type = 'button';
          button.addEventListener('click', () => decideAction(row.handle, {scope, selection: Array.from(selections.querySelectorAll('input:checked'), input => input.value)}, epoch));
          actions.append(button);
        }
      }
      for (const choice of row.choices) {
        if (!Object.hasOwn(actionLabels, choice)) continue;
        const button = node('button', 'quiet-button', actionLabels[choice]);
        button.type = 'button'; button.dataset.decision = choice;
        const epoch = state.epoch;
        button.addEventListener('click', () => decideAction(row.handle, choice, epoch));
        actions.append(button);
      }
      card.append(status, actions); card.outcome = status; card.actions = actions;
      actionReview.cards.set(row.handle, card); $('action-review-list').append(card);
    }
    const uncertain = actionReview.uncertain.has(row.handle);
    const sending = actionReview.sending.has(row.handle), submitted = actionReview.submitted.has(row.handle);
    card.outcome.textContent = uncertain ? 'Decision delivery uncertain. Waiting for the request to resolve; it will not be resent.'
      : sending ? 'Submitting decision…' : submitted ? actionStates.proposed : actionItemOutcome(row) || 'Request unavailable';
    if (row.status === 'pending' && remaining > 0 && !uncertain && !sending && !submitted) {
      card.outcome.textContent += ` · expires in ${Math.ceil(remaining)}s`;
    }
    if (row.status === 'pending' && remaining <= 0) card.outcome.textContent = 'Request expired · refreshing outcome';
    for (const button of card.actions.children) {
      button.disabled = !ready || actionReview.stale || sending || uncertain || submitted || row.status !== 'pending' || remaining <= 0;
    }
  }
  $('action-review-count').textContent = pending ? String(pending) : '';
  $('action-review-button').setAttribute('aria-label', pending ? `Action review: ${pending} requests awaiting a decision` : 'Action review');
  $('action-review-empty').hidden = actionReview.rows.size > 0;
}

async function loadActionReview() {
  if (!state.config?.operator_ack_enabled || actionReview.loading || actionReview.epoch !== state.epoch) return;
  const epoch = state.epoch, sequence = ++actionReview.sequence;
  actionReview.loading = true; actionReview.lastPoll = performance.now();
  try {
    const result = await fetchJSON('/api/approvals');
    if (epoch !== state.epoch || sequence !== actionReview.sequence) return;
    if (!Array.isArray(result.requests)) throw new Error('Action review response is unavailable');
    const next = new Map();
    for (const row of result.requests) {
      if (!row || typeof row.handle !== 'string' || typeof row.agent !== 'string'
          || !row.preview || typeof row.preview.kind !== 'string' || !Array.isArray(row.choices)
          || !Number.isFinite(row.remaining) || !Object.hasOwn(actionStates, row.status)) {
        throw new Error('Action review response is unavailable');
      }
      next.set(row.handle, { ...row, observedAt: performance.now() });
      if (row.status !== 'pending') { actionReview.uncertain.delete(row.handle); actionReview.submitted.delete(row.handle); }
    }
    for (const [handle, card] of actionReview.cards) {
      if (!next.has(handle)) { card.remove(); actionReview.cards.delete(handle); actionReview.uncertain.delete(handle); actionReview.submitted.delete(handle); }
    }
    actionReview.rows = next; actionReview.stale = false;
    $('action-review-status').textContent = 'Only the offered decision for this current project request will be sent. A submitted decision is not proof that the action completed.';
  } catch (error) {
    if (epoch !== state.epoch || sequence !== actionReview.sequence) return;
    actionReview.stale = true;
    $('action-review-status').textContent = `Action review unavailable: ${error.message}. Decisions are disabled until refreshed.`;
  } finally {
    if (epoch === state.epoch && sequence === actionReview.sequence) {
      actionReview.loading = false; renderActionReview();
    }
  }
}

async function decideAction(handle, decision, epoch) {
  const row = actionReview.rows.get(handle);
  if (epoch !== state.epoch || actionReview.epoch !== epoch || !state.config?.operator_ack_enabled
      || actionReview.stale || !row || row.status !== 'pending' || actionRemaining(row) <= 0
      || !(typeof decision === 'string' ? row.choices.includes(decision) : row.choices.includes('custom')) || actionReview.sending.has(handle) || actionReview.uncertain.has(handle) || actionReview.submitted.has(handle)) return;
  actionReview.sending.add(handle); renderActionReview();
  try {
    await fetchJSON('/api/approvals/decision', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Agent-Chat-CSRF': state.config.csrf_token },
      body: JSON.stringify({ handle, decision }),
    });
    if (epoch !== state.epoch) return;
    actionReview.submitted.add(handle);
    const current = actionReview.rows.get(handle);
    if (current) current.status = 'proposed';
  } catch (error) {
    if (epoch !== state.epoch) return;
    // A lost HTTP response is not permission to retry a one-use decision.
    actionReview.uncertain.add(handle);
    $('action-review-status').textContent = error.message;
  } finally {
    if (epoch === state.epoch) {
      actionReview.sending.delete(handle); renderActionReview(); await loadActionReview();
    }
  }
}

$('action-review-button').addEventListener('click', () => {
  $('action-review-dialog').showModal(); loadActionReview();
});
$('action-review-close').addEventListener('click', () => $('action-review-dialog').close());
$('action-review-refresh').addEventListener('click', loadActionReview);

async function connect() {
  state.resourceOffset = 0; state.resourceBusy = false; state.resourceRenderKey = null;
  const epoch = ++state.epoch;
  resetActionReview();
  state.loadingHistory = false;
  state.historyDirection = null; state.historyError = null;
  state.snapshot = null; state.messages.clear(); state.originals.clear(); state.expanded.clear();
  state.historyLoaded = false; state.hasNewer = false; state.hasOlder = false;
  state.pending = null; state.paused = false; state.newCount = 0;
  $('pause-button').setAttribute('aria-pressed', 'false'); $('pause-label').textContent = 'Pause feed'; $('pause-icon').textContent = 'Ⅱ';
  renderMessages(true);
  state.source?.close();
  state.config = null;
  $('message-input').disabled = true;
  $('send-button').disabled = true;
  projectControls();
  try {
    state.config = await fetchJSON('/api/config');
    if (state.rootView) state.project = state.config.project.id;
    renderProjects(state.config);
    renderActionReview(); loadActionReview();
    if (!state.rootView && !localStorage.getItem(SETUP_STORAGE_KEY)) openSetupGuide();
    applySnapshot(await fetchJSON(resourceSnapshotURL()));
    $('message-input').disabled = state.rootView && !state.root;
    $('send-button').disabled = state.rootView && !state.root;
    composerStatus('');
    startSnapshotSource(epoch);
    if (location.pathname === '/communications') showCommunication(false);
  } catch (error) {
    if (epoch !== state.epoch) return;
    composerStatus(error.message + '. Reconnect to try again.', true);
    setConnection(false);
  }
}

function switchProject(id, rootView = false) {
  hideCommunication();
  if ($('reset-dialog').open) $('reset-dialog').close();
  if (state.busy || state.sending || (id === state.project && rootView === state.rootView)) return;
  if ($('media-dialog').open) {
    mediaViewer.opener = null;
    $('media-dialog').close();
  }
  if ($('attachment-gallery').open) $('attachment-gallery').close();
  state.drafts.set(`${state.rootView ? 'root' : 'project'}:${state.project}`, { body: $('message-input').value, attachments: state.attachments });
  closeMeasurement();
  state.measurement = null;
  state.measurementSaving = false;
  state.project = id;
  state.rootView = rootView; state.rootOverviewKey = null;
  renderRootAck();
  state.viewGeneration++; state.viewLoading = false;
  const url = new URL(location.href);
  url.pathname = rootView ? '/root' : '/';
  if (id === 'default') url.searchParams.delete('project'); else url.searchParams.set('project', id);
  history.replaceState(null, '', url);
  state.snapshot = null; state.messages.clear(); state.originals.clear(); state.expanded.clear();
  state.selected = null; state.query = ''; state.ack = 'all'; state.toMe = false; state.highlighted = null;
  state.pending = null; state.paused = false; state.hasOlder = false; state.newCount = 0;
  state.historyLoaded = false; state.hasNewer = false; state.loadingHistory = false;
  state.historyDirection = null; state.historyError = null;
  $('pause-button').setAttribute('aria-pressed', 'false'); $('pause-label').textContent = 'Pause feed'; $('pause-icon').textContent = 'Ⅱ';
  $('search').value = ''; $('ack-filter').value = 'all';
  const draftKey = `${rootView ? 'root' : 'project'}:${id}`;
  const draft = state.drafts.get(draftKey);
  state.drafts.delete(draftKey);
  $('message-input').value = draft?.body || '';
  state.attachments = draft?.attachments || [];
  renderDraftImages();
  clearReply(); hideMentions(); closePanels();
  $('agent-list').replaceChildren(); $('resource-list').replaceChildren();
  $('available-list').replaceChildren(); $('available-resources').hidden = true;
  $('agent-count').textContent = '0'; $('all-count').textContent = '0';
  $('held-count').textContent = '0'; $('mobile-resource-count').textContent = '0';
  $('waiting-count').textContent = ''; $('bridge-caption').textContent = 'Connecting…';
  renderMessages(true);
  return connect();
}

$('root-conversation')?.addEventListener('click', async () => {
  const root = state.root;
  if (!root || state.busy || state.sending) return;
  await switchProject(root.home_project, true);
});

$('usage-button').addEventListener('click', () => {
  state.usageDialogDirty = false;
  usageError();
  renderUsage(state.usage);
  $('usage-dialog').showModal();
  $('usage-enabled').focus();
});
$('measurement-button').addEventListener('click', () => { measurementError(); $('measurement-dialog').showModal(); $('measurement-duration').focus(); loadMeasurement(); });
$('cancel-measurement').addEventListener('click', closeMeasurement);
$('measurement-dialog').addEventListener('close', () => {
  clearMeasurementPolling();
  state.measurementRequest++;
  state.measurementLoading = false;
  state.measurementSaving = false;
  state.measurement = null;
  $('measurement-pause-at-end').checked = false;
  $('measurement-report').hidden = true;
  $('measurement-status').textContent = 'Loading measurement status…';
  measurementControls();
});
$('start-measurement').addEventListener('click', () => saveMeasurement('start'));
$('stop-measurement').addEventListener('click', () => saveMeasurement('stop'));
$('cancel-usage').addEventListener('click', () => $('usage-dialog').close());
$('usage-enabled').addEventListener('change', () => {
  state.usageDialogDirty = true;
  usageFormControls();
});
$('close-media').addEventListener('click', () => $('media-dialog').close());
$('previous-media').addEventListener('click', () => stepMedia(-1));
$('next-media').addEventListener('click', () => stepMedia(1));
$('media-dialog').addEventListener('keydown', event => {
  if (event.target.tagName === 'VIDEO' || event.ctrlKey || event.metaKey || event.altKey) return;
  if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
    event.preventDefault();
    event.stopPropagation();
    stepMedia(event.key === 'ArrowLeft' ? -1 : 1);
  }
});
$('media-dialog').addEventListener('close', () => {
  for (const video of $('media-stage').querySelectorAll('video')) video.pause();
  $('media-stage').replaceChildren();
  const opener = mediaViewer?.opener;
  const id = opener?.dataset.attachmentId;
  mediaViewer = null;
  const currentOpener = opener?.isConnected ? opener
    : [...document.querySelectorAll('#messages [data-attachment-id], #communication-messages .communication-image-viewer[data-attachment-id]')]
      .find(item => item.dataset.attachmentId === id);
  if (currentOpener) currentOpener.focus({ preventScroll: true });
});
$('usage-threshold').addEventListener('input', () => { state.usageDialogDirty = true; usageError(); });
$('usage-form').addEventListener('submit', (event) => { event.preventDefault(); saveUsage('configure'); });
$('resume-usage').addEventListener('click', () => saveUsage('resume'));
for (const [id, rename] of [['new-project', false], ['rename-project', true]]) {
  $(id).addEventListener('click', () => {
    $('project-form').dataset.rename = String(rename);
    $('project-dialog-title').textContent = rename ? 'Rename project' : 'New project';
    $('project-name').value = rename ? state.projects.find(p => p.id === state.project).name : '';
    if ($('project-creation-scope')) {
      $('project-creation-scope').hidden = rename;
      $('project-workspaces').value = '';
      $('project-instructions').value = '';
    }
    $('project-error').textContent = '';
    $('project-error').hidden = true;
    $('project-dialog').showModal(); $('project-name').focus();
  });
}
$('cancel-project').addEventListener('click', () => $('project-dialog').close());
$('manage-project-workspaces').addEventListener('click', openWorkspaceGroups);
$('manage-root-workspaces').addEventListener('click', openWorkspaceGroups);
$('cancel-workspace-groups').addEventListener('click', () => $('workspace-groups-dialog').close());
$('workspace-groups-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (state.busy) return;
  const assignments = [...$('workspace-groups-list').querySelectorAll('textarea[data-project-id]')]
    .map(area => ({ id: area.dataset.projectId, workspaces: workspaceLines(area.value) }))
    .filter(item => item.workspaces.length);
  if (!assignments.length) {
    $('workspace-groups-error').textContent = 'Enter at least one workspace for a project. Blank project groups are left unchanged.';
    $('workspace-groups-error').hidden = false;
    return;
  }
  $('workspace-groups-error').textContent = '';
  $('workspace-groups-error').hidden = true;
  state.busy = true; projectControls(); $('save-workspace-groups').disabled = true;
  try {
    await postProjectJSON('/api/projects/workspaces-batch', { assignments });
    $('workspace-groups-dialog').close();
    state.config = await fetchJSON('/api/config');
    renderProjects(state.config);
    applySnapshot(await fetchJSON(resourceSnapshotURL()));
  } catch (error) {
    $('workspace-groups-error').textContent = error.message;
    $('workspace-groups-error').hidden = false;
  } finally { state.busy = false; projectControls(); $('save-workspace-groups').disabled = false; }
});
$('project-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (state.busy) return;
  const rename = event.currentTarget.dataset.rename === 'true';
  $('project-error').textContent = '';
  $('project-error').hidden = true;
  const saveLabel = $('save-project').textContent;
  $('save-project').textContent = rename ? 'Saving…' : 'Creating…';
  state.busy = true; projectControls(); $('save-project').disabled = true;
  try {
    const path = rename ? '/api/projects/rename' : '/api/projects';
    const workspaces = workspaceLines($('project-workspaces').value);
    const scope = !rename && (workspaces.length || $('project-instructions').value.trim())
      ? { ...(workspaces.length ? { workspaces } : {}), instructions:$('project-instructions').value } : {};
    const result = await postProjectJSON(path, { name: $('project-name').value, ...(rename ? { id: state.project } : scope) });
    $('project-dialog').close();
    state.busy = false;
    if (rename) applySnapshot(await fetchJSON(resourceSnapshotURL())); else switchProject(result.project.id);
  } catch (error) { $('project-error').textContent = error.message; $('project-error').hidden = false; }
  finally { state.busy = false; projectControls(); $('save-project').disabled = false; $('save-project').textContent = saveLabel; }
});
$('all-conversations').addEventListener('click', () => selectAgent(null));
$('setup-guide').addEventListener('click', openSetupGuide);
$('dismiss-setup').addEventListener('click', dismissSetupGuide);
$('setup-back').addEventListener('click', () => { setupStep = Math.max(0, setupStep - 1); renderSetupGuide(); });
$('setup-next').addEventListener('click', () => { if (setupStep === $('setup-steps').children.length - 1) dismissSetupGuide(); else { setupStep += 1; renderSetupGuide(); } });
$('setup-dialog').addEventListener('cancel', () => localStorage.setItem(SETUP_STORAGE_KEY, 'complete'));
$('search').addEventListener('input', (event) => { state.query = event.target.value.toLowerCase(); renderMessages(true); });
$('ack-filter').addEventListener('change', (event) => { state.ack = event.target.value; renderMessages(true); });
$('to-me-filter').addEventListener('click', () => { state.toMe = !state.toMe; loadConversationView(); });
$('retry-button').addEventListener('click', connect);
$('pause-button').addEventListener('click', () => {
  state.paused = !state.paused;
  $('pause-button').setAttribute('aria-pressed', String(state.paused));
  $('pause-label').textContent = state.paused ? 'Resume feed' : 'Pause feed';
  $('pause-icon').textContent = state.paused ? '▷' : 'Ⅱ';
  if (!state.paused && state.pending) { applySnapshot(state.pending); state.pending = null; }
  renderResources();
  setConnection(state.connected);
});
$('new-messages').addEventListener('click', showLatest);
$('show-latest').addEventListener('click', showLatest);
bindFeedEdges($('feed'), state, loadMessagePage);
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
  if ($('media-dialog').open) return;
  if (event.key === 'Escape') closePanels();
  if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) {
    event.preventDefault(); $(communication.active ? 'communication-text' : 'search').focus();
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
$('attach-button').addEventListener('click', () => $('image-input').click());
$('image-input').addEventListener('change', (event) => {
  addImages([...event.target.files]);
  event.target.value = '';
});
let imageDragDepth = 0;
const draggingFiles = (event) => [...(event.dataTransfer?.types || [])].includes('Files');
$('composer').addEventListener('dragenter', (event) => {
  if (!draggingFiles(event)) return;
  event.preventDefault();
  imageDragDepth += 1;
  if (!state.sending && !state.busy && state.config) $('composer').classList.add('drag-over');
});
$('composer').addEventListener('dragleave', (event) => {
  if (!draggingFiles(event)) return;
  imageDragDepth = Math.max(0, imageDragDepth - 1);
  if (!imageDragDepth) $('composer').classList.remove('drag-over');
});
document.addEventListener('dragover', (event) => {
  if (draggingFiles(event)) event.preventDefault();
});
document.addEventListener('drop', (event) => {
  if (!draggingFiles(event)) return;
  event.preventDefault();
  imageDragDepth = 0;
  $('composer').classList.remove('drag-over');
  if ($('composer').contains(event.target)) addImages([...event.dataTransfer.files]);
});
document.addEventListener('click', (event) => { if (!$('composer').contains(event.target)) hideMentions(); });
$('composer').addEventListener('submit', async (event) => {
  event.preventDefault();
  if (state.sending || !state.config) return;
  const draft = $('message-input').value.trimStart();
  const parsed = state.rootView ? {tags: [], bodyStart: 0, invalid: false} : addressTags(draft);
  const sessions = state.rootView ? [] : mentionSessions();
  const recipients = [];
  for (const tag of parsed.tags) {
    const session = sessions.find((item) => item.key === tag.key || item.id === tag.key);
    if (!session) { composerStatus(`Unknown or ambiguous agent: @${tag.key}. Choose a suggestion.`, true); return; }
    if (!recipients.includes(session.id)) recipients.push(session.id);
  }
  if (parsed.invalid) { composerStatus('Complete the @agent tag or remove it to message all agents.', true); return; }
  const broadcast = !state.rootView && !parsed.tags.length && !state.reply;
  if (!parsed.tags.length) recipients.push(...(state.rootView ? [state.root?.home_session].filter(Boolean) : (state.reply ? replyRecipients(state.reply) : sessions.map((session) => session.id))));
  const body = draft.slice(parsed.bodyStart).trim();
  if (!body && !state.attachments.length) { composerStatus('Write a message or attach a file.', true); return; }
  if (!recipients.length) { composerStatus('No agents are registered yet.', true); return; }
  if (state.reply && recipients.some((id) => !replyRecipients(state.reply).includes(id))) {
    composerStatus('An @agent differs from this reply. Cancel the reply to start a new conversation.', true);
    return;
  }
  const to = state.rootView ? state.root.home_session
    : broadcast ? null : (recipients.length === 1 && !state.reply?.batch_id ? recipients[0] : recipients);
  hideMentions();
  state.sending = true;
  projectControls();
  $('send-button').disabled = true;
  $('message-input').disabled = true;
  composerStatus('Sending…');
  try {
    const message = { to, body, ...(state.reply ? { reply_to: state.reply.id } : {}) };
    const headers = { 'X-Agent-Chat-CSRF': state.config.csrf_token };
    let payload;
    if (state.attachments.length) {
      payload = new FormData();
      payload.append('message', JSON.stringify(message));
      for (const attachment of state.attachments) {
        const upload = fileExtension(attachment.file.name) === 'eml'
          ? new Blob([attachment.file], { type: 'application/octet-stream' })
          : attachment.file;
        payload.append('images', upload, attachment.file.name);
      }
    } else {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(message);
    }
    const sent = await fetchJSON('/api/messages', { method: 'POST', headers, body: payload });
    $('message-input').value = '';
    clearImages();
    clearReply();
    composerStatus(broadcast ? `Group info sent to ${sent.messages.length} agents; no wake requested` : `Sent; wake requested for ${recipients.map((id) => agentLabel(id)).join(', ')}`);
    try {
      applySnapshot(await fetchJSON(resourceSnapshotURL()));
      showLatest();
    } catch { setConnection(false); }
  } catch (error) {
    composerStatus(error.message, true);
  } finally {
    state.sending = false;
    projectControls();
    $('send-button').disabled = false;
    $('message-input').disabled = false;
  }
});
$('resource-previous').addEventListener('click', () => changeResourcePage(-1));
$('resource-next').addEventListener('click', () => changeResourcePage(1));
setInterval(() => {
  renderActionReview();
  if (!document.hidden && performance.now() - actionReview.lastPoll >= 2000) loadActionReview();
  if (!state.paused) updateResourceCountdowns();
  if (communication.active && !communication.updating) loadCommunication('update');
}, 1000);
window.addEventListener('beforeunload', () => state.source?.close());
connect();


// This history is independent of the conversation's loaded message window.
const gallery = { items: [], cursor: null, pendingMatches: [], request: 0, root: false, timer: null, selectedProject: '', partial: false };
function galleryTypeKey(attachment) {
  const extension = fileExtension(typeof attachment?.name === 'string' ? attachment.name : '');
  const mime = typeof attachment?.mime === 'string' ? attachment.mime.split(';', 1)[0].trim().toLowerCase() : '';
  if (IMAGE_EXTENSIONS.has(extension) || IMAGE_MIMES.has(mime)) return 'image';
  if (VIDEO_EXTENSIONS.has(extension) || VIDEO_MIMES.has(mime)) return 'video';
  if (AUDIO_EXTENSIONS.has(extension) || AUDIO_MIMES.has(mime)) return 'audio';
  if (MODEL_EXTENSIONS.has(extension)) return 'model';
  if (DOCUMENT_EXTENSIONS.has(extension) || mime.startsWith('text/') ||
      ['application/pdf', 'application/json', 'application/xml', 'message/rfc822'].includes(mime)) return 'document';
  if (extension || (mime && mime !== 'application/octet-stream')) return 'other';
  return 'unknown';
}
function renderGalleryItems(items) {
  for (const attachment of items) {
    const card = node('article', 'gallery-card');
    const visual = node('button', 'gallery-preview');
    visual.type = 'button';
    visual.setAttribute('aria-label', `Open ${attachment.name}`);
    let audio = null;
    if (AUDIO_MIMES.has(attachment.mime)) {
      audio = node('audio', 'gallery-audio');
      audio.controls = true;
      audio.preload = 'metadata';
      audio.src = attachment.url;
      audio.setAttribute('aria-label', `Play ${attachment.name}`);
    } else if (IMAGE_MIMES.has(attachment.mime)) {
      const image = node('img'); image.src = attachment.url; image.alt = attachment.name; image.loading = 'lazy';
      visual.append(image);
    } else if (VIDEO_MIMES.has(attachment.mime)) {
      const video = node('video'); video.src = attachment.url; video.preload = 'metadata'; video.muted = true; video.playsInline = true;
      visual.append(video, node('span', 'gallery-file-type', '▶ Video'));
    } else visual.append(node('span', 'gallery-file-type', attachmentType(attachment)));
    const media = IMAGE_MIMES.has(attachment.mime) || VIDEO_MIMES.has(attachment.mime);
    visual.addEventListener('click', () => {
      if (media) openMediaViewer({ attachments: gallery.items.map(item => ({ ...item, galleryURL: item.url })) }, attachment, visual);
      else window.open(attachment.url, '_blank', 'noopener');
    });
    const name = node('a', 'gallery-file-name', attachment.name);
    name.href = attachment.url; name.download = attachment.name; name.title = attachment.name;
    const details = node('p', 'gallery-file-meta', `${attachment.project_name} · ${attachment.size >= 1048576 ? (attachment.size / 1048576).toFixed(1) + ' MB' : Math.max(1, Math.round(attachment.size / 1024)) + ' KB'} · ${dateFormat.format(new Date(attachment.created_at * 1000))}`);
    const download = attachmentDownload(attachment, 'quiet-button gallery-download', true);
    card.append(audio || visual, name, download, details); $('gallery-grid').append(card);
  }
}
async function loadGallery(reset = true) {
  const request = ++gallery.request;
  if (reset) { gallery.items = []; gallery.cursor = null; gallery.pendingMatches = []; gallery.partial = false; $('gallery-grid').replaceChildren(); }
  const filter = $('gallery-type').value;
  $('gallery-status').textContent = 'Loading attachments…';
  $('gallery-more').disabled = true;
  if (reset) $('gallery-more').hidden = true;
  const path = gallery.root ? '/api/root/attachment-gallery' : '/api/attachment-gallery';
  let hasMore = gallery.pendingMatches.length > 0 || Boolean(gallery.cursor);
  try {
    let foundThisLoad = 0;
    if (filter !== 'all' && gallery.pendingMatches.length) {
      const visible = gallery.pendingMatches.splice(0, 48);
      gallery.items.push(...visible); foundThisLoad += visible.length; renderGalleryItems(visible);
      hasMore = gallery.pendingMatches.length > 0 || Boolean(gallery.cursor);
    }
    let firstFetch = reset || filter === 'all';
    while (firstFetch || (filter !== 'all' && foundThisLoad < 48 && hasMore)) {
      firstFetch = false;
      const query = new URLSearchParams();
      if (!gallery.root && $('gallery-project').value) query.set('project', $('gallery-project').value);
      if ($('gallery-search').value) query.set('q', $('gallery-search').value);
      if (gallery.cursor) query.set('before', gallery.cursor);
      query.set('limit', '48');
      const result = await fetchUsage(`${path}?${query}`, { cache: 'no-store' });
      if (request !== gallery.request || !$('attachment-gallery').open) return;
      if (!gallery.root) fillGalleryProjects(result.projects);
      const pageItems = Array.isArray(result.attachments) ? result.attachments : [];
      const matches = filter === 'all' ? pageItems : pageItems.filter(item => galleryTypeKey(item) === filter);
      const room = filter === 'all' ? matches.length : Math.max(0, 48 - foundThisLoad);
      const visible = matches.slice(0, room);
      if (filter !== 'all' && matches.length > visible.length) gallery.pendingMatches.push(...matches.slice(visible.length));
      gallery.items.push(...visible); foundThisLoad += visible.length;
      gallery.cursor = result.next_cursor; hasMore = Boolean(result.has_more) || gallery.pendingMatches.length > 0;
      gallery.partial ||= Boolean(result.partial);
      renderGalleryItems(visible);
      // Keep the existing 48-card page size, but continue scanning metadata-only
      // pages so a filter can find matches beyond the first unfiltered page.
    }
    $('gallery-more').hidden = !hasMore;
    if (filter === 'all') {
      $('gallery-status').textContent = gallery.items.length ? `${gallery.items.length} files${hasMore ? ' · More available' : ''}` : 'No attachments found.';
    } else if (!gallery.items.length) {
      const typeLabel = $('gallery-type').selectedOptions[0].textContent.toLowerCase();
      const search = $('gallery-search').value.trim();
      $('gallery-status').textContent = search ? `No attachments match ${typeLabel} and “${search}”.` : `No attachments match ${typeLabel}.`;
    } else {
      $('gallery-status').textContent = `${gallery.items.length} matching files${hasMore ? ' · More matches may be available' : ''}`;
    }
    if (gallery.partial) $('gallery-status').textContent += ' Some project attachments are unavailable. Refresh to retry.';
  } catch (error) {
    if (request !== gallery.request || !$('attachment-gallery').open) return;
    $('gallery-status').textContent = `${gallery.items.length ? `${gallery.items.length} matching files. ` : ''}Could not load attachments. ${error.message}. Refresh to retry.`;
  } finally {
    if (request === gallery.request) $('gallery-more').disabled = false;
  }
}
function fillGalleryProjects(projects) {
  $('gallery-project').replaceChildren(new Option('All projects', ''));
  for (const project of projects) $('gallery-project').append(new Option(project.name, project.id));
  if (state.root) $('gallery-project').append(new Option('Private Root conversation', '__root__'));
  if (![...$('gallery-project').options].some(option => option.value === gallery.selectedProject)) gallery.selectedProject = '';
  $('gallery-project').value = gallery.selectedProject;
}
function selectGalleryScope() {
  gallery.root = gallery.selectedProject === '__root__';
  $('gallery-title').textContent = gallery.root ? 'Root attachments' : 'Attachments';
  $('gallery-scope').textContent = gallery.root ? 'Files sent in your private Root conversation.' : 'Files sent in project rooms. Private Root attachments stay in Root.';
}
function positionGallery() {
  const left = Math.max(0,document.querySelector('.main').getBoundingClientRect().left);
  const right = Math.min(innerWidth,document.querySelector('.workspace').getBoundingClientRect().right);
  $('attachment-gallery').style.setProperty('--gallery-content-center',`${(left+right)/2}px`);
  $('attachment-gallery').style.setProperty('--gallery-content-width',`${right-left}px`);
}
window.addEventListener('resize', () => { if ($('attachment-gallery').open) positionGallery(); });
$('open-attachments').addEventListener('click', () => {
  fillGalleryProjects(state.projects); selectGalleryScope(); $('gallery-search').value = ''; $('gallery-type').value = 'all';
  closePanels(); positionGallery(); $('attachment-gallery').showModal(); loadGallery();
});
$('close-gallery').addEventListener('click', () => $('attachment-gallery').close());
$('attachment-gallery').addEventListener('close', () => {
  gallery.request++; clearTimeout(gallery.timer);
  for (const video of $('gallery-grid').querySelectorAll('video')) video.pause();
});
$('gallery-project').addEventListener('change', () => {
  clearTimeout(gallery.timer);
  gallery.selectedProject = $('gallery-project').value; selectGalleryScope(); loadGallery();
});
$('gallery-search').addEventListener('input', () => {
  gallery.request++; clearTimeout(gallery.timer); gallery.timer = setTimeout(() => loadGallery(), 250);
});
$('gallery-type').addEventListener('change', () => { clearTimeout(gallery.timer); loadGallery(); });
$('gallery-refresh').addEventListener('click', () => { clearTimeout(gallery.timer); loadGallery(); });
$('gallery-more').addEventListener('click', () => { clearTimeout(gallery.timer); loadGallery(false); });

// Aggregate navigation stays in this authenticated document; it does not
// turn visibility into recipient authority or discard the current draft.
const communication = {active:false, generation:0, observedAt:null, messages:new Map(), busy:false, updating:false, expanded:new Set(),
  hasOlder:false, hasNewer:false, historyLoaded:false, loadingHistory:false, historyDirection:null, historyError:null,
  scrollTop:0, newCount:0, pollSeen:new Set(), get items() { return [...this.messages.values()]; }};
function attachmentDownload(attachment, className, iconOnly = false) {
  const link=node('a',className,iconOnly ? '' : `Download ${attachment.name}`);
  const label = text => {
    if (iconOnly) { link.setAttribute('aria-label',text); link.title=text; }
    else (link.querySelector('.attachment-name') || link).textContent=text;
  };
  if (iconOnly) {
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
    svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('width','18');svg.setAttribute('height','18');svg.setAttribute('aria-hidden','true');
    const path=document.createElementNS(svg.namespaceURI,'path');
    path.setAttribute('d','M12 3v12m-5-5 5 5 5-5M5 16v5h14v-5');
    path.setAttribute('fill','none');path.setAttribute('stroke','currentColor');path.setAttribute('stroke-width','1.8');path.setAttribute('stroke-linecap','round');path.setAttribute('stroke-linejoin','round');
    svg.append(path);link.append(svg);label(`Download ${attachment.name}`);
  }
  link.href=attachment.url;link.download=attachment.name;
  link.addEventListener('click',async event=>{
    event.preventDefault();if(link.getAttribute('aria-disabled')==='true')return;
    const epoch=state.epoch;link.setAttribute('aria-disabled','true');
    try {
      const response=await fetch(attachment.url,{credentials:'same-origin',cache:'no-store'});
      if(!response.ok)throw new Error(`HTTP ${response.status}`);
      const bytes=await response.blob();if(epoch!==state.epoch)return;
      const url=URL.createObjectURL(bytes),save=node('a');save.href=url;save.download=attachment.name;
      document.body.append(save);save.click();save.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
      label(`Download ${attachment.name}`);
    }catch(error){if(epoch===state.epoch)label(`Retry download ${attachment.name}: ${error.message}`);}
    finally{link.removeAttribute('aria-disabled');}
  });return link;
}
function operatorAckButton(message, project = state.project, rootView = state.rootView) {
  if (!state.config?.operator_ack_enabled) return null;
  const action = message.operator_ack || (message.recipient_session === state.config.sender.id && message.sender_session !== state.config.sender.id
    ? {id:message.id,acked_at:message.acked_at} : null);
  if (!action || action.acked_at != null) return null;
  const button = node('button','message-ack','Acknowledge');
  button.title='Acknowledges only your received delivery; never another recipient or full-read consumption.';
  if(message.deliveries?.length)button.textContent='Acknowledge your copy';
  button.type='button';button.setAttribute('aria-label','Acknowledge message');
  button.addEventListener('click',async()=>{
    if(button.disabled)return;button.disabled=true;
    const epoch=state.epoch, generation=communication.generation;
    const endpoint = rootView ? '/api/root/messages/ack' : `/api/messages/ack?project=${encodeURIComponent(project)}`;
    try {
      await fetchJSON(endpoint,{method:'POST',headers:{'Content-Type':'application/json','X-Agent-Chat-CSRF':state.config.csrf_token},body:JSON.stringify({id:action.id})});
      if(epoch!==state.epoch || generation!==communication.generation)return;
      action.acked_at=Date.now()/1000;message.acked_at=action.acked_at;
      button.textContent='Acknowledged';button.setAttribute('aria-label','Acknowledged');
    } catch(error) {
      if(epoch!==state.epoch || generation!==communication.generation)return;
      button.disabled=false;button.textContent=`Retry acknowledgement: ${error.message}`;
    }
  });
  return button;
}
function rootAckScope() {
  if (!state.rootView || communication.active || !state.config?.operator_ack_enabled || !state.root?.home_session) return null;
  return [state.epoch,state.root.home_project,state.root.home_session,state.root.thread_id,state.config.sender.id].join(':');
}
function decisionScope() {
  if (!state.rootView || communication.active || !state.root?.home_session || !state.config?.sender?.id) return null;
  return `${state.root.home_project}:${state.root.home_session}:${state.config.sender.id}`;
}
function decisionStorageKey(kind, scope, id = '') {
  return `agent-chat.operator-decisions.v1:${kind}:${encodeURIComponent(scope)}${id ? `:${encodeURIComponent(id)}` : ''}`;
}
function decisionStorageGet(key) { try { return localStorage.getItem(key); } catch { return null; } }
function decisionStorageSet(key, value) { try { localStorage.setItem(key, value); return true; } catch { return false; } }
function renderDecisionNotifications(pending, total = null) {
  const panel = $('decision-notifications');
  if (!panel) return;
  const scope = decisionScope();
  panel.hidden = !scope;
  if (!scope) return;
  if (decisionAlerts.scope !== scope) decisionAlerts.scope = scope;
  decisionAlerts.pending = Array.isArray(pending) ? pending.filter(item => typeof item?.message_id === 'string') : [];
  decisionAlerts.total = Number.isInteger(total) && total >= decisionAlerts.pending.length ? total : decisionAlerts.pending.length;
  $('decision-count').textContent = String(decisionAlerts.total);
  $('decision-list-summary').textContent = decisionAlerts.total > decisionAlerts.pending.length
    ? `Showing the ${decisionAlerts.pending.length} most recent of ${decisionAlerts.total} pending decisions.`
    : decisionAlerts.total ? `${decisionAlerts.total} pending decision${decisionAlerts.total === 1 ? '' : 's'}.` : 'No pending decisions.';
  const list = $('decision-list'); list.replaceChildren();
  for (const item of decisionAlerts.pending) {
    const row = node('li', 'decision-item');
    row.dataset.messageId = item.message_id;
    const status = item.reply_received ? 'Reply received; author outcome not recorded'
      : item.clarification_recorded ? 'Clarification recorded; final answer pending' : 'Waiting for your reply';
    row.append(node('span', 'decision-item-status', status));
    const open = node('button', 'quiet-button decision-open', 'Open message');
    open.type = 'button';
    open.addEventListener('click', () => { panel.open = false; jumpToMessage(item.message_id); });
    row.append(open); list.append(row);
  }
  const optIn = $('decision-notification-opt-in');
  const enabled = decisionStorageGet(decisionStorageKey('enabled', scope)) === '1';
  const available = typeof window.Notification === 'function';
  if (!available) {
    optIn.textContent = 'Desktop alerts unavailable'; optIn.disabled = true;
    $('decision-status').textContent = 'Browser notifications are unavailable. The in-app decision list remains available.';
    return;
  }
  optIn.disabled = false;
  optIn.textContent = enabled ? 'Disable desktop alerts' : 'Enable desktop alerts';
  optIn.setAttribute('aria-pressed', String(enabled));
  if (window.Notification.permission === 'denied') {
    $('decision-status').textContent = 'Browser notifications are blocked. The in-app decision list remains available.';
  } else if (enabled && window.Notification.permission === 'granted') {
    $('decision-status').textContent = 'Desktop alerts are enabled for new decisions while this page is open.';
  } else {
    $('decision-status').textContent = 'In-app decision list is available. Desktop alerts are optional.';
  }
  const baselineKey = decisionStorageKey('baseline', scope);
  const hasBaseline = decisionStorageGet(baselineKey) === '1';
  for (const item of decisionAlerts.pending) {
    const seenKey = decisionStorageKey('seen', scope, item.message_id);
    if (decisionStorageGet(seenKey) !== null) continue;
    if (!decisionStorageSet(seenKey, '1')) continue;
    if (!hasBaseline || !enabled || window.Notification.permission !== 'granted') continue;
    try {
      const notification = new window.Notification('Decision requested', {
        body: 'A message needs your decision.', tag: `agent-chat-decision-${item.message_id}`,
      });
      notification.onclick = () => { window.focus(); jumpToMessage(item.message_id); };
    } catch {
      $('decision-status').textContent = 'A desktop alert could not be shown. The in-app decision list remains available.';
    }
  }
  if (!hasBaseline) decisionStorageSet(baselineKey, '1');
}
async function toggleDecisionNotifications() {
  const scope = decisionScope(); if (!scope) return;
  const enabledKey = decisionStorageKey('enabled', scope);
  if (decisionStorageGet(enabledKey) === '1') {
    try { localStorage.removeItem(enabledKey); } catch {}
    $('decision-status').textContent = 'Desktop alerts are off. The in-app decision list remains available.';
    renderDecisionNotifications(decisionAlerts.pending, decisionAlerts.total); return;
  }
  if (typeof window.Notification !== 'function') {
    $('decision-status').textContent = 'Browser notifications are unavailable. The in-app decision list remains available.'; return;
  }
  let permission = window.Notification.permission;
  if (permission === 'default') {
    try { permission = await window.Notification.requestPermission(); }
    catch { permission = 'denied'; }
  }
  if (permission !== 'granted') {
    $('decision-status').textContent = 'Browser notifications were not enabled. The in-app decision list remains available.';
    return;
  }
  if (!decisionStorageSet(enabledKey, '1') || !decisionStorageSet(decisionStorageKey('baseline', scope), '1')) {
    $('decision-status').textContent = 'Desktop alert preferences could not be saved. The in-app decision list remains available.';
    return;
  }
  for (const item of decisionAlerts.pending) {
    if (!decisionStorageSet(decisionStorageKey('seen', scope, item.message_id), '1')) {
      try { localStorage.removeItem(enabledKey); } catch {}
      $('decision-status').textContent = 'Desktop alert deduplication could not be saved. The in-app decision list remains available.';
      return;
    }
  }
  renderDecisionNotifications(decisionAlerts.pending, decisionAlerts.total);
}
if ($('decision-notification-opt-in')) $('decision-notification-opt-in').addEventListener('click', toggleDecisionNotifications);
function renderRootAck() {
  const scope = rootAckScope(), button = $('root-ack-all'), status = $('root-ack-status');
  if (rootAck.scope !== scope) {
    rootAck.scope = scope; rootAck.pending = null; rootAck.loading = false; rootAck.error = ''; rootAck.request++;
  }
  button.hidden = status.hidden = !scope;
  button.disabled = rootAck.running || state.busy || state.sending || rootAck.pending === 0 || (rootAck.pending == null && (rootAck.loading || !rootAck.error));
  button.textContent = rootAck.running ? `Acknowledging ${rootAck.done}/${rootAck.total}…`
    : rootAck.error ? `Retry Ack all${rootAck.pending == null ? '' : ` (${rootAck.pending})`}`
      : rootAck.pending == null ? 'Checking acknowledgements…' : `Ack all${rootAck.pending ? ` (${rootAck.pending})` : ''}`;
  button.title = rootAck.error || (rootAck.pending === 0 ? 'No pending Root messages addressed to you' : 'Acknowledge every pending message addressed to you in this Root conversation, including older history');
  status.textContent = rootAck.error || (rootAck.running ? button.textContent : rootAck.pending == null ? '' : `${rootAck.pending} pending Root messages addressed to you`);
}
async function rootAckPlan(scope) {
  const plan = await fetchJSON('/api/root/messages/ack-plan',{}, {projectScoped:false});
  if (scope !== rootAckScope()) throw new DOMException('Root conversation changed','AbortError');
  if (plan.project_id !== state.root.home_project || plan.recipient_session !== state.config.sender.id) throw new Error('Root acknowledgement context changed. Retry.');
  return plan;
}
async function refreshRootAckPlan() {
  renderRootAck();
  const scope = rootAckScope();
  if (!scope || rootAck.loading || rootAck.running) return;
  const request = ++rootAck.request; rootAck.loading = true; renderRootAck();
  try {
    const plan = await rootAckPlan(scope);
    if (request !== rootAck.request) return;
    rootAck.pending = plan.count;
    if (!plan.count) rootAck.error = '';
  } catch(error) {
    if (request === rootAck.request && error.name !== 'AbortError') rootAck.error = rootAck.error || error.message;
  } finally {
    if (request === rootAck.request) { rootAck.loading = false; renderRootAck(); }
  }
}
$('root-ack-all').addEventListener('click',async () => {
  const scope = rootAckScope();
  if (!scope || rootAck.running || state.busy || state.sending) return;
  rootAck.running = true; rootAck.loading = false; rootAck.request++;
  rootAck.done = rootAck.total = 0; rootAck.error = ''; renderRootAck();
  try {
    const plan = await rootAckPlan(scope); rootAck.total = plan.count; rootAck.pending = plan.count; renderRootAck();
    for (const id of plan.ids) {
      if (scope !== rootAckScope()) throw new DOMException('Root conversation changed','AbortError');
      await fetchJSON('/api/root/messages/ack',{method:'POST',headers:{'Content-Type':'application/json','X-Agent-Chat-CSRF':state.config.csrf_token},body:JSON.stringify({id})},{projectScoped:false});
      rootAck.done++; rootAck.pending = Math.max(0,rootAck.total-rootAck.done);
      for (const message of [...state.messages.values(),...state.originals.values(),...(state.snapshot?.messages || [])]) {
        if (message.id === id) {
          message.acked_at = Date.now()/1000;
          if (message.operator_ack) message.operator_ack.acked_at = message.acked_at;
        }
      }
      renderRootAck();
    }
  } catch(error) {
    if (scope === rootAckScope() && error.name !== 'AbortError') rootAck.error = `${rootAck.done} acknowledged. ${error.message}`;
  } finally {
    rootAck.running = false;
    if (scope === rootAckScope()) { rootAck.pending = null; renderMessages(false); }
    await refreshRootAckPlan();
  }
});

function communicationURL() {
  const url = new URL(location.href); url.pathname = '/communications'; return url;
}
function showCommunication(push = true) {
  if (state.sending || state.busy) return;
  communication.active = true; communication.generation++;
  $('communication-panel').hidden = false;
  document.querySelector('.main').classList.add('show-communication');
  document.body.classList.add('communication-readonly');
  document.body.classList.remove('root-screen');
  $('composer').inert = true;
  for (const dialog of document.querySelectorAll('dialog[open]')) dialog.close();
  closePanels(); renderAgents(); communicationContext(); renderRootAck();
  document.title = 'All communication · Agent chat';
  $('all-communication-view')?.classList.add('selected');
  $('all-communication-view')?.setAttribute('aria-current', 'page');
  $('root-conversation')?.classList.remove('selected');
  for (const button of $('project-list').children) {
    button.classList.remove('selected'); button.setAttribute('aria-current', 'false');
  }
  if (push && location.pathname !== '/communications') history.pushState({communication:true}, '', communicationURL());
  loadCommunication('reset');
}
function hideCommunication() {
  communication.active = false; communication.generation++; communication.busy = false; communication.updating = false;
  $('communication-panel').hidden = true;
  document.querySelector('.main').classList.remove('show-communication');
  document.body.classList.remove('communication-readonly');
  $('composer').inert = false;
  $('all-communication-view')?.classList.remove('selected');
  $('all-communication-view')?.removeAttribute('aria-current');
  if (state.config) { renderProjects(state.config); renderAgents(); }
}
function communicationContext() {
  const selected = $('communication-project').value;
  const project = state.projects.find(item => item.id === selected);
  $('communication-scope').textContent = project?.name || 'All projects';
}
async function communicationFullBody(message, generation) {
  if (!message.body_truncated) return message.body;
  const result = await fetchJSON('/api/communications/message?' + new URLSearchParams({project:message.project_id, id:message.id}), {}, {projectScoped:false});
  if (generation !== communication.generation || !communication.active) return null;
  message.body = result.message.body; message.body_truncated = false;
  return message.body;
}
function renderCommunication() {
  const fragment = document.createDocumentFragment();
  let previousDate = '';
  for (const message of communication.items) {
    const key = message.project_id + ':' + message.id;
    const date = new Date(message.created_at * 1000).toDateString();
    if (date !== previousDate) fragment.append(node('div', 'date-divider', dateLabel(message.created_at)));
    previousDate = date;
    const own = message.sender_role === 'human_operator' && !message.service_notice;
    const sender = message.service_notice ? 'Service notice' : own ? 'You' : message.sender_agent || message.sender_session;
    const article = node('article', 'message communication-item' + (own ? ' own' : ''));
    article.id = 'communication-' + key;
    article.dataset.messageId = message.id; article.dataset.projectId = message.project_id;
    article.append(avatar(sender));
    const content = node('div', 'message-content'), meta = node('div', 'message-meta');
    const time = node('time', 'message-time', timeFormat.format(new Date(message.created_at * 1000)));
    time.dateTime = new Date(message.created_at * 1000).toISOString(); time.title = new Date(message.created_at * 1000).toLocaleString();
    const operator = message.operator_ack?.recipient_session;
    const recipients = messageDeliveries(message).map(item => operator && item.recipient_session === operator ? 'You' : item.recipient_agent || item.recipient_session).join(', ');
    const projectName = typeof message.project_name === 'string' ? message.project_name.trim() : '';
    meta.append(node('span', 'sender-button', sender),
      node('span', 'recipient', projectName && recipients ? `${recipients} (${projectName})` : recipients));
    meta.append(time);
    const card = node('div', 'message-card');
    const timeline = deliveryTimeline(message.delivery_receipt, communication.observedAt ?? message.created_at);
    if (timeline) card.append(timeline);
    if (message.reply_preview) {
      const quote = node('div', 'reply-quote');
      quote.append(node('strong', '', `↩ ${message.reply_preview.sender_agent || 'Original message'}`), node('span', '', message.reply_preview.body));
      card.append(quote);
    }
    const publicationPlan = message.body.startsWith('Publication plan v1\n');
    const long = message.body_truncated || (!publicationPlan && (message.body.length > 900 || message.body.split('\n').length > 8));
    const body = node('div', 'message-body markdown');
    if (publicationPlan) body.style.whiteSpace = 'pre-wrap';
    const renderBody = () => {
      if (publicationPlan) body.replaceChildren(document.createTextNode((message.body_truncated ? 'Incomplete plan — load the full message before deciding.\n' : '') + message.body));
      else body.replaceChildren((!message.body_truncated && renderObservationReport(message.body)) || CoordMarkdown.render(message.body));
      body.classList.toggle('collapsed', long && !communication.expanded.has(key));
    };
    renderBody(); card.append(body);
    if (long) {
      const full = node('button', 'expand-message', communication.expanded.has(key) ? 'Show less' : 'Read full message');
      full.type = 'button'; full.setAttribute('aria-expanded', String(communication.expanded.has(key)));
      full.addEventListener('click', async () => {
        const generation = communication.generation; full.disabled = true;
        try {
          if (await communicationFullBody(message, generation) === null) return;
          if (communication.expanded.has(key)) communication.expanded.delete(key); else communication.expanded.add(key);
          const anchor = communicationAnchor();
          renderCommunication(); restoreFeedAnchor($('communication-feed'), $('communication-messages'), anchor);
          communication.scrollTop = $('communication-feed').scrollTop;
        } catch (error) {
          if (generation === communication.generation) full.textContent = 'Retry full message: ' + error.message;
        } finally { full.disabled = false; }
      });
      card.append(full);
    }
    if (message.attachments?.length) {
      const files = node('div', 'message-attachments');
      for (const attachment of message.attachments) {
        const item = node('div', 'communication-attachment');
        const imageAttachment = IMAGE_MIMES.has(attachment.mime);
        const audioAttachment = AUDIO_MIMES.has(attachment.mime);
        if (imageAttachment) {
          const opener = node('button', 'communication-image-viewer');
          opener.type = 'button';
          opener.dataset.attachmentId = attachment.id;
          opener.setAttribute('aria-label', `View ${attachment.name} in attachment viewer`);
          opener.setAttribute('aria-haspopup', 'dialog');
          const preview = node('img', 'communication-attachment-preview');
          preview.src = attachment.url;
          preview.alt = attachment.name;
          preview.loading = 'lazy';
          preview.addEventListener('error', () => {
            if (preview.isConnected) preview.replaceWith(node('span', 'attachment-document-type', attachmentType(attachment)));
          }, {once: true});
          opener.append(preview);
          opener.addEventListener('click', () => {
            const viewerMessage = {attachments: message.attachments.map(item => ({...item, galleryURL: item.url}))};
            openMediaViewer(viewerMessage, attachment, opener);
          });
          item.append(opener);
        } else if (audioAttachment) {
          const player = node('audio', 'communication-attachment-audio');
          player.controls = true;
          player.preload = 'metadata';
          player.src = attachment.url;
          player.setAttribute('aria-label', `Play ${attachment.name}`);
          player.addEventListener('error', () => {
            if (player.isConnected) player.replaceWith(node('span', 'attachment-document-type', attachmentType(attachment)));
          }, {once: true});
          item.append(player);
        }
        const download = attachmentDownload(attachment, 'message-attachment');
        download.setAttribute('aria-label', `Download ${attachment.name}`);
        download.replaceChildren(...(imageAttachment || audioAttachment
          ? [node('span', 'attachment-name', `Download ${attachment.name}`)]
          : [node('span', 'attachment-document-type', attachmentType(attachment)),
            node('span', 'attachment-name', `Download ${attachment.name}`)]));
        item.append(download);
        files.append(item);
      }
      card.append(files);
    }
    const bottom = node('div', 'message-bottom');
    const acked = message.operator_ack?.acked_at ?? message.acked_at;
    bottom.append(node('span', acked != null ? 'acknowledged' : 'pending', acked != null ? '✓ Acknowledged' : '· Awaiting acknowledgement'));
    const actions = node('span', 'message-actions'), copy = node('button', 'copy-message', 'Copy');
    copy.type = 'button'; copy.setAttribute('aria-label', `Copy message from ${sender}`);
    copy.addEventListener('click', async () => {
      const generation = communication.generation; copy.disabled = true;
      try {
        const text = await communicationFullBody(message, generation);
        if (text === null) return;
        await copyMessageText(text);
        if (generation === communication.generation) copy.textContent = 'Copied!';
      } catch { if (generation === communication.generation) copy.textContent = 'Copy failed'; }
      finally { copy.disabled = false; }
    });
    actions.append(copy, node('span', 'message-id', message.id.slice(-8)));
    bottom.append(actions); content.append(meta, card, bottom); article.append(content); fragment.append(article);
  }
  $('communication-messages').replaceChildren(fragment);
  $('communication-empty').hidden = communication.items.length > 0;
}
const communicationKey = item => item.project_id + ':' + item.id;
function compareCommunication(a,b) {
  const text = (left,right) => left < right ? -1 : left > right ? 1 : 0;
  return a.created_at-b.created_at || text(a.project_id,b.project_id) || a.seq-b.seq || text(a.id,b.id);
}
function mergeCommunication(previous,item) {
  if (!previous) return item;
  const full = !previous.body_truncated && item.body_truncated ? previous.body : null;
  Object.assign(previous,item);
  if (full !== null) { previous.body = full; previous.body_truncated = false; }
  return previous;
}
function communicationAnchor() {
  return feedAnchor($('communication-feed'),$('communication-messages'),() => true);
}
function communicationHistoryStatus() {
  renderFeedHistory(communication,'communication-');
  if (!$('communication-status').textContent) $('communication-status').textContent = `${communication.messages.size} ${communication.messages.size === 1 ? 'message' : 'messages'}`;
  if (communication.partial) $('communication-status').textContent += ' · Some projects unavailable';
  if (communication.pollError) $('communication-status').textContent = 'Unavailable: ' + communication.pollError;
}
async function loadCommunication(mode = 'reset') {
  if (!communication.active) return;
  if (mode === 'update' ? communication.updating || (communication.loadingHistory && communication.historyDirection === 'reset') : communication.busy && mode !== 'reset') return;
  if ((mode === 'older' && !communication.hasOlder) || (mode === 'newer' && !communication.hasNewer)) return;
  if (mode === 'reset') { communication.generation++; communication.updating = false; }
  const generation = communication.generation, feed = $('communication-feed');
  const previous = JSON.stringify(communication.items);
  const params = new URLSearchParams({limit:String(MESSAGE_LIMIT)});
  if ($('communication-project').value) params.set('project',$('communication-project').value);
  if ($('communication-text').value) params.set('q',$('communication-text').value);
  if (mode === 'older') params.set('before',communication.items[0]?.history_cursor || communication.cursor);
  if (mode === 'newer') params.set('after',communication.items.at(-1)?.history_cursor);
  if (mode === 'update') communication.updating = true; else communication.busy = true;
  if (mode !== 'update') { communication.loadingHistory = true; communication.historyDirection = mode; communication.historyError = null; }
  communicationHistoryStatus();
  try {
    const result = await fetchJSON('/api/communications?' + params,{}, {projectScoped:false});
    if (generation !== communication.generation || !communication.active) return;
    communication.observedAt = result.observed_at;
    const anchor = communicationAnchor();
    const following = !communication.hasNewer && feed.scrollHeight-feed.scrollTop-feed.clientHeight < 90;
    if (mode === 'reset') {
      communication.messages.clear(); communication.expanded.clear();
      communication.historyLoaded = false; communication.hasNewer = false; communication.newCount = 0;
    }
    if (mode === 'reset' || mode === 'update') {
      const latest = communication.items.at(-1);
      if (mode === 'update') communication.newCount += result.messages.filter(item =>
        !communication.pollSeen.has(communicationKey(item)) && (!latest || compareCommunication(item,latest)>0)).length;
      communication.pollSeen = new Set(result.messages.map(communicationKey));
      applyLiveFeed(communication,result.messages,{initial:mode === 'reset',following,hasOlder:result.has_more,
        key:communicationKey,compare:compareCommunication,merge:mergeCommunication});
      if (mode === 'reset') communication.cursor = result.next_cursor;
    } else {
      const messages = new Map(communication.messages);
      for (const item of result.messages) messages.set(communicationKey(item),mergeCommunication(messages.get(communicationKey(item)),item));
      const window = historyWindow(messages,compareCommunication,mode === 'newer');
      if (anchor.id && !window.rows.some(item => 'communication-' + communicationKey(item) === anchor.id)) return;
      communication.messages = new Map(window.rows.map(item => [communicationKey(item),item]));
      communication.historyLoaded = true;
      if (mode === 'newer') { communication.hasNewer = result.has_more; if (window.trimmed) communication.hasOlder = true; }
      else { communication.hasOlder = result.has_more; communication.cursor = result.next_cursor; if (window.trimmed) communication.hasNewer = true; }
    }
    for (const key of communication.expanded) if (!communication.messages.has(key)) communication.expanded.delete(key);
    if ((mode === 'reset' || following || mode === 'newer') && !communication.hasNewer) communication.newCount = 0;
    communication.partial = result.partial; communication.pollError = null;
    const catalog = JSON.stringify(result.projects);
    if (communication.catalog !== catalog) {
      communication.catalog = catalog; const selected = $('communication-project').value;
      $('communication-project').replaceChildren(new Option('All projects',''),...result.projects.map(item => new Option(item.name,item.id)));
      $('communication-project').value = selected;
    }
    if (mode !== 'update' || previous !== JSON.stringify(communication.items)) {
      renderCommunication();
      if (mode === 'reset' || (mode === 'update' && following && !communication.hasNewer)) feed.scrollTop = feed.scrollHeight;
      else restoreFeedAnchor(feed,$('communication-messages'),anchor);
      communication.scrollTop = feed.scrollTop;
    }
    communicationContext();
  } catch (error) {
    if (generation === communication.generation && error.name !== 'AbortError') {
      if (mode === 'older' || mode === 'newer') communication.historyError = mode;
      else communication.pollError = error.message;
    }
  } finally {
    if (generation === communication.generation) {
      if (mode === 'update') communication.updating = false;
      else { communication.busy = false; communication.loadingHistory = false; }
      communicationHistoryStatus();
    }
  }
}
$('all-communication-view')?.addEventListener('click', event => { event.preventDefault(); showCommunication(); });
$('communication-filters').addEventListener('submit', event => { event.preventDefault(); loadCommunication('reset'); });
$('communication-project').addEventListener('change', () => loadCommunication('reset'));
bindFeedEdges($('communication-feed'), communication, newer => loadCommunication(newer ? 'newer' : 'older'));
$('communication-show-latest').addEventListener('click', () => loadCommunication('reset'));
$('communication-new-messages').addEventListener('click', () => loadCommunication('reset'));
$('all-conversations').addEventListener('click', () => {
  if (communication.active) { hideCommunication(); const url = new URL(location.href); url.pathname = state.rootView ? '/root' : '/'; history.pushState(null, '', url); }
});
window.addEventListener('popstate', () => { if (location.pathname === '/communications') showCommunication(false); else hideCommunication(); });

$('project-activity-filter').addEventListener('change', (event) => {
  state.activeProjectsOnly = event.target.checked;
  state.rootOverviewKey = null;
  renderProjects({projects: state.projects, root_overview: state.rootOverviewData,
                  root_orchestrator: state.root, project: state.config?.project});
});
