/** Read-only project topology and activity, rendered from the caller's latest state poll. */
export type ActivityProject = {id:string; name:string; po_id:string};
export type ActivityAgent = {id:string; name:string; role:string; project_id:string; status:string; parent_id?:string};
export type ActivityJob = {id:string; agent_id:string; project_id:string; task_id:string; status:string; source_id?:string; kind?:string};
export type ActivityMessage = {id:string; room_id:string; sender:string; recipient:string; text:string; at:string; project_id?:string};
export type ActivityTask = {id:string; project_id:string; title:string; status:string; owner_id:string};
export type ActivityState = {projects:ActivityProject[]; agents:ActivityAgent[]; jobs:ActivityJob[]; messages:ActivityMessage[]; tasks:ActivityTask[]};
const escape = (value:unknown) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]!));
const short = (value:string, limit:number) => Array.from(value).slice(0,limit).join('') + (Array.from(value).length > limit ? '…' : '');
const time = (value:string) => {const d = new Date(value); return Number.isNaN(d.getTime()) ? '' : d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});};

/** Returns escaped HTML; caller inserts it into its own secondary panel and re-renders on state updates. */
export function renderProjectActivity(state:ActivityState, projectId:string, roomId:string):string {
 const project = state.projects.find(p => p.id === projectId);
 if (!project) return '<section class="project-activity" aria-label="Project activity"><p class="pa-empty">Select a project to view its activity.</p></section>';
 const agents = state.agents.filter(a => a.project_id === projectId);
 const root = state.agents.find(a => a.id === 'root');
 const nodes = root ? [root, ...agents.filter(a=>a.id!==root.id)] : agents;
 const ids = new Set(nodes.map(a => a.id));
 const byId = new Map(nodes.map(a => [a.id,a]));
 const jobs = state.jobs.filter(j => j.project_id === projectId && ids.has(j.agent_id));
 const tasks = state.tasks.filter(t => t.project_id === projectId);
 const taskById = new Map(tasks.map(t=>[t.id,t]));
 const active = new Set(['pending','submitted','running','uncertain']);
 const now = Date.now();
 const recentEdge = (parent:string, child:string) => state.messages.some(m=>{
  const at = Date.parse(m.at);
  return m.project_id===projectId && Number.isFinite(at) && at<=now && now-at<=60_000 &&
   ((m.sender===parent && m.recipient===child)||(m.sender===child && m.recipient===parent));
 });
 const who = (id:string) => id === 'operator' ? 'You' : byId.get(id)?.name || (id === 'root' ? 'Root' : id);
 const node = (agent:ActivityAgent, visited:Set<string>):string => {
  if (visited.has(agent.id)) return '';
  visited.add(agent.id);
  const current = jobs.filter(j=>j.agent_id===agent.id && active.has(j.status));
  const working = agent.status!=='stopped' && current.some(j=>j.status==='running');
  const status=agent.status==='stopped'?'Stopped':working?'Working':(current.some(j=>j.status==='uncertain') || tasks.some(t=>t.owner_id===agent.id && t.status==='blocked'))?'Needs attention':current.some(j=>j.status==='submitted')?'Starting':current.some(j=>j.status==='pending')?'Queued':'Idle';
  const work = current.map(j=>{const task=taskById.get(j.task_id); return `<li><span>${escape(j.status)}</span>${task ? ` · ${escape(short(task.title,100))}` : j.kind ? ` · ${escape(j.kind)}` : ''}</li>`;}).join('') + tasks.filter(t=>t.owner_id===agent.id && t.status==='blocked' && !current.some(j=>j.task_id===t.id)).map(t=>`<li><span>blocked</span> · ${escape(short(t.title,100))}</li>`).join('');
  const children = nodes.filter(a=>a.id!==agent.id && a.parent_id===agent.id && !visited.has(a.id));
  const recent = agent.parent_id && ids.has(agent.parent_id) && recentEdge(agent.parent_id,agent.id);
  return `<li class="pa-branch${recent?' pa-recent':''}"><div class="pa-node${working?' pa-working':''}"><span class="pa-avatar" aria-hidden="true">${escape((agent.name || agent.role || '?').slice(0,1).toUpperCase())}</span><div><strong>${escape(agent.name || agent.id)}</strong><small>${escape(agent.role)} · <span class="activity-dot ${working?'busy':''}" aria-hidden="true"></span> ${escape(status)}</small>${work?`<ul class="pa-work" aria-label="Current work">${work}</ul>`:''}</div></div>${children.length?`<ul class="pa-children">${children.map(child=>node(child,visited)).join('')}</ul>`:''}</li>`;
 };
 const visited = new Set<string>();
 const roots = nodes.filter(a=>!a.parent_id || !ids.has(a.parent_id) || a.parent_id===a.id);
 let tree = roots.map(a=>node(a,visited)).join('');
 // Malformed cycles are shown without inventing an edge or omitting an agent.
 tree += nodes.filter(a=>!visited.has(a.id)).map(a=>node(a,visited)).join('');
 const messages = state.messages.filter(m=>m.room_id===roomId).slice(-8).reverse();
 const exchanges = messages.map(m=>`<li><div class="pa-exchange-meta"><strong>${escape(who(m.sender))}</strong><span aria-hidden="true">→</span><strong>${escape(who(m.recipient))}</strong><time>${escape(time(m.at))}</time></div><p>${escape(short(m.text,180))}</p></li>`).join('');
 return `<section class="project-activity" aria-label="${escape(project.name)} activity"><div class="pa-heading"><span>PROJECT ACTIVITY</span><h2>${escape(project.name)}</h2></div><h3>Team</h3>${tree?`<ul class="pa-tree">${tree}</ul>`:'<p class="pa-empty">No team members available yet.</p>'}<h3>Recent exchanges</h3>${exchanges?`<ol class="pa-exchanges">${exchanges}</ol>`:'<p class="pa-empty">No project messages yet.</p>'}</section>`;
}
