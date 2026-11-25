/** Compact project overview; messages remain scoped to the selected room. */
export type ActivityProject = {id:string; name:string; po_id:string; goal_objective?:string; goal_status?:string};
export type ActivityAgent = {id:string; name:string; role:string; project_id:string; status:string; parent_id?:string};
export type ActivityJob = {id:string; agent_id:string; project_id:string; task_id:string; status:string; diagnostic?:string; source_id?:string; kind?:string};
export type ActivityMessage = {id:string; room_id:string; sender:string; recipient:string; text:string; at:string; project_id?:string};
export type ActivityTask = {id:string; project_id:string; title:string; status:string; owner_id:string; retry_handoff_eligible?:boolean; continue_po_eligible?:boolean};
export type ActivityState = {projects:ActivityProject[]; agents:ActivityAgent[]; jobs:ActivityJob[]; messages:ActivityMessage[]; tasks:ActivityTask[]};
const escape = (value:unknown) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]!));
const short = (value:string, limit:number) => Array.from(value).slice(0,limit).join('') + (Array.from(value).length > limit ? '…' : '');
const time = (at:string) => {const d=new Date(at);return Number.isNaN(d.getTime())?'':d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});};

export function renderProjectActivity(state:ActivityState, projectId:string, roomId:string, open:Set<string>, statusFor:(id:string)=>{label:string;active:boolean}, diagnostics:(task:ActivityTask)=>string, handoff:(task:ActivityTask)=>string):string {
 const project=state.projects.find(p=>p.id===projectId);
 if(!project)return '';
 const agents=state.agents.filter(a=>a.project_id===projectId);
 const root=state.agents.find(a=>a.id==='root');
 const members=root?[root,...agents.filter(a=>a.id!==root.id)]:agents;
 const who=(id:string)=>id==='operator'?'You':id==='service'?'Agent Chat':members.find(a=>a.id===id)?.name || id;
 const tasks=state.tasks.filter(t=>t.project_id===projectId).slice(-5).reverse();
 const jobs=state.jobs.filter(j=>j.project_id===projectId);
 const isOpen=(key:string)=>open.has(key)?'open':'';
 const team=members.map(a=>{const status=statusFor(a.id);return `<li><span class="activity-dot ${status.active?'busy':''}" aria-hidden="true"></span><strong>${escape(a.name)}</strong><small>${escape(a.role)} · ${escape(status.label)}</small></li>`;}).join('');
 const work=tasks.map(t=>{const attempts=jobs.filter(j=>j.task_id===t.id);const latest=attempts.at(-1);const status=latest && ['pending','submitted','running'].includes(latest.status)?({pending:'Queued',submitted:'Starting',running:'Working'} as Record<string,string>)[latest.status]:latest?.status==='uncertain'?'Needs attention':latest?.status==='completed' && latest.diagnostic?.trim()?'Failed':t.status;return `<li><strong>${escape(t.title)}</strong><small>${escape(status)} · ${escape(who(t.owner_id))}</small>${diagnostics(t)}${handoff(t)}</li>`;}).join('');
 const messages=state.messages.filter(m=>m.room_id===roomId).slice(-8).reverse();
 const exchanges=messages.map(m=>`<li><small>${escape(who(m.sender))} · ${escape(time(m.at))}</small><p>${escape(short(m.text,180))}</p></li>`).join('');
 const goal=project.goal_objective?`<details data-diagnostic="panel:goal:${escape(projectId)}" ${isOpen('panel:goal:'+projectId)}><summary>PO goal <small>${escape(project.goal_status || '')}</small></summary><p class="pa-goal">${escape(project.goal_objective)}</p></details>`:'';
 return `<section class="project-activity" aria-label="${escape(project.name)} activity"><h2>${escape(project.name)}</h2><h3>Team</h3>${team?`<ul class="pa-team">${team}</ul>`:'<p class="pa-empty">No team members available yet.</p>'}${goal}<details data-diagnostic="panel:work:${escape(projectId)}" ${isOpen('panel:work:'+projectId)}><summary>Work <small>${tasks.length}</small></summary>${work?`<ul class="pa-work">${work}</ul>`:'<p class="pa-empty">No recent work to show yet.</p>'}</details><details data-diagnostic="panel:activity:${escape(projectId+':'+roomId)}" ${isOpen('panel:activity:'+projectId+':'+roomId)}><summary>Activity</summary>${exchanges?`<ol class="pa-exchanges">${exchanges}</ol>`:'<p class="pa-empty">No messages in this conversation yet.</p>'}</details></section>`;
}
