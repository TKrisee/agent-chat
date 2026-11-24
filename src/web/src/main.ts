import './style.css';
import {ReadReceipts} from './read-receipts';
import {previewAttachment} from './attachment-preview';
import {renderProjectActivity} from './project-activity';
import './project-activity.css';
import {renderMessageMarkdown} from './message-markdown';
type Attachment = {id:string; name:string; media_type:string; url:string};
type Message = {id:string; sender:string; recipient:string; role:string; text:string; at:string; task_id?:string; project_id?:string; attachments?:Attachment[]; reply_to?:string; operator_read_at?:string; mention_ids?:string[]};
type Project = {id:string; name:string; status:string; po_id:string};
type Task = {id:string; project_id:string; title:string; status:string; owner_id:string};
type Job = {id:string; agent_id:string; project_id:string; task_id:string; status:string; diagnostic?:string; kind?:string; source_id?:string};
type Agent = {id:string; name:string; role:string; project_id:string; status:string; model:string; parent_id?:string};
type State = {messages:Message[]; projects:Project[]; tasks:Task[]; agents:Agent[]; jobs:Job[]};
const app = document.querySelector<HTMLDivElement>('#app')!;
let state:State = {messages:[],projects:[],tasks:[],agents:[],jobs:[]};
type UpdateInfo={available:boolean;current:string;version:string;state:string;error:string};
let updateInfo:UpdateInfo|null=null;let updateBusy=false;let updateRestarting=false;
async function updateRequest(action=''):Promise<void>{
 if(updateBusy)return;updateBusy=true;
 try{const r=await fetch('/api/update'+(action?'/'+action:''),{method:action?'POST':'GET',cache:'no-store'});if(r.status===404){updateInfo=null;return;}const available=r.headers.get('X-AgentChat-Update-Available')==='true';const status=r.headers.get('X-AgentChat-Update-State')||'idle';const detail=await r.text();if(!r.ok)throw new Error(detail||`Update request failed (${r.status}).`);updateInfo={available,current:r.headers.get('X-AgentChat-Update-Current')||'',version:r.headers.get('X-AgentChat-Update-Version')||'',state:status,error:status==='error'?detail:''};updateRestarting=status==='restarting';}
 catch(e){if(updateRestarting)updateInfo={available:true,current:'',version:'',state:'restarting',error:''};else if(updateInfo)updateInfo={...updateInfo,state:'error',error:e instanceof Error?e.message:'Update unavailable.'};}
 finally{updateBusy=false;renderUpdate();}
}
function renderUpdate():void{const el=app.querySelector<HTMLElement>('#update-panel');if(!el)return;const u=updateInfo;if(!u?.available){el.hidden=true;return;}el.hidden=false;const action=u.state==='available'?'download':u.state==='ready'?'apply':u.state==='idle'||u.state==='current'||u.state==='error'?'check':'';el.innerHTML=`<strong>App updates</strong><small>${esc(u.state==='restarting'?'Restarting…':u.state==='downloading'?'Downloading…':u.state==='checking'?'Checking…':u.state==='ready'?`Version ${u.version} is ready`:u.state==='available'?`Version ${u.version} available`:u.state==='current'?'Up to date':u.state==='error'?(u.error||'Update failed'):`Current: ${u.current}`)}</small>${action?`<button type="button" data-update-action="${action}">${action==='download'?'Download':action==='apply'?'Restart to update':'Check for updates'}</button>`:''}`;el.querySelector<HTMLButtonElement>('[data-update-action]')?.addEventListener('click',e=>{const action=(e.currentTarget as HTMLButtonElement).dataset.updateAction!;if(action==='apply')updateRestarting=true;void updateRequest(action);});}
const workPanelKey = 'agent-chat.work-panel';
let workPanelVisible = true;
try { workPanelVisible = localStorage.getItem(workPanelKey) !== 'hidden'; } catch { /* Storage may be unavailable. */ }
const openDetails = new Set<string>();
let lastStateBody: string | undefined;
let lastActivityMarkup = '';
let stateStale = false; let lastPoll = 0; let refreshInFlight:Promise<void>|null = null;
let recipient = 'root'; let selectedProjectId = '';
const selectionRecipientKey = 'agent-chat.selection.recipient';
const selectionProjectKey = 'agent-chat.selection.project';
let selectionReady = false;
function rememberSelection() { try { sessionStorage.setItem(selectionRecipientKey,recipient);sessionStorage.setItem(selectionProjectKey,selectedProjectId); } catch { /* Storage may be unavailable. */ } }
function restoreSelection() {
 let savedRecipient = 'root', savedProject = '';
 try { savedRecipient=sessionStorage.getItem(selectionRecipientKey) || 'root';savedProject=sessionStorage.getItem(selectionProjectKey) || ''; } catch { /* Use the default. */ }
 const validProject=!!savedProject && state.projects.some(p=>p.id===savedProject);
 if(savedRecipient==='root' && !savedProject){recipient='root';selectedProjectId='';}
 else if(validProject && savedRecipient==='group:'+savedProject){recipient=savedRecipient;selectedProjectId=savedProject;}
 else if(validProject && state.agents.some(a=>a.id===savedRecipient && a.project_id===savedProject)){recipient=savedRecipient;selectedProjectId=savedProject;}
 else {recipient='root';selectedProjectId='';}
 selectionReady=true;rememberSelection();
}
const railKey='agent-chat.sidebar-collapsed';
let railCollapsed=false; try {railCollapsed=localStorage.getItem(railKey)==='true';} catch {} let loading = true; let error = ''; let sending = false; let sendTarget = ''; let mobileNav = false;
type Mention = {id:string; label:string; start:number; end:number};
const mentionDrafts = new Map<string,Mention[]>();
let mentionPickerTarget=''; let mentionTriggerStart=-1; let mentionQuery='';
const caretByChannel = new Map<string,[number,number]>();
function readMentions(target:string):Mention[] {
 if(mentionDrafts.has(target))return mentionDrafts.get(target)!;
 let items:Mention[]=[];
 try { const raw=sessionStorage.getItem('draft-mentions:'+target) || '';
  if(raw)items=raw.split('|').map(part=>{const [id,start,end,label]=part.split(':');return {id:decodeURIComponent(id),label:decodeURIComponent(label),start:Number(start),end:Number(end)};})
   .filter(m=>m.id && m.label && Number.isInteger(m.start) && Number.isInteger(m.end) && m.start>=0 && m.end>m.start);
 } catch { items=[]; }
 mentionDrafts.set(target,items);return items;
}
function writeMentions(target:string,items:Mention[]):void {
 mentionDrafts.set(target,items);
 try{sessionStorage.setItem('draft-mentions:'+target,items.map(m=>[encodeURIComponent(m.id),m.start,m.end,encodeURIComponent(m.label)].join(':')).join('|'));}catch{}
}
function adjustMentions(target:string,before:string,after:string):void {
 let start=0;while(start<before.length && start<after.length && before[start]===after[start])start++;
 let oldEnd=before.length,newEnd=after.length;
 while(oldEnd>start && newEnd>start && before[oldEnd-1]===after[newEnd-1]){oldEnd--;newEnd--;}
 const delta=newEnd-oldEnd;
 writeMentions(target,readMentions(target).flatMap(m=>{
  if(oldEnd<=m.start)return [{...m,start:m.start+delta,end:m.end+delta}];
  if(start>=m.end)return [m];
  return [];
 }));
}
// Picker edits know their exact replacement range; common text in adjacent names
// must not make the general input-diff heuristic discard an untouched mention.
function adjustMentionsForRange(target:string,start:number,end:number,insertedLength:number):void {
 const delta=insertedLength-(end-start);
 writeMentions(target,readMentions(target).flatMap(m=>{
  if(m.end<=start)return [m];
  if(m.start>=end)return [{...m,start:m.start+delta,end:m.end+delta}];
  return [];
 }));
}
function mentionIds(target:string):string[] {return readMentions(target).map(m=>m.id);}
function validMentionDraft(target:string,text:string,projectId:string):boolean {
 const seen=new Set<string>();let lastEnd=0;
 for(const m of [...readMentions(target)].sort((a,b)=>a.start-b.start)) {
  if(seen.size>=32)return false;
  const agent=state.agents.find(a=>a.id===m.id && a.project_id===projectId && (a.role==='po'||a.role==='worker'));
  if(!agent || agent.name!==m.label || seen.has(m.id) || m.start<lastEnd || text.slice(m.start,m.end)!=='@'+m.label)return false;
  seen.add(m.id);lastEnd=m.end;
 }
 return true;
}
function encodeIds(ids:string[]):string{return ids.map(encodeURIComponent).join('|');}
function pendingMentions(target:string):string[] | null {try{const raw=sessionStorage.getItem('pending-mentions:'+target);if(raw===null)return null;const ids=raw?raw.split('|').map(decodeURIComponent):[];return ids.length<=32 && new Set(ids).size===ids.length && ids.every(id=>/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id))?ids:null;}catch{return null;}}
const sameMentions=(a:string[],b:string[])=>{if(new Set(a).size!==a.length || new Set(b).size!==b.length)return false;const left=[...a].sort(),right=[...b].sort();return left.length===right.length && left.every((id,i)=>id===right[i]);};
const filesByChannel = new Map<string,File[]>();
const draftFiles = (target:string) => filesByChannel.get(target) || [];
const filePreviews = new Map<File,string>();
function previewURL(file:File):string {let url=filePreviews.get(file);if(!url){url=URL.createObjectURL(file);filePreviews.set(file,url);}return url;}
function setDraftFiles(target:string,files:File[]):void {
 const previous=draftFiles(target);if(files.length)filesByChannel.set(target,files);else filesByChannel.delete(target);
 for(const file of previous)if(![...filesByChannel.values()].some(items=>items.includes(file))){const url=filePreviews.get(file);if(url){URL.revokeObjectURL(url);filePreviews.delete(file);}}
}
window.addEventListener('pagehide',e=>{if(e.persisted)return;for(const url of filePreviews.values())URL.revokeObjectURL(url);filePreviews.clear();});
function draftFileCard(f:File,i:number,locked:boolean):string {
 const preview=/^image\/(png|jpeg|gif|webp)$/.test(f.type);
 return `<div class="file-card">${preview?`<img class="file-thumbnail" src="${esc(previewURL(f))}" alt="Preview of ${esc(f.name)}"><span class="file-fallback" hidden aria-hidden="true">▤</span>`:'<span class="file-fallback" aria-hidden="true">▤</span>'}<div class="file-info"><strong>${esc(f.name)}</strong><small>${f.size<1024?`${f.size} B`:`${Math.ceil(f.size/1024)} KB`}</small></div><button type="button" data-remove-file="${i}" aria-label="Remove ${esc(f.name)}" ${locked?'disabled':''}>×</button></div>`;
}

const drafts = new Map<string,string>();
function channelDraft(target:string):string { try { return drafts.get(target) ?? sessionStorage.getItem('draft-text:'+target) ?? ''; } catch { return drafts.get(target) ?? ''; } }
function storeDraft(target:string,text:string):void { drafts.set(target,text);try{sessionStorage.setItem('draft-text:'+target,text);}catch{} }
function clearDraft(target:string):void { drafts.delete(target);writeMentions(target,[]);try{sessionStorage.removeItem('draft-text:'+target);}catch{} }
const draft = () => channelDraft(recipient);
const pendingId = (target:string) => sessionStorage.getItem('pending-id:'+target);
const pendingText = (target:string) => sessionStorage.getItem('pending-text:'+target) || '';
const replyKey = (target:string) => 'draft-reply:'+target;
const pendingReply = (target:string) => sessionStorage.getItem('pending-reply:'+target) || '';
const draftReply = (target:string) => sessionStorage.getItem(replyKey(target)) || '';
let copyFeedback = '';
const unread = (target:string) => state.messages.filter(m=>m.sender===target && m.recipient==='operator' && !m.operator_read_at).length;
const unreadBadge = (target:string) => unread(target) ? `<span class="unread" aria-label="${unread(target)} unread messages">${unread(target)}</span>` : '';
const quote = (id:string, messages:Message[]) => {const m=messages.find(x=>x.id===id);return m ? `<strong>${esc(name(m.sender))}</strong><span>${esc(Array.from(m.text).slice(0,160).join(''))}</span>` : '<span>Earlier message unavailable</span>';};
const retryReady = new Set<string>();
const conflicts = new Set<string>();
let renderedRecipient = ''; let scrollByRecipient = new Map<string,number>();
const esc = (s:unknown) => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]!));
const taskDiagnostics = (task:Task) => task.status === 'blocked' ? state.jobs.filter(j=>j.task_id===task.id && j.project_id===task.project_id && j.status==='uncertain' && j.diagnostic?.trim()).map(j=>`<div class="task-diagnostic"><span>Outcome unknown. Ask Root to check before trying again.</span><details data-diagnostic="task:${esc(j.id)}" ${openDetails.has('task:'+j.id)?'open':''}><summary>Details</summary><p>${esc(j.diagnostic!.trim())}</p></details></div>`).join('') : '';
const safeURL = (a:Attachment) => `/api/attachments/${encodeURIComponent(a.id)}`;
const name = (id:string) => id === 'root' ? 'Root' : state.projects.find(p=>'group:'+p.id===id)?.name ? `${state.projects.find(p=>'group:'+p.id===id)!.name} project chat` : state.agents.find(a=>a.id===id)?.name || id;
const project = () => state.projects.find(p=>p.id===selectedProjectId);
const inGroup = () => !!selectedProjectId && recipient==='group:'+selectedProjectId;
const projectAgents = (id:string) => state.agents.filter(a=>a.project_id===id);
const projectUnread = (id:string) => {const ids=new Set(projectAgents(id).map(a=>a.id));return state.messages.filter(m=>m.project_id===id && ids.has(m.sender) && m.recipient==='operator' && !m.operator_read_at).length;};
const projectBadge = (id:string) => projectUnread(id) ? `<span class="unread" aria-label="${projectUnread(id)} unread messages">${projectUnread(id)}</span>` : '';
function isSharedParent(m:Message):boolean {
 const p=state.projects.find(project=>project.id===m.project_id);
 if(!p || m.recipient!=='group:'+p.id || !!m.task_id)return false;
 if(m.sender==='operator' && m.role==='operator')return true;
 return state.agents.some(a=>a.id===m.sender && a.role===m.role && a.project_id===p.id && (a.role==='worker' || (a.role==='po' && a.id===p.po_id)));
}
const readable = (m:Message) => m.sender!=='operator' && (inGroup()
 ? m.project_id===selectedProjectId && (m.recipient==='operator' || (m.recipient===recipient && isSharedParent(m)))
 : m.sender===recipient && m.recipient==='operator');
// Only valid shared posts and operator/registered-agent pairs can be reply parents.
function groupReplyTarget(m:Message):string | undefined {
 if(inGroup() && m.recipient===recipient && isSharedParent(m)) return recipient;
 const other=m.sender==='operator' ? m.recipient : m.recipient==='operator' ? m.sender : '';
 const agent=state.agents.find(a=>a.id===other);
 return agent && (agent.role==='root' || ((agent.role==='po' || agent.role==='worker') && !!agent.project_id)) ? agent.id : undefined;
}
const date = (at:string) => {const d = new Date(at); return Number.isNaN(d.getTime()) ? '' : d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});};
function attachment(a:Attachment) { const url=safeURL(a); return `<div class="attachment-row"><a class="attachment-download" data-download="${esc(a.id)}" href="${url}?download=1" download="${esc(a.name)}" aria-label="Download ${esc(a.name)}" title="Download"><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M12 3v11"/><path d="m8 10 4 4 4-4"/><path d="M5 17v2a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2"/></svg></a><button type="button" class="attachment attachment-tile" data-attachment-preview="${esc(a.id)}" aria-label="Preview ${esc(a.name)}">${a.media_type?.startsWith('image/') ? `<img src="${url}" alt="" loading="lazy">` : '<span class="file-icon" aria-hidden="true">▤</span>'}<span><strong>${esc(a.name)}</strong><small>${esc(a.media_type || 'Attachment')} · Preview</small></span></button></div>`; }
function agentActivity(target:string): {label:string; detail?:string; guidance?:string; key?:string; busy:boolean} {
 if(loading) return {label:'Loading',busy:false};
 if(stateStale) return {label:'Status unavailable',busy:false};
 const groupProject=state.projects.find(p=>'group:'+p.id===target);
 const jobs=groupProject ? state.jobs.filter(j=>j.project_id===groupProject.id && j.kind==='project_message') : state.jobs.filter(j=>j.agent_id===target && j.kind!=='project_message');
 const newest=(status:string)=>[...jobs].reverse().find(j=>j.status===status);
 const current=newest('running') || newest('submitted') || newest('pending') || newest('uncertain');
 if(current){const reason=current.diagnostic?.trim();
  const label=current.status==='running'?'Working':current.status==='submitted'?(reason?'Starting · retrying':'Starting'):current.status==='pending'?(reason?'Queued · retrying':'Queued'):'Blocked · uncertain';
  const guidance=current.status==='uncertain'?'The outcome is unknown. Ask Root to check the outcome before trying again.':reason?'Waiting to retry automatically. No action needed now.':undefined;
  return {label,detail:reason,guidance,key:current.id,busy:current.status!=='uncertain'};
 }
 const last=jobs[jobs.length-1];
 if(last?.status==='completed' && last.diagnostic?.trim()) {const detail=last.diagnostic.trim();const historical=last.id==='c22b9643-a9f5-441f-8fcf-fde5ebdcd903' && detail==='invalid runtime result: a new project requires an unused ID and service-owned PO';const guidance=historical?'The project was not created. Send the request again as a new message.':detail.startsWith('invalid runtime result:')?'The result was not applied. Ask Root to review what happened.':'Ask Root to review what happened before trying again.';return {label:'Failed',detail,guidance,key:last.id,busy:false};}
 return {label:'Idle',busy:false};
}
function activityMarkup(projectId:string):string {
 lastActivityMarkup=renderProjectActivity(state,projectId);
 return lastActivityMarkup;
}
function refreshActivity():void {
 const p=project();
 const current=app.querySelector<HTMLElement>('.project-activity');
 if(!p || !current) return;
 const next=renderProjectActivity(state,p.id);
 if(next!==lastActivityMarkup){current.outerHTML=next;lastActivityMarkup=next;}
}
const panelToggleIcon = `<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><rect x="3" y="4" width="18" height="16" rx="2"></rect><path d="M15 4v16"></path><path class="toggle-chevron" d="m9 9 3 3-3 3"></path></svg>`;
function render() {
 const activeAction = document.activeElement instanceof HTMLElement && app.contains(document.activeElement) ? document.activeElement : null;
 const actionFocus = activeAction ? (['reply','copy','attachmentPreview','download'] as const).map(key=>({key,value:activeAction.dataset[key]})).find(x=>x.value) : undefined;
 const actionMessage = activeAction?.closest<HTMLElement>('[data-message-id]')?.dataset.messageId;
 const mentionFocus=activeAction?.id==='mention-toggle' ? 'toggle' : activeAction?.dataset.mentionChoice ? 'choice:'+activeAction.dataset.mentionChoice : activeAction?.dataset.unmention ? 'remove:'+activeAction.dataset.unmention : '';
 const oldScroll = app.querySelector<HTMLElement>('#chat-scroll');
 const oldPanelTop = app.querySelector<HTMLElement>('#work-panel-content')?.scrollTop || 0;
 const oldContentTop = app.querySelector<HTMLElement>('.content')?.scrollTop || 0;
 const same = renderedRecipient === recipient;
 const oldTop = oldScroll?.scrollTop || 0;
 const follow = !oldScroll || oldScroll.scrollHeight - oldScroll.clientHeight - oldTop < 60;
 const focused = document.activeElement?.id === 'message';
 const detailsFocused = document.activeElement instanceof HTMLElement && document.activeElement.tagName === 'SUMMARY' ? document.activeElement.parentElement?.getAttribute('data-diagnostic') : null;
 const toggleFocused = document.activeElement?.id === 'work-toggle';
 const oldInput = app.querySelector<HTMLTextAreaElement>('#message');
 const selection = focused ? [oldInput?.selectionStart || 0, oldInput?.selectionEnd || 0] : null;
 const p=project(); const group=inGroup(); const msgs=state.messages.filter(m => group ? m.project_id===selectedProjectId : ((m.sender==='operator' && m.recipient===recipient) || (m.sender===recipient && m.recipient==='operator'))); const agents=p?projectAgents(p.id):[]; const subagents=agents.filter(a=>!!a.parent_id && agents.some(parent=>parent.id===a.parent_id));
 const activity=agentActivity(recipient);
 const selectedMentions=group?readMentions(recipient):[];
 const mentionOptions=group && p && selectedMentions.length<32?projectAgents(p.id).filter(a=>(a.role==='po'||a.role==='worker') && !!a.name && !/[\r\n]/.test(a.name) && !selectedMentions.some(m=>m.id===a.id) && a.name.toLocaleLowerCase().includes(mentionQuery.toLocaleLowerCase())):[];
 const mentionOpen=group && mentionPickerTarget===recipient && !pendingId(recipient);
 const tasks=state.tasks.filter(t=>!p||t.project_id===p.id).slice(-5).reverse();
 app.innerHTML=`<div class="shell"><aside class="sidebar ${mobileNav?'open':''} ${railCollapsed?'rail-collapsed':''}" aria-label="Conversations"><button type="button" class="rail-toggle" id="rail-toggle" aria-label="${railCollapsed?'Expand':'Collapse'} conversations" aria-expanded="${!railCollapsed}">${panelToggleIcon}</button><div class="brand"><span class="brandmark">✳</span><div><b>Agent Chat</b><small>YOUR WORKSPACE</small></div></div><div class="side-heading">CONVERSATIONS</div><button class="conversation ${recipient==='root'?'selected':''}" data-recipient="root" aria-label="Root conversation, ${unread('root')} unread"><span class="avatar root-avatar">✳</span><span><b>Root</b><small>General conversation</small></span>${unreadBadge('root')}<span class="chevron">›</span></button><div class="side-heading projects-heading">PROJECTS <span>${state.projects.length}</span></div><div class="project-list">${state.projects.length ? state.projects.map(x=>`<button class="conversation ${selectedProjectId===x.id?'selected':''}" data-project="${esc(x.id)}" aria-label="${esc(x.name)} project, ${projectUnread(x.id)} unread"><span class="avatar project-avatar">${esc(x.name.slice(0,1).toUpperCase())}</span><span><b>${esc(x.name)}</b><small>${esc(x.status || 'Project')}</small></span>${projectBadge(x.id)}<span class="chevron">›</span></button>${selectedProjectId===x.id?`<div class="project-agents"><div class="agent-count">${projectAgents(x.id).length} agents · ${projectAgents(x.id).filter(a=>!!a.parent_id && projectAgents(x.id).some(parent=>parent.id===a.parent_id)).length} subagents</div>${projectAgents(x.id).map(a=>`<button type="button" class="agent-choice ${recipient===a.id?'selected':''}" data-agent="${esc(a.id)}" aria-label="Chat with ${esc(a.name)}, ${unread(a.id)} unread"><span>${esc(a.name)}</span>${unreadBadge(a.id)}</button>`).join('')}</div>`:''}`).join('') : '<p class="side-empty">Projects will appear here.</p>'}</div></aside><main class="main"><div class="content"><section class="chat" aria-label="Conversation with ${esc(group?p?.name || 'project':name(recipient))}"><header class="channel-header"><button class="menu" type="button" aria-label="Open conversations" aria-expanded="${mobileNav}">☰</button><h1>${esc(group?p?.name || 'Project':name(recipient))}</h1></header><div class="chat-scroll" id="chat-scroll">${loading?'<p class="notice" role="status">Loading conversation…</p>':''}${error?`<div class="notice error" role="alert">${esc(error)} <button id="retry">Try again</button></div>`:''}${!loading&&!error&&!msgs.length?'<div class="empty">No messages yet. Start the conversation below.</div>':''}<div class="messages">${msgs.map(m=>`<article class="message" data-message-id="${esc(m.id)}"><div class="message-avatar ${m.sender==='root'?'root-avatar':''}">${esc(name(m.sender).slice(0,1).toUpperCase())}</div><div class="message-body"><div class="message-meta"><strong>${esc(name(m.sender))}</strong>${group?`<span>→ ${esc(name(m.recipient))}</span>`:''}<span>${esc(date(m.at))}</span></div>${m.reply_to?`<blockquote class="reply-quote">${quote(m.reply_to,msgs)}</blockquote>`:''}<div class="message-text" data-markdown-id="${esc(m.id)}">${esc(m.text)}</div><div class="message-actions" role="group" aria-label="Message actions">${(!group || groupReplyTarget(m))?`<button type="button" data-reply="${esc(m.id)}" aria-label="Reply to message from ${esc(name(m.sender))}" title="Reply" ${pendingId(group ? (groupReplyTarget(m) || '') : recipient)?'disabled':''}><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M9 7 4 12l5 5"/><path d="M4 12h9a7 7 0 0 1 7 7"/></svg></button>`:''}<button type="button" data-copy="${esc(m.id)}" aria-label="Copy message from ${esc(name(m.sender))}" title="Copy"><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><rect x="8" y="8" width="11" height="12" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h2"/></svg></button></div>${m.attachments?.length?`<div class="attachments">${m.attachments.map(attachment).join('')}</div>`:''}</div></article>`).join('')}</div></div><form class="composer" id="composer"><div class="composer-heading"><label for="message">${group?`Message ${esc(p?.name || 'project')} chat`:`Message ${esc(p?name(recipient):'Root')}`}</label>${group?`<button type="button" id="mention-toggle" class="mention-toggle" aria-expanded="${mentionOpen}" aria-controls="mention-options" ${pendingId(recipient)||selectedMentions.length>=32?'disabled':''}>@ Mention</button>`:''}<div class="agent-activity" role="status" aria-live="polite"><span class="activity-dot ${activity.busy?'busy':''}"></span>${esc(activity.label)}</div></div>${group&&selectedMentions.length?`<div class="mention-selected" aria-label="Selected mentions">${selectedMentions.map(m=>`<button type="button" data-unmention="${esc(m.id)}" aria-label="Remove mention ${esc(m.label)}" ${pendingId(recipient)?'disabled':''}>@${esc(m.label)} ×</button>`).join('')}</div>`:''}${mentionOpen?`<div id="mention-options" class="mention-options" role="group" aria-label="Choose a project agent">${mentionOptions.length?mentionOptions.map(a=>`<button type="button" data-mention-choice="${esc(a.id)}" aria-label="Mention ${esc(a.name)}, ${esc(a.role)}, ${esc(a.id)}"><strong>${esc(a.name)}</strong><small>${esc(a.role)} · ${esc(a.id)}</small></button>`).join(''):selectedMentions.length>=32?'<span>Up to 32 agents per message.</span>':'<span>No matching agents.</span>'}</div>`:''}${activity.guidance?`<div class="status-guidance"><span>${esc(activity.guidance)}</span>${activity.detail?`<details id="status-details" data-diagnostic="status:${esc(activity.key || '')}" ${activity.key && openDetails.has('status:'+activity.key)?'open':''}><summary id="status-details-summary">Details</summary><p>${esc(activity.detail)}</p></details>`:''}</div>`:''}${(pendingId(recipient)?pendingReply(recipient):draftReply(recipient))?`<div class="compose-reply"><div><small>Replying to</small>${quote(pendingId(recipient)?pendingReply(recipient):draftReply(recipient),msgs)}</div><button type="button" id="cancel-reply" aria-label="Cancel reply" ${pendingId(recipient)?'disabled':''}>×</button></div>`:''}<div role="status" aria-live="polite" class="copy-feedback">${esc(copyFeedback)}</div><div class="compose-box">${draftFiles(recipient).length?`<div class="file-list" aria-label="Attachments">${draftFiles(recipient).map((f,i)=>draftFileCard(f,i,!!pendingId(recipient))).join('')}</div>`:''}<div class="compose-entry"><div class="attachment-control"><button type="button" id="attach-files" class="attach-button" aria-label="Add attachment" aria-expanded="false" aria-controls="attachment-menu" title="Add attachment" ${pendingId(recipient)?'disabled':''}>＋</button><div id="attachment-menu" class="attachment-menu" hidden><button type="button" id="upload-file">Upload a file</button></div></div><input id="file-input" type="file" multiple hidden><span class="sr-only" id="composer-help">Press Enter to send or retry delivery. Press Shift and Enter for a new line.</span><textarea id="message" aria-describedby="composer-help" enterkeyhint="send" rows="1" placeholder="${group?'Write a shared message…':'Write a message…'}" ${sending&&recipient===sendTarget?'disabled':''} ${group&&pendingId(recipient)?'readonly':''}>${esc(group&&pendingId(recipient)?pendingText(recipient):pendingId(recipient) && !draft() ? pendingText(recipient) : draft())}</textarea></div>${pendingId(recipient)?`<p class="pending" role="status">${group && pendingMentions(recipient)===null?'Saved mention targets are unavailable. Check the outcome before retrying.':conflicts.has(recipient)?'The message ID conflicts with another request. Delivery needs investigation.':retryReady.has(recipient)?'Delivery not confirmed. Press Enter to retry the saved message.':group?'Delivery not confirmed. Retry sends the saved message and mentions.':'Delivery not confirmed. Check status before retrying the same message.'} <button type="button" id="check-delivery">Check status</button>${retryReady.has(recipient) && sessionStorage.getItem('pending-files:'+recipient)==='yes' && !draftFiles(recipient).length?' <button type="button" id="discard-pending">Start a new message</button>':''}</p>`:''}</div></form></section><aside id="work-panel" class="context ${workPanelVisible ? '' : 'collapsed'}" aria-label="Work context"><div class="work-panel-bar"><button id="work-toggle" class="work-toggle" type="button" aria-controls="work-panel-content" aria-expanded="${workPanelVisible}" aria-label="${workPanelVisible ? 'Collapse work panel' : 'Expand work panel'}" title="${workPanelVisible ? 'Collapse work panel' : 'Expand work panel'}">${panelToggleIcon}</button></div><div id="work-panel-content" class="context-body" ${workPanelVisible ? '' : 'hidden'}>${p?activityMarkup(p.id):''}<div id="update-panel" class="update-panel" hidden></div><div class="context-top"><span class="eyebrow">AT A GLANCE</span><h2>${p?'Project details':'Your work'}</h2><p>${p?'A little context for this conversation.':'Recent work across your workspace.'}</p></div>${p?`<div class="detail"><span>PROJECT LEAD</span><strong>${esc(name(p.po_id))}</strong><small>${esc(p.status)}</small></div>`:''}${p?`<div class="project-counts">${agents.length} agents · ${subagents.length} subagents</div>`:''}<div class="context-heading"><h3>Recent work</h3><span>${tasks.length}</span></div>${tasks.length?tasks.map(t=>`<div class="task"><span class="task-dot"></span><div><strong>${esc(t.title)}</strong><small>${esc(t.status)} · ${esc(name(t.owner_id))}</small>${taskDiagnostics(t)}</div></div>`).join(''):'<p class="context-empty">No recent work to show yet.</p>'}</div></aside></div></main></div>`;
 renderUpdate();
 app.querySelectorAll<HTMLElement>('[data-markdown-id]').forEach(el=>{const message=msgs.find(m=>m.id===el.dataset.markdownId);if(message)renderMessageMarkdown(el,message.text);});
 app.querySelectorAll<HTMLButtonElement>('[data-recipient]').forEach(b=>b.onclick=()=>{const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll)scrollByRecipient.set(recipient,scroll.scrollTop);mentionPickerTarget='';mentionQuery='';recipient=b.dataset.recipient!;selectedProjectId='';rememberSelection();mobileNav=false;readReceipts.contextChanged();const content=app.querySelector<HTMLElement>('.content');if(content)content.scrollTop=0;render();});
 app.querySelectorAll<HTMLButtonElement>('[data-project]').forEach(b=>b.onclick=()=>{const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll)scrollByRecipient.set(recipient,scroll.scrollTop);mentionPickerTarget='';mentionQuery='';selectedProjectId=b.dataset.project!;recipient='group:'+selectedProjectId;rememberSelection();mobileNav=false;readReceipts.contextChanged();render();});
 app.querySelectorAll<HTMLButtonElement>('[data-agent]').forEach(b=>b.onclick=()=>{const a=state.agents.find(x=>x.id===b.dataset.agent && x.project_id===selectedProjectId);if(!a)return;const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll)scrollByRecipient.set(recipient,scroll.scrollTop);recipient=a.id;rememberSelection();mobileNav=false;readReceipts.contextChanged();render();});
 app.querySelector<HTMLButtonElement>('#rail-toggle')!.onclick=()=>{railCollapsed=!railCollapsed;try{localStorage.setItem(railKey,String(railCollapsed));}catch{}render();app.querySelector<HTMLButtonElement>('#rail-toggle')?.focus();};
 const toggle = app.querySelector<HTMLButtonElement>('#work-toggle')!;
 toggle.onclick=()=>{workPanelVisible=!workPanelVisible;try{localStorage.setItem(workPanelKey,workPanelVisible?'visible':'hidden');}catch{};toggle.setAttribute('aria-expanded',String(workPanelVisible));const label=workPanelVisible?'Collapse work panel':'Expand work panel';toggle.setAttribute('aria-label',label);toggle.title=label;app.querySelector<HTMLElement>('#work-panel')!.classList.toggle('collapsed',!workPanelVisible);app.querySelector<HTMLElement>('#work-panel-content')!.hidden=!workPanelVisible;};
 if(toggleFocused) toggle.focus({preventScroll:true});
 app.querySelectorAll<HTMLDetailsElement>('details[data-diagnostic]').forEach(details=>{const key=details.dataset.diagnostic!;details.ontoggle=()=>{if(details.open)openDetails.add(key);else openDetails.delete(key);};if(key===detailsFocused)details.querySelector<HTMLElement>('summary')?.focus({preventScroll:true});});
 app.querySelector<HTMLButtonElement>('.menu')!.onclick=()=>{mobileNav=!mobileNav;readReceipts.contextChanged();render();};
 app.querySelectorAll<HTMLButtonElement>('[data-attachment-preview]').forEach(b=>b.onclick=()=>{const a=msgs.flatMap(m=>m.attachments||[]).find(x=>x.id===b.dataset.attachmentPreview);if(a){previewAttachment({id:a.id,name:a.name},b);document.querySelector('dialog.attachment-preview')?.addEventListener('close',()=>readReceipts.schedule(),{once:true});readReceipts.schedule();}});
 app.querySelectorAll<HTMLButtonElement>('[data-reply]').forEach(b=>b.onclick=()=>{
  const message=msgs.find(m=>m.id===b.dataset.reply);
  const target=group && message ? groupReplyTarget(message) : recipient;
  if(!message || !target || pendingId(target))return;
  if(group && target!==recipient){
   const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll)scrollByRecipient.set(recipient,scroll.scrollTop);
   const agent=state.agents.find(a=>a.id===target);if(!agent)return;
   mentionPickerTarget='';mentionQuery='';recipient=target;selectedProjectId=agent.role==='root'?'':agent.project_id;rememberSelection();mobileNav=false;
   readReceipts.contextChanged();const content=app.querySelector<HTMLElement>('.content');if(content)content.scrollTop=0;
  }
  sessionStorage.setItem(replyKey(target),message.id);render();app.querySelector<HTMLTextAreaElement>('#message')?.focus();
 });
 app.querySelector<HTMLButtonElement>('#cancel-reply')?.addEventListener('click',()=>{sessionStorage.removeItem(replyKey(recipient));render();app.querySelector<HTMLTextAreaElement>('#message')?.focus();});
 app.querySelectorAll<HTMLButtonElement>('[data-copy]').forEach(b=>b.onclick=async()=>{const m=msgs.find(x=>x.id===b.dataset.copy);if(!m)return;try{await navigator.clipboard.writeText(m.text);copyFeedback='Message copied.';}catch{copyFeedback='Could not copy message.';}const status=app.querySelector<HTMLElement>('.copy-feedback');if(status)status.textContent=copyFeedback;});
 app.querySelector<HTMLButtonElement>('#retry')?.addEventListener('click',()=>void refresh());
 const addFiles=(incoming:FileList|File[])=>{if(pendingId(recipient))return;const files=[...draftFiles(recipient),...Array.from(incoming)];if(files.length>32 || files.some(f=>f.size>8*1024*1024) || files.reduce((n,f)=>n+f.size,0)>32*1024*1024){error='Choose up to 32 files, each at most 8 MiB and 32 MiB total.';render();return;}setDraftFiles(recipient,files);error='';render();};
 const picker=app.querySelector<HTMLInputElement>('#file-input')!;
 const attach=app.querySelector<HTMLButtonElement>('#attach-files')!;
 const attachmentMenu=app.querySelector<HTMLElement>('#attachment-menu')!;
 attach.onclick=()=>{const open=attachmentMenu.hidden;attachmentMenu.hidden=!open;attach.setAttribute('aria-expanded',String(open));if(open)app.querySelector<HTMLButtonElement>('#upload-file')?.focus();};
 app.querySelector<HTMLButtonElement>('#upload-file')!.onclick=()=>{closeAttachmentMenu();picker.click();};
 picker.onchange=()=>{if(picker.files)addFiles(picker.files);};
 app.querySelectorAll<HTMLImageElement>('.file-thumbnail').forEach(img=>{img.onerror=()=>{img.hidden=true;const fallback=img.nextElementSibling as HTMLElement | null;if(fallback)fallback.hidden=false;};});
 app.querySelectorAll<HTMLButtonElement>('[data-remove-file]').forEach(b=>b.onclick=()=>{setDraftFiles(recipient,draftFiles(recipient).filter((_,i)=>i!==Number(b.dataset.removeFile)));render();});
 const chat=app.querySelector<HTMLElement>('.chat')!;
 chat.ondragover=e=>{if(e.dataTransfer?.types.includes('Files')){e.preventDefault();chat.classList.add('dragging');}};
 chat.ondragleave=e=>{if(!chat.contains(e.relatedTarget as Node))chat.classList.remove('dragging');};
 chat.ondrop=e=>{e.preventDefault();chat.classList.remove('dragging');if(e.dataTransfer?.files)addFiles(e.dataTransfer.files);};
 const input=app.querySelector<HTMLTextAreaElement>('#message')!;
 resizeComposer(input);
 const saveCaret=()=>caretByChannel.set(recipient,[input.selectionStart,input.selectionEnd]);
 const focusComposerAtSaved=()=>{const composer=app.querySelector<HTMLTextAreaElement>('#message');if(!composer)return;composer.focus();const caret=caretByChannel.get(recipient);if(caret)composer.setSelectionRange(caret[0],caret[1]);};
 input.addEventListener('select',saveCaret);input.addEventListener('click',saveCaret);input.addEventListener('keyup',saveCaret);
 input.addEventListener('input',event=>{
  resizeComposer(input);
  const target=recipient;const before=channelDraft(target);const after=input.value;
  const previousMentions=group?readMentions(target).length:0;
  if(group)adjustMentions(target,before,after);
  storeDraft(target,after);saveCaret();
  if(group && !pendingId(target)){
   const edit=event as InputEvent;const pos=input.selectionStart;
   if(edit.data==='@' && input.selectionStart===input.selectionEnd){mentionPickerTarget=target;mentionTriggerStart=pos-1;mentionQuery='';render();}
   else if(mentionPickerTarget===target && mentionTriggerStart>=0){
    const fragment=after.slice(mentionTriggerStart,pos);
    if(fragment.startsWith('@') && !/[\r\n@]/.test(fragment.slice(1)) && fragment.length<=81){mentionQuery=fragment.slice(1);render();}
    else {mentionPickerTarget='';mentionQuery='';render();}
   } else if(readMentions(target).length!==previousMentions)render();
  }
 });
 input.addEventListener('keydown',e=>{
  if(group && mentionPickerTarget===recipient && e.key==='Escape'){e.preventDefault();mentionPickerTarget='';mentionQuery='';render();focusComposerAtSaved();return;}
  if(group && mentionPickerTarget===recipient && (e.key==='ArrowDown'||(e.key==='Enter'&&!e.shiftKey&&!e.isComposing))){e.preventDefault();app.querySelector<HTMLButtonElement>('[data-mention-choice]')?.focus();return;}
  if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();app.querySelector<HTMLFormElement>('#composer')!.requestSubmit();}
 });
 app.querySelector<HTMLButtonElement>('#mention-toggle')?.addEventListener('click',()=>{
  if(pendingId(recipient))return;
  if(mentionPickerTarget===recipient){mentionPickerTarget='';mentionQuery='';render();focusComposerAtSaved();return;}
  mentionPickerTarget=recipient;mentionTriggerStart=-1;mentionQuery='';render();
  (app.querySelector<HTMLButtonElement>('[data-mention-choice]') || app.querySelector<HTMLButtonElement>('#mention-toggle'))?.focus();
 });
 app.querySelector<HTMLElement>('#mention-options')?.addEventListener('keydown',e=>{if(e.key==='Escape'){e.preventDefault();mentionPickerTarget='';mentionQuery='';render();focusComposerAtSaved();}});
 app.querySelectorAll<HTMLButtonElement>('[data-mention-choice]').forEach(button=>button.addEventListener('click',()=>{
  if(!group || !p || pendingId(recipient))return;
  const target=recipient;const agent=state.agents.find(a=>a.id===button.dataset.mentionChoice && a.project_id===p.id && (a.role==='po'||a.role==='worker'));
  if(!agent || readMentions(target).length>=32 || readMentions(target).some(m=>m.id===agent.id))return;
  const before=channelDraft(target);const caret=caretByChannel.get(target) || [before.length,before.length];
  let [start,end]=caret;
  if(mentionPickerTarget===target && mentionTriggerStart>=0 && before.slice(mentionTriggerStart,end)==='@'+mentionQuery){start=mentionTriggerStart;}
  start=Math.max(0,Math.min(start,before.length));end=Math.max(start,Math.min(end,before.length));
  const token='@'+agent.name;const after=before.slice(0,start)+token+' '+before.slice(end);
  adjustMentionsForRange(target,start,end,token.length+1);storeDraft(target,after);
  writeMentions(target,[...readMentions(target),{id:agent.id,label:agent.name,start,end:start+token.length}]);
  const pos=start+token.length+1;caretByChannel.set(target,[pos,pos]);mentionPickerTarget='';mentionQuery='';render();
  const composer=app.querySelector<HTMLTextAreaElement>('#message');composer?.focus();composer?.setSelectionRange(pos,pos);
 }));
 app.querySelectorAll<HTMLButtonElement>('[data-unmention]').forEach(button=>button.addEventListener('click',()=>{
  if(!group || pendingId(recipient))return;
  const target=recipient;const mention=readMentions(target).find(m=>m.id===button.dataset.unmention);if(!mention)return;
  const before=channelDraft(target);const end=before[mention.end]===' '?mention.end+1:mention.end;
  const after=before.slice(0,mention.start)+before.slice(end);adjustMentionsForRange(target,mention.start,end,0);storeDraft(target,after);
  caretByChannel.set(target,[mention.start,mention.start]);render();const composer=app.querySelector<HTMLTextAreaElement>('#message');composer?.focus();composer?.setSelectionRange(mention.start,mention.start);
 }));
 if(mentionFocus==='toggle')app.querySelector<HTMLButtonElement>('#mention-toggle')?.focus({preventScroll:true});
 else if(mentionFocus.startsWith('choice:'))Array.from(app.querySelectorAll<HTMLButtonElement>('[data-mention-choice]')).find(b=>b.dataset.mentionChoice===mentionFocus.slice(7))?.focus({preventScroll:true});
 else if(mentionFocus.startsWith('remove:'))Array.from(app.querySelectorAll<HTMLButtonElement>('[data-unmention]')).find(b=>b.dataset.unmention===mentionFocus.slice(7))?.focus({preventScroll:true});
 app.querySelector<HTMLButtonElement>('#check-delivery')?.addEventListener('click',()=>void refresh());
 app.querySelector<HTMLButtonElement>('#discard-pending')?.addEventListener('click',()=>{if(!retryReady.has(recipient))return;for(const key of ['pending-id:','pending-text:','pending-reply:','pending-mentions:','pending-files:'])sessionStorage.removeItem(key+recipient);retryReady.delete(recipient);error='';render();});
 const newScroll=app.querySelector<HTMLElement>('#chat-scroll')!;
 newScroll.scrollTop=same ? (follow ? newScroll.scrollHeight : oldTop) : (scrollByRecipient.get(recipient) ?? newScroll.scrollHeight);
 app.querySelector<HTMLElement>('.content')!.scrollTop=same ? oldContentTop : 0;
 app.querySelector<HTMLElement>('#work-panel-content')!.scrollTop=same ? oldPanelTop : 0;
 renderedRecipient=recipient;
 if(selection){input.focus({preventScroll:true});input.setSelectionRange(selection[0],selection[1]);}
 else if(same && actionFocus) {const replacement=Array.from(app.querySelectorAll<HTMLElement>('[data-reply],[data-copy],[data-attachment-preview],[data-download]')).find(el=>el.dataset[actionFocus.key]===actionFocus.value && el.closest<HTMLElement>('[data-message-id]')?.dataset.messageId===actionMessage);replacement?.focus({preventScroll:true});}
 app.querySelector<HTMLFormElement>('#composer')!.onsubmit=e=>{e.preventDefault();void send();};
 readReceipts.schedule();
}
function refresh():Promise<void> {
 if(refreshInFlight)return refreshInFlight;
 lastPoll=Date.now();
 refreshInFlight=(async()=>{
  let needsRender=loading || stateStale || !!error;
  try {
   const r=await fetch('/api/state',{cache:'no-store',signal:AbortSignal.timeout(8000)});
   if(!r.ok)throw new Error(`Could not load conversations (${r.status}).`);
   const body=await r.clone().text();
   if(body!==lastStateBody){
    const data=await r.json() as State;
    state={messages:data.messages||[],projects:data.projects||[],tasks:data.tasks||[],agents:data.agents||[],jobs:data.jobs||[]};
    lastStateBody=body;
    needsRender=true;
   }
   if(!selectionReady){restoreSelection();needsRender=true;}
   for(const target of ['root',...state.agents.map(a=>a.id),...state.projects.map(p=>'group:'+p.id)]) {
    const id=pendingId(target);
    if(id && target.startsWith('group:') && pendingMentions(target)===null){if(retryReady.delete(target))needsRender=true;continue;}
    if(id && state.messages.some(m=>m.id===id && m.sender==='operator' && m.recipient===target && m.text===pendingText(target) && (m.reply_to||'')===pendingReply(target) && (!target.startsWith('group:') || (m.project_id===target.slice(6) && pendingMentions(target)!==null && sameMentions(m.mention_ids||[],pendingMentions(target)!))))){
     retryReady.delete(target);if(channelDraft(target).trim()===pendingText(target)) clearDraft(target);
     sessionStorage.removeItem('pending-id:'+target);sessionStorage.removeItem('pending-text:'+target);sessionStorage.removeItem('pending-files:'+target);setDraftFiles(target,[]);
     if(draftReply(target)===pendingReply(target))sessionStorage.removeItem(replyKey(target));
     sessionStorage.removeItem('pending-reply:'+target);sessionStorage.removeItem('pending-mentions:'+target);needsRender=true;
    } else if(id && state.messages.some(m=>m.id===id)){
     if(!conflicts.has(target) || retryReady.has(target))needsRender=true;
     conflicts.add(target);retryReady.delete(target);
    } else if(id && !conflicts.has(target)){
     if(!retryReady.has(target))needsRender=true;
     retryReady.add(target);
    }
   }
   stateStale=false;error='';
  } catch(e){
   const nextError=e instanceof Error?e.message:'Could not load conversations.';
   if(!stateStale || error!==nextError)needsRender=true;
   stateStale=true;error=nextError;
  } finally {
   loading=false;
   if(needsRender)render();else refreshActivity();
  }
 })();
 return refreshInFlight.finally(()=>{refreshInFlight=null;});
}
async function send(){const target=recipient;const groupProject=state.projects.find(p=>'group:'+p.id===target);if(target.startsWith('group:')&&!groupProject)return;const prior=pendingId(target);const text=prior ? pendingText(target) : draft().trim();if(!text||sending||(prior&&!retryReady.has(target)))return;const files=draftFiles(target);if(prior && sessionStorage.getItem('pending-files:'+target)==='yes' && !files.length){error='The original files are no longer available. Check delivery status before starting a new message.';render();return;}const id=prior || crypto.randomUUID();const reply=prior ? pendingReply(target) : draftReply(target);const savedMentions=prior && groupProject ? pendingMentions(target) : [];if(prior && groupProject && savedMentions===null){error='Saved mention targets are unavailable. Check the outcome before retrying.';render();return;}const mentions=prior ? savedMentions! : groupProject ? mentionIds(target) : [];if(groupProject && !prior && !validMentionDraft(target,channelDraft(target),groupProject.id)){error='A selected mention is no longer valid. Remove it and choose the agent again.';render();return;}retryReady.delete(target);sessionStorage.setItem('pending-id:'+target,id);sessionStorage.setItem('pending-text:'+target,text);sessionStorage.setItem('pending-reply:'+target,reply);if(!prior){sessionStorage.setItem('pending-mentions:'+target,encodeIds(mentions));sessionStorage.setItem('pending-files:'+target,files.length?'yes':'no');}mentionPickerTarget='';mentionQuery='';sending=true;sendTarget=target;render();try{const form=new FormData();form.set('id',id);form.set('text',text);form.set(groupProject?'project_id':'recipient',groupProject?groupProject.id:target);if(reply)form.set('reply_to',reply);for(const mention of mentions)form.append('mention_id',mention);for(const file of files)form.append('files',file);const r=await fetch('/api/messages/upload',{method:'POST',body:form});if(!r.ok){if(r.status===409) conflicts.add(target);throw new Error(`Delivery not confirmed (${r.status}). Check status before trying again.`);}if(channelDraft(target).trim()===text) clearDraft(target);sessionStorage.removeItem('pending-id:'+target);sessionStorage.removeItem('pending-text:'+target);if(draftReply(target)===reply)sessionStorage.removeItem(replyKey(target));sessionStorage.removeItem('pending-reply:'+target);sessionStorage.removeItem('pending-mentions:'+target);sessionStorage.removeItem('pending-files:'+target);setDraftFiles(target,[]);if(refreshInFlight)await refreshInFlight;await refresh();}catch(e){error=e instanceof Error?e.message:'Delivery not confirmed. Check status before trying again.';await refresh();}finally{sending=false;sendTarget='';render();}}
function resizeComposer(input:HTMLTextAreaElement):void {
 input.style.height='auto';
 input.style.height=`${input.scrollHeight}px`;
}
window.addEventListener('resize',()=>{const input=app.querySelector<HTMLTextAreaElement>('#message');if(input)resizeComposer(input);});
function closeAttachmentMenu(restoreFocus=false):void {
 const menu=app.querySelector<HTMLElement>('#attachment-menu');
 if(!menu || menu.hidden)return;
 menu.hidden=true;const button=app.querySelector<HTMLButtonElement>('#attach-files');button?.setAttribute('aria-expanded','false');if(restoreFocus)button?.focus();
}
document.addEventListener('pointerdown',e=>{if(!(e.target instanceof Node) || !app.querySelector('.attachment-control')?.contains(e.target))closeAttachmentMenu();});
document.addEventListener('keydown',e=>{const menu=app.querySelector<HTMLElement>('#attachment-menu');if(e.key==='Escape' && menu && !menu.hidden){e.preventDefault();e.stopPropagation();closeAttachmentMenu(true);}},true);
const readReceipts = new ReadReceipts({messages:()=>state.messages,recipient:()=>recipient,obstructed:()=>!selectionReady || mobileNav || !!document.querySelector('dialog.attachment-preview[open]'),eligible:m=>readable(m as Message),refresh});
render(); void refresh(); void updateRequest();
window.setInterval(()=>{if(!document.hidden)void updateRequest();},5000);
// Refresh in the background without interrupting a draft or an in-flight send.
window.setInterval(()=>{if(!document.hidden && !sending && Date.now()-lastPoll >= (agentActivity(recipient).busy ? 2000 : 5000)) void refresh();},1000);
