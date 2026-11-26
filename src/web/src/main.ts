import './style.css';
import {ReadReceipts} from './read-receipts';
import {isPreviewableImage, previewAttachment} from './attachment-preview';
import {renderProjectActivity} from './project-activity';
import './project-activity.css';
import {renderMessageMarkdown, mentionsOperator} from './message-markdown';
type Attachment = {id:string; name:string; media_type:string; url:string};
type Message = {id:string; room_id:string; notice_detail?:'scope_request'; sender:string; recipient:string; role:string; text:string; at:string; task_id?:string; project_id?:string; attachments?:Attachment[]; reply_to?:string; operator_read_at?:string; mention_ids?:string[]; approval_id?:string};
type Project = {id:string; name:string; status:string; po_id:string; goal_objective?:string; goal_status?:string};
type Task = {id:string; project_id:string; title:string; status:string; owner_id:string; retry_handoff_eligible?:boolean; continue_po_eligible?:boolean; resolve_hold_eligible?:boolean};
type Job = {id:string; agent_id:string; project_id:string; task_id:string; status:string; diagnostic?:string; kind?:string; source_id?:string; superseded?:boolean};
type Agent = {id:string; name:string; role:string; project_id:string; status:string; model:string; parent_id?:string; one_shot_retired?:boolean};
type Approval = {id:string;status:string;decision_note?:string};
type State = {messages:Message[]; projects:Project[]; tasks:Task[]; agents:Agent[]; jobs:Job[]; approvals:Approval[]};
const app = document.querySelector<HTMLDivElement>('#app')!;
let state:State = {messages:[],projects:[],tasks:[],agents:[],jobs:[],approvals:[]};
type UpdateInfo={available:boolean;current:string;version:string;state:string;error:string};
let updateInfo:UpdateInfo|null=null;let updateBusy=false;let updateRestarting=false;
let expectedUpdate:{version:string;started:number}|null=null;let updateUnconfirmed=false;let reloadTriggered=false;let updateDeferred=false;
const updateWaitLimit=120000;
async function updateRequest(action=''):Promise<void>{
 if(updateBusy || reloadTriggered || (updateUnconfirmed && !action))return;
 if(action==='check')updateUnconfirmed=false;
 if(action==='apply'){
  const current=updateInfo?.current;const version=updateInfo?.version;
  if(current && version && current!==version){expectedUpdate={version,started:Date.now()};updateRestarting=true;updateDeferred=false;}
 }
 updateBusy=true;
 try {
  const r=await fetch('/api/update'+(action?'/'+action:''),{method:action?'POST':'GET',cache:'no-store',signal:AbortSignal.timeout(8000)});
  if(r.status===404 && !expectedUpdate){updateInfo=null;return;}
  const detail=await r.text();
  if(!r.ok){if(action==='apply'){expectedUpdate=null;updateRestarting=false;}throw new Error(detail||`Update request failed (${r.status}).`);}
  const available=r.headers.get('X-AgentChat-Update-Available')==='true';const current=r.headers.get('X-AgentChat-Update-Current')||'';const status=r.headers.get('X-AgentChat-Update-State')||'idle';
  if(!action && expectedUpdate && available && current===expectedUpdate.version && r.headers.get('X-AgentChat-Update-Committed')==='true'){
   const health=await fetch('/api/state',{cache:'no-store',signal:AbortSignal.timeout(8000)});
   if(health.ok){
    const verify=await fetch('/api/update',{cache:'no-store',signal:AbortSignal.timeout(8000)});
    if(verify.ok && verify.headers.get('X-AgentChat-Update-Current')===expectedUpdate.version && verify.headers.get('X-AgentChat-Update-Committed')==='true'){
     if(hasUnsentWork()){updateDeferred=true;updateInfo={available:true,current,version:expectedUpdate.version,state:'deferred',error:''};return;}
     reloadTriggered=true;expectedUpdate=null;window.location.reload();return;
    }
   }
  }
  if(expireUpdate())return;
  updateInfo={available,current,version:r.headers.get('X-AgentChat-Update-Version')||'',state:expectedUpdate?(updateDeferred?'deferred':'restarting'):status,error:status==='error'?detail:''};
  updateRestarting=!!expectedUpdate || status==='restarting';
 } catch(e) {
  if(expireUpdate())return;
  if(expectedUpdate || updateRestarting)updateInfo={available:true,current:updateInfo?.current||'',version:'',state:updateDeferred?'deferred':'restarting',error:''};
  else if(updateInfo)updateInfo={...updateInfo,state:'error',error:e instanceof Error?e.message:'Update unavailable.'};
 } finally {updateBusy=false;renderUpdate();}
}
function hasUnsentWork():boolean {
 if(sending || stopSending || settingsDirty || commandSending || [...filesByChannel.values()].some(files=>files.length>0) || [...drafts.values()].some(text=>text.trim().length>0))return true;
 try {for(let i=0;i<sessionStorage.length;i++){const key=sessionStorage.key(i);if(key && (key.startsWith('draft-text:') || key.startsWith('pending-id:') || key.startsWith('draft-reply:')) && sessionStorage.getItem(key)?.trim())return true;}}
 catch{return true;}
 return false;
}
function expireUpdate():boolean {
 if(!expectedUpdate || Date.now()-expectedUpdate.started<updateWaitLimit || (updateDeferred && hasUnsentWork()))return false;
 expectedUpdate=null;updateRestarting=false;updateDeferred=false;updateUnconfirmed=true;
 updateInfo={available:true,current:updateInfo?.current||'',version:'',state:'error',error:'Update could not be confirmed. Check the running version.'};return true;
}
function renderUpdate():void{const el=app.querySelector<HTMLElement>('#update-panel');if(!el)return;const u=updateInfo;if(!u?.available){el.hidden=true;return;}el.hidden=false;const action=u.state==='available'?'download':u.state==='ready'?'apply':u.state==='idle'||u.state==='current'||u.state==='error'?'check':'';el.innerHTML=`<strong>App updates</strong><small>${esc(u.state==='deferred'?'Update ready. Finish or clear drafts to reload.':u.state==='restarting'?'Restarting…':u.state==='downloading'?'Downloading…':u.state==='checking'?'Checking…':u.state==='ready'?`Version ${u.version} is ready`:u.state==='available'?`Version ${u.version} available`:u.state==='current'?'Up to date':u.state==='error'?(u.error||'Update failed'):`Current: ${u.current}`)}</small>${action?`<button type="button" data-update-action="${action}">${action==='download'?'Download':action==='apply'?'Restart to update':'Check for updates'}</button>`:''}`;el.querySelector<HTMLButtonElement>('[data-update-action]')?.addEventListener('click',e=>{const action=(e.currentTarget as HTMLButtonElement).dataset.updateAction!;void updateRequest(action);});}
type WeeklyUsage={known:boolean;remaining:number|null;reset:number|null;observed:number|null;enabled:boolean;warn:number;reserve:number;paused:boolean};
let weeklyUsage:WeeklyUsage|null=null;let weeklyError='';let settingsOpen=true;let guardEnabled=false;let guardWarn='40';let guardReserve='30';let settingsDirty=false;let savingGuard=false;
function numberHeader(r:Response,key:string):number|null {const raw=r.headers.get(key);if(raw===null || raw.trim()==='')return null;const n=Number(raw);return Number.isFinite(n)?n:null;}
async function loadWeeklyUsage(body?:URLSearchParams):Promise<void> {
 try {
  const r=await fetch(body?'/api/usage/settings':'/api/usage',{method:body?'POST':'GET',...(body?{body}:{}),cache:'no-store'});
  if(!r.ok)throw new Error(body?(await r.text()) || 'Could not save settings.':'Weekly allowance unavailable.');
  const remaining=numberHeader(r,'X-AgentChat-Weekly-Remaining');const warn=numberHeader(r,'X-AgentChat-Guard-Warn');const reserve=numberHeader(r,'X-AgentChat-Guard-Reserve');const enabled=r.headers.get('X-AgentChat-Guard-Enabled');
  if(warn===null || reserve===null || (enabled!=='true' && enabled!=='false'))throw new Error('Weekly allowance unavailable.');
  weeklyUsage={known:r.headers.get('X-AgentChat-Usage-State')==='known' && remaining!==null,remaining,reset:numberHeader(r,'X-AgentChat-Weekly-Reset'),observed:numberHeader(r,'X-AgentChat-Usage-Observed'),enabled:enabled==='true',warn,reserve,paused:r.headers.get('X-AgentChat-Guard-Paused')==='true'};weeklyError='';
  if(!settingsDirty || body){guardEnabled=weeklyUsage.enabled;guardWarn=String(warn);guardReserve=String(reserve);if(body)settingsDirty=false;}
 } catch(e) {weeklyError=e instanceof Error?e.message:'Weekly allowance unavailable.';if(!body)weeklyUsage=null;}
 render();
}
function weeklyLabel():string {return weeklyUsage?.known?`Weekly · ${weeklyUsage.remaining}% left`:'Weekly · unavailable';}
type SetupState='checking'|'downloading'|'ready'|'error'|'not_required';
let pendingSourceReload:'managed'|'override'|null=null;
let setupState:SetupState='checking';let setupSource='';let setupVersion='';let setupError='';let setupReady=false;let setupBlocked=false;let setupBusy=false;let setupCanRetry=false;let setupCanManaged=false;let setupCanOverride=false;let setupSwitchError='';let dataStarted=false;
function setupMarkup():string {
 const label=setupState==='checking'?'Preparing Codex…':setupState==='downloading'?'Downloading Codex…':setupState==='ready'?'Codex is ready':setupState==='not_required'?'Codex is not required in this mode':'Codex setup could not finish';
 return `<section class="setup-section" aria-label="Codex setup"><h2>Codex setup</h2><p role="status">${esc(label)}${setupVersion?` · Managed version ${esc(setupVersion)}`:''}</p>${setupSource==='override'?'<p>External CLI selected</p>':setupSource==='managed'?'<p>Managed CLI selected</p>':''}${setupSource==='override' && setupCanManaged?`<button type="button" id="setup-source" data-source="managed" ${setupBusy?'disabled':''}>Use managed CLI</button>`:setupSource==='managed' && setupCanOverride?`<button type="button" id="setup-source" data-source="override" ${setupBusy?'disabled':''}>Use external CLI</button>`:''}${setupSwitchError?`<p class="settings-error" role="alert">${esc(setupSwitchError)}</p>`:''}${setupState==='error' && setupError?`<p class="settings-error">${esc(setupError)}</p>`:''}${setupState==='error' && setupCanRetry?'<button type="button" id="setup-retry">Retry setup</button>':''}</section>`;
}
async function checkSetup(retry=false, source?:'managed'|'override'):Promise<void>{
 if(setupBusy)return;setupBusy=true;
 const priorSource=setupSource;
 const previous=[setupState,setupSource,setupVersion,setupError,setupReady,setupCanRetry,setupCanManaged,setupCanOverride].join('|');
 if(source){setupSwitchError='';render();}
 else if(retry){setupState='checking';setupCanRetry=false;render();}
 try {
  const r=await fetch(source?'/api/codex-setup/source':retry?'/api/codex-setup/retry':'/api/codex-setup',{method:source||retry?'POST':'GET',...(source?{body:new URLSearchParams({source})}:{}),cache:'no-store',signal:AbortSignal.timeout(8000)});
  if(!r.ok){if(source){setupSwitchError=r.status===409?'Cannot switch CLI while work is unsettled or the choice is unavailable.':'Could not switch CLI. Please try again.';return;}throw new Error('Waiting for the Agent Chat service.');}
  const next=r.headers.get('X-AgentChat-Codex-State');
  if(next!=='downloading' && next!=='ready' && next!=='error' && next!=='not_required')throw new Error('Waiting for the Agent Chat service.');
  setupState=next;setupSource=r.headers.get('X-AgentChat-Codex-Source')||'';setupCanManaged=r.headers.get('X-AgentChat-Codex-Can-Select-Managed')==='true';setupCanOverride=r.headers.get('X-AgentChat-Codex-Can-Select-Override')==='true';setupVersion=r.headers.get('X-AgentChat-Codex-Managed-Version')||'';
  if(source && r.status===202 && source!==priorSource && setupSource===source)pendingSourceReload=source;
  else if(pendingSourceReload && setupSource!==pendingSourceReload)pendingSourceReload=null;
  setupCanRetry=next==='error';setupError=next==='error'?(await r.text()):'';
  setupReady=next==='ready'||next==='not_required';
  if(!setupReady){setupBlocked=true;settingsOpen=true;}
  else {if(!setupBlocked && !source && !pendingSourceReload)settingsOpen=false;if(!dataStarted){dataStarted=true;void refresh();void updateRequest();void loadWeeklyUsage();}}
 } catch {if(source)setupSwitchError='Could not confirm the CLI selection. Check setup before trying again.';else {setupState='error';setupError='Waiting for the Agent Chat service.';setupCanRetry=false;setupReady=false;setupBlocked=true;settingsOpen=true;}}
 finally{setupBusy=false;const current=[setupState,setupSource,setupVersion,setupError,setupReady,setupCanRetry,setupCanManaged,setupCanOverride].join('|');if(source || retry || previous!==current)render();if(pendingSourceReload && setupState==='ready' && setupSource===pendingSourceReload && !hasUnsentWork() && !expectedUpdate && !updateRestarting && !updateBusy && !reloadTriggered){reloadTriggered=true;pendingSourceReload=null;window.location.reload();}}
}
function settingsMarkup():string {const u=weeklyUsage;return `<section class="settings-page" aria-label="Settings" ${settingsOpen?'':'hidden'}><div class="settings-content">${setupMarkup()}${setupReady?`<div id="update-panel" class="update-panel" hidden></div><h2>Weekly allowance</h2><p>${esc(weeklyLabel())}${u?.paused?' · New work held':''}</p>${u?.reset?`<p class="settings-muted">Resets ${esc(new Date(u.reset*1000).toLocaleString())}</p>`:''}${weeklyError?`<p role="alert" class="settings-error">${esc(weeklyError)}</p>`:''}${!u?'<p class="settings-muted">Allowance settings are unavailable right now.</p>':`<form id="guard-settings"><label class="guard-switch"><input type="checkbox" name="enabled" ${guardEnabled?'checked':''}> Pause new work when the weekly reserve is reached</label><label>Warn when remaining allowance is at or below <span><input name="warn" type="number" min="0" max="100" step="1" value="${esc(guardWarn)}" required> %</span></label><label>Pause new work when remaining allowance is at or below <span><input name="reserve" type="number" min="0" max="100" step="1" value="${esc(guardReserve)}" required> %</span></label><p class="settings-muted">When enabled, queued work waits if allowance information is unavailable or reaches the reserve. It resumes when enough allowance is available; work already running can finish.</p><button type="submit" ${savingGuard?'disabled':''}>${savingGuard?'Saving…':'Save settings'}</button></form>`}`:''}</div></section>`;}
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
let selectionEpoch=0;
function rememberSelection() { selectionEpoch++;try { sessionStorage.setItem(selectionRecipientKey,recipient);sessionStorage.setItem(selectionProjectKey,selectedProjectId); } catch { /* Storage may be unavailable. */ } }
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
const narrowLayout=window.matchMedia('(max-width: 760px)');
let railCollapsed=false; try {railCollapsed=localStorage.getItem(railKey)==='true';} catch {} let loading = true; let error = ''; let sending = false; let stopSending = false; let sendTarget = ''; let mobileNav = false;
type Mention = {id:string; label:string; start:number; end:number};
const mentionDrafts = new Map<string,Mention[]>();
let slashDismissed='';let slashIndex=0;
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
const fileErrors = new Map<string,string>();
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
const missingPendingFiles = (target:string) => !!pendingId(target) && sessionStorage.getItem('pending-files:'+target)==='yes' && !draftFiles(target).length;
let composeNotice='';
let attachmentMenuTarget='';let filePickerTarget='';
const filePicker=document.createElement('input');
filePicker.id='file-input';filePicker.type='file';filePicker.multiple=true;filePicker.hidden=true;
document.body.append(filePicker);
function addDraftFiles(target:string,incoming:FileList|File[]):void {
 if(pendingId(target)&&!missingPendingFiles(target))return;
 const files=[...draftFiles(target),...Array.from(incoming)];
 if(files.length>32 || files.some(f=>f.size>8*1024*1024) || files.reduce((n,f)=>n+f.size,0)>64*1024*1024){fileErrors.set(target,'Files were not added. Choose up to 32 files, each at most 8 MiB and 64 MiB total.');render();return;}
 fileErrors.delete(target);setDraftFiles(target,files);composeNotice='';error='';render();
}
filePicker.addEventListener('change',()=>{const target=filePickerTarget || recipient;const files=Array.from(filePicker.files || []);filePicker.value='';filePickerTarget='';if(files.length)addDraftFiles(target,files);});
filePicker.addEventListener('cancel',()=>{filePickerTarget='';});

const replyKey = (target:string) => 'draft-reply:'+target;
const pendingReply = (target:string) => sessionStorage.getItem('pending-reply:'+target) || '';
const draftReply = (target:string) => sessionStorage.getItem(replyKey(target)) || '';
let copyFeedback = '';
const openScopes=new Set<string>();
const scopeDetails=new Map<string,{loading:boolean;text?:string;error?:string}>();
let stopReceipt = '';
const roomFor = (target:string) => target.startsWith('group:') ? target : 'dm:'+['operator',target].sort().join(':');
const unread = (target:string) => state.messages.filter(m=>m.room_id===roomFor(target) && m.sender!=='operator' && !m.operator_read_at).length;
const unreadBadge = (target:string) => unread(target) ? `<span class="unread" aria-label="${unread(target)} unread messages">${unread(target)}</span>` : '';
const quote = (id:string, messages:Message[]) => {const m=messages.find(x=>x.id===id);return m ? `<strong>${esc(name(m.sender))}</strong><span>${esc(Array.from(m.text).slice(0,160).join(''))}</span>` : '<span>Earlier message unavailable</span>';};
const retryReady = new Set<string>();
const conflicts = new Set<string>();
let renderedRecipient = ''; let scrollByRecipient = new Map<string,number>();
type ScrollAnchor = {target:string; epoch:number; width:number; height:number; bottom:boolean; id?:string; offset:number};
let resizeAnchor:ScrollAnchor | undefined;
function rememberScroll(scroll:HTMLElement):void {
 const top=scroll.getBoundingClientRect().top;
 const item=Array.from(scroll.querySelectorAll<HTMLElement>('[data-message-id]')).find(el=>el.getBoundingClientRect().bottom>top);
 resizeAnchor={target:recipient,epoch:selectionEpoch,width:window.innerWidth,height:window.innerHeight,bottom:scroll.scrollHeight-scroll.clientHeight-scroll.scrollTop<3,id:item?.dataset.messageId,offset:item ? item.getBoundingClientRect().top-top : 0};
}
function restoreResizeAnchor():void {
 const saved=resizeAnchor, scroll=app.querySelector<HTMLElement>('#chat-scroll');
 if(!saved || !scroll || saved.target!==recipient || saved.epoch!==selectionEpoch || settingsOpen)return;
 if(saved.bottom)scroll.scrollTop=scroll.scrollHeight;
 else if(saved.id){const item=Array.from(scroll.querySelectorAll<HTMLElement>('[data-message-id]')).find(el=>el.dataset.messageId===saved.id);if(item)scroll.scrollTop+=item.getBoundingClientRect().top-scroll.getBoundingClientRect().top-saved.offset;}
 scrollByRecipient.set(recipient,scroll.scrollTop);rememberScroll(scroll);
}
let sendScrollIntent:{target:string;id:string;epoch:number;confirmed:boolean}|null=null;
function confirmSendScroll(target:string,id:string):void {if(sendScrollIntent?.target===target && sendScrollIntent.id===id)sendScrollIntent.confirmed=true;}
const esc = (s:unknown) => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]!));
const approvalBusy=new Set<string>();
const approvalErrors=new Map<string,string>();
const approvalDrafts=new Map<string,string>();
function linkedApproval(m:Message):Approval|undefined {
 if(recipient!=='root' || m.room_id!==roomFor('root') || m.sender!=='root' || m.recipient!=='operator' || !m.approval_id) return undefined;
 return state.approvals.find(a=>a.id===m.approval_id);
}
function approvalMarkup(m:Message):string {
 const approval=linkedApproval(m);
 if(!approval) return '';
 if(approval.status==='approved' || approval.status==='denied') return `<div class="approval-status" role="status">${approval.status==='approved'?'Approved':'Denied'}${approval.decision_note?`<p>${esc(approval.decision_note)}</p>`:''}</div>`;
 if(approval.status!=='pending') return '';
 const disabled=approvalBusy.has(approval.id)?'disabled':'';
 return `<div class="approval-controls" aria-label="Approval decision"><label class="approval-note">Optional note<textarea data-approval-note="${esc(approval.id)}" maxlength="4096" rows="2" ${disabled}>${esc(approvalDrafts.get(approval.id)||'')}</textarea></label><div class="approval-actions"><button type="button" data-approval="${esc(approval.id)}" data-decision="approved" ${disabled}>Approve</button><button type="button" data-approval="${esc(approval.id)}" data-decision="denied" ${disabled}>Deny</button></div>${approvalErrors.has(approval.id)?`<span role="alert">${esc(approvalErrors.get(approval.id))}</span>`:''}</div>`;
}
const taskDiagnostics = (task:Task) => {
 const attempts=state.jobs.filter(j=>j.task_id===task.id && j.project_id===task.project_id && j.status==='uncertain' && j.diagnostic?.trim());
 const current=attempts.filter(j=>!j.superseded).map(j=>`<div class="task-diagnostic"><span>Outcome unknown. Ask Root to check before trying again.</span><details data-diagnostic="task:${esc(j.id)}" ${openDetails.has('task:'+j.id)?'open':''}><summary>Details</summary><p>${esc(j.diagnostic!.trim())}</p></details></div>`).join('');
 const past=attempts.filter(j=>j.superseded);
 const history=past.length?`<details class="task-history" data-diagnostic="task-history:${esc(task.id)}" ${openDetails.has('task-history:'+task.id)?'open':''}><summary>Past attempts (${past.length})</summary><p>Previous effects remain unknown.</p>${past.map(j=>`<p>${esc(j.diagnostic!.trim())}</p>`).join('')}</details>`:'';
 return current+history;
};
const safeURL = (a:Attachment) => `/api/attachments/${encodeURIComponent(a.id)}`;
const name = (id:string) => id === 'root' ? 'Root' : state.projects.find(p=>'group:'+p.id===id)?.name ? `${state.projects.find(p=>'group:'+p.id===id)!.name} project chat` : state.agents.find(a=>a.id===id)?.name || id;
const project = () => state.projects.find(p=>p.id===selectedProjectId);
const inGroup = () => !!selectedProjectId && recipient==='group:'+selectedProjectId;
const projectAgents = (id:string) => state.agents.filter(a=>a.project_id===id);
const agentChoices = (id:string) => {
 const agents=projectAgents(id), past=agents.filter(a=>a.one_shot_retired), current=agents.filter(a=>!a.one_shot_retired);
 const button=(a:Agent)=>`<button type="button" class="agent-choice ${recipient===a.id?'selected':''}" data-agent="${esc(a.id)}" aria-label="Chat with ${esc(a.name)}, ${esc(memberActivity(a).label)}, ${unread(a.id)} unread"><span>${esc(a.name)}</span><span class="activity-dot ${memberActivity(a).active?'busy':''}" aria-hidden="true" title="${esc(memberActivity(a).label)}"></span>${unreadBadge(a.id)}</button>`;
 const key='past-agents:'+id;return current.map(button).join('')+(past.length?`<details data-diagnostic="${esc(key)}" ${openDetails.has(key)?'open':''}><summary>Past one-shot workers (${past.length}) · ${past.reduce((n,a)=>n+unread(a.id),0)} unread</summary>${past.map(button).join('')}</details>`:'');
};
const projectUnread = (id:string) => unread('group:'+id)+projectAgents(id).reduce((count,a)=>count+unread(a.id),0);
const projectBadge = (id:string) => projectUnread(id) ? `<span class="unread" aria-label="${projectUnread(id)} unread messages">${projectUnread(id)}</span>` : '';
const readable = (m:Message) => m.room_id===roomFor(recipient) && m.sender!=='operator';
const date = (at:string) => {const d = new Date(at); return Number.isNaN(d.getTime()) ? '' : d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});};
function attachment(a:Attachment) { const url=safeURL(a); return `<div class="attachment-row"><a class="attachment-download" data-download="${esc(a.id)}" href="${url}?download=1" download="${esc(a.name)}" aria-label="Download ${esc(a.name)}" title="Download"><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M12 3v11"/><path d="m8 10 4 4 4-4"/><path d="M5 17v2a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2"/></svg></a><button type="button" class="attachment attachment-tile" data-attachment-preview="${esc(a.id)}" aria-label="Preview ${esc(a.name)}">${a.media_type?.startsWith('image/') ? `<img src="${url}" alt="" loading="lazy">` : '<span class="file-icon" aria-hidden="true">▤</span>'}<span><strong>${esc(a.name)}</strong><small>${esc(a.media_type || 'Attachment')} · Preview</small></span></button></div>`; }
function attachments(items:Attachment[]):string {
 const images=items.filter(isPreviewableImage);
 if(images.length<2)return items.map(attachment).join('');
 return `<div class="attachment-image-grid">${images.map(attachment).join('')}</div>${items.filter(a=>!isPreviewableImage(a)).map(attachment).join('')}`;
}
function memberActivity(agent:Agent):{label:string;active:boolean} {
 if(loading || stateStale)return {label:'Status unavailable',active:false};
 if(agent.status==='stopped')return {label:'Stopped',active:false};
 const jobs=state.jobs.filter(j=>j.agent_id===agent.id);
 if(jobs.some(j=>j.status==='running'))return {label:'Working',active:true};
 if(jobs.some(j=>j.status==='submitted'))return {label:'Starting',active:false};
 if(jobs.some(j=>j.status==='pending'))return {label:'Queued',active:false};
 if(jobs.at(-1)?.status==='uncertain' || state.tasks.some(t=>t.owner_id===agent.id && t.status==='blocked'))return {label:'Needs attention',active:false};
 if(jobs.at(-1)?.status==='completed' && jobs.at(-1)?.diagnostic?.trim())return {label:'Failed',active:false};
 return {label:'Idle',active:false};
}
function agentActivity(target:string): {label:string; detail?:string; guidance?:string; key?:string; busy:boolean} {
 if(loading) return {label:'Loading',busy:false};
 if(stateStale) return {label:'Status unavailable',busy:false};
 const groupProject=state.projects.find(p=>'group:'+p.id===target);
 const jobs=groupProject ? state.jobs.filter(j=>j.project_id===groupProject.id && j.kind==='project_message') : state.jobs.filter(j=>j.agent_id===target && j.kind!=='project_message');
 const newest=(status:string)=>[...jobs].reverse().find(j=>j.status===status);
 const current=newest('running') || newest('submitted') || newest('pending') || (jobs.at(-1)?.status==='uncertain' ? jobs.at(-1) : undefined);
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
 lastActivityMarkup=renderProjectActivity(state,projectId,roomFor(recipient),openDetails,id=>{const agent=state.agents.find(a=>a.id===id);return agent?memberActivity(agent):{label:'Status unavailable',active:false};},taskDiagnostics);
 return lastActivityMarkup;
}
function bindPanelDetails():void {
 app.querySelectorAll<HTMLDetailsElement>('details[data-diagnostic]').forEach(details=>{const key=details.dataset.diagnostic!;details.ontoggle=()=>{if(details.open)openDetails.add(key);else openDetails.delete(key);};});
}
function refreshActivity():void {
 const p=project();const current=app.querySelector<HTMLElement>('.project-activity');
 if(!p || !current)return;
 const next=renderProjectActivity(state,p.id,roomFor(recipient),openDetails,id=>{const agent=state.agents.find(a=>a.id===id);return agent?memberActivity(agent):{label:'Status unavailable',active:false};},taskDiagnostics);
 if(next!==lastActivityMarkup){current.outerHTML=next;lastActivityMarkup=next;bindPanelDetails();}
}
const panelToggleIcon = `<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><rect x="3" y="4" width="18" height="16" rx="2"></rect><path d="M15 4v16"></path><path class="toggle-chevron" d="m9 9 3 3-3 3"></path></svg>`;
function render() {
 if(attachmentMenuTarget && (attachmentMenuTarget!==recipient || settingsOpen || !setupReady))attachmentMenuTarget='';
 const attachmentOpen=attachmentMenuTarget===recipient;
 const activeAction = document.activeElement instanceof HTMLElement && app.contains(document.activeElement) ? document.activeElement : null;
 const actionFocus = activeAction ? (['reply','copy','attachmentPreview','download','scope'] as const).map(key=>({key,value:activeAction.dataset[key]})).find(x=>x.value) : undefined;
 const attachmentFocus=activeAction?.id==='upload-file'?'upload':activeAction?.id==='attach-files'?'toggle':'';
 const actionMessage = activeAction?.closest<HTMLElement>('[data-message-id]')?.dataset.messageId;
 const mentionFocus=activeAction?.dataset.mentionChoice ? 'choice:'+activeAction.dataset.mentionChoice : activeAction?.dataset.unmention ? 'remove:'+activeAction.dataset.unmention : '';
 const oldScroll = app.querySelector<HTMLElement>('#chat-scroll');
 const oldPanelTop = app.querySelector<HTMLElement>('#work-panel-content')?.scrollTop || 0;
 const oldContentTop = app.querySelector<HTMLElement>('.content')?.scrollTop || 0;
 const same = renderedRecipient === recipient;
 const oldTop = oldScroll?.scrollTop || 0;
 const follow = !oldScroll || oldScroll.scrollHeight - oldScroll.clientHeight - oldTop < 60;
 const focused = document.activeElement?.id === 'message';
 const focusedApproval = document.activeElement instanceof HTMLTextAreaElement ? document.activeElement.dataset.approvalNote : undefined;
 const approvalSelection = focusedApproval && document.activeElement instanceof HTMLTextAreaElement ? [document.activeElement.selectionStart,document.activeElement.selectionEnd] : undefined;
 const detailsFocused = document.activeElement instanceof HTMLElement && document.activeElement.tagName === 'SUMMARY' ? document.activeElement.parentElement?.getAttribute('data-diagnostic') : null;
 const toggleFocused = document.activeElement?.id === 'work-toggle';
 const oldInput = app.querySelector<HTMLTextAreaElement>('#message');
 const selection = focused ? [oldInput?.selectionStart || 0, oldInput?.selectionEnd || 0] : null;
 const p=project(); const group=inGroup(); const msgs=state.messages.filter(m => m.room_id===roomFor(recipient)); const agents=p?projectAgents(p.id):[]; const subagents=agents.filter(a=>!!a.parent_id && agents.some(parent=>parent.id===a.parent_id));
 const groupUnread = group ? msgs.filter(m=>readable(m) && !m.operator_read_at) : [];
 const firstUnread = groupUnread[0];
 const latestRead = group ? msgs.filter(m=>readable(m) && !!m.operator_read_at).at(-1) : undefined;
 const baseActivity=agentActivity(recipient);
 const groupRunning=!loading && !stateStale && group && p ? new Set(state.jobs.filter(j=>j.project_id===p.id && j.status==='running' && agents.some(a=>a.id===j.agent_id)).map(j=>j.agent_id)).size : 0;
 const activity=group ? (groupRunning ? {label:`${groupRunning} ${groupRunning===1?'agent':'agents'} running`,busy:true,active:true} : baseActivity.guidance ? {...baseActivity,active:false} : {label:'',busy:false,active:false}) : (()=>{const active=!loading && !stateStale && state.jobs.some(j=>j.agent_id===recipient && j.status==='running');return {...baseActivity,label:active && baseActivity.label==='Idle'?'Working':baseActivity.label,active};})();
 const selectedMentions=group?readMentions(recipient):[];
 const mentionOptions=group && p && selectedMentions.length<32?projectAgents(p.id).filter(a=>(a.role==='po'||a.role==='worker') && !!a.name && !/[\r\n]/.test(a.name) && !selectedMentions.some(m=>m.id===a.id) && a.name.toLocaleLowerCase().includes(mentionQuery.toLocaleLowerCase())):[];
 const mentionOpen=group && mentionPickerTarget===recipient && !pendingId(recipient);
 const slashCommands=recipient==='root'?[{name:'/stop',usage:'/stop <project ID>',description:'Stop project work'}]:group && p?[{name:'/workspace',usage:'/workspace <absolute-directory>',description:'Set the project workspace'},...(state.tasks.some(t=>t.project_id===p.id && t.resolve_hold_eligible)?[{name:'/resolve-hold',usage:'/resolve-hold <task UUID> <decision>',description:'Send a held-task decision privately to the project owner'}]:[]),...(state.tasks.some(t=>t.project_id===p.id && t.continue_po_eligible)?[{name:'/continue',usage:'/continue [task UUID] instruction',description:'Continue with the project owner'}]:[]),...(state.tasks.some(t=>t.project_id===p.id && t.retry_handoff_eligible)?[{name:'/retry-handoff',usage:'/retry-handoff [task UUID]',description:'Retry a blocked handoff'}]:[])]:state.agents.some(a=>a.id===recipient && a.role==='worker')?[{name:'/previews',usage:'/previews on|off',description:'Control direct image previews'}]:[];
 const slashText=channelDraft(recipient);const slashPrefix=/^\/[a-z-]*$/.test(slashText)?slashText:'';
 const slashOptions=slashPrefix && slashDismissed!==recipient+':'+slashPrefix && !pendingId(recipient)?slashCommands.filter(c=>c.name.startsWith(slashPrefix)):[];
 slashIndex=Math.max(0,Math.min(slashIndex,slashOptions.length-1));
 const tasks=state.tasks.filter(t=>!p||t.project_id===p.id).slice(-5).reverse();
 app.innerHTML=`<div class="shell"><aside id="conversation-panel" class="sidebar ${mobileNav?'open':''} ${railCollapsed?'rail-collapsed':''}" aria-label="Conversations" ${narrowLayout.matches && !mobileNav?'inert':''}><div class="brand"><span class="brandmark">✳</span><div><b>Agent Chat</b><small>YOUR WORKSPACE</small></div></div><div class="conversation-navigation" ${setupReady?'':'hidden'}><div class="side-heading">CONVERSATIONS</div><button class="conversation ${recipient==='root'?'selected':''}" data-recipient="root" aria-label="Root conversation, ${unread('root')} unread"><span class="avatar root-avatar">✳</span><span><b>Root</b><small>General conversation</small></span>${unreadBadge('root')}<span class="chevron">›</span></button><div class="side-heading projects-heading">PROJECTS <span>${state.projects.length}</span></div><div class="project-list">${state.projects.length ? state.projects.map(x=>`<button class="conversation ${selectedProjectId===x.id?'selected':''}" data-project="${esc(x.id)}" aria-label="${esc(x.name)} project, ${projectUnread(x.id)} unread"><span class="avatar project-avatar">${esc(x.name.slice(0,1).toUpperCase())}</span><span><b>${esc(x.name)}</b><small>${esc(x.status || 'Project')}</small></span>${projectBadge(x.id)}<span class="chevron">›</span></button>${selectedProjectId===x.id?`<div class="project-agents"><div class="agent-count">${projectAgents(x.id).length} agents · ${projectAgents(x.id).filter(a=>!!a.parent_id && projectAgents(x.id).some(parent=>parent.id===a.parent_id)).length} subagents</div>${agentChoices(x.id)}</div>`:''}`).join('') : '<p class="side-empty">Projects will appear here.</p>'}</div></div><div class="sidebar-tools"><button type="button" id="weekly-control" ${setupReady?'':'hidden'} aria-label="${esc(weeklyLabel())}, open settings" aria-describedby="weekly-tooltip"><span aria-hidden="true"><svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.7"><circle cx="12" cy="12" r="8.5"></circle><path d="M12 3.5v8.5h8.5" stroke-linejoin="round"></path><path d="M12 3.5a8.5 8.5 0 0 1 8.5 8.5H12z" fill="currentColor" stroke="none" opacity=".45"></path></svg></span></button><button type="button" id="open-settings" aria-label="Settings"><span aria-hidden="true"><svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"><circle cx="12" cy="12" r="5.5"></circle><circle cx="12" cy="12" r="2"></circle><path d="M12 3.5v3M12 17.5v3M3.5 12h3M17.5 12h3M6 6l2.1 2.1m7.8 7.8L18 18M18 6l-2.1 2.1m-7.8 7.8L6 18"></path></svg></span></button></div></aside><main class="main"><div class="content"><section class="chat" aria-label="Conversation with ${esc(group?p?.name || 'project':name(recipient))}"><header class="channel-header"><button type="button" class="rail-toggle" id="rail-toggle" aria-label="${(narrowLayout.matches ? mobileNav : !railCollapsed)?'Collapse':'Expand'} conversations" aria-expanded="${narrowLayout.matches ? mobileNav : !railCollapsed}" aria-controls="conversation-panel">${panelToggleIcon}</button><h1 tabindex="-1">${settingsOpen?'Settings':esc(group?p?.name || 'Project':name(recipient))}</h1><button id="work-toggle" class="work-toggle" type="button" aria-controls="work-panel-content" aria-expanded="${workPanelVisible}" aria-label="${workPanelVisible ? 'Collapse work panel' : 'Expand work panel'}" title="${workPanelVisible ? 'Collapse work panel' : 'Expand work panel'}" ${settingsOpen?'hidden':''}>${panelToggleIcon}</button></header>${settingsMarkup()}${firstUnread ? `<div class="group-unread-banner" role="status"><button type="button" id="jump-group-read" aria-label="${latestRead ? 'Jump to latest read message' : 'Jump to first unread message'}">${groupUnread.length} unread · since ${esc(date(firstUnread.at))}</button><button type="button" id="mark-group-read">Mark as read</button></div>` : ''}<div class="chat-scroll" id="chat-scroll" ${settingsOpen?'hidden':''}>${recipient==='root' && stopReceipt?`<p class="notice" role="status">${esc(stopReceipt)}</p>`:''}${loading?'<p class="notice" role="status">Loading conversation…</p>':''}${error?`<div class="notice error" role="alert">${esc(error)} <button id="retry">Try again</button></div>`:''}${!loading&&!error&&!msgs.length?'<div class="empty">No messages yet. Start the conversation below.</div>':''}<div class="messages">${msgs.map(m=>(m.id===firstUnread?.id ? `<div class="unread-divider" role="separator" aria-label="First unread message"><span>Unread messages</span></div>` : '') + (m.sender==='service' && m.role==='system' && m.room_id.startsWith('group:') ? `<article class="workflow-notice" data-message-id="${esc(m.id)}" aria-label="Agent Chat workflow notice"><div><strong>Agent Chat</strong><span>${esc(date(m.at))}</span></div><p>${esc(m.text)}</p>${m.notice_detail==='scope_request'?`<button type="button" class="scope-toggle" data-scope="${esc(m.id)}" aria-expanded="${openScopes.has(m.id)}" aria-controls="scope-${esc(m.id)}">${openScopes.has(m.id)?'Hide scope request':'View scope request'}</button><div id="scope-${esc(m.id)}" class="scope-detail" ${openScopes.has(m.id)?'':'hidden'}>${scopeDetails.get(m.id)?.loading?'<span role="status">Loading scope request…</span>':scopeDetails.get(m.id)?.error?`<span role="alert">${esc(scopeDetails.get(m.id)?.error)}</span>`:`<pre>${esc(scopeDetails.get(m.id)?.text || '')}</pre>`}</div>`:''}</article>` : `<article class="message ${linkedApproval(m)?.status==='pending' ? 'pending-approval ' : ''}${group && m.sender!=='operator' && m.sender!=='service' && m.role!=='system' && ((m.mention_ids||[]).includes('operator') || mentionsOperator(m.text) || (!!m.reply_to && state.messages.some(parent=>parent.id===m.reply_to && parent.room_id===m.room_id && parent.sender==='operator')))?'operator-mention':''}" data-message-id="${esc(m.id)}"><div class="message-avatar ${m.sender==='root'?'root-avatar':''}">${esc(name(m.sender).slice(0,1).toUpperCase())}</div><div class="message-body"><div class="message-meta"><strong>${esc(name(m.sender))}</strong>${group?`<span>→ ${esc(name(m.recipient))}</span>`:''}<span>${esc(date(m.at))}</span></div>${m.reply_to?`<blockquote class="reply-quote">${quote(m.reply_to,msgs)}</blockquote>`:''}<div class="message-text" data-markdown-id="${esc(m.id)}">${esc(m.text)}</div><div class="message-actions" role="group" aria-label="Message actions"><button type="button" data-reply="${esc(m.id)}" aria-label="Reply to message from ${esc(name(m.sender))}" title="Reply" ${pendingId(recipient)?'disabled':''}><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="M9 7 4 12l5 5"/><path d="M4 12h9a7 7 0 0 1 7 7"/></svg></button><button type="button" data-copy="${esc(m.id)}" aria-label="Copy message from ${esc(name(m.sender))}" title="Copy"><svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><rect x="8" y="8" width="11" height="12" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h2"/></svg></button></div>${approvalMarkup(m)}${m.attachments?.length?`<div class="attachments">${attachments(m.attachments)}</div>`:''}</div></article>`)).join('')}</div></div>${commandReceipts.get(recipient)?`<p class="notice command-receipt ${commandReceipts.get(recipient)?.fading?'fading':''}" role="status">${esc(commandReceipts.get(recipient)?.text)}</p>`:''}<form class="composer" id="composer" ${settingsOpen?'hidden':''} aria-busy="${(sending&&recipient===sendTarget)||(stopSending&&recipient==='root')}"><div class="composer-heading"><label for="message">${group?`Message ${esc(p?.name || 'project')} chat`:`Message ${esc(p?name(recipient):'Root')}`}</label>${activity.label?`<div class="agent-activity" role="status" aria-live="polite"><span class="activity-dot ${activity.active?'busy':''}" aria-hidden="true"></span>${esc(activity.label)}</div>`:''}</div>${group&&selectedMentions.length?`<div class="mention-selected" aria-label="Selected mentions">${selectedMentions.map(m=>`<button type="button" data-unmention="${esc(m.id)}" aria-label="Remove mention ${esc(m.label)}" ${pendingId(recipient)?'disabled':''}>@${esc(m.label)} ×</button>`).join('')}</div>`:''}${slashOptions.length?`<div class="mention-options" role="listbox" aria-label="Slash commands">${slashOptions.map((c,i)=>`<button type="button" role="option" aria-selected="${i===slashIndex}" data-slash-choice="${esc(c.name)}"><strong>${esc(c.usage)}</strong><small>${esc(c.description)}</small></button>`).join('')}</div>`:''}${mentionOpen?`<div id="mention-options" class="mention-options" role="group" aria-label="Choose a project agent">${mentionOptions.length?mentionOptions.map(a=>`<button type="button" data-mention-choice="${esc(a.id)}" aria-label="Mention ${esc(a.name)}, ${esc(a.role)}, ${esc(a.id)}"><strong>${esc(a.name)}</strong><small>${esc(a.role)} · ${esc(a.id)}</small></button>`).join(''):selectedMentions.length>=32?'<span>Up to 32 agents per message.</span>':'<span>No matching agents.</span>'}</div>`:''}${activity.guidance?`<div class="status-guidance"><span>${esc(activity.guidance)}</span>${activity.detail?`<details id="status-details" data-diagnostic="status:${esc(activity.key || '')}" ${activity.key && openDetails.has('status:'+activity.key)?'open':''}><summary id="status-details-summary">Details</summary><p>${esc(activity.detail)}</p></details>`:''}</div>`:''}${(pendingId(recipient)?pendingReply(recipient):draftReply(recipient))?`<div class="compose-reply"><div><small>Replying to</small>${quote(pendingId(recipient)?pendingReply(recipient):draftReply(recipient),msgs)}</div><button type="button" id="cancel-reply" aria-label="Cancel reply" ${pendingId(recipient)?'disabled':''}>×</button></div>`:''}<div role="status" aria-live="polite" class="copy-feedback">${esc(copyFeedback)}</div><div class="compose-box">${fileErrors.has(recipient)?`<p class="notice" role="alert">${esc(fileErrors.get(recipient)!)}</p>`:''}${draftFiles(recipient).length?`<div class="file-list" role="region" aria-label="Attachments" tabindex="0">${draftFiles(recipient).map((f,i)=>draftFileCard(f,i,!!pendingId(recipient))).join('')}</div>`:''}<div class="compose-entry"><div class="attachment-control"><button type="button" id="attach-files" class="attach-button" aria-label="Add attachment" aria-expanded="${attachmentOpen}" aria-controls="attachment-menu" title="Add attachment" ${pendingId(recipient)&&!missingPendingFiles(recipient)?'disabled':''}>＋</button><div id="attachment-menu" class="attachment-menu" ${attachmentOpen?'':'hidden'}><button type="button" id="upload-file">Upload a file</button></div></div><span class="sr-only" id="composer-help">Press Enter to send or retry delivery. Press Shift and Enter for a new line.</span><textarea id="message" aria-describedby="composer-help" enterkeyhint="send" rows="1" placeholder="${group?'Write a shared message…':'Write a message…'}" ${group&&pendingId(recipient)?'readonly':''}>${esc(group&&pendingId(recipient)?pendingText(recipient):pendingId(recipient) && !draft() ? pendingText(recipient) : draft())}</textarea></div><span class="sr-only" role="status" aria-live="polite">${composeNotice || (pendingId(recipient)?conflicts.has(recipient)?'The message could not be retried because its ID conflicts with another request.':retryReady.has(recipient)?'Press Enter to retry the saved message.':'Checking the saved message.':'')}</span></div></form></section><aside id="work-panel" class="context ${workPanelVisible ? '' : 'collapsed'}" aria-label="Work context" ${settingsOpen?'hidden':''} ${workPanelVisible ? '' : 'inert'}><div id="work-panel-content" class="context-body">${p?activityMarkup(p.id):`<div class="context-top"><span class="eyebrow">AT A GLANCE</span><h2>Your work</h2><p>Recent work across your workspace.</p></div><div class="context-heading"><h3>Recent work</h3><span>${tasks.length}</span></div>${tasks.length?tasks.map(t=>`<div class="task"><span class="task-dot"></span><div><strong>${esc(t.title)}</strong><small>${esc(t.status)} · ${esc(name(t.owner_id))}</small>${taskDiagnostics(t)}</div></div>`).join(''):'<p class="context-empty">No recent work to show yet.</p>'}`}</div></aside></div></main></div><div id="weekly-tooltip" class="weekly-tooltip" role="tooltip" hidden>${esc(weeklyLabel())}</div>`;
 renderUpdate();
 app.querySelector<HTMLButtonElement>('#setup-retry')?.addEventListener('click',()=>void checkSetup(true));
 app.querySelector<HTMLButtonElement>('#setup-source')?.addEventListener('click',e=>void checkSetup(false,(e.currentTarget as HTMLButtonElement).dataset.source as 'managed'|'override'));
 const openSettings=()=>{selectionEpoch++;const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll)scrollByRecipient.set(recipient,scroll.scrollTop);settingsOpen=true;mobileNav=false;settingsDirty=false;if(weeklyUsage){guardEnabled=weeklyUsage.enabled;guardWarn=String(weeklyUsage.warn);guardReserve=String(weeklyUsage.reserve);}readReceipts.contextChanged();render();app.querySelector<HTMLElement>('.channel-header h1')?.focus();};
 app.querySelector<HTMLButtonElement>('#mark-group-read')?.addEventListener('click', event => { (event.currentTarget as HTMLButtonElement).disabled=true; void readReceipts.markCurrentRoomRead(); });
 app.querySelector<HTMLButtonElement>('#jump-group-read')?.addEventListener('click', () => {
  const scroll=app.querySelector<HTMLElement>('#chat-scroll');
  const target=latestRead || firstUnread;
  if (!scroll || !target) return;
  const element=Array.from(scroll.querySelectorAll<HTMLElement>('[data-message-id]')).find(el=>el.dataset.messageId===target.id);
  if (!element) return;
  readReceipts.pauseForJump();
  scroll.scrollTop += element.getBoundingClientRect().top-scroll.getBoundingClientRect().top;
  scrollByRecipient.set(recipient,scroll.scrollTop);
  rememberScroll(scroll);
 });
 app.querySelector<HTMLButtonElement>('#open-settings')!.onclick=openSettings;
 const weeklyButton=app.querySelector<HTMLButtonElement>('#weekly-control')!;
 weeklyButton.onclick=openSettings;
 const tooltip=app.querySelector<HTMLElement>('#weekly-tooltip')!;
 const showWeekly=()=>{const rect=weeklyButton.getBoundingClientRect();tooltip.style.left=`${Math.min(rect.right+8,window.innerWidth-180)}px`;tooltip.style.top=`${Math.max(8,rect.top)}px`;tooltip.hidden=false;};
 const hideWeekly=()=>{tooltip.hidden=true;};
 weeklyButton.addEventListener('mouseenter',showWeekly);weeklyButton.addEventListener('mouseleave',hideWeekly);weeklyButton.addEventListener('focus',showWeekly);weeklyButton.addEventListener('blur',hideWeekly);
 const guardForm=app.querySelector<HTMLFormElement>('#guard-settings');
 guardForm?.addEventListener('input',()=>{const form=new FormData(guardForm);guardEnabled=form.has('enabled');guardWarn=String(form.get('warn')||'');guardReserve=String(form.get('reserve')||'');settingsDirty=true;});
 guardForm?.addEventListener('submit',e=>{e.preventDefault();if(savingGuard)return;savingGuard=true;const body=new URLSearchParams({enabled:String(guardEnabled),warn_percent:guardWarn,reserve_percent:guardReserve});void loadWeeklyUsage(body).finally(()=>{savingGuard=false;render();});render();});
 app.querySelectorAll<HTMLElement>('[data-markdown-id]').forEach(el=>{const message=msgs.find(m=>m.id===el.dataset.markdownId);if(message)renderMessageMarkdown(el,message.text,['operator','Root',...state.agents.filter(a=>!message.project_id || a.project_id===message.project_id).map(a=>a.name)]);});
 app.querySelectorAll<HTMLButtonElement>('[data-recipient]').forEach(b=>b.onclick=()=>{const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll&&!settingsOpen)scrollByRecipient.set(recipient,scroll.scrollTop);if(settingsOpen)renderedRecipient='';mentionPickerTarget='';mentionQuery='';settingsOpen=false;settingsDirty=false;recipient=b.dataset.recipient!;selectedProjectId='';rememberSelection();mobileNav=false;readReceipts.contextChanged();const content=app.querySelector<HTMLElement>('.content');if(content)content.scrollTop=0;render();});
 app.querySelectorAll<HTMLButtonElement>('[data-project]').forEach(b=>b.onclick=()=>{const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll&&!settingsOpen)scrollByRecipient.set(recipient,scroll.scrollTop);if(settingsOpen)renderedRecipient='';mentionPickerTarget='';mentionQuery='';settingsOpen=false;settingsDirty=false;selectedProjectId=b.dataset.project!;recipient='group:'+selectedProjectId;rememberSelection();mobileNav=false;readReceipts.contextChanged();render();});
 app.querySelectorAll<HTMLButtonElement>('[data-agent]').forEach(b=>b.onclick=()=>{const a=state.agents.find(x=>x.id===b.dataset.agent && x.project_id===selectedProjectId);if(!a)return;const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(scroll&&!settingsOpen)scrollByRecipient.set(recipient,scroll.scrollTop);if(settingsOpen)renderedRecipient='';settingsOpen=false;settingsDirty=false;recipient=a.id;rememberSelection();mobileNav=false;readReceipts.contextChanged();render();});
 const rail=app.querySelector<HTMLButtonElement>('#rail-toggle')!;
 rail.onclick=()=>{const panel=app.querySelector<HTMLElement>('#conversation-panel')!;let expanded:boolean;if(narrowLayout.matches){mobileNav=!mobileNav;panel.classList.toggle('open',mobileNav);panel.inert=!mobileNav;expanded=mobileNav;readReceipts.contextChanged();}else{railCollapsed=!railCollapsed;panel.classList.toggle('rail-collapsed',railCollapsed);expanded=!railCollapsed;try{localStorage.setItem(railKey,String(railCollapsed));}catch{}}rail.setAttribute('aria-expanded',String(expanded));rail.setAttribute('aria-label',`${expanded?'Collapse':'Expand'} conversations`);};
 const toggle = app.querySelector<HTMLButtonElement>('#work-toggle')!;
 toggle.onclick=()=>{workPanelVisible=!workPanelVisible;try{localStorage.setItem(workPanelKey,workPanelVisible?'visible':'hidden');}catch{};toggle.setAttribute('aria-expanded',String(workPanelVisible));const label=workPanelVisible?'Collapse work panel':'Expand work panel';toggle.setAttribute('aria-label',label);toggle.title=label;const panel=app.querySelector<HTMLElement>('#work-panel')!;panel.classList.toggle('collapsed',!workPanelVisible);panel.inert=!workPanelVisible;};
 if(toggleFocused) toggle.focus({preventScroll:true});
 bindPanelDetails();if(detailsFocused)app.querySelector<HTMLDetailsElement>(`details[data-diagnostic="${CSS.escape(detailsFocused)}"]`)?.querySelector<HTMLElement>('summary')?.focus({preventScroll:true});
 app.querySelectorAll<HTMLButtonElement>('[data-attachment-preview]').forEach(b=>b.onclick=()=>{const message=msgs.find(m=>(m.attachments||[]).some(a=>a.id===b.dataset.attachmentPreview));const a=message?.attachments?.find(x=>x.id===b.dataset.attachmentPreview);if(a){previewAttachment(a,b,message?.attachments||[]);document.querySelector('dialog.attachment-preview')?.addEventListener('close',()=>readReceipts.schedule(),{once:true});readReceipts.schedule();}});
 app.querySelectorAll<HTMLButtonElement>('[data-scope]').forEach(b=>b.onclick=()=>{
  const id=b.dataset.scope!;if(openScopes.has(id)){openScopes.delete(id);render();return;}openScopes.add(id);
  if(scopeDetails.get(id)?.text!==undefined){render();return;}
  scopeDetails.set(id,{loading:true});render();
  void fetch(`/api/workflow-notices/${encodeURIComponent(id)}/scope-request`,{cache:'no-store'}).then(async response=>{
   if(!response.ok)throw new Error('Could not load the scope request.');
   scopeDetails.set(id,{loading:false,text:await response.text()});
  }).catch(()=>scopeDetails.set(id,{loading:false,error:'Could not load the scope request.'})).finally(()=>render());
 });
 app.querySelectorAll<HTMLTextAreaElement>('[data-approval-note]').forEach(input=>input.oninput=()=>{const id=input.dataset.approvalNote;if(id)approvalDrafts.set(id,input.value);});
 if(focusedApproval) { const input=Array.from(app.querySelectorAll<HTMLTextAreaElement>('[data-approval-note]')).find(el=>el.dataset.approvalNote===focusedApproval);if(input && !input.disabled){input.focus({preventScroll:true});if(approvalSelection)input.setSelectionRange(approvalSelection[0],approvalSelection[1]);} }
 app.querySelectorAll<HTMLButtonElement>('[data-approval]').forEach(b=>b.onclick=()=>{
  const id=b.dataset.approval, decision=b.dataset.decision;
  if(!id || (decision!=='approved' && decision!=='denied') || approvalBusy.has(id) || !state.approvals.some(a=>a.id===id && a.status==='pending')) return;
  const note=(approvalDrafts.get(id)||'').trim();
  if(new TextEncoder().encode(note).length>4096){approvalErrors.set(id,'The note must be at most 4096 UTF-8 bytes.');render();return;}
  approvalBusy.add(id);approvalErrors.delete(id);render();
  void (async()=>{
   try {
    const body=note?JSON.stringify({decision,note}):(decision==='approved'?'{"decision":"approved"}':'{"decision":"denied"}');
    const response=await fetch(`/api/approvals/${encodeURIComponent(id)}`,{method:'POST',headers:{'Content-Type':'application/json'},body});
    if(!response.ok) throw new Error('decision failed');
    approvalDrafts.delete(id);
    await refresh();
   } catch { approvalErrors.set(id,'Could not record the decision. Please try again.'); }
   finally { approvalBusy.delete(id);render(); }
  })();
 });
 app.querySelectorAll<HTMLButtonElement>('[data-reply]').forEach(b=>b.onclick=()=>{
  const message=msgs.find(m=>m.id===b.dataset.reply);
  const target=recipient;
  if(!message || !target || pendingId(target))return;
  sessionStorage.setItem(replyKey(target),message.id);render();app.querySelector<HTMLTextAreaElement>('#message')?.focus();
 });
 app.querySelector<HTMLButtonElement>('#cancel-reply')?.addEventListener('click',()=>{sessionStorage.removeItem(replyKey(recipient));render();app.querySelector<HTMLTextAreaElement>('#message')?.focus();});
 app.querySelectorAll<HTMLButtonElement>('[data-copy]').forEach(b=>b.onclick=async()=>{const m=msgs.find(x=>x.id===b.dataset.copy);if(!m)return;try{await navigator.clipboard.writeText(m.text);copyFeedback='';}catch{copyFeedback='Could not copy message.';}const status=app.querySelector<HTMLElement>('.copy-feedback');if(status)status.textContent=copyFeedback;});
 app.querySelector<HTMLButtonElement>('#retry')?.addEventListener('click',()=>void refresh());
 const attach=app.querySelector<HTMLButtonElement>('#attach-files')!;
 const attachmentMenu=app.querySelector<HTMLElement>('#attachment-menu')!;
 attach.onclick=()=>{const open=attachmentMenu.hidden;attachmentMenuTarget=open?recipient:'';attachmentMenu.hidden=!open;attach.setAttribute('aria-expanded',String(open));if(open)app.querySelector<HTMLButtonElement>('#upload-file')?.focus();};
 app.querySelector<HTMLButtonElement>('#upload-file')!.onclick=()=>{filePickerTarget=recipient;closeAttachmentMenu();filePicker.click();};
 app.querySelectorAll<HTMLImageElement>('.file-thumbnail').forEach(img=>{img.onerror=()=>{img.hidden=true;const fallback=img.nextElementSibling as HTMLElement | null;if(fallback)fallback.hidden=false;};});
 app.querySelectorAll<HTMLButtonElement>('[data-remove-file]').forEach(b=>b.onclick=()=>{fileErrors.delete(recipient);setDraftFiles(recipient,draftFiles(recipient).filter((_,i)=>i!==Number(b.dataset.removeFile)));render();});
 const chat=app.querySelector<HTMLElement>('.chat')!;
 chat.ondragover=e=>{if(e.dataTransfer?.types.includes('Files')){e.preventDefault();chat.classList.add('dragging');}};
 chat.ondragleave=e=>{if(!chat.contains(e.relatedTarget as Node))chat.classList.remove('dragging');};
 chat.ondrop=e=>{e.preventDefault();chat.classList.remove('dragging');if(e.dataTransfer?.files)addDraftFiles(recipient,e.dataTransfer.files);};
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
  if(!(event as InputEvent).isComposing && (/^\/[a-z-]*$/.test(after) || /^\/[a-z-]*$/.test(before))){slashDismissed='';slashIndex=0;render();return;}
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
  if(slashOptions.length && !e.isComposing){
   if(e.key==='Escape'){e.preventDefault();slashDismissed=recipient+':'+slashPrefix;render();focusComposerAtSaved();return;}
   if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();slashIndex=(slashIndex+(e.key==='ArrowDown'?1:-1)+slashOptions.length)%slashOptions.length;render();focusComposerAtSaved();return;}
   if((e.key==='Enter'&&!e.shiftKey)||e.key==='Tab'){e.preventDefault();chooseSlash(slashOptions[slashIndex].name);return;}
  }
  if(group && mentionPickerTarget===recipient && e.key==='Escape'){e.preventDefault();mentionPickerTarget='';mentionQuery='';render();focusComposerAtSaved();return;}
  if(group && mentionPickerTarget===recipient && (e.key==='ArrowDown'||(e.key==='Enter'&&!e.shiftKey&&!e.isComposing))){e.preventDefault();app.querySelector<HTMLButtonElement>('[data-mention-choice]')?.focus();return;}
  if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();app.querySelector<HTMLFormElement>('#composer')!.requestSubmit();}
 });
 function chooseSlash(name:string):void {const target=recipient;const value=name+' ';storeDraft(target,value);slashDismissed='';slashIndex=0;caretByChannel.set(target,[value.length,value.length]);render();focusComposerAtSaved();}
 app.querySelectorAll<HTMLButtonElement>('[data-slash-choice]').forEach(button=>button.addEventListener('click',()=>chooseSlash(button.dataset.slashChoice!)));
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
 if(mentionFocus.startsWith('choice:'))Array.from(app.querySelectorAll<HTMLButtonElement>('[data-mention-choice]')).find(b=>b.dataset.mentionChoice===mentionFocus.slice(7))?.focus({preventScroll:true});
 else if(mentionFocus.startsWith('remove:'))Array.from(app.querySelectorAll<HTMLButtonElement>('[data-unmention]')).find(b=>b.dataset.unmention===mentionFocus.slice(7))?.focus({preventScroll:true});
 const newScroll=app.querySelector<HTMLElement>('#chat-scroll')!;
 const intent=sendScrollIntent;
 if(intent && (intent.epoch!==selectionEpoch || intent.target!==recipient || settingsOpen))sendScrollIntent=null;
 const scrollToSent=!!sendScrollIntent?.confirmed && msgs.some(m=>m.id===sendScrollIntent?.id);
 newScroll.scrollTop=scrollToSent ? newScroll.scrollHeight : same ? (follow ? newScroll.scrollHeight : oldTop) : (scrollByRecipient.get(recipient) ?? newScroll.scrollHeight);
 if (!scrollToSent && !same && !scrollByRecipient.has(recipient) && firstUnread) {
  const divider = newScroll.querySelector<HTMLElement>('.unread-divider');
  if (divider) newScroll.scrollTop += divider.getBoundingClientRect().top - newScroll.getBoundingClientRect().top;
 }
 if(scrollToSent){scrollByRecipient.set(recipient,newScroll.scrollTop);sendScrollIntent=null;rememberScroll(newScroll);}
 if(!resizeAnchor || resizeAnchor.target!==recipient || (resizeAnchor.width===window.innerWidth && resizeAnchor.height===window.innerHeight))rememberScroll(newScroll);
 app.querySelector<HTMLElement>('.content')!.scrollTop=same ? oldContentTop : 0;
 app.querySelector<HTMLElement>('#work-panel-content')!.scrollTop=same ? oldPanelTop : 0;
 renderedRecipient=recipient;
 if(selection){input.focus({preventScroll:true});input.setSelectionRange(selection[0],selection[1]);}
 else if(same && attachmentFocus && (attachmentFocus==='toggle' || attachmentOpen))app.querySelector<HTMLButtonElement>(attachmentFocus==='upload'?'#upload-file':'#attach-files')?.focus({preventScroll:true});
 else if(same && actionFocus) {const replacement=Array.from(app.querySelectorAll<HTMLElement>('[data-reply],[data-copy],[data-attachment-preview],[data-download],[data-scope]')).find(el=>el.dataset[actionFocus.key]===actionFocus.value && el.closest<HTMLElement>('[data-message-id]')?.dataset.messageId===actionMessage);replacement?.focus({preventScroll:true});}
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
    state={messages:data.messages||[],projects:data.projects||[],tasks:data.tasks||[],agents:data.agents||[],jobs:data.jobs||[],approvals:data.approvals||[]};
    lastStateBody=body;
    needsRender=true;
   }
   if(!selectionReady){restoreSelection();needsRender=true;}
   for(const target of ['root',...state.agents.map(a=>a.id),...state.projects.map(p=>'group:'+p.id)]) {
    const id=pendingId(target);
    if(id && target.startsWith('group:') && pendingMentions(target)===null){if(retryReady.delete(target))needsRender=true;continue;}
    if(id && state.messages.some(m=>m.id===id && m.sender==='operator' && m.room_id===roomFor(target) && m.text===pendingText(target) && (m.reply_to||'')===pendingReply(target) && (!target.startsWith('group:') || (pendingMentions(target)!==null && sameMentions(m.mention_ids||[],pendingMentions(target)!))))){
     confirmSendScroll(target,id);retryReady.delete(target);if(channelDraft(target).trim()===pendingText(target)) clearDraft(target);
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
   if(!settingsOpen){if(needsRender)render();else refreshActivity();}
  }
 })();
 return refreshInFlight.finally(()=>{refreshInFlight=null;});
}
let commandSending=false;
type CommandReceipt={text:string; fading:boolean; timer?:number};
const commandReceipts=new Map<string,CommandReceipt>();
function clearCommandReceipt(target:string):void {const previous=commandReceipts.get(target);if(previous?.timer)window.clearInterval(previous.timer);commandReceipts.delete(target);}
function setCommandReceipt(target:string,text:string,success=false):void {
 clearCommandReceipt(target);const receipt:CommandReceipt={text,fading:false};commandReceipts.set(target,receipt);
 if(success){let visibleMs=0,last=Date.now();receipt.timer=window.setInterval(()=>{const now=Date.now();if(recipient===target && !settingsOpen && !document.hidden)visibleMs+=now-last;last=now;if(visibleMs<5000)return;window.clearInterval(receipt.timer);receipt.fading=true;if(recipient===target)render();window.setTimeout(()=>{if(commandReceipts.get(target)!==receipt)return;clearCommandReceipt(target);if(recipient===target)render();},250);},100);}
 if(recipient===target)render();
}
async function workspaceCommand(target:string, projectId:string, text:string):Promise<void>{
 const report=(message:string,success=false)=>setCommandReceipt(target,message,success);
 if(commandSending || sending || stopSending)return;
 const path=text.slice('/workspace'.length).trim();
 if(!path || draftFiles(target).length || draftReply(target)){report('Use /workspace <absolute-directory>, without attachments or a reply.');return;}
 commandSending=true;clearCommandReceipt(target);if(recipient===target)render();
 try {const response=await fetch(`/api/projects/${encodeURIComponent(projectId)}/workspace`,{method:'POST',body:new URLSearchParams({path})});
  if(response.ok){if(channelDraft(target).trim()===text)clearDraft(target);report('Project workspace updated.',true);await refresh();}
  else {const detail=response.status===400?'Enter an absolute path to an existing directory.':response.status===404?'Project was not found.':response.status===409?'Cannot change workspace while project work is pending or active.':`Could not update the workspace (HTTP ${response.status}).`;report(`${detail} The command remains in your draft.`);}
 } catch {report('Could not confirm the workspace update. The command remains in your draft.');}
 finally {commandSending=false;if(recipient===target)render();}
}
async function previewsCommand(target:string,text:string):Promise<void>{
 if(commandSending || sending || stopSending)return;
 const value=text.slice('/previews'.length).trim();
 if((value!=='on' && value!=='off') || draftFiles(target).length || draftReply(target)) {setCommandReceipt(target,'Use /previews on or /previews off without attachments or a reply.');return;}
 commandSending=true;clearCommandReceipt(target);if(recipient===target)render();
 try {
  const response=await fetch(`/api/agents/${encodeURIComponent(target)}/operator-previews`,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({enabled:value==='on'?'true':'false'})});
  if(!response.ok) throw new Error('preference failed');
  if(channelDraft(target).trim()===text)clearDraft(target);
  setCommandReceipt(target,value==='on'?'Direct image previews enabled.':'Direct image previews disabled.',true);
 } catch {setCommandReceipt(target,'Could not change direct image previews. The command remains in your draft.');}
 finally {commandSending=false;if(recipient===target)render();}
}
async function resolveHoldCommand(target:string, projectId:string, text:string):Promise<void>{
 const report=(message:string,success=false)=>setCommandReceipt(target,message,success);
 if(commandSending || sending || stopSending)return;
 const match=/^\/resolve-hold\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s+([\s\S]+)$/i.exec(text);
 if(!match || !match[2].trim() || new TextEncoder().encode(match[2].trim()).length>4096 || draftFiles(target).length || draftReply(target)){report('Use /resolve-hold <full task UUID> <decision>, without attachments or a reply (maximum 4 KiB).');return;}
 const note=match[2].trim(),key='hold-command:'+target, prior=sessionStorage.getItem(key+':id');
 if(!prior && (stateStale || !state.tasks.some(t=>t.id===match[1] && t.project_id===projectId && t.resolve_hold_eligible))){report('This exact held task is not currently available. The command remains in your draft.');return;}
 if(prior && (sessionStorage.getItem(key+':task')!==match[1] || sessionStorage.getItem(key+':note')!==note)){report('A prior decision may already be queued. Check its status before changing it.');return;}
 const id=prior||crypto.randomUUID();sessionStorage.setItem(key+':id',id);sessionStorage.setItem(key+':task',match[1]);sessionStorage.setItem(key+':note',note);
 commandSending=true;clearCommandReceipt(target);if(recipient===target)render();
 try {const response=await fetch(`/api/tasks/${encodeURIComponent(match[1])}/resolve-hold`,{method:'POST',body:new URLSearchParams({id,project:projectId,note})});
  if(response.status===202){if(channelDraft(target).trim()===text)clearDraft(target);sessionStorage.removeItem(key+':id');sessionStorage.removeItem(key+':task');sessionStorage.removeItem(key+':note');report('Hold decision queued privately for the project owner.',true);await refresh();}
  else if(response.status===409){report('The held task is no longer available. The command remains in your draft.');await refresh();}
  else report(`Could not confirm the decision (HTTP ${response.status}). The command remains in your draft.`);
 }catch{report('Could not confirm the decision. The command remains in your draft; press Enter to retry.');}
 finally{commandSending=false;if(recipient===target)render();}
}
async function projectCommand(target:string, projectId:string, text:string):Promise<void>{
 const report=(message:string,success=false)=>setCommandReceipt(target,message,success);
 if(commandSending || sending || stopSending)return;
 const continuation=/^\/continue(?:\s|$)/.test(text);
 const rest=text.slice(continuation?9:14).trim();
 const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
 const first=rest.split(/\s+/,1)[0]||'';const explicit=uuid.test(first)?first:'';
 const note=(explicit?rest.slice(first.length):rest).trim();
 if(draftFiles(target).length || draftReply(target) || (!continuation && rest && (!explicit || note)) || (continuation && (!note || new TextEncoder().encode(note).length>4096))){report(continuation?'Use /continue [task UUID] instruction, without attachments or a reply (maximum 4 KiB).':'Use /retry-handoff [task UUID], without attachments or a reply.');return;}
 if(stateStale){report('Task status is unavailable. Refresh before trying this command.');return;}
 const eligible=state.tasks.filter(t=>t.project_id===projectId && (continuation?t.continue_po_eligible:t.retry_handoff_eligible));
 const task=explicit?eligible.find(t=>t.id===explicit):eligible.length===1?eligible[0]:undefined;
 if(!task){report(eligible.length>1 && !explicit?`Multiple eligible tasks. Add the full task UUID: ${eligible.map(t=>`${t.title} (${t.id})`).join('; ')}`:'No matching eligible task. The command remains in your draft.');return;}
 const key='po-command:'+target;let requestId='';
 if(continuation){const oldId=sessionStorage.getItem(key+':id');const oldTask=sessionStorage.getItem(key+':task');const oldNote=sessionStorage.getItem(key+':note');if(oldId && (oldTask!==task.id || oldNote!==note)){report('A previous continuation may already be queued. Check its status before changing the instruction.');return;}requestId=oldId||crypto.randomUUID();sessionStorage.setItem(key+':id',requestId);sessionStorage.setItem(key+':task',task.id);sessionStorage.setItem(key+':note',note);}
 commandSending=true;clearCommandReceipt(target);if(recipient===target)render();
 try{const response=await fetch(`/api/tasks/${encodeURIComponent(task.id)}/${continuation?'continue-po':'retry-handoff'}`,{method:'POST',...(continuation?{body:new URLSearchParams({id:requestId,note})}:{})});
  if(response.status===202){if(channelDraft(target).trim()===text)clearDraft(target);if(continuation){sessionStorage.removeItem(key+':id');sessionStorage.removeItem(key+':task');sessionStorage.removeItem(key+':note');}report(continuation?'PO continuation queued.':'Handoff correction queued for Root. Delivery is not yet confirmed.',true);await refresh();}
  else if(response.status===409){report('This task is no longer eligible. The command remains in your draft.');await refresh();}
  else report(`Could not confirm the request (HTTP ${response.status}). The command remains in your draft.`);
 }catch{report('Could not confirm the request. The command remains in your draft; press Enter to retry.');}
 finally{commandSending=false;if(recipient===target)render();}
}
async function stopFromComposer(text:string):Promise<void> {
 clearCommandReceipt('root');
 const match=/^\/stop ([a-z0-9][a-z0-9-]{0,62})$/.exec(text);
 if(!match || draftFiles('root').length || draftReply('root')){stopReceipt='Use /stop followed by an exact project ID, without attachments or a reply.';render();return;}
 const id=match[1];
 if(!state.projects.some(p=>p.id===id)){stopReceipt=`Project ${id} was not found. The command remains in your draft.`;render();return;}
 stopSending=true;stopReceipt='';render();
 try {
  const response=await fetch(`/api/projects/${encodeURIComponent(id)}/stop`,{method:'POST'});
  if(!response.ok)throw new Error('Stop request failed');
  if(channelDraft('root').trim()===text)clearDraft('root');
  if(refreshInFlight)await refreshInFlight;await refresh();
  if(stateStale)stopReceipt=`Stop requested for ${id}. The current outcome is unavailable.`;
  else {const count=state.jobs.filter(j=>j.project_id===id && ['submitted','running','uncertain'].includes(j.status)).length;setCommandReceipt('root',`Stop requested for ${id}. ${count} ${count===1?'job remains':'jobs remain'} submitted, running or uncertain.`,true);}
 } catch {stopReceipt=`Could not confirm the stop request for ${id}. The command remains in your draft.`;}
 finally {stopSending=false;render();}
}
async function send(){if(!setupReady)return;const target=recipient;const command=draft().trim();if(target==='root' && !pendingId(target) && /^\/stop(?:\s|$)/.test(command)){if(!stopSending)await stopFromComposer(command);return;}if(!pendingId(target) && state.agents.some(a=>a.id===target && a.role==='worker') && /^\/previews(?:\s|$)/.test(command)){await previewsCommand(target,command);return;}const groupProject=state.projects.find(p=>'group:'+p.id===target);if(target.startsWith('group:')&&!groupProject)return;if(groupProject && !pendingId(target) && /^\/resolve-hold(?:\s|$)/.test(command)){await resolveHoldCommand(target,groupProject.id,command);return;}if(groupProject && !pendingId(target) && /^\/workspace(?:\s|$)/.test(command)){await workspaceCommand(target,groupProject.id,command);return;}if(groupProject && !pendingId(target) && /^\/(?:continue|retry-handoff)(?:\s|$)/.test(command)){await projectCommand(target,groupProject.id,command);return;}const prior=pendingId(target);const text=prior ? pendingText(target) : draft().trim();if((!text && !draftFiles(target).length && !prior)||sending||stopSending)return;if(prior&&!retryReady.has(target)){await refresh();if(!retryReady.has(target))return;}const files=draftFiles(target);if(prior && missingPendingFiles(target)){composeNotice='Reattach the files before retrying the saved message.';render();app.querySelector<HTMLButtonElement>('#attach-files')?.focus();return;}const id=prior || crypto.randomUUID();const reply=prior ? pendingReply(target) : draftReply(target);const savedMentions=prior && groupProject ? pendingMentions(target) : [];if(prior && groupProject && savedMentions===null){error='Saved mention targets are unavailable. Check the outcome before retrying.';render();return;}const mentions=prior ? savedMentions! : groupProject ? mentionIds(target) : [];if(groupProject && !prior && !validMentionDraft(target,channelDraft(target),groupProject.id)){error='A selected mention is no longer valid. Remove it and choose the agent again.';render();return;}sendScrollIntent={target,id,epoch:selectionEpoch,confirmed:false};retryReady.delete(target);sessionStorage.setItem('pending-id:'+target,id);sessionStorage.setItem('pending-text:'+target,text);sessionStorage.setItem('pending-reply:'+target,reply);if(!prior){sessionStorage.setItem('pending-mentions:'+target,encodeIds(mentions));sessionStorage.setItem('pending-files:'+target,files.length?'yes':'no');}mentionPickerTarget='';mentionQuery='';composeNotice='';sending=true;sendTarget=target;render();try{const form=new FormData();form.set('id',id);form.set('text',text);form.set(groupProject?'project_id':'recipient',groupProject?groupProject.id:target);if(reply)form.set('reply_to',reply);for(const mention of mentions)form.append('mention_id',mention);for(const file of files)form.append('files',file);const r=await fetch('/api/messages/upload',{method:'POST',body:form});if(!r.ok){if(r.status===409){conflicts.add(target);if(sendScrollIntent?.id===id)sendScrollIntent=null;}throw new Error(`Delivery not confirmed (${r.status}). Check status before trying again.`);}confirmSendScroll(target,id);if(channelDraft(target).trim()===text) clearDraft(target);sessionStorage.removeItem('pending-id:'+target);sessionStorage.removeItem('pending-text:'+target);if(draftReply(target)===reply)sessionStorage.removeItem(replyKey(target));sessionStorage.removeItem('pending-reply:'+target);sessionStorage.removeItem('pending-mentions:'+target);sessionStorage.removeItem('pending-files:'+target);setDraftFiles(target,[]);if(refreshInFlight)await refreshInFlight;await refresh();}catch{await refresh();}finally{sending=false;sendTarget='';render();}}
function resizeComposer(input:HTMLTextAreaElement):void {
 input.style.height='auto';
 input.style.height=`${input.scrollHeight}px`;
}
narrowLayout.addEventListener('change',()=>{mobileNav=false;readReceipts.contextChanged();render();});
document.addEventListener('scroll',event=>{const scroll=app.querySelector<HTMLElement>('#chat-scroll');if(event.target===scroll && scroll && (!resizeAnchor || (resizeAnchor.width===window.innerWidth && resizeAnchor.height===window.innerHeight)))rememberScroll(scroll);},{capture:true,passive:true});
window.addEventListener('resize',()=>{const input=app.querySelector<HTMLTextAreaElement>('#message');if(input)resizeComposer(input);requestAnimationFrame(restoreResizeAnchor);});
function closeAttachmentMenu(restoreFocus=false):void {
 attachmentMenuTarget='';
 const menu=app.querySelector<HTMLElement>('#attachment-menu');
 if(!menu || menu.hidden)return;
 menu.hidden=true;const button=app.querySelector<HTMLButtonElement>('#attach-files');button?.setAttribute('aria-expanded','false');if(restoreFocus)button?.focus();
}
document.addEventListener('pointerdown',e=>{if(!(e.target instanceof Node) || !app.querySelector('.attachment-control')?.contains(e.target))closeAttachmentMenu();});
document.addEventListener('keydown',e=>{const menu=app.querySelector<HTMLElement>('#attachment-menu');if(e.key==='Escape' && menu && !menu.hidden){e.preventDefault();e.stopPropagation();closeAttachmentMenu(true);}},true);
const readReceipts = new ReadReceipts({messages:()=>state.messages,recipient:()=>recipient,obstructed:()=>!setupReady || settingsOpen || !selectionReady || mobileNav || !!document.querySelector('dialog.attachment-preview[open]'),eligible:m=>readable(m as Message),refresh});
render(); void checkSetup();
window.setInterval(()=>{if(!document.hidden && (!setupReady || pendingSourceReload))void checkSetup();},2000);
window.setInterval(()=>{if(setupReady && !document.hidden && !settingsOpen && !settingsDirty && !savingGuard)void loadWeeklyUsage();},60000);
window.setInterval(()=>{if(setupReady && !document.hidden)void updateRequest();},5000);
// Refresh in the background without interrupting a draft or an in-flight send.
window.setInterval(()=>{if(setupReady && !document.hidden && !sending && Date.now()-lastPoll >= (agentActivity(recipient).busy ? 2000 : 5000)) void refresh();},1000);
