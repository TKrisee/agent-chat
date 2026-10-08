'use strict';
const byId=id=>document.getElementById(id);
let cursor=null,busy=false,messages=[],selectedProject='';
function element(tag,text,className){const node=document.createElement(tag);if(text!==undefined)node.textContent=text;if(className)node.className=className;return node;}
function actor(name,id){return name||id||'Unknown';}
function initials(name){return name.split(/[\s_-]+/).filter(Boolean).slice(0,2).map(word=>word[0]).join('').toUpperCase()||'?';}
function card(item,detail=false){
  const sender=actor(item.sender_agent,item.sender_session),recipient=actor(item.recipient_agent,item.recipient_session);
  const node=element('article',undefined,'communication-message');node.dataset.messageId=item.id;node.dataset.projectId=item.project_id;
  if(item.instruction_origin==='authenticated_operator_ui')node.classList.add('operator-message');
  const tone=[...(item.sender_session||sender)].reduce((sum,char)=>sum+char.charCodeAt(0),0)%3;
  const avatar=element('span',initials(sender),'communication-avatar avatar-tone-'+tone);avatar.setAttribute('aria-hidden','true');node.append(avatar);
  const content=element('div',undefined,'communication-message-content'),header=element('header');
  header.append(element('span',sender,'message-sender'),element('span','→ '+recipient,'message-route'),element('span',item.project_name||item.project_id,'message-project'));
  const time=element('time',new Date(item.created_at*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}),'message-time');time.dateTime=new Date(item.created_at*1000).toISOString();time.title=new Date(item.created_at*1000).toLocaleString();header.append(time);
  if(!detail){const button=element('button','···','message-details');button.type='button';button.setAttribute('aria-label','Message details from '+sender);button.addEventListener('click',()=>showDetail(item.project_id,item.id));header.append(button);}
  content.append(header);const bubble=element('div',undefined,'communication-bubble');
  if(item.reply_to){const preview=item.reply_preview;const button=element('button','↪ '+(preview?actor(preview.sender_agent,preview.sender_session)+': '+preview.body:'View original message'),'reply-link');button.type='button';button.addEventListener('click',()=>showDetail(item.project_id,item.reply_to));bubble.append(button);}
  bubble.append(element('div',item.body,'message-body'));
  if(item.body_truncated&&!detail){const button=element('button','Read full message','full-message');button.type='button';button.addEventListener('click',()=>showDetail(item.project_id,item.id));bubble.append(button);}
  if(item.attachments?.length)bubble.append(element('p',item.attachments.map(a=>a.name+' ('+a.size+' bytes)').join(' · '),'message-extra'));
  content.append(bubble);node.append(content);
  if(detail){const record=element('dl',undefined,'message-record');for(const [label,value] of [['Message ID',item.id],['Project',item.project_id],['Sender session',item.sender_session],['Recipient session',item.recipient_session],['Created',new Date(item.created_at*1000).toLocaleString()],['Acknowledged',item.acked_at?new Date(item.acked_at*1000).toLocaleString():'Not recorded'],['Instruction origin',item.instruction_origin||'Unknown'],['Reply to',item.reply_to||'None'],['Batch',item.batch_id||'None']])record.append(element('dt',label),element('dd',value));content.append(record);}
  return node;
}
async function get(url){const response=await fetch(url,{credentials:'same-origin',cache:'no-store'});const data=await response.json();if(!response.ok)throw new Error(data.error||'History request failed ('+response.status+')');return data;}
async function showDetail(project,id){
  const dialog=byId('communication-detail'),content=byId('communication-detail-content');content.replaceChildren(element('p','Loading…'));if(!dialog.open)dialog.showModal();
  try{const data=await get('/api/communications/message?'+new URLSearchParams({project,id}));content.replaceChildren(card(data.message,true));}catch(error){content.replaceChildren(element('p',error.message));}
}
function render(){
  const fragment=document.createDocumentFragment();let day='';
  for(const item of messages){const date=new Date(item.created_at*1000),key=date.toLocaleDateString();if(key!==day){const divider=element('div',undefined,'communication-day');divider.append(element('span',date.toLocaleDateString([],{weekday:'short',month:'long',day:'numeric',year:'numeric'})));fragment.append(divider);day=key;}fragment.append(card(item));}
  byId('communication-messages').replaceChildren(fragment);
}
async function load(older=false,reset=false){
  if(busy)return;busy=true;byId('communication-older').disabled=true;
  const feed=byId('communication-feed'),atBottom=feed.scrollHeight-feed.scrollTop-feed.clientHeight<60;
  const visible=[...byId('communication-messages').querySelectorAll('article')].find(node=>node.getBoundingClientRect().bottom>feed.getBoundingClientRect().top);
  const anchor=visible?{project:visible.dataset.projectId,id:visible.dataset.messageId,top:visible.getBoundingClientRect().top}:null;
  const params=new URLSearchParams({limit:'50'});if(selectedProject)params.set('project',selectedProject);for(const [id,key] of [['communication-agent','agent'],['communication-text','q']])if(byId(id).value)params.set(key,byId(id).value);
  if(older&&cursor)params.set('before',cursor);
  try{
    const data=await get('/api/communications?'+params);
    byId('communication-projects').replaceChildren(...[{id:'',name:'All projects'},...data.projects].map(project=>{
      const button=element('button',project.name,'room-button');button.type='button';button.dataset.projectId=project.id;button.classList.toggle('selected',project.id===selectedProject);button.setAttribute('aria-current',project.id===selectedProject?'page':'false');button.addEventListener('click',()=>{if(busy)return;selectedProject=project.id;load(false,true);});return button;
    }));
    const hadMessages=messages.length>0;if(reset)messages=[];
    const indexed=new Map(messages.map(item=>[item.project_id+':'+item.id,item]));for(const item of data.messages)indexed.set(item.project_id+':'+item.id,item);
    messages=[...indexed.values()].sort((a,b)=>a.created_at-b.created_at||a.project_id.localeCompare(b.project_id)||a.seq-b.seq||a.id.localeCompare(b.id));render();
    if(older||reset||!hadMessages){cursor=data.next_cursor;byId('communication-older').hidden=!data.has_more;}
    byId('communication-empty').hidden=messages.length!==0;
    byId('communication-status').textContent=messages.length+' messages · '+data.loaded_projects+'/'+data.selected_projects+' projects'+(data.partial?' · Partial history':'');
    const errors=byId('communication-errors');errors.hidden=!data.partial;errors.replaceChildren(...data.errors.map(e=>element('p',e.project_name+' ('+e.project_id+'): '+e.error)));
    if(reset||(!older&&atBottom))feed.scrollTop=feed.scrollHeight;
    else if(anchor){const same=[...byId('communication-messages').querySelectorAll('article')].find(node=>node.dataset.projectId===anchor.project&&node.dataset.messageId===anchor.id);if(same)feed.scrollTop+=same.getBoundingClientRect().top-anchor.top;}
  }catch(error){byId('communication-status').textContent='Could not load communication';byId('communication-errors').hidden=false;byId('communication-errors').replaceChildren(element('p',error.message));}
  finally{busy=false;byId('communication-older').disabled=false;}
}
byId('communication-filters').addEventListener('submit',event=>{event.preventDefault();load(false,true);});
byId('communication-refresh').addEventListener('click',()=>load());
byId('communication-older').addEventListener('click',()=>load(true));
byId('communication-close').addEventListener('click',()=>byId('communication-detail').close());
load(false,true);
