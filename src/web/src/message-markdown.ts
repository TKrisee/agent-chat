import MarkdownIt from 'markdown-it';
type Token = ReturnType<InstanceType<typeof MarkdownIt>['parse']>[number];
import './message-markdown.css';

const markdown = new MarkdownIt({html: false, linkify: false, breaks: false});
const cache = new Map<string, Promise<string | null>>();
let mermaidSequence = 0;
let mermaidModule: Promise<typeof import('mermaid')> | undefined;
let purifierModule: Promise<typeof import('dompurify')> | undefined;

function safeLink(value: string): string | null {
  try {
    const url = new URL(value, location.href);
    return ['http:', 'https:', 'mailto:'].includes(url.protocol) ? url.href : null;
  } catch { return null; }
}

// Directives, HTML, links and remote resources have no place in a static chat diagram.
function safeDiagram(source: string): boolean {
  return source.length <= 20000 && !/(?:%%\s*\{|^\s*---|(?:^|[;\n])\s*(?:click|link|links|style|classDef|linkStyle)\b|<\s*\/?\s*[a-z!]|(?:https?|data|javascript|file):|\/\/|@\s*(?:\{|import|font-face)|\b(?:img|image)\s*[:=]|\burl\s*\()/im.test(source);
}

async function diagram(source: string): Promise<string | null> {
  if (!safeDiagram(source)) return null;
  const existing = cache.get(source);
  if (existing) return existing;
  const promise = (async () => {
    try {
      mermaidModule ||= import('mermaid');
      purifierModule ||= import('dompurify');
      const [mermaidImport, purifierImport] = await Promise.all([mermaidModule, purifierModule]);
      const mermaid = mermaidImport.default;
      mermaid.initialize({startOnLoad: false, securityLevel: 'strict', suppressErrorRendering: true,
        flowchart: {htmlLabels: false}});
      const rendered = await mermaid.render(`chat-mermaid-${++mermaidSequence}`, source);
      const clean = purifierImport.default.sanitize(rendered.svg, {
        USE_PROFILES: {svg: true, svgFilters: true},
        FORBID_TAGS: ['foreignObject', 'a', 'image', 'script'],
      });
      return typeof clean === 'string' && clean.includes('<svg') ? clean : null;
    } catch { return null; }
  })();
  cache.set(source, promise);
  if (cache.size > 100) cache.delete(cache.keys().next().value!);
  return promise;
}

function addDiagram(parent: HTMLElement, source: string): void {
  const wrapper = document.createElement('div');
  wrapper.className = 'chat-diagram';
  const fallback = document.createElement('pre');
  fallback.textContent = source;
  wrapper.append(fallback);
  parent.append(wrapper);
  void diagram(source).then(svg => {
    if (!svg) return;
    const frame = document.createElement('iframe');
    frame.title = 'Mermaid diagram';
    frame.setAttribute('sandbox', '');
    frame.className = 'chat-diagram-frame';
    // This sandbox contains only sanitized SVG; its CSP blocks all external resources.
    frame.srcdoc = `<!doctype html><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src 'none'; font-src 'none'"><style>html,body{margin:0;background:transparent}svg{display:block;width:100%;height:auto}</style>${svg}`;
    const parsed = new DOMParser().parseFromString(svg, 'image/svg+xml').documentElement;
    const viewBox = parsed.getAttribute('viewBox')?.trim().split(/[\s,]+/).map(Number);
    if (viewBox?.length === 4 && viewBox.every(Number.isFinite) && viewBox[2] > 0 && viewBox[3] > 0) {
      frame.style.aspectRatio = `${viewBox[2]} / ${viewBox[3]}`;
      frame.style.maxWidth = `${viewBox[2]}px`;
    }
    wrapper.replaceChildren(frame);
  });
}

const allowed = new Set(['p','h1','h2','h3','h4','h5','h6','blockquote','ul','ol','li','table','thead','tbody','tr','th','td','em','strong','s']);
function appendTokens(root: HTMLElement, tokens: Token[]): void {
  const stack: HTMLElement[] = [root];
  for (const token of tokens) {
    const parent = stack[stack.length - 1];
    if (token.type === 'inline') { appendTokens(parent, token.children || []); continue; }
    if (token.type === 'text' || token.type === 'html_inline' || token.type === 'html_block') { parent.append(document.createTextNode(token.content)); continue; }
    if (token.type === 'softbreak') { parent.append(document.createTextNode('\n')); continue; }
    if (token.type === 'hardbreak') { parent.append(document.createElement('br')); continue; }
    if (token.type === 'code_inline') { const code=document.createElement('code'); code.textContent=token.content; parent.append(code); continue; }
    if (token.type === 'fence' || token.type === 'code_block') {
      if (token.type === 'fence' && token.info.trim().split(/\s+/)[0].toLowerCase() === 'mermaid') { addDiagram(parent, token.content); continue; }
      const pre=document.createElement('pre'); const code=document.createElement('code'); code.textContent=token.content; pre.append(code); parent.append(pre); continue;
    }
    if (token.type === 'image') { parent.append(document.createTextNode(token.content)); continue; }
    if (token.type === 'hr') { parent.append(document.createElement('hr')); continue; }
    if (token.type === 'link_open') {
      const href=safeLink(String(token.attrGet('href') || ''));
      const el=document.createElement(href ? 'a' : 'span');
      if (href && el instanceof HTMLAnchorElement) { el.href=href; el.target='_blank'; el.rel='noopener noreferrer'; }
      parent.append(el); stack.push(el); continue;
    }
    if (token.type === 'link_close') { if(stack.length>1)stack.pop(); continue; }
    if (token.nesting === 1 && allowed.has(token.tag)) {
      const element=document.createElement(token.tag);
      if (token.tag === 'ol') { const start=String(token.attrGet('start') || ''); if(start && /^\d+$/.test(start)) element.setAttribute('start',start); }
      parent.append(element); stack.push(element); continue;
    }
    if (token.nesting === -1 && allowed.has(token.tag) && stack.length>1) stack.pop();
  }
}

/** Render untrusted message text without changing the source used for copy/reply. */
export function renderMessageMarkdown(container: HTMLElement, source: string, mentionNames: string[] = []): void {
  container.classList.add('message-markdown');
  container.replaceChildren();
  appendTokens(container, markdown.parse(source, {}));
  const names=[...new Set(mentionNames.filter(Boolean))].sort((a,b)=>b.length-a.length);
  if(!names.length)return;
  const pattern=new RegExp(`(^|[^\\p{L}\\p{N}\\p{M}_@])(@(?:${names.map(name=>name.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')).join('|')}))(?![\\p{L}\\p{N}\\p{M}_@-]|\\.[a-z0-9])`,'giu');
  const walker=document.createTreeWalker(container,NodeFilter.SHOW_TEXT);
  const nodes:Text[]=[];
  while(walker.nextNode()){const node=walker.currentNode as Text;if(!node.parentElement?.closest('a,code,pre'))nodes.push(node);}
  for(const node of nodes){
    const value=node.data;const fragment=document.createDocumentFragment();let end=0;pattern.lastIndex=0;
    for(const match of value.matchAll(pattern)){
      const start=match.index!+match[1].length, finish=start+match[2].length;
      const word=value.slice(0,start).split(/\s/).pop() || '';
      if(/(?:https?:\/\/|www\.)/i.test(word))continue;
      fragment.append(document.createTextNode(value.slice(end,start)));
      const highlight=document.createElement('span');highlight.className='message-mention';highlight.textContent=value.slice(start,finish);fragment.append(highlight);end=finish;
    }
    if(end){fragment.append(document.createTextNode(value.slice(end)));node.replaceWith(fragment);}
  }
}

/** Match an operator address in message prose, excluding quoted and code content. */
export function mentionsOperator(source: string): boolean {
  let quotedBlock=0;
  for (const token of markdown.parse(source, {})) {
    if (token.type==='blockquote_open') { quotedBlock++; continue; }
    if (token.type==='blockquote_close') { quotedBlock--; continue; }
    if (quotedBlock || token.type!=='inline') continue;
    let link=0;
    for (const child of token.children || []) {
      if (child.type==='link_open') { link++; continue; }
      if (child.type==='link_close') { link--; continue; }
      if (link || child.type!=='text') continue;
      const prose=child.content.replace(/"[^"\n]*"|“[^”\n]*”/g, '');
      if (/(?:^|[^\w@])@operator(?![\w@-]|\.[a-z0-9])/i.test(prose)) return true;
    }
  }
  return false;
}
