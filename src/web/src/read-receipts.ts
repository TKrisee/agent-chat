/** Read receipts are only observed in the selected, unobscured transcript. */
export type ReadMessage = {id:string; sender:string; recipient:string; operator_read_at?:string};
type Options = {
  messages: () => ReadMessage[];
  recipient: () => string;
  eligible: (m: ReadMessage) => boolean;
  obstructed: () => boolean;
  refresh: () => Promise<void>;
};

export class ReadReceipts {
  private readonly inFlight = new Set<string>();
  private readonly attempted = new Set<string>();
  private scheduled = false;
  private scrollTimer: number | undefined;
  private refreshAfterScroll = false;
  constructor(private readonly options: Options) {
    window.addEventListener('focus', () => { this.attempted.clear(); this.schedule(); });
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) { this.attempted.clear(); this.schedule(); }
    });
    document.addEventListener('scroll', () => this.onScroll(), {capture:true,passive:true});
    document.addEventListener('close', () => this.schedule(), true);
    window.addEventListener('resize', () => this.schedule());
    window.visualViewport?.addEventListener('resize', () => this.schedule());
    window.visualViewport?.addEventListener('scroll', () => this.onScroll(), {passive:true});
  }
  private onScroll() {
    if (!this.options.recipient().startsWith('group:')) { this.schedule(); return; }
    const target = this.options.recipient();
    window.clearTimeout(this.scrollTimer);
    this.scrollTimer = window.setTimeout(() => {
      this.scrollTimer = undefined;
      if (target !== this.options.recipient()) return;
      if (this.refreshAfterScroll) { this.refreshAfterScroll = false; void this.options.refresh(); }
      this.schedule();
    }, 180);
  }
  contextChanged() { window.clearTimeout(this.scrollTimer); this.scrollTimer = undefined; this.refreshAfterScroll = false; this.attempted.clear(); this.schedule(); }
  schedule() {
    if (this.scheduled || this.scrollTimer !== undefined) return;
    this.scheduled = true;
    requestAnimationFrame(() => { this.scheduled = false; void this.observe(); });
  }
  private visible(element: HTMLElement, viewport: HTMLElement): boolean {
    const item = element.getBoundingClientRect();
    const bounds = viewport.getBoundingClientRect();
    const visual = window.visualViewport;
    const left = Math.max(item.left, bounds.left, visual?.offsetLeft ?? 0);
    const right = Math.min(item.right, bounds.right, visual ? visual.offsetLeft + visual.width : window.innerWidth);
    const top = Math.max(item.top, bounds.top, visual?.offsetTop ?? 0);
    const bottom = Math.min(item.bottom, bounds.bottom, visual ? visual.offsetTop + visual.height : window.innerHeight);
    if (right <= left || bottom - top < Math.min(32, item.height)) return false;
    const hit = document.elementFromPoint((left + right) / 2, (top + bottom) / 2);
    return !!hit && (element === hit || element.contains(hit));
  }
  private async observe() {
    if (this.scrollTimer !== undefined || document.hidden || !document.hasFocus() || this.options.obstructed()) return;
    const viewport = document.querySelector<HTMLElement>('#chat-scroll');
    if (!viewport) return;
    const target = this.options.recipient();
    const messages = this.options.messages().filter(m => this.options.eligible(m) && !m.operator_read_at);
    const displayed = new Map<string,HTMLElement>();
    viewport.querySelectorAll<HTMLElement>('[data-message-id]').forEach(el => { if (el.dataset.messageId) displayed.set(el.dataset.messageId, el); });
    const ids: string[] = [];
    for (const msg of messages) {
      const element = displayed.get(msg.id);
      if (!element || !this.visible(element, viewport)) { this.attempted.delete(msg.id); continue; }
      if (!this.attempted.has(msg.id) && !this.inFlight.has(msg.id)) ids.push(msg.id);
    }
    ids.splice(128);
    if (!ids.length || document.hidden || !document.hasFocus() || this.options.obstructed() || target !== this.options.recipient()) return;
    ids.forEach(id => { this.attempted.add(id); this.inFlight.add(id); });
    try {
      const response = await fetch('/api/messages/read', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message_ids:ids})});
      if (response.status === 204 && target === this.options.recipient()) {
        if (this.scrollTimer !== undefined) this.refreshAfterScroll = true;
        else await this.options.refresh();
      }
    } catch { /* A later visibility transition may retry; do not spin. */ }
    finally { ids.forEach(id => this.inFlight.delete(id)); }
  }
}
