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
  private markingRoom = false;
  private readonly scrolledVisible = new Set<string>();
  private userScrollUntil = 0;
  private pausedForJump = false;
  constructor(private readonly options: Options) {
    window.addEventListener('focus', () => { this.attempted.clear(); this.schedule(); });
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) { this.attempted.clear(); this.schedule(); }
    });
    const userScroll = (event: Event) => {
      const viewport = document.querySelector<HTMLElement>('#chat-scroll');
      if (event.isTrusted && viewport && event.target instanceof Node && viewport.contains(event.target)) { this.pausedForJump = false; this.userScrollUntil = Date.now() + 600; }
    };
    document.addEventListener('wheel', userScroll, {capture:true,passive:true});
    document.addEventListener('touchmove', userScroll, {capture:true,passive:true});
    document.addEventListener('keydown', event => { if (['ArrowUp','ArrowDown','PageUp','PageDown','Home','End',' '].includes(event.key)) userScroll(event); }, true);
    document.addEventListener('scroll', () => this.onScroll(), {capture:true,passive:true});
    document.addEventListener('close', () => this.schedule(), true);
    window.addEventListener('resize', () => this.schedule());
    window.visualViewport?.addEventListener('resize', () => this.schedule());
    window.visualViewport?.addEventListener('scroll', () => this.onScroll(), {passive:true});
  }
  private onScroll() {
    if (this.pausedForJump) return;
    if (!this.options.recipient().startsWith('group:')) { this.schedule(); return; }
    if (Date.now() <= this.userScrollUntil) this.collectScrolledVisible();
    const target = this.options.recipient();
    window.clearTimeout(this.scrollTimer);
    this.scrollTimer = window.setTimeout(() => {
      this.scrollTimer = undefined;
      if (target !== this.options.recipient()) return;
      if (this.refreshAfterScroll) { this.refreshAfterScroll = false; void this.options.refresh(); }
      this.schedule();
    }, 180);
  }
  contextChanged() { window.clearTimeout(this.scrollTimer); this.scrollTimer = undefined; this.refreshAfterScroll = false; this.scrolledVisible.clear(); this.userScrollUntil = 0; this.pausedForJump = false; this.attempted.clear(); this.schedule(); }
  pauseForJump() { window.clearTimeout(this.scrollTimer); this.scrollTimer = undefined; this.scrolledVisible.clear(); this.userScrollUntil = 0; this.pausedForJump = true; }
  async markCurrentRoomRead() {
    const target = this.options.recipient();
    if (!target.startsWith('group:') || this.options.obstructed() || this.markingRoom) return;
    this.markingRoom = true;
    const ids = this.options.messages().filter(m => this.options.eligible(m) && !m.operator_read_at && !this.inFlight.has(m.id)).map(m => m.id);
    try {
      for (let i = 0; i < ids.length && target === this.options.recipient(); i += 128) {
        const batch = ids.slice(i, i + 128);
        batch.forEach(id => this.inFlight.add(id));
        try { if (!(await this.send(batch)).ok) break; }
        catch { break; }
        finally { batch.forEach(id => this.inFlight.delete(id)); }
      }
      if (target === this.options.recipient()) await this.options.refresh();
    } finally { this.markingRoom = false; }
  }
  private send(ids: string[]) {
    return fetch('/api/messages/read', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message_ids:ids})});
  }
  schedule() {
    if (this.pausedForJump || this.scheduled || this.scrollTimer !== undefined) return;
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
  private collectScrolledVisible() {
    if (document.hidden || !document.hasFocus() || this.options.obstructed()) return;
    const viewport = document.querySelector<HTMLElement>('#chat-scroll');
    if (!viewport) return;
    const unread = new Set(this.options.messages().filter(m => this.options.eligible(m) && !m.operator_read_at).map(m => m.id));
    viewport.querySelectorAll<HTMLElement>('[data-message-id]').forEach(el => {
      const id = el.dataset.messageId;
      if (id && unread.has(id) && this.visible(el, viewport)) this.scrolledVisible.add(id);
    });
  }
  private async observe() {
    if (this.pausedForJump || this.markingRoom || this.scrollTimer !== undefined || document.hidden || !document.hasFocus() || this.options.obstructed()) return;
    const viewport = document.querySelector<HTMLElement>('#chat-scroll');
    if (!viewport) return;
    const target = this.options.recipient();
    const messages = this.options.messages().filter(m => this.options.eligible(m) && !m.operator_read_at);
    const displayed = new Map<string,HTMLElement>();
    viewport.querySelectorAll<HTMLElement>('[data-message-id]').forEach(el => { if (el.dataset.messageId) displayed.set(el.dataset.messageId, el); });
    const ids: string[] = [];
    for (const msg of messages) {
      const element = displayed.get(msg.id);
      if (!this.scrolledVisible.has(msg.id) && (!element || !this.visible(element, viewport))) { this.attempted.delete(msg.id); continue; }
      if (!this.attempted.has(msg.id) && !this.inFlight.has(msg.id)) ids.push(msg.id);
    }
    ids.splice(128);
    if (!ids.length || document.hidden || !document.hasFocus() || this.options.obstructed() || target !== this.options.recipient()) return;
    ids.forEach(id => { this.scrolledVisible.delete(id); this.attempted.add(id); this.inFlight.add(id); });
    try {
      const response = await this.send(ids);
      if (response.status === 204 && target === this.options.recipient()) {
        if (this.scrollTimer !== undefined) this.refreshAfterScroll = true;
        else await this.options.refresh();
      }
    } catch { /* A later visibility transition may retry; do not spin. */ }
    finally { ids.forEach(id => this.inFlight.delete(id)); }
  }
}
