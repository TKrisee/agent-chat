import './attachment-preview.css';

type Attachment = {id: string; name: string};
let closeCurrent: (() => void) | undefined;

/** Open a preview outside the app's frequently re-rendered transcript. */
export function previewAttachment(attachment: Attachment, trigger: HTMLElement): void {
  closeCurrent?.();
  const dialog = document.createElement('dialog');
  dialog.className = 'attachment-preview';
  dialog.setAttribute('aria-labelledby', 'attachment-preview-title');
  const header = document.createElement('div');
  header.className = 'attachment-preview-header';
  const title = document.createElement('h2');
  title.id = 'attachment-preview-title';
  title.textContent = attachment.name;
  const close = document.createElement('button');
  close.type = 'button';
  close.className = 'attachment-preview-close';
  close.setAttribute('aria-label', 'Close preview');
  close.textContent = '×';
  header.append(title, close);
  const content = document.createElement('div');
  content.className = 'attachment-preview-content';
  content.setAttribute('aria-live', 'polite');
  content.textContent = 'Loading preview…';
  const footer = document.createElement('div');
  footer.className = 'attachment-preview-footer';
  const download = document.createElement('a');
  download.href = `/api/attachments/${encodeURIComponent(attachment.id)}?download=1`;
  download.download = attachment.name;
  download.textContent = 'Download';
  footer.append(download);
  dialog.append(header, content, footer);
  document.body.append(dialog);

  const controller = new AbortController();
  let objectURL: string | undefined;
  let closed = false;
  const restoreFocus = () => {
    if (trigger.isConnected) { trigger.focus({preventScroll: true}); return; }
    const replacement = Array.from(document.querySelectorAll<HTMLElement>('[data-attachment-preview]'))
      .find(element => element.dataset.attachmentPreview === attachment.id);
    replacement?.focus({preventScroll: true});
  };
  const cleanup = () => {
    if (closed) return;
    closed = true;
    controller.abort();
    if (objectURL) URL.revokeObjectURL(objectURL);
    dialog.remove();
    if (closeCurrent === dismiss) closeCurrent = undefined;
    restoreFocus();
  };
  const dismiss = () => { if (dialog.open) dialog.close(); cleanup(); };
  closeCurrent = dismiss;
  close.addEventListener('click', dismiss);
  dialog.addEventListener('close', cleanup);
  dialog.showModal();
  close.focus();

  const unavailable = () => {
    if (closed) return;
    content.replaceChildren();
    const message = document.createElement('p');
    message.className = 'attachment-preview-unavailable';
    message.textContent = 'Preview unavailable. Download the attachment to open it.';
    content.append(message);
  };
  void (async () => {
    try {
      const response = await fetch(`/api/attachments/${encodeURIComponent(attachment.id)}/preview`, {signal: controller.signal});
      if (!response.ok) { unavailable(); return; }
      const mediaType = (response.headers.get('content-type') || '').split(';', 1)[0].trim().toLowerCase();
      if (mediaType.startsWith('text/')) {
        const text = await response.text();
        if (closed) return;
        const pre = document.createElement('pre');
        pre.textContent = text;
        content.replaceChildren(pre);
        return;
      }
      const raster = ['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/avif', 'image/bmp', 'image/x-icon'].includes(mediaType);
      const pdf = mediaType === 'application/pdf';
      if (pdf && 'pdfViewerEnabled' in navigator && !navigator.pdfViewerEnabled) { unavailable(); return; }
      const audio = mediaType.startsWith('audio/') || mediaType === 'application/ogg';
      const video = mediaType.startsWith('video/');
      if (!(raster || pdf || audio || video)) { unavailable(); return; }
      const blob = await response.blob();
      if (closed) return;
      objectURL = URL.createObjectURL(blob);
      let element: HTMLImageElement | HTMLIFrameElement | HTMLAudioElement | HTMLVideoElement;
      if (raster) {
        const image = document.createElement('img');
        image.alt = attachment.name;
        image.src = objectURL;
        element = image;
      } else if (pdf) {
        const frame = document.createElement('iframe');
        frame.title = `PDF preview: ${attachment.name}`;
        frame.src = objectURL;
        element = frame;
      } else {
        const player = document.createElement(audio ? 'audio' : 'video');
        player.controls = true;
        player.preload = 'metadata';
        player.src = objectURL;
        element = player;
      }
      element.addEventListener('error', unavailable, {once: true});
      content.replaceChildren(element);
    } catch {
      if (!controller.signal.aborted) unavailable();
    }
  })();
}
