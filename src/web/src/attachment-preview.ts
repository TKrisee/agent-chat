import './attachment-preview.css';

type Attachment = {id: string; name: string; media_type?: string};
const imageTypes = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/avif', 'image/bmp', 'image/x-icon']);
export const isPreviewableImage = (attachment: Attachment): boolean => imageTypes.has((attachment.media_type || '').split(';', 1)[0].trim().toLowerCase());
let closeCurrent: (() => void) | undefined;

/** Open a preview outside the app's frequently re-rendered transcript. */
export function previewAttachment(attachment: Attachment, trigger: HTMLElement, messageAttachments: Attachment[] = []): void {
  closeCurrent?.();
  const images = isPreviewableImage(attachment) ? messageAttachments.filter(isPreviewableImage) : [];
  const gallery = images.some(image => image.id === attachment.id) ? images : [attachment];
  let index = gallery.findIndex(image => image.id === attachment.id);
  const dialog = document.createElement('dialog');
  dialog.className = 'attachment-preview';
  dialog.setAttribute('aria-labelledby', 'attachment-preview-title');
  const header = document.createElement('div');
  header.className = 'attachment-preview-header';
  const title = document.createElement('h2');
  title.id = 'attachment-preview-title';
  const close = document.createElement('button');
  close.type = 'button'; close.className = 'attachment-preview-close';
  close.setAttribute('aria-label', 'Close preview'); close.textContent = '×';
  header.append(title, close);
  const content = document.createElement('div');
  content.className = 'attachment-preview-content'; content.setAttribute('aria-live', 'polite');
  const navigation = document.createElement('div'); navigation.className = 'attachment-preview-navigation';
  const previous = document.createElement('button'); previous.type = 'button'; previous.textContent = '←'; previous.setAttribute('aria-label', 'Previous image');
  const next = document.createElement('button'); next.type = 'button'; next.textContent = '→'; next.setAttribute('aria-label', 'Next image');
  const thumbnails = document.createElement('div'); thumbnails.className = 'attachment-preview-thumbnails'; thumbnails.setAttribute('aria-label', 'Images in this message');
  if (gallery.length > 1) {
    gallery.forEach((item, i) => {
      const button = document.createElement('button'); button.type = 'button'; button.setAttribute('aria-label', `Show ${item.name}`);
      const image = document.createElement('img'); image.alt = ''; image.loading = 'lazy'; image.src = `/api/attachments/${encodeURIComponent(item.id)}/preview`;
      button.append(image); button.addEventListener('click', () => show(i)); thumbnails.append(button);
    });
    navigation.append(previous, thumbnails, next);
  }
  const footer = document.createElement('div'); footer.className = 'attachment-preview-footer';
  const download = document.createElement('a'); download.textContent = 'Download'; footer.append(download);
  dialog.append(header, content, navigation, footer); document.body.append(dialog);

  let controller: AbortController | undefined;
  let objectURL: string | undefined;
  let closed = false, generation = 0;
  const restoreFocus = () => {
    if (trigger.isConnected) { trigger.focus({preventScroll: true}); return; }
    const replacement = Array.from(document.querySelectorAll<HTMLElement>('[data-attachment-preview]')).find(element => element.dataset.attachmentPreview === attachment.id);
    replacement?.focus({preventScroll: true});
  };
  const cleanup = () => {
    if (closed) return;
    closed = true; controller?.abort(); if (objectURL) URL.revokeObjectURL(objectURL);
    dialog.remove(); if (closeCurrent === dismiss) closeCurrent = undefined; restoreFocus();
  };
  const dismiss = () => { if (dialog.open) dialog.close(); cleanup(); };
  closeCurrent = dismiss; close.addEventListener('click', dismiss); dialog.addEventListener('close', cleanup);
  previous.addEventListener('click', () => show(index - 1)); next.addEventListener('click', () => show(index + 1));
  dialog.addEventListener('keydown', event => {
    if (gallery.length < 2 || event.isComposing || event.altKey || event.ctrlKey || event.metaKey) return;
    if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') { event.preventDefault(); show(index + (event.key === 'ArrowLeft' ? -1 : 1)); }
  });
  dialog.showModal(); close.focus(); show(index);

  function show(nextIndex: number): void {
    if (closed || nextIndex < 0 || nextIndex >= gallery.length) return;
    index = nextIndex; const item = gallery[index]; const current = ++generation;
    controller?.abort(); controller = new AbortController(); const signal = controller.signal;
    if (objectURL) { URL.revokeObjectURL(objectURL); objectURL = undefined; }
    title.textContent = item.name; download.href = `/api/attachments/${encodeURIComponent(item.id)}?download=1`; download.download = item.name;
    previous.disabled = index === 0; next.disabled = index === gallery.length - 1;
    Array.from(thumbnails.children).forEach((button, i) => button.setAttribute('aria-pressed', String(i === index)));
    content.textContent = 'Loading preview…';
    const unavailable = () => {
      if (closed || current !== generation) return;
      const message = document.createElement('p'); message.className = 'attachment-preview-unavailable';
      message.textContent = 'Preview unavailable. Download the attachment to open it.'; content.replaceChildren(message);
    };
    void (async () => {
      try {
        const response = await fetch(`/api/attachments/${encodeURIComponent(item.id)}/preview`, {signal});
        if (!response.ok) { unavailable(); return; }
        const mediaType = (response.headers.get('content-type') || '').split(';', 1)[0].trim().toLowerCase();
        if (mediaType.startsWith('text/')) {
          const text = await response.text(); if (closed || current !== generation) return;
          const pre = document.createElement('pre'); pre.textContent = text; content.replaceChildren(pre); return;
        }
        const raster = imageTypes.has(mediaType), pdf = mediaType === 'application/pdf';
        if (pdf && 'pdfViewerEnabled' in navigator && !navigator.pdfViewerEnabled) { unavailable(); return; }
        const audio = mediaType.startsWith('audio/') || mediaType === 'application/ogg', video = mediaType.startsWith('video/');
        if (!(raster || pdf || audio || video)) { unavailable(); return; }
        const blob = await response.blob(); if (closed || current !== generation) return;
        objectURL = URL.createObjectURL(blob);
        let element: HTMLImageElement | HTMLIFrameElement | HTMLAudioElement | HTMLVideoElement;
        if (raster) { const image = document.createElement('img'); image.alt = item.name; image.src = objectURL; element = image; }
        else if (pdf) { const frame = document.createElement('iframe'); frame.title = `PDF preview: ${item.name}`; frame.src = objectURL; element = frame; }
        else { const player = document.createElement(audio ? 'audio' : 'video'); player.controls = true; player.preload = 'metadata'; player.src = objectURL; element = player; }
        element.addEventListener('error', unavailable, {once: true}); content.replaceChildren(element);
      } catch { if (!signal.aborted) unavailable(); }
    })();
  }
}
