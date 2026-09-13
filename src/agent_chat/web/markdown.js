/* Deliberately small Markdown subset. Build DOM nodes only: message text is never HTML. */
(() => {
  'use strict';
  const element = (tag, text) => {
    const result = document.createElement(tag);
    if (text !== undefined) result.textContent = text;
    return result;
  };
  const safeLink = value => {
    if (/[\u0000-\u0020\u007f]/.test(value)) return null;
    try {
      const url = new URL(value);
      return ['http:', 'https:', 'mailto:'].includes(url.protocol) ? url.href : null;
    } catch { return null; }
  };

  function inline(source, parent, depth = 0) {
    if (depth >= 12) { parent.append(document.createTextNode(source)); return; }
    const tokens = /\\([\\`*_[\]{}()#+.!|>~-])|(?<!`)(`+)(?!`)([\s\S]*?)(?<!`)\2(?!`)|\[([^\]\n]+)\]\(([^)\s]+)\)|(\*\*|__)(\S(?:[^\n]*?\S)?)\6|(\*|_)(\S(?:[^\n]*?\S)?)\8/g;
    let offset = 0;
    for (const match of source.matchAll(tokens)) {
      parent.append(document.createTextNode(source.slice(offset, match.index)));
      let child;
      if (match[1]) child = document.createTextNode(match[1]);
      else if (match[2]) child = element('code', match[3].replace(/\n/g, ' '));
      else if (match[4]) {
        const href = safeLink(match[5]);
        if (href) {
          child = element('a');
          child.href = href;
          child.target = '_blank';
          child.rel = 'noopener noreferrer';
          // Link labels remain text: nested anchors and image embeds are not supported.
          child.textContent = match[4];
        }
      } else {
        const delimiter = match[6] || match[8];
        const before = source[match.index - 1] || '';
        const after = source[match.index + match[0].length] || '';
        // Preserve underscores inside identifiers and file names.
        if (!delimiter.startsWith('_') || !/[\p{L}\p{N}]/u.test(before + after)) {
          child = element(match[6] ? 'strong' : 'em');
          inline(match[7] || match[9], child, depth + 1);
        }
      }
      parent.append(child || document.createTextNode(match[0]));
      offset = match.index + match[0].length;
    }
    parent.append(document.createTextNode(source.slice(offset)));
  }

  function cells(line) {
    const text = line.trim();
    const result = [];
    let cell = '', fence = 0, pipes = 0;
    for (let i = 0; i < text.length; i += 1) {
      if (text[i] === '\\' && i + 1 < text.length) { cell += text[i] + text[++i]; continue; }
      if (text[i] === '`') {
        const start = i;
        while (text[i + 1] === '`') i += 1;
        const length = i - start + 1;
        if (!fence) fence = length;
        else if (fence === length) fence = 0;
        cell += text.slice(start, i + 1);
      } else if (text[i] === '|' && !fence) {
        result.push(cell.trim()); cell = ''; pipes += 1;
      } else cell += text[i];
    }
    if (!pipes) return null;
    result.push(cell.trim());
    if (text.startsWith('|')) result.shift();
    if (result.at(-1) === '' && text.endsWith('|')) result.pop();
    return result;
  }

  const heading = line => /^ {0,3}(#{1,6})\s+(.*)$/.exec(line);
  const fence = line => /^ {0,3}(`{3,}|~{3,})(.*)$/.exec(line);
  const listItem = line => /^( {0,3})([-+*]|\d{1,9}[.)])\s+(.*)$/.exec(line);
  const quote = line => /^ {0,3}> ?/.test(line);
  const rule = line => /^ {0,3}(?:(?:\*\s*){3,}|(?:-\s*){3,}|(?:_\s*){3,})$/.test(line);
  function tableAt(lines, index) {
    if (index + 1 >= lines.length) return null;
    const header = cells(lines[index]), divider = cells(lines[index + 1]);
    return header && divider && header.length === divider.length &&
      divider.every(cell => /^:?-{3,}:?$/.test(cell)) ? { header, divider } : null;
  }
  const startsBlock = (lines, i) => heading(lines[i]) || fence(lines[i]) ||
    listItem(lines[i]) || quote(lines[i]) || rule(lines[i]) || tableAt(lines, i);

  function blocks(lines, parent, depth = 0) {
    if (depth >= 12) { parent.append(element('p', lines.join('\n'))); return; }
    let i = 0;
    while (i < lines.length) {
      if (!lines[i].trim()) { i += 1; continue; }
      const code = fence(lines[i]);
      if (code) {
        const content = [];
        i += 1;
        while (i < lines.length) {
          const close = fence(lines[i]);
          if (close && close[1][0] === code[1][0] && close[1].length >= code[1].length && !close[2].trim()) { i += 1; break; }
          content.push(lines[i++]);
        }
        const pre = element('pre');
        pre.append(element('code', content.join('\n')));
        parent.append(pre);
        continue;
      }
      const title = heading(lines[i]);
      if (title) {
        const h = element('h' + title[1].length);
        inline(title[2].replace(/\s+#+\s*$/, ''), h);
        parent.append(h); i += 1; continue;
      }
      if (rule(lines[i])) { parent.append(element('hr')); i += 1; continue; }
      const table = tableAt(lines, i);
      if (table) {
        const wrapper = element('div');
        wrapper.className = 'markdown-table';
        wrapper.tabIndex = 0;
        wrapper.setAttribute('role', 'region');
        wrapper.setAttribute('aria-label', 'Message table');
        const grid = element('table'), head = element('thead'), body = element('tbody');
        const appendRow = (values, target, tag) => {
          const row = element('tr');
          table.header.forEach((_, column) => {
            const cell = element(tag);
            if (tag === 'th') cell.scope = 'col';
            const alignment = table.divider[column];
            cell.className = alignment.endsWith(':') ? (alignment.startsWith(':') ? 'align-center' : 'align-right') : 'align-left';
            inline(values[column] || '', cell);
            row.append(cell);
          });
          target.append(row);
        };
        appendRow(table.header, head, 'th');
        i += 2;
        while (i < lines.length && lines[i].trim()) {
          const values = cells(lines[i]);
          if (!values || values.length > table.header.length) break;
          appendRow(values, body, 'td'); i += 1;
        }
        grid.append(head, body); wrapper.append(grid); parent.append(wrapper); continue;
      }
      if (quote(lines[i])) {
        const content = [];
        while (i < lines.length && quote(lines[i])) content.push(lines[i++].replace(/^ {0,3}> ?/, ''));
        const block = element('blockquote');
        blocks(content, block, depth + 1); parent.append(block); continue;
      }
      const first = listItem(lines[i]);
      if (first) {
        const ordered = /^\d/.test(first[2]), list = element(ordered ? 'ol' : 'ul');
        if (ordered) list.start = Number.parseInt(first[2], 10);
        while (i < lines.length) {
          const item = listItem(lines[i]);
          if (!item || item[1].length !== first[1].length || /^\d/.test(item[2]) !== ordered || rule(lines[i])) break;
          const content = [item[3]], indent = item[0].length - item[3].length;
          i += 1;
          // Indented continuation lines include nested lists and fenced code.
          while (i < lines.length && lines[i].startsWith(' '.repeat(indent))) content.push(lines[i++].slice(indent));
          const li = element('li');
          blocks(content, li, depth + 1); list.append(li);
        }
        parent.append(list); continue;
      }
      const paragraph = [lines[i++]];
      while (i < lines.length && lines[i].trim() && !startsBlock(lines, i)) paragraph.push(lines[i++]);
      const p = element('p');
      inline(paragraph.join('\n'), p); parent.append(p);
    }
  }
  window.CoordMarkdown = Object.freeze({
    render(source) {
      const fragment = document.createDocumentFragment();
      blocks(String(source).replace(/\r\n?/g, '\n').split('\n'), fragment);
      return fragment;
    },
  });
})();
