export type SelectChoice = {value: string; label: string; disabled?: boolean};
export type SelectGroup = {label?: string; choices: SelectChoice[]};

type SelectProps = {
  id: string;
  labelId: string;
  value: string;
  groups: SelectGroup[];
  disabled: boolean;
  onChange: (value: string) => void;
};

let openSelect: {trigger: HTMLButtonElement; close: () => void} | undefined;

export function closeSettingsSelect(): void {
  openSelect?.close();
}

export function settingsSelect({id, labelId, value, groups, disabled, onChange}: SelectProps): HTMLButtonElement {
  const trigger = document.createElement('button');
  trigger.type = 'button';
  trigger.id = id;
  trigger.className = 'settings-select-trigger';
  trigger.disabled = disabled;
  trigger.setAttribute('role', 'combobox');
  trigger.setAttribute('aria-labelledby', labelId);
  trigger.setAttribute('aria-expanded', 'false');

  const selectedText = document.createElement('span');
  selectedText.id = `${id}-value`;
  selectedText.className = 'settings-select-value';
  selectedText.textContent = groups.flatMap(group => group.choices).find(choice => choice.value === value)?.label || '';
  trigger.setAttribute('aria-describedby', selectedText.id);
  const indicator = document.createElement('span');
  indicator.className = 'settings-select-chevron';
  indicator.setAttribute('aria-hidden', 'true');
  trigger.append(selectedText, indicator);

  function show(initialKey?: string): void {
    closeSettingsSelect();
    const popup = document.createElement('div');
    popup.className = 'settings-select-popup';
    const list = document.createElement('div');
    list.id = `${id}-options`;
    list.className = 'settings-select-viewport';
    list.setAttribute('role', 'listbox');
    list.setAttribute('aria-labelledby', labelId);

    const listeners = new AbortController();
    const {signal} = listeners;
    const options: {choice: SelectChoice; row: HTMLDivElement}[] = [];
    let active = -1;
    let search = '';
    let lastKey = 0;
    let scrollTimer: number | undefined;
    let scrollDirection: 'up' | 'down' | undefined;

    function stopScrolling() {
      window.clearInterval(scrollTimer);
      scrollTimer = undefined;
      scrollDirection = undefined;
    }

    function close() {
      stopScrolling();
      listeners.abort();
      popup.remove();
      trigger.removeAttribute('aria-controls');
      trigger.removeAttribute('aria-activedescendant');
      trigger.setAttribute('aria-expanded', 'false');
      openSelect = undefined;
    }

    function choose(index: number) {
      const option = options[index];
      if (option) {
        value = option.choice.value;
        selectedText.textContent = option.choice.label;
        onChange(value);
      }
      close();
    }

    function highlight(index: number, reveal = true) {
      const next = options[index];
      if (!next) return;
      options[active]?.row.removeAttribute('data-highlighted');
      active = index;
      next.row.setAttribute('data-highlighted', '');
      trigger.setAttribute('aria-activedescendant', next.row.id);
      if (!reveal) return;
      const viewport = list.getBoundingClientRect();
      const row = next.row.getBoundingClientRect();
      if (row.top < viewport.top) list.scrollTop += row.top - viewport.top;
      else if (row.bottom > viewport.bottom) list.scrollTop += row.bottom - viewport.bottom;
    }

    let rowNumber = 0;
    for (const group of groups) {
      const container = group.label ? document.createElement('div') : list;
      if (group.label) {
        container.setAttribute('role', 'group');
        container.setAttribute('aria-label', group.label);
        const title = document.createElement('div');
        title.className = 'settings-select-group-label';
        title.setAttribute('aria-hidden', 'true');
        title.textContent = group.label;
        container.append(title);
        list.append(container);
      }
      for (const choice of group.choices) {
        const row = document.createElement('div');
        row.id = `${list.id}-${rowNumber++}`;
        row.className = 'settings-select-option';
        row.setAttribute('role', 'option');
        row.setAttribute('aria-selected', String(choice.value === value));
        row.textContent = choice.label;
        row.addEventListener('mousedown', event => event.preventDefault(), {signal});
        if (choice.disabled) {
          row.setAttribute('aria-disabled', 'true');
        } else {
          const index = options.length;
          options.push({choice, row});
          row.addEventListener('pointermove', event => {
            if (event.pointerType === 'mouse') highlight(index, false);
          }, {signal});
          row.addEventListener('click', () => choose(index), {signal});
        }
        container.append(row);
      }
    }

    function scrollControl(direction: 'up' | 'down'): HTMLButtonElement {
      const button = document.createElement('button');
      button.className = `settings-select-scroll settings-select-scroll-${direction}`;
      button.type = 'button';
      button.tabIndex = -1;
      button.setAttribute('aria-label', direction === 'up' ? 'Scroll models up' : 'Scroll models down');
      const chevron = document.createElement('span');
      chevron.className = 'settings-select-chevron';
      chevron.setAttribute('aria-hidden', 'true');
      button.append(chevron);
      const step = direction === 'up' ? -1 : 1;
      button.addEventListener('mousedown', event => event.preventDefault(), {signal});
      button.addEventListener('click', () => { list.scrollTop += step * 100; }, {signal});
      button.addEventListener('pointerenter', event => {
        if (event.pointerType !== 'mouse' || button.disabled) return;
        stopScrolling();
        scrollDirection = direction;
        scrollTimer = window.setInterval(() => { list.scrollTop += step * 10; }, 30);
      }, {signal});
      button.addEventListener('pointerleave', stopScrolling, {signal});
      return button;
    }

    const up = scrollControl('up');
    const down = scrollControl('down');
    popup.append(up, list, down);
    document.body.append(popup);
    openSelect = {trigger, close};
    trigger.setAttribute('aria-controls', list.id);
    trigger.setAttribute('aria-expanded', 'true');

    function syncScrollControls() {
      up.disabled = list.scrollTop < 1;
      down.disabled = list.scrollTop + list.clientHeight >= list.scrollHeight - 1;
      if ((scrollDirection === 'up' && up.disabled) || (scrollDirection === 'down' && down.disabled)) stopScrolling();
    }

    function place() {
      const rect = trigger.getBoundingClientRect();
      const visual = window.visualViewport;
      const left = (visual?.offsetLeft ?? 0) + 6;
      const top = (visual?.offsetTop ?? 0) + 6;
      const right = left + (visual?.width ?? window.innerWidth) - 12;
      const bottom = top + (visual?.height ?? window.innerHeight) - 12;
      const above = rect.top - top - 4;
      const below = bottom - rect.bottom - 4;
      if (!trigger.isConnected || rect.bottom <= top || rect.top >= bottom || Math.max(above, below) < 40) {
        close();
        return;
      }
      const width = Math.min(rect.width, right - left);
      popup.style.width = `${width}px`;
      popup.style.left = `${Math.max(left, Math.min(rect.left, right - width))}px`;
      const desired = Math.min(284, list.scrollHeight + 2);
      const opensBelow = below >= desired || below >= above;
      const height = Math.min(284, opensBelow ? below : above);
      popup.style.maxHeight = `${height}px`;
      up.hidden = down.hidden = list.scrollHeight + 2 <= height;
      popup.style.top = `${opensBelow ? rect.bottom + 4 : rect.top - popup.offsetHeight - 4}px`;
      syncScrollControls();
    }

    function matchTyped(key: string) {
      const now = performance.now();
      const letter = key.toLocaleLowerCase();
      search = (now - lastKey < 750 ? search : '') + letter;
      lastKey = now;
      const cycling = Array.from(search).every(character => character === letter);
      const prefix = cycling ? letter : search;
      const start = cycling ? active + 1 : 0;
      for (let offset = 0; offset < options.length; offset++) {
        const index = (start + offset) % options.length;
        if (options[index].choice.label.toLocaleLowerCase().startsWith(prefix)) {
          highlight(index);
          break;
        }
      }
    }

    function navigate(event: KeyboardEvent) {
      const key = event.key;
      switch (key) {
        case 'Escape':
          event.preventDefault();
          close();
          break;
        case 'Enter':
        case ' ':
          event.preventDefault();
          choose(active);
          break;
        case 'Tab':
          choose(active);
          break;
        case 'Home':
        case 'End':
        case 'ArrowUp':
        case 'ArrowDown':
        case 'PageUp':
        case 'PageDown': {
          event.preventDefault();
          const offset = key === 'ArrowUp' ? -1 : key === 'ArrowDown' ? 1 : key === 'PageUp' ? -10 : 10;
          const index = key === 'Home' ? 0 : key === 'End' ? options.length - 1 : active + offset;
          highlight(Math.max(0, Math.min(index, options.length - 1)));
          break;
        }
        default:
          if (key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
            event.preventDefault();
            matchTyped(key);
          }
      }
    }

    list.addEventListener('scroll', syncScrollControls, {signal});
    document.addEventListener('pointerdown', event => {
      if (event.target instanceof Node && !popup.contains(event.target) && !trigger.contains(event.target)) close();
    }, {signal});
    document.addEventListener('scroll', event => {
      if (!(event.target instanceof Node) || !popup.contains(event.target)) place();
    }, {capture: true, signal});
    window.addEventListener('resize', place, {signal});
    window.visualViewport?.addEventListener('resize', place, {signal});
    window.visualViewport?.addEventListener('scroll', place, {signal});
    trigger.addEventListener('blur', close, {signal});
    trigger.addEventListener('keydown', navigate, {signal});

    place();
    if (openSelect?.trigger !== trigger) return;
    const index = options.findIndex(option => option.choice.value === value);
    highlight(initialKey === 'Home' ? 0 : initialKey === 'End' ? options.length - 1 : Math.max(0, index));
    if (initialKey && initialKey !== ' ' && initialKey.length === 1) matchTyped(initialKey);
  }

  trigger.addEventListener('click', () => {
    if (openSelect?.trigger === trigger) closeSettingsSelect();
    else {
      trigger.focus({preventScroll: true});
      show();
    }
  });
  trigger.addEventListener('keydown', event => {
    if (openSelect?.trigger === trigger) return;
    const key = event.key;
    if (['ArrowDown','ArrowUp','Home','End','Enter',' '].includes(key) ||
        (key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey)) {
      event.preventDefault();
      show(key);
    }
  });
  return trigger;
}
