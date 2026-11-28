export const settingsSections = [
  ['models', 'Models'],
  ['providers', 'Local providers'],
  ['usage', 'Usage limits'],
  ['codex', 'Codex setup'],
  ['updates', 'Updates'],
] as const;
export type SettingsSection = typeof settingsSections[number][0];

export type AppRoute =
  | {kind: 'legacy' | 'root' | 'unknown'}
  | {kind: 'settings'; section: SettingsSection}
  | {kind: 'group' | 'agent'; id: string};

const projectSlug = /^[a-z0-9][a-z0-9-]{0,62}$/;
const agentID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
// Keep top-level routes in sync with the service's app shell paths.
const reserved = new Set(['root', 'settings', 'groups', 'agents', 'api', 'assets', 'files', 'attachments']);

export function routeAt(path: string): AppRoute {
  if (path === '/') return {kind:'legacy'};
  if (path === '/root') return {kind:'root'};
  if (path === '/settings') return {kind:'settings',section:'models'};
  const [, first, second, ...remaining] = path.split('/');
  if (remaining.length) return {kind:'unknown'};
  if (first === 'settings') {
    const section = settingsSections.find(([key]) => key === second)?.[0];
    if (section) return {kind:'settings',section};
  }
  if (first === 'groups' && second && projectSlug.test(second)) return {kind:'group',id:second};
  if (first === 'agents' && second && agentID.test(second)) return {kind:'agent',id:second};
  if (second === undefined && projectSlug.test(first) && !reserved.has(first)) return {kind:'group',id:first};
  return {kind:'unknown'};
}

export function settingsPath(section: SettingsSection): string {
  return section === 'models' ? '/settings' : `/settings/${section}`;
}

export function groupPath(project: {id:string; name:string}): string {
  const slug = project.name.normalize('NFKD').replace(/\p{Mark}/gu,'')
    .toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');
  // A friendly path is an alias for the immutable ID. Renames never retarget it.
  return slug === project.id && projectSlug.test(slug) && !reserved.has(slug)
    ? `/${slug}`
    : `/groups/${encodeURIComponent(project.id)}`;
}

export function agentPath(id: string): string {
  return `/agents/${encodeURIComponent(id)}`;
}
