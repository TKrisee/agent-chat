"""Data-only publication proposal bound to an existing author-owned decision.

This module does not contact Git, publish, or turn a decision into authority.
"""
from dataclasses import dataclass
from hashlib import sha256


def _text(value):
    if not isinstance(value, str) or not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError('plan fields must be nonempty single-line strings')
    return value


def _object_id(value):
    if (not isinstance(value, str) or len(value) not in (40, 64)
            or any(c not in '0123456789abcdef' for c in value) or not any(c != '0' for c in value)):
        raise ValueError('commit must be a full lowercase SHA-1 or SHA-256 object ID')
    return value


@dataclass(frozen=True)
class Repository:
    name: str
    destination: str
    ref: str
    commit: str

    def __post_init__(self):
        for value in (self.name, self.destination, self.ref, self.commit): _text(value)
        _object_id(self.commit)


@dataclass(frozen=True)
class Gitlink:
    parent: str
    path: str
    child: str
    commit: str

    def __post_init__(self):
        for value in (self.parent, self.path, self.child, self.commit): _text(value)
        _object_id(self.commit)


@dataclass(frozen=True)
class Plan:
    repositories: tuple[Repository, ...]
    gitlinks: tuple[Gitlink, ...]

    def __post_init__(self):
        if not isinstance(self.repositories, tuple) or not self.repositories or not all(isinstance(r, Repository) for r in self.repositories):
            raise ValueError('repositories must be a nonempty immutable tuple')
        if not isinstance(self.gitlinks, tuple) or not all(isinstance(g, Gitlink) for g in self.gitlinks):
            raise ValueError('gitlinks must be an immutable tuple')
        by_name = {r.name: r for r in self.repositories}
        if len(by_name) != len(self.repositories): raise ValueError('repository names must be unique')
        seen = set()
        for link in self.gitlinks:
            if link.parent not in by_name or link.child not in by_name or link.parent == link.child:
                raise ValueError('gitlink endpoints must be distinct planned repositories')
            if link.commit != by_name[link.child].commit: raise ValueError('gitlink must match planned child commit')
            key = (link.parent, link.path)
            if key in seen: raise ValueError('duplicate parent gitlink path')
            seen.add(key)


def decision_body(plan: Plan) -> str:
    if not isinstance(plan, Plan): raise ValueError('expected immutable plan')
    # The full visible body, including order and every field, is the binding.
    lines = ['Publication plan v1', 'Repositories:']
    for r in plan.repositories:
        lines.append(f'  name={r.name!r} destination={r.destination!r} ref={r.ref!r} commit={r.commit!r}')
    lines.append('Gitlinks:')
    for g in plan.gitlinks:
        lines.append(f'  parent={g.parent!r} path={g.path!r} child={g.child!r} commit={g.commit!r}')
    content = '\n'.join(lines) + '\n'
    return content + 'plan-sha256=' + sha256(content.encode('utf-8')).hexdigest() + '\n'


def decision_state(author, message_id: str, plan: Plan) -> str:
    """Return the author-owned decision state only when its entire proposal matches."""
    sent = author.sent_message(message_id)
    decision = author.decision_status(message_id)
    if sent['message']['body'] != decision_body(plan): return 'mismatch'
    if not decision['terminal'] or decision['answer_outcome'] not in ('accepted', 'declined'):
        return 'pending'
    return decision['answer_outcome']


def from_fields(value) -> Plan:
    """Validate declared fields received from an App caller, without discovery."""
    if not isinstance(value, dict) or set(value) != {'repositories', 'gitlinks'}:
        raise ValueError('plan requires repositories and gitlinks')
    repositories, gitlinks = value['repositories'], value['gitlinks']
    if not isinstance(repositories, list) or not isinstance(gitlinks, list) or not 1 <= len(repositories) <= 50 or len(gitlinks) > 100:
        raise ValueError('plan exceeds repository or gitlink limits')
    if not all(isinstance(row, list) and len(row) == 4 for row in repositories + gitlinks):
        raise ValueError('each plan entry requires exactly four fields')
    if any(not isinstance(x, str) or len(x.encode('utf-8')) > 512 for row in repositories + gitlinks for x in row):
        raise ValueError('plan field exceeds limit')
    return Plan(tuple(Repository(*row) for row in repositories), tuple(Gitlink(*row) for row in gitlinks))
