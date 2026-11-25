"""Pure admission-intent checks, NOT authentication, provisioning, or a grant.

Evidence inputs must come from a separately authenticated, verified producer.
These value objects cannot establish provenance: matching values or SHA strings
are not proof that the claimed files/settings/identity exist. No I/O occurs.
The future adapter must verify evidence before calling prepare_intent and must
still perform configured-Root authorization, runtime readback and domain commit.
"""
from dataclasses import astuple, dataclass, field
from hashlib import sha256
from pathlib import PurePosixPath
from uuid import UUID


class IntentError(ValueError):
    pass


class IntentConflict(IntentError):
    pass


def _text(value, label):
    if type(value) is not str or not value or len(value) > 4096 or any(ord(c) < 32 for c in value):
        raise IntentError(f'{label}: bounded nonempty text required')


def _path(value, label):
    _text(value, label)
    path = PurePosixPath(value)
    if path.anchor != '/' or '..' in path.parts or str(path) != value or value == '/':
        raise IntentError(f'{label}: normalized absolute non-root path required')
    return path


def _uuid(value, label):
    try:
        if type(value) is not str or str(UUID(value)) != value:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise IntentError(f'{label}: canonical UUID required') from None


def _typed(value, expected, label):
    if type(value) is not expected:
        raise IntentError(f'{label}: {expected.__name__} required')


def _roots(values, label):
    if type(values) is not tuple:
        raise IntentError(f'{label}: immutable distinct tuple required')
    for value in values:
        _path(value, label)
    if len(set(values)) != len(values):
        raise IntentError(f'{label}: immutable distinct tuple required')


@dataclass(frozen=True, slots=True)
class EvidencePin:
    reference: str
    digest: str

    def __post_init__(self):
        _text(self.reference, 'evidence reference')
        if type(self.digest) is not str or len(self.digest) != 64 or any(c not in '0123456789abcdef' for c in self.digest):
            raise IntentError('evidence pin: lowercase SHA256 required')


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    project: str
    worker: str
    main: str
    host: str

    def __post_init__(self):
        for label in ('project', 'worker', 'host'):
            _text(getattr(self, label), label)
        _uuid(self.main, 'main')


@dataclass(frozen=True, slots=True)
class Domain:
    root: str
    gitdir: str
    common_dir: str
    index: str
    leaf_prefix: str
    workspace_resource: str
    canonical_resource: str
    index_resource: str

    def __post_init__(self):
        root = _path(self.root, 'root')
        gitdir = _path(self.gitdir, 'gitdir')
        common = _path(self.common_dir, 'common dir')
        _path(self.index, 'index')
        if gitdir == common or not gitdir.is_relative_to(common):
            raise IntentError('detached worktree gitdir must be below the common gitdir')
        if common.is_relative_to(root) or root.is_relative_to(common):
            raise IntentError('worktree and backing must be independent nonnested paths')
        if self.index != str(gitdir / 'index'):
            raise IntentError('index must belong to the exact detached gitdir')
        for logical, label in ((self.leaf_prefix[:-1], 'leaf prefix'), (self.index_resource, 'index resource')):
            _text(logical, label)
            if not logical.startswith('file:'):
                raise IntentError(label + ': file resource required')
            relative = logical[5:]
            value = PurePosixPath(relative)
            if not relative or value.is_absolute() or '..' in value.parts or str(value) != relative:
                raise IntentError(label + ': normalized repository-relative resource required')
        if not self.leaf_prefix.endswith('/'):
            raise IntentError('leaf prefix must end with slash')
        if self.workspace_resource != 'workspace:' + self.root:
            raise IntentError('workspace resource must name the exact physical root')
        _text(self.canonical_resource, 'canonical resource')
        if (any(c.isspace() for c in self.canonical_resource) or '..' in self.canonical_resource.split('/')
                or self.canonical_resource.startswith(('heavy-unity:', 'file:', 'workspace:'))):
            raise IntentError('canonical native identity must be separate from capacity and backing resources')


@dataclass(frozen=True, slots=True)
class PhysicalIdentity:
    path: str
    device: int
    inode: int

    def __post_init__(self):
        _path(self.path, 'physical path')
        if type(self.device) is not int or self.device < 0 or type(self.inode) is not int or self.inode <= 0:
            raise IntentError('physical identity requires explicit device/inode values')


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    model: str
    provider: str
    effort: str
    approval_policy: str
    approvals_reviewer: str
    permission_profile: str | None
    sandbox: str
    network_access: bool
    exclude_tmpdir: bool
    exclude_slash_tmp: bool
    cwd: str
    workspace_roots: tuple[str, ...]
    writable_roots: tuple[str, ...]

    def __post_init__(self):
        for label in ('model', 'provider', 'effort', 'approval_policy', 'approvals_reviewer', 'sandbox'):
            _text(getattr(self, label), label)
        if self.permission_profile is not None:
            _text(self.permission_profile, 'permission profile')
        for label in ('network_access', 'exclude_tmpdir', 'exclude_slash_tmp'):
            if type(getattr(self, label)) is not bool:
                raise IntentError(f'{label}: explicit boolean required')
        _path(self.cwd, 'cwd')
        _roots(self.workspace_roots, 'workspace roots')
        _roots(self.writable_roots, 'writable roots')
        if self.cwd not in self.workspace_roots:
            raise IntentError('cwd must be included in runtime workspace roots')

    def protected(self):
        return astuple(self)[:10]


@dataclass(frozen=True, slots=True)
class IdentityEvidence:
    identity: WorkerIdentity
    pin: EvidencePin

    def __post_init__(self):
        _typed(self.identity, WorkerIdentity, 'observed identity')
        _typed(self.pin, EvidencePin, 'identity pin')


@dataclass(frozen=True, slots=True)
class DomainEvidence:
    domain: Domain
    root: PhysicalIdentity
    git_file: PhysicalIdentity
    gitdir: PhysicalIdentity
    common_dir: PhysicalIdentity
    index: PhysicalIdentity
    provisioning: EvidencePin
    producer_reference: str
    producer: EvidencePin

    def __post_init__(self):
        _typed(self.domain, Domain, 'observed domain')
        expected = (self.domain.root, self.domain.root + '/.git', self.domain.gitdir,
                    self.domain.common_dir, self.domain.index)
        identities = (self.root, self.git_file, self.gitdir, self.common_dir, self.index)
        for value, path in zip(identities, expected):
            _typed(value, PhysicalIdentity, 'physical evidence')
            if value.path != path:
                raise IntentError('physical evidence path differs from domain mapping')
        if len({(value.device, value.inode) for value in identities}) != len(identities):
            raise IntentError('independent backing identities must not alias')
        _typed(self.provisioning, EvidencePin, 'provisioning pin')
        _typed(self.producer, EvidencePin, 'producer pin')
        _text(self.producer_reference, 'private trusted producer reference')


@dataclass(frozen=True, slots=True)
class SettingsEvidence:
    current: RuntimeSettings
    selected: RuntimeSettings
    pin: EvidencePin

    def __post_init__(self):
        _typed(self.current, RuntimeSettings, 'current settings')
        _typed(self.selected, RuntimeSettings, 'selected settings')
        _typed(self.pin, EvidencePin, 'settings pin')
        if self.current.protected() != self.selected.protected():
            raise IntentError('workspace intent cannot change protected settings')
        if self.selected.sandbox != 'workspaceWrite' or self.selected.permission_profile not in (None, ':workspace'):
            raise IntentError('runtime permission mapping unresolved for this policy/profile')


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    request_id: str
    identity: WorkerIdentity
    domain: Domain
    settings: RuntimeSettings

    def __post_init__(self):
        _uuid(self.request_id, 'request id')
        _typed(self.identity, WorkerIdentity, 'requested identity')
        _typed(self.domain, Domain, 'requested domain')
        _typed(self.settings, RuntimeSettings, 'requested settings')


@dataclass(frozen=True, slots=True)
class AdmissionIntent:
    request: AdmissionRequest
    identity: IdentityEvidence
    domain: DomainEvidence
    settings: SettingsEvidence
    digest: str = field(init=False)

    def __post_init__(self):
        _typed(self.request, AdmissionRequest, 'request')
        _typed(self.identity, IdentityEvidence, 'identity evidence')
        _typed(self.domain, DomainEvidence, 'domain evidence')
        _typed(self.settings, SettingsEvidence, 'settings evidence')
        if self.request.identity != self.identity.identity:
            raise IntentError('requested project/worker/main/host differs from identity evidence')
        if self.request.domain != self.domain.domain:
            raise IntentError('requested domain differs from physical/provisioning evidence')
        if self.request.settings != self.settings.selected:
            raise IntentError('requested settings differ from independently selected settings')
        selected, root = self.settings.selected, self.domain.domain.root
        if selected.cwd != root or selected.workspace_roots != (root,):
            raise IntentError('isolated runtime intent must select only the admitted physical root')
        allowed_writes = ((), (root,)) if selected.permission_profile == ':workspace' else ((root,),)
        if selected.writable_roots not in allowed_writes:
            raise IntentError('runtime intent includes unbound writable roots')
        material = (astuple(self.request), astuple(self.identity), astuple(self.domain), astuple(self.settings))
        object.__setattr__(self, 'digest', sha256(repr(material).encode('utf-8')).hexdigest())

    @property
    def status(self):
        return 'intent_only'

    @property
    def runtime_authorized(self):
        return False

    @property
    def remaining_requirements(self):
        return ('authenticated configured-Root authorization', 'fresh physical/provenance verification',
                'reviewed same-identity runtime transition', 'actual provider settings readback',
                'native-domain commit', 'fresh ownership/capacity and guarded closure')


def prepare_intent(request, *, identity, domain, settings):
    """Compare separately obtained evidence; never authenticate it from a request."""
    return AdmissionIntent(request, identity, domain, settings)


def reconcile_intent(previous, proposed):
    """Pure, fail-closed retry check. Caller owns durable atomic storage separately."""
    _typed(proposed, AdmissionIntent, 'proposed intent')
    if previous is None:
        return proposed
    _typed(previous, AdmissionIntent, 'previous intent')
    if previous.request.request_id != proposed.request.request_id:
        raise IntentConflict('a different request cannot replace an existing intent')
    if previous != proposed:
        raise IntentConflict('same request id has different identity/domain/settings/evidence')
    return previous
