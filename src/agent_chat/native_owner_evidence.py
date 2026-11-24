"""Bounded macOS kernel/code-signing samples; not an atomic ownership lease."""
import ctypes as C
from dataclasses import dataclass
import hashlib
import ipaddress
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import time
from urllib.parse import urlsplit

class EvidenceError(ValueError):
    pass

IMAGE = Path('/Users/kristoftischler/Library/Application Support/Herd/config/nvm/versions/node/v22.22.3/lib/node_modules/@openai/codex/node_modules/@openai/codex-darwin-arm64/vendor/aarch64-apple-darwin/bin/codex')
IMAGE_SHA = 'fc8d77cf7585aded2b9bc487dad29c4ee2f066123e4e76dd3b17653a8322ce42'

class Bsd(C.Structure):
    _fields_ = [(n, C.c_uint32) for n in ('flags','status','xstatus','pid','ppid','uid','gid','ruid','rgid','svuid','svgid','rfu_1')] + [('comm',C.c_char*16),('name',C.c_char*32)] + [(n,C.c_uint32) for n in ('nfiles','pgid','pjobc','tdev','tpgid')] + [('nice',C.c_int32),('start_sec',C.c_uint64),('start_usec',C.c_uint64)]

@dataclass(frozen=True)
class Process:
    pid: int
    parent: int
    group: int
    uid: int
    ruid: int
    birth_sec: int
    birth_usec: int
    image: str

@dataclass(frozen=True)
class Sample:
    endpoint: tuple
    listener_pid: int
    chain: tuple
    image_sha: str | None
    code_hash: str
    peer: tuple | None


def endpoint(value):
    u = urlsplit(value)
    if (u.scheme != 'ws' or u.hostname != '127.0.0.1' or not u.port or
        u.username or u.password or u.query or u.fragment or u.path not in ('','/')):
        raise EvidenceError('only exact numeric local ws listener is supported')
    return ('127.0.0.1', u.port)


def lsof_records(text):
    records, pid, fd = [], None, None
    for line in text.splitlines():
        if line.startswith('p'):
            if not line[1:].isdigit(): raise EvidenceError('invalid lsof PID')
            pid, fd = int(line[1:]), None
        elif line.startswith('f'): fd = line[1:]
        elif line.startswith('n'):
            if not pid or fd is None: raise EvidenceError('unbound lsof socket')
            records.append((pid, fd, line[1:]))
    return tuple(records)

def _kernel_code_hash(pid):
    # Darwin XNU sys/codesign.h and osfmk/kern/cs_blobs.h.
    cs_ops_status, cs_ops_cdhash = 0, 5
    cs_valid, cs_signed = 0x00000001, 0x20000000
    cs_killed, cs_debugged = 0x01000000, 0x10000000
    csops = C.CDLL(None, use_errno=True).csops
    csops.argtypes = [C.c_int, C.c_uint, C.c_void_p, C.c_size_t]
    csops.restype = C.c_int
    digest = (C.c_ubyte * 20)()
    if csops(pid, cs_ops_cdhash, digest, C.sizeof(digest)) != 0 or not any(digest):
        raise EvidenceError('running process kernel code hash unavailable')
    status = C.c_uint()
    if (csops(pid, cs_ops_status, C.byref(status), C.sizeof(status)) != 0
            or status.value & (cs_valid | cs_signed) != (cs_valid | cs_signed)
            or status.value & (cs_killed | cs_debugged)):
        raise EvidenceError('running process signing state is invalid or unverified')
    return bytes(digest).hex()


def _dynamic_disk_association(pid, expected_birth, image):
    if not isinstance(pid,int) or pid<=1: raise EvidenceError('invalid PID')
    kernel = DarwinEvidence()
    before=kernel.process(pid)
    if (before.birth_sec,before.birth_usec)!=tuple(expected_birth): raise EvidenceError('selected PID birth changed')
    cf=C.CDLL('/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation')
    sec=C.CDLL('/System/Library/Frameworks/Security.framework/Security')
    P=C.c_void_p; I=C.c_long; U=C.c_uint32; O=C.c_int32
    def bind(lib,name,args,result):
        fn=getattr(lib,name);fn.argtypes=args;fn.restype=result;return fn
    number=bind(cf,'CFNumberCreate',[P,C.c_int,P],P)
    dictionary=bind(cf,'CFDictionaryCreate',[P,C.POINTER(P),C.POINTER(P),I,P,P],P)
    urlcreate=bind(cf,'CFURLCreateFromFileSystemRepresentation',[P,C.POINTER(C.c_ubyte),I,C.c_bool],P)
    release=bind(cf,'CFRelease',[P],None)
    getvalue=bind(cf,'CFDictionaryGetValue',[P,P],P)
    typeid=bind(cf,'CFGetTypeID',[P],C.c_ulong)
    dataid=bind(cf,'CFDataGetTypeID',[],C.c_ulong)
    length=bind(cf,'CFDataGetLength',[P],I)
    data_ptr=bind(cf,'CFDataGetBytePtr',[P],C.POINTER(C.c_ubyte))
    equal=bind(cf,'CFEqual',[P,P],C.c_bool)
    guest_fn=bind(sec,'SecCodeCopyGuestWithAttributes',[P,P,U,C.POINTER(P)],O)
    check_dynamic=bind(sec,'SecCodeCheckValidity',[P,U,P],O)
    projection=bind(sec,'SecCodeCopyStaticCode',[P,U,C.POINTER(P)],O)
    static_create=bind(sec,'SecStaticCodeCreateWithPath',[P,U,C.POINTER(P)],O)
    check_static=bind(sec,'SecStaticCodeCheckValidity',[P,U,P],O)
    signing=bind(sec,'SecCodeCopySigningInformation',[P,U,C.POINTER(P)],O)
    guest_key=P.in_dll(sec,'kSecGuestAttributePid').value
    hash_key=P.in_dll(sec,'kSecCodeInfoUnique').value
    if not guest_key or not hash_key: raise EvidenceError('signing constants unavailable')
    owned=[]
    def own(value):
        if not value: raise EvidenceError('CoreFoundation allocation failed')
        owned.append(value);return value
    def code_hash(code):
        info=P()
        if signing(code,2,C.byref(info))!=0 or not info.value: raise EvidenceError('signing information unavailable')
        own(info.value);value=getvalue(info,hash_key)
        if not value or typeid(value)!=dataid(): raise EvidenceError('signing hash unavailable')
        size=length(value)
        if size<20 or size>32: raise EvidenceError('unexpected signing hash length')
        pointer=data_ptr(value)
        if not pointer: raise EvidenceError('signing hash bytes unavailable')
        return value,C.string_at(pointer,size).hex()
    try:
        pid_value=C.c_int(pid);num=own(number(None,9,C.byref(pid_value)))
        keys=(P*1)(guest_key);values=(P*1)(num)
        attrs=own(dictionary(None,keys,values,1,None,None))
        guest=P()
        if guest_fn(None,attrs,0,C.byref(guest))!=0 or not guest.value: raise EvidenceError('dynamic guest unavailable')
        own(guest.value)
        if check_dynamic(guest,1<<29,None)!=0: raise EvidenceError('dynamic validity failed')
        live=P()
        if projection(guest,0,C.byref(live))!=0 or not live.value: raise EvidenceError('dynamic projection unavailable')
        own(live.value);live_value,live_hex=code_hash(live)
        path=os.fsencode(image);pathbuf=(C.c_ubyte*len(path)).from_buffer_copy(path)
        url=own(urlcreate(None,pathbuf,len(path),False));disk=P()
        if static_create(url,0,C.byref(disk))!=0 or not disk.value: raise EvidenceError('disk code unavailable')
        own(disk.value)
        if check_static(disk,1<<29,None)!=0: raise EvidenceError('disk validity failed')
        disk_value,_=code_hash(disk)
        if not equal(live_value,disk_value): raise EvidenceError('dynamic and disk signing hashes differ')
        after=kernel.process(pid)
        if before!=after: raise EvidenceError('process changed during signing observation')
        if not re.fullmatch('[0-9a-f]{40,64}',live_hex): raise EvidenceError('invalid signing hash')
        return live_hex
    finally:
        for value in reversed(owned): release(value)

def dynamic_disk_association(pid, expected_birth, image):
    try:
        return _dynamic_disk_association(pid, expected_birth, image)
    except EvidenceError:
        raise
    except (OSError, AttributeError, TypeError, ValueError) as error:
        raise EvidenceError('system signing evidence unavailable') from error


def dynamic_signed_identity(pid, expected_birth, image):
    """Check the running process signer without attributing current disk bytes to it."""
    from .provider_image_admission import REQUIREMENT
    try:
        kernel = DarwinEvidence()
        before = kernel.process(pid)
        if ((before.birth_sec, before.birth_usec) != tuple(expected_birth)
                or Path(before.image) != Path(image) or before.uid != os.geteuid() or before.ruid != os.getuid()):
            raise EvidenceError('running signed process location, UID, or generation differs')
        code_hash = _kernel_code_hash(pid)
        # Use the documented PID target in the system-owned root directory,
        # with no path of that name that codesign could select instead.
        numeric_path = '/' + str(pid)
        if os.path.lexists(numeric_path):
            raise EvidenceError('running signature target is ambiguous')
        checked = subprocess.run(['/usr/bin/codesign', '--verify', '-R=' + REQUIREMENT, str(pid)],
                                 cwd='/', capture_output=True, text=True, timeout=10)
        if checked.returncode:
            raise EvidenceError('running provider signer verification failed')
        if os.path.lexists(numeric_path) or _kernel_code_hash(pid) != code_hash or kernel.process(pid) != before:
            raise EvidenceError('running signed process changed during verification')
        return code_hash
    except EvidenceError:
        raise
    except (OSError, AttributeError, TypeError, ValueError, subprocess.TimeoutExpired) as error:
        raise EvidenceError('running process signing evidence unavailable') from error


def observed_generation(pid, birth, image, legacy_sha):
    """Return disk SHA only for a proven legacy match; otherwise prove the live signer."""
    from .provider_recovery_peer import Blocked, digest, trusted_file
    try:
        if digest(trusted_file(image, os.getuid(), root_owned=True)) == legacy_sha:
            return legacy_sha, dynamic_disk_association(pid, birth, image)
    except (EvidenceError, Blocked, OSError):
        pass
    return None, dynamic_signed_identity(pid, birth, image)


def _admitted_bridge_pin(registered):
    """Bind an admitted pin to the actual configured bridge process generation."""
    from . import provider_service, provider_owner, service
    config=service._installed_server_config()
    manager=service.ServiceManager(system='Darwin', home=Path(__import__('pwd').getpwuid(config.uid).pw_dir),
                                   server_bin=str(config.executable))
    inspector=provider_service.OwningContinuity(manager)
    before=inspector.inspect()
    process=before.get('process')
    if not process or process.get('pid')!=registered['pid']:
        raise EvidenceError('admitted image bridge generation differs')
    _,command=provider_service.configured(manager)
    address=provider_service.option(command,'--codex-server')
    if address!=registered['endpoint']: raise EvidenceError('admitted image endpoint differs')
    manifest=provider_service.option(command,'--managed-provider-manifest')
    pin=provider_service.option(command,'--managed-provider-sha256')
    configuration,_,_=provider_owner.manifest(manifest,pin,address)
    if inspector.inspect()!=before: raise EvidenceError('admitted image client generation changed')
    return configuration['ImageSHA256']


def expected_generation_sha(registered, disk_sha):
    if disk_sha==IMAGE_SHA: return IMAGE_SHA
    try:
        expected=_admitted_bridge_pin(registered)
    except EvidenceError: raise
    except Exception as error: raise EvidenceError('admitted image generation unavailable; see docs/provider-image-admission.md: '+str(error)) from error
    if disk_sha!=expected: raise EvidenceError('admitted native disk image changed; see docs/provider-image-admission.md')
    return expected


class DarwinEvidence:
    def __init__(self):
        if platform.system() != 'Darwin': raise EvidenceError('macOS kernel contract required')
        self.lib = C.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        self.lib.proc_pidinfo.argtypes = [C.c_int,C.c_int,C.c_uint64,C.c_void_p,C.c_int]
        self.lib.proc_pidpath.argtypes = [C.c_int,C.c_void_p,C.c_uint32]
        if C.sizeof(Bsd) != 136: raise EvidenceError('unsupported process identity layout')

    def process(self, pid):
        b = Bsd()
        if self.lib.proc_pidinfo(pid,3,0,C.byref(b),C.sizeof(b)) != C.sizeof(b) or b.pid != pid:
            raise EvidenceError('process identity unavailable')
        path = C.create_string_buffer(4096)
        if self.lib.proc_pidpath(pid,path,len(path)) <= 0: raise EvidenceError('image path unavailable')
        return Process(b.pid,b.ppid,b.pgid,b.uid,b.ruid,b.start_sec,b.start_usec,os.fsdecode(path.value))

    @staticmethod
    def lsof(*args):
        p = subprocess.run(['/usr/sbin/lsof','-nP','-a',*args,'-F','pfn'], capture_output=True,text=True,timeout=3)
        if p.returncode != 0 or p.stderr: raise EvidenceError('exact socket sample unavailable')
        return lsof_records(p.stdout)

    def sample_independent_listener(self, endpoint_value, expected_pid, expected_birth, rpc, expected_image_sha):
        """Sample an explicitly pinned existing listener; this is not caller ownership."""
        if not isinstance(expected_image_sha,str) or not re.fullmatch('[0-9a-f]{64}',expected_image_sha):
            raise EvidenceError('invalid expected listener identity')
        return self._sample_listener(endpoint_value,expected_pid,expected_birth,rpc,expected_image_sha)

    def sample_signed_listener(self, endpoint_value, expected_pid, expected_birth, rpc):
        """Bind the exact running listener without requiring it to match the disk release."""
        return self._sample_listener(endpoint_value,expected_pid,expected_birth,rpc,None)

    def _sample_listener(self, endpoint_value, expected_pid, expected_birth, rpc, expected_image_sha):
        ep = endpoint(endpoint_value)
        if (isinstance(expected_pid, bool) or not isinstance(expected_pid, int) or expected_pid <= 1
                or not isinstance(expected_birth, tuple) or len(expected_birth) != 2
                or any(isinstance(x, bool) or not isinstance(x, int) or x < 0 for x in expected_birth)):
            raise EvidenceError('invalid expected listener identity')
        connection = rpc.connection_identity()
        rows = self.lsof('-iTCP@127.0.0.1:'+str(ep[1]), '-sTCP:LISTEN')
        if len(rows) != 1 or rows[0][0] != expected_pid or rows[0][2] != '%s:%s' % ep:
            raise EvidenceError('listener is absent, ambiguous or differs from expected PID')
        native = self.process(expected_pid)
        if (native.uid != os.geteuid() or native.ruid != os.getuid()
                or (native.birth_sec, native.birth_usec) != expected_birth or Path(native.image) != IMAGE):
            raise EvidenceError('listener process identity differs')
        if expected_image_sha is None:
            code_hash = dynamic_signed_identity(expected_pid, expected_birth, IMAGE)
        else:
            if hashlib.sha256(IMAGE.read_bytes()).hexdigest() != expected_image_sha:
                raise EvidenceError('native disk image changed')
            code_hash = dynamic_disk_association(expected_pid, expected_birth, IMAGE)
        local, peer = connection['local'], connection['peer']
        if tuple(peer) != ep or not ipaddress.ip_address(local[0]).is_loopback:
            raise EvidenceError('connected peer changed')
        name = '%s:%s->%s:%s' % (*ep, *local)
        matches = [(p,f,n) for p,f,n in self.lsof('-p',str(expected_pid),'-iTCP','-sTCP:ESTABLISHED') if n == name]
        if len(matches) != 1 or matches[0][0] != expected_pid:
            raise EvidenceError('peer is not owned by selected native PID')
        if self.process(expected_pid) != native or rpc.connection_identity() != connection:
            raise EvidenceError('listener or connection changed during kernel sample')
        return Sample(ep, expected_pid, (native,), expected_image_sha, code_hash,
                      (tuple(local), tuple(peer), matches[0][1]))

    def sample(self, registered, rpc=None):
        ep = endpoint(registered['endpoint'])
        now = time.time()
        if not 0 <= now-registered['heartbeat'] <= 15: raise EvidenceError('bridge heartbeat is stale')
        rows = self.lsof('-iTCP@127.0.0.1:'+str(ep[1]),'-sTCP:LISTEN')
        owners = {pid for pid,fd,name in rows if name == '%s:%s'%ep}
        if len(owners) != 1 or len(rows) != 1: raise EvidenceError('listener is absent or ambiguous')
        native = next(iter(owners)); chain = []
        pid = native
        for _ in range(5):
            item = self.process(pid)
            if item.uid != os.geteuid() or item.ruid != os.getuid(): raise EvidenceError('process UID differs')
            chain.append(item)
            if pid == registered['pid']: break
            if item.parent <= 1: raise EvidenceError('listener is outside registered bridge ancestry')
            pid = item.parent
        if chain[-1].pid != registered['pid']: raise EvidenceError('bridge ancestry exceeds bound')
        birth = chain[-1].birth_sec + chain[-1].birth_usec/1e6
        if registered['heartbeat'] < birth: raise EvidenceError('bridge PID was replaced after heartbeat')
        if Path(chain[0].image) != IMAGE: raise EvidenceError('unselected native image')
        try:
            expected_sha, code_hash = observed_generation(native, (chain[0].birth_sec, chain[0].birth_usec), IMAGE, IMAGE_SHA)
        except EvidenceError as original:
            if len(chain)!=2 or chain[0].parent!=chain[1].pid:
                raise
            from .provider_owner import Blocked, configured_child_attestation
            try:
                expected_sha,code_hash=configured_child_attestation(registered['endpoint'],chain[0],chain[1])
            except (Blocked,OSError,ValueError):
                raise EvidenceError('current native signer unavailable and managed launch provenance was not verified') from original
        pair = None
        if rpc is not None:
            sock = rpc._sock
            if sock is None: raise EvidenceError('native socket disconnected')
            local, peer = sock.getsockname(), sock.getpeername()
            if peer != ep or not ipaddress.ip_address(local[0]).is_loopback: raise EvidenceError('connected peer changed')
            name = '%s:%s->%s:%s' % (*ep,*local)
            matches = [(p,f,n) for p,f,n in self.lsof('-p',str(native),'-iTCP','-sTCP:ESTABLISHED') if n == name]
            if len(matches)!=1 or matches[0][0]!=native: raise EvidenceError('peer is not owned by selected native PID')
            pair = (tuple(local),tuple(peer),matches[0][1])
        # A second process sample brackets the non-atomic socket/signing work.
        if tuple(self.process(p.pid) for p in chain) != tuple(chain): raise EvidenceError('process changed during kernel sample')
        return Sample(ep,native,tuple(chain),expected_sha,code_hash,pair)


def same_owner(before, after, *, require_peer=False):
    if (before.endpoint,before.listener_pid,before.chain,before.image_sha,before.code_hash) != (after.endpoint,after.listener_pid,after.chain,after.image_sha,after.code_hash):
        raise EvidenceError('native owner generation changed')
    if require_peer and (before.peer is None or before.peer != after.peer):
        raise EvidenceError('connected peer generation changed')
