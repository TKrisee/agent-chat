"""Explicit replacement locations/config; no inherited administrator environment."""
import ctypes as C
import os,pwd,stat,plistlib,subprocess
from pathlib import Path
from . import provider_recovery_peer as v2
from .provider_recovery_wire import jq,field
from .provider_recovery_provenance import open_store,generation

KEYS={'HOME','CODEX_HOME','CODEX_SQLITE_HOME','PATH','TMPDIR','LANG'}

def kernel_locations(selected):
    """Decode only HOME/CODEX_HOME; never copy/decode/log unrelated environment."""
    generation(selected)
    libc=C.CDLL('/usr/lib/libSystem.B.dylib',use_errno=True)
    libc.sysctl.argtypes=[C.POINTER(C.c_int),C.c_uint,C.c_void_p,C.POINTER(C.c_size_t),C.c_void_p,C.c_size_t]
    maximum=C.c_int();size=C.c_size_t(C.sizeof(maximum))
    if libc.sysctl((C.c_int*2)(1,8),2,C.byref(maximum),C.byref(size),None,0) or not 4<maximum.value<=2*1024*1024:
        raise v2.Blocked('kernel location observation unavailable')
    buffer=C.create_string_buffer(maximum.value+4);size=C.c_size_t(len(buffer))
    try:
        if libc.sysctl((C.c_int*3)(1,49,selected.pid),3,buffer,C.byref(size),None,0):
            raise v2.Blocked('selected process kernel location observation unavailable')
        argc=C.c_int.from_buffer(buffer).value
        if not 0<argc<=4096 or not 4<size.value<=len(buffer):raise v2.Blocked('kernel argument structure invalid')
        def end(offset):
            while offset<size.value and buffer[offset]!=b'\0':offset+=1
            if offset>=size.value:raise v2.Blocked('kernel argument/environment terminator missing')
            return offset
        offset=end(4)
        while offset<size.value and buffer[offset]==b'\0':offset+=1
        for _ in range(argc):offset=end(offset)+1
        locations={}
        while offset<size.value:
            stop=end(offset)
            for key in ('HOME','CODEX_HOME'):
                prefix=(key+'=').encode()
                if stop-offset>=len(prefix) and C.string_at(C.addressof(buffer)+offset,len(prefix))==prefix:
                    if key in locations:raise v2.Blocked('duplicate selected location environment key')
                    locations[key]=os.fsdecode(C.string_at(C.addressof(buffer)+offset+len(prefix),stop-offset-len(prefix)))
            offset=stop+1
        generation(selected)
        return locations
    finally:
        C.memset(C.addressof(buffer),0,len(buffer))

def directory(path,uid):
    value=Path(path)
    if not value.is_absolute():raise v2.Blocked('replacement location must be absolute')
    value=value.resolve(strict=True);info=value.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=uid:raise v2.Blocked('replacement location is not same-user owned directory')
    return str(value)


def current_config_sha(path, uid):
    """Validate the configured file and record its bytes for this operation."""
    try:
        path.lstat()
    except FileNotFoundError:
        return '<ABSENT>'
    raw = v2.trusted_file(path, uid)
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        try:
            import tomli as tomllib
        except ModuleNotFoundError:
            raise v2.Blocked('install Agent Chat with Tomli on Python 3.10 to validate the provider config') from None
    try:
        tomllib.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise v2.Blocked('current provider config.toml is not valid UTF-8 TOML') from None
    return v2.digest(raw)


def manifest(config,uid):
    env=config.get('EnvironmentVariables')
    if not isinstance(env,dict) or set(env)!=KEYS or any(not isinstance(x,str) or not x for x in env.values()):
        raise v2.Blocked('explicit replacement non-secret environment is required; inherited defaults prohibited')
    for key in ('HOME','CODEX_HOME','CODEX_SQLITE_HOME','TMPDIR'):directory(env[key],uid)
    if env['HOME']!=str(Path(pwd.getpwuid(uid).pw_dir).resolve()):raise v2.Blocked('replacement HOME differs from intended same-user account home')
    if any(not Path(p).is_absolute() or not Path(p).is_dir() for p in env['PATH'].split(':')):
        raise v2.Blocked('replacement PATH requires explicit absolute directories')
    # Legacy manifest digests remain immutable. Their config SHA is historical.
    config_sha=current_config_sha(Path(env['CODEX_HOME'])/'config.toml',uid)
    db=Path(env['CODEX_SQLITE_HOME'])/'state_5.sqlite'
    info=db.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=uid:raise v2.Blocked('replacement state store is not existing same-user regular file')
    # Exact storage override prevents config.toml from silently selecting another SQLite home.
    override='sqlite_home='+subprocess.run(['jq','-Rn','--arg','p',env['CODEX_SQLITE_HOME'],'$p'],capture_output=True,text=True,check=True,timeout=3).stdout.strip()
    argv=config['ProgramArguments']
    if argv[-2:]!=['-c',override]:raise v2.Blocked('replacement command must explicitly bind selected sqlite_home')
    return dict(env),config_sha,(str(db),info.st_dev,info.st_ino)

def bind(selected,config):
    env,config_sha,store=manifest(config,selected.uid)
    actual=open_store(selected)
    if store!=(str(actual[0]),actual[1],actual[2]):raise v2.Blocked('replacement store differs from exact selected open native store')
    locations=kernel_locations(selected)
    old_home=locations.get('HOME')
    codehome=locations.get('CODEX_HOME') or (str(Path(old_home)/'.codex') if old_home else None)
    if not codehome or directory(codehome,selected.uid)!=env['CODEX_HOME']:
        raise v2.Blocked('replacement CODEX_HOME differs from selected actual location')
    values=jq('{}','{version:6,environment:{},config_sha256:$sha,state_database:{path:$path,device:($dev|tonumber),inode:($ino|tonumber)},source:"selected kernel locations and actual open SQLite store; replacement environment is explicit"}',sha=config_sha,path=store[0],dev=store[1],ino=store[2])
    for key,value in env.items():values=jq(values,'.environment[$key]=$value',key=key,value=value)
    return values

def check_baseline(config,baseline,uid):
    env,sha,store=manifest(config,uid)
    expected=jq(baseline,'.replacement_configuration')
    if field(expected,'.version|tostring')!='6':raise v2.Blocked('staged explicit replacement configuration unavailable')
    for key,value in env.items():
        if field(expected,'.environment.'+key)!=value:raise v2.Blocked('staged replacement environment differs')
    if sha!=field(expected,'.config_sha256') or store!=(field(expected,'.state_database.path'),int(field(expected,'.state_database.device')),int(field(expected,'.state_database.inode'))):
        raise v2.Blocked('staged replacement configuration/store identity differs')
    return env

def verify_new(selected,baseline):
    expected=jq(baseline,'.replacement_configuration')
    actual=open_store(selected);locations=kernel_locations(selected)
    if (str(actual[0]),actual[1],actual[2])!=(field(expected,'.state_database.path'),int(field(expected,'.state_database.device')),int(field(expected,'.state_database.inode'))):
        raise v2.Blocked('replacement opened a different native state store')
    for key in ('HOME','CODEX_HOME'):
        if locations.get(key)!=field(expected,'.environment.'+key):raise v2.Blocked('replacement actual kernel location differs')
    return jq(expected,'.+{observed_pid:($pid|tonumber),birth_sec:($sec|tonumber),birth_usec:($usec|tonumber)}',pid=selected.pid,sec=selected.birth_sec,usec=selected.birth_usec)
