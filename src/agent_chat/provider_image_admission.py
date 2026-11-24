"""Exact-image enrollment primitive; never changes active provider state."""
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
from .provider_recovery_peer import Blocked, trusted_file

# Extracted from the exact legacy-pinned executable (native_owner_evidence.IMAGE_SHA).
REQUIREMENT = ('identifier "codex" and anchor apple generic and '
               'certificate 1[field.1.2.840.113635.100.6.2.6] exists and '
               'certificate leaf[field.1.2.840.113635.100.6.1.13] exists and '
               'certificate leaf[subject.OU] = "2DC432GLL2"')


def verify_signature(path, requirement):
    checked=subprocess.run(['/usr/bin/codesign','--verify','--strict','--verbose=2','-R='+requirement,str(path)],
                           capture_output=True,text=True,timeout=15)
    if checked.returncode: raise Blocked('provider signature rejected or unavailable')
    with tempfile.TemporaryDirectory(prefix='agent-chat-signing-') as directory:
        prefix=str(Path(directory)/'cert-')
        extracted=subprocess.run(['/usr/bin/codesign','-d','--extract-certificates='+prefix,str(path)],
                                 capture_output=True,text=True,timeout=15)
        if extracted.returncode: raise Blocked('provider signing certificate chain unavailable')
        certificates=[]
        for index in range(10):
            cert=Path(prefix+str(index))
            if not cert.exists(): break
            if not cert.is_file() or cert.is_symlink(): raise Blocked('unsafe extracted certificate')
            certificates.extend(('-c',str(cert)))
        if not certificates: raise Blocked('provider signing certificate chain unavailable')
        verified=subprocess.run(['/usr/bin/security','verify-cert','-p','codeSign','-R','ocsp',*certificates],
                                capture_output=True,text=True,timeout=30)
        if verified.returncode: raise Blocked('provider certificate trust or revocation rejected')


def designated_requirement(path):
    result=subprocess.run(['/usr/bin/codesign','-dr','-',str(path)],capture_output=True,text=True,timeout=15)
    if result.returncode: raise Blocked('baseline signing requirement unavailable')
    lines=[line.split('designated => ',1)[1].strip()
           for line in (result.stdout+'\n'+result.stderr).splitlines()
           if line.startswith('designated => ')]
    if len(lines)!=1: raise Blocked('baseline signing requirement unavailable or ambiguous')
    return lines[0]


def file_digest(path):
    return hashlib.sha256(trusted_file(Path(path),os.getuid(),root_owned=True)).hexdigest()


def baseline(path, expected):
    digest=file_digest(path)
    if digest!=expected: raise Blocked('baseline exact digest differs')
    # Compare by testing the pinned requirement against the already pinned image,
    # avoiding lossy parsing of codesign's display formatting.
    verify_signature(path,REQUIREMENT)
    requirement=designated_requirement(path)
    if not requirement or '2DC432GLL2' not in requirement or 'anchor apple generic' not in requirement:
        raise Blocked('baseline designated requirement differs')
    if file_digest(path)!=digest: raise Blocked('baseline changed during verification')
    return {'BaselineSHA256':digest,'Requirement':REQUIREMENT}


def candidate(path, record):
    if (not isinstance(record,dict) or set(record)!={'BaselineSHA256','Requirement'}
            or record['Requirement']!=REQUIREMENT or not isinstance(record['BaselineSHA256'],str)
            or len(record['BaselineSHA256'])!=64):
        raise Blocked('baseline signer record invalid')
    digest=file_digest(path)
    verify_signature(path,REQUIREMENT)
    if file_digest(path)!=digest: raise Blocked('candidate changed during verification')
    return {'ImageSHA256':digest,'BaselineSHA256':record['BaselineSHA256'],'Requirement':REQUIREMENT}


def enroll_baseline(directory):
    """Capture signer only while the installed image still equals the legacy pin."""
    import plistlib
    from . import native_owner_evidence as native, provider_service as service
    directory=service.private_directory(directory)
    record=baseline(native.IMAGE,native.IMAGE_SHA)
    path=directory/'provider-signing-baseline.plist'
    service.publish(path,plistlib.dumps(record,sort_keys=True))
    return path


def verify_manifest_transition(old_raw, new_raw, old_pin, new_pin):
    import plistlib
    if hashlib.sha256(old_raw).hexdigest()!=old_pin or hashlib.sha256(new_raw).hexdigest()!=new_pin:
        raise Blocked('provider manifest digest changed')
    try:
        old=plistlib.loads(old_raw); new=plistlib.loads(new_raw)
    except (ValueError, TypeError, plistlib.InvalidFileException) as error:
        raise Blocked('provider manifest unreadable') from error
    if not isinstance(old,dict) or not isinstance(new,dict): raise Blocked('provider manifest malformed')
    if 'ImageSHA256' not in old or 'ImageSHA256' not in new: raise Blocked('provider image pin unavailable')
    if {k:v for k,v in old.items() if k!='ImageSHA256'} != {k:v for k,v in new.items() if k!='ImageSHA256'}:
        raise Blocked('provider manifest settings or command changed')


def admit(directory, template, template_sha, address):
    """Write a new immutable provider manifest; never update a running profile."""
    import plistlib
    from . import native_owner_evidence as native, provider_service as service
    directory=service.private_directory(directory)
    baseline_path=directory/'provider-signing-baseline.plist'
    record=plistlib.loads(trusted_file(baseline_path,os.getuid()))
    if record.get('BaselineSHA256')!=native.IMAGE_SHA: raise Blocked('baseline is not legacy pinned')
    raw=trusted_file(Path(template),os.getuid())
    if hashlib.sha256(raw).hexdigest()!=template_sha: raise Blocked('template manifest pin differs')
    config=plistlib.loads(raw)
    image=str(native.IMAGE.resolve())
    if (config.get('Image')!=image or config.get('Endpoint')!=address
        or config.get('ProgramArguments',[])[:4]!=[image,'app-server','--listen',address]
        or len(config.get('ProgramArguments',[]))!=6):
        raise Blocked('template executable or endpoint differs')
    evidence=candidate(native.IMAGE,record)
    if config.get('ImageSHA256')==evidence['ImageSHA256']:
        return Path(template),template_sha
    config['ImageSHA256']=evidence['ImageSHA256']
    newraw=plistlib.dumps(config,sort_keys=True)
    manifest_sha=hashlib.sha256(newraw).hexdigest()
    manifest=directory/('provider-admitted-'+manifest_sha+'.plist')
    receipt=directory/('provider-admission-'+manifest_sha+'.plist')
    evidence.update({'Image':image,'Endpoint':address,'ManifestSHA256':manifest_sha,'PreviousManifestSHA256':template_sha})
    verify_manifest_transition(raw,newraw,template_sha,manifest_sha)
    service.publish(receipt,plistlib.dumps(evidence,sort_keys=True))
    service.publish(manifest,newraw)
    return manifest,manifest_sha


def _replace_exact(path, before, after):
    """Idempotently replace one owner-controlled file without accepting drift."""
    import tempfile
    path=Path(path)
    current=trusted_file(path,os.getuid())
    if current==after: return
    if current!=before: raise Blocked('activation file changed outside this request')
    fd,name=tempfile.mkstemp(prefix='.provider-activation-',dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as stream:
            os.fchmod(stream.fileno(),path.stat().st_mode & 0o700)
            stream.write(after);stream.flush();os.fsync(stream.fileno())
        if trusted_file(path,os.getuid())!=before: raise Blocked('activation file changed before replace')
        os.replace(name,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        try: os.unlink(name)
        except FileNotFoundError: pass


def assert_stopped(manager, address):
    from . import provider_owner as owner
    from .client_continuity import ClientContinuity
    controller=ClientContinuity(manager)
    result=controller._run(['launchctl','print',controller.target])
    if result.returncode==0:
        state=controller._service()
        if state['pid'] is not None: raise Blocked('client must be stopped for provider activation')
    elif result.stdout.strip() or 'Could not find service' not in result.stderr:
        raise Blocked('client service absence unavailable')
    owner.assert_listener_absent(address)


def activate(manager, directory, manifest_path, new_pin, old_pin, address):
    """Resume one durable, stopped-service ledger/profile/wrapper transition."""
    import plistlib, shlex
    from . import provider_service as service, provider_owner as owner
    from .client_continuity import ClientContinuity
    directory=service.private_directory(directory)
    manifest_path=Path(manifest_path)
    owner.manifest(manifest_path,new_pin,address,require_manifest_image=True)
    receipt=plistlib.loads(trusted_file(manifest_path.parent/('provider-admission-'+new_pin+'.plist'),os.getuid()))
    if receipt.get('PreviousManifestSHA256')!=old_pin: raise Blocked('admission is not bound to prior manifest')
    profile_path=directory/'profile.plist'; wrapper_path=manager.paths('client')['wrapper']
    intent_path=directory/('provider-activation-'+new_pin+'.plist')
    if intent_path.exists():
        intent=plistlib.loads(trusted_file(intent_path,os.getuid()))
        if (intent.get('OldPin'),intent.get('NewPin'),intent.get('Address'),intent.get('Manifest'))!=(old_pin,new_pin,address,str(manifest_path)):
            raise Blocked('activation intent conflicts')
    else:
        old_wrapper,command=service.configured(manager)
        old_profile=trusted_file(profile_path,os.getuid());profile=plistlib.loads(old_profile)
        if profile.get('SHA256')!=old_pin or service.option(command,'--managed-provider-sha256')!=old_pin:
            raise Blocked('active profile or wrapper does not match previous pin')
        if service.option(command,'--managed-provider-manifest')!=profile.get('Manifest'):
            raise Blocked('active manifest path differs')
        command[command.index('--managed-provider-sha256')+1]=new_pin
        command[command.index('--managed-provider-manifest')+1]=str(manifest_path)
        lines=old_wrapper.decode().splitlines()
        new_wrapper=('\n'.join('exec '+' '.join(shlex.quote(x) for x in command) if line.startswith('exec ') else line for line in lines)+'\n').encode()
        profile.update(Manifest=str(manifest_path),SHA256=new_pin)
        new_profile=plistlib.dumps(profile,sort_keys=True)
        old_manifest=trusted_file(Path(plistlib.loads(old_profile)['Manifest']),os.getuid())
        verify_manifest_transition(old_manifest,trusted_file(manifest_path,os.getuid()),old_pin,new_pin)
        intent={'OldManifest':old_manifest,'OldPin':old_pin,'NewPin':new_pin,'Address':address,'Manifest':str(manifest_path),
                'OldWrapper':old_wrapper,'NewWrapper':new_wrapper,'OldProfile':old_profile,'NewProfile':new_profile}
        service.publish(intent_path,plistlib.dumps(intent,sort_keys=True))
    old_manifest_path=Path(plistlib.loads(intent['OldProfile'])['Manifest'])
    if trusted_file(old_manifest_path,os.getuid())!=intent['OldManifest']:
        raise Blocked('previous manifest changed during activation')
    verify_manifest_transition(intent['OldManifest'],trusted_file(manifest_path,os.getuid()),old_pin,new_pin)
    assert_stopped(manager,address)
    ledger_path=directory/'owned-provider.sqlite3'
    import sqlite3
    from contextlib import closing
    with closing(sqlite3.connect(ledger_path)) as db:
        row=db.execute('SELECT pin,endpoint FROM owner_config WHERE singleton=1').fetchone()
    if row==(old_pin,address):
        with owner.LaunchLedger(ledger_path,old_pin,address) as ledger:
            ledger.rollover_manifest(old_pin,new_pin,{'old_pin':old_pin,'new_pin':new_pin,'receipt':receipt})
    elif row!=(new_pin,address): raise Blocked('activation ledger pin conflicts')
    assert_stopped(manager,address);_replace_exact(wrapper_path,intent['OldWrapper'],intent['NewWrapper'])
    assert_stopped(manager,address);_replace_exact(profile_path,intent['OldProfile'],intent['NewProfile'])
    return {'manifest':str(manifest_path),'sha256':new_pin,'state':'activated_stopped'}


def upgrade(manager,directory,address):
    """Admit and activate exactly one installed update, resumable after interruption."""
    import plistlib
    from . import provider_service as service
    directory=service.private_directory(directory)
    assert_stopped(manager,address)
    pending=directory/'provider-upgrade-pending.plist'
    if pending.exists():
        plan=plistlib.loads(trusted_file(pending,os.getuid()))
        if set(plan)!={'OldPin','NewPin','Manifest','Address'} or plan['Address']!=address:
            raise Blocked('pending provider upgrade conflicts')
        old_pin,new_pin,manifest=plan['OldPin'],plan['NewPin'],Path(plan['Manifest'])
    else:
        profile=plistlib.loads(trusted_file(directory/'profile.plist',os.getuid()))
        old_pin=profile['SHA256'];old_manifest=Path(profile['Manifest'])
        manifest,new_pin=admit(directory,old_manifest,old_pin,address)
        if new_pin==old_pin:
            return {'state':'already_current','manifest':str(manifest),'sha256':new_pin}
        plan={'OldPin':old_pin,'NewPin':new_pin,'Manifest':str(manifest),'Address':address}
        service.publish(pending,plistlib.dumps(plan,sort_keys=True))
    result=activate(manager,directory,manifest,new_pin,old_pin,address)
    if plistlib.loads(trusted_file(pending,os.getuid()))!=plan: raise Blocked('pending provider upgrade changed')
    pending.unlink()
    fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)
    return result
