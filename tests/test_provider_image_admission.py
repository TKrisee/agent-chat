import hashlib
import plistlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from agent_chat import provider_image_admission as admission

class ImageAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)
        self.old=self.base/'old'; self.old.write_bytes(b'trusted-old')
        self.new=self.base/'new'; self.new.write_bytes(b'patch-update')
        self.digest=hashlib.sha256(self.old.read_bytes()).hexdigest()
    def test_baseline_requires_exact_old_and_approved_requirement(self):
        with mock.patch.object(admission,'verify_signature',return_value=None), mock.patch.object(admission,'designated_requirement',return_value=admission.REQUIREMENT):
            record=admission.baseline(self.old,self.digest)
        self.assertEqual(record['BaselineSHA256'],self.digest)
        with self.assertRaises(admission.Blocked): admission.baseline(self.old,'0'*64)
    def test_new_image_requires_same_requirement_and_platform_assessment(self):
        base={'BaselineSHA256':self.digest,'Requirement':admission.REQUIREMENT}
        with mock.patch.object(admission,'verify_signature') as verify:
            result=admission.candidate(self.new,base)
        self.assertEqual(result['ImageSHA256'],hashlib.sha256(b'patch-update').hexdigest())
        verify.assert_called_once_with(self.new,admission.REQUIREMENT)
    def test_unknown_baseline_and_signature_rejection_fail_closed(self):
        with self.assertRaises(admission.Blocked): admission.candidate(self.new,{'Requirement':'arbitrary'})
        with mock.patch.object(admission,'verify_signature',side_effect=admission.Blocked('untrusted')):
            with self.assertRaises(admission.Blocked): admission.candidate(self.new,{'BaselineSHA256':self.digest,'Requirement':admission.REQUIREMENT})

    def test_admission_creates_new_pinned_manifest_without_mutating_template(self):
        from agent_chat import native_owner_evidence as native, provider_service
        directory=self.base/'private';directory.mkdir(mode=0o700)
        image=self.new.resolve();address='ws://127.0.0.1:45678'
        config={'Image':str(image),'ImageSHA256':self.digest,'Endpoint':address,
                'ProgramArguments':[str(image),'app-server','--listen',address,'-c','sqlite_home="/tmp"']}
        template=self.base/'template.plist';raw=plistlib.dumps(config);template.write_bytes(raw)
        (directory/'provider-signing-baseline.plist').write_bytes(plistlib.dumps({'BaselineSHA256':self.digest,'Requirement':admission.REQUIREMENT}))
        with mock.patch.object(native,'IMAGE',image), mock.patch.object(native,'IMAGE_SHA',self.digest), mock.patch.object(admission,'verify_signature'):
            path,pin=admission.admit(directory,template,hashlib.sha256(raw).hexdigest(),address)
        self.assertEqual(template.read_bytes(),raw)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),pin)
        result=plistlib.loads(path.read_bytes())
        self.assertEqual(result['ImageSHA256'],hashlib.sha256(self.new.read_bytes()).hexdigest())
        self.assertTrue((directory/('provider-admission-'+pin+'.plist')).is_file())
    def test_signature_and_assessment_errors_reject(self):
        for code in (1,2):
            with mock.patch.object(admission.subprocess,'run',return_value=mock.Mock(returncode=code)):
                with self.assertRaises(admission.Blocked): admission.verify_signature(self.new,admission.REQUIREMENT)

class GenerationPinTests(unittest.TestCase):
    def test_unbound_generation_cannot_use_new_digest(self):
        from agent_chat import native_owner_evidence as native
        with mock.patch.object(native,'_admitted_bridge_pin',side_effect=native.EvidenceError('wrong process')):
            with self.assertRaises(native.EvidenceError): native.expected_generation_sha({'pid':7},'f'*64)
    def test_legacy_digest_never_reclassified(self):
        from agent_chat import native_owner_evidence as native
        with mock.patch.object(native,'_admitted_bridge_pin',side_effect=AssertionError('no live lookup')):
            self.assertEqual(native.expected_generation_sha({'pid':7},native.IMAGE_SHA),native.IMAGE_SHA)

class RequirementOutputTests(unittest.TestCase):
    def test_designated_requirement_from_either_stream_and_ambiguity(self):
        line='designated => identifier codex and anchor apple generic and certificate leaf[subject.OU] = "2DC432GLL2"'
        for out,err in ((line,'Executable=/tmp/a'),('Executable=/tmp/a',line)):
            with mock.patch.object(admission.subprocess,'run',return_value=mock.Mock(returncode=0,stdout=out,stderr=err)):
                self.assertIn('2DC432GLL2',admission.designated_requirement(Path('/tmp/a')))
        with mock.patch.object(admission.subprocess,'run',return_value=mock.Mock(returncode=0,stdout=line,stderr=line)):
            with self.assertRaises(admission.Blocked): admission.designated_requirement(Path('/tmp/a'))

    def test_new_digest_requires_exact_running_bridge_and_manifest(self):
        from types import SimpleNamespace
        from agent_chat import native_owner_evidence as native,provider_service,provider_owner,service
        expected='a'*64
        manager=mock.Mock()
        inspector=mock.Mock();inspector.inspect.return_value={'process':{'pid':55}}
        config=SimpleNamespace(uid=__import__('os').getuid(),executable='/tmp/executable')
        command=['bridge','--codex-server','ws://127.0.0.1:1234','--managed-provider-manifest','/tmp/manifest','--managed-provider-sha256','b'*64]
        registration={'pid':55,'endpoint':'ws://127.0.0.1:1234'}
        with mock.patch.object(service,'_installed_server_config',return_value=config), mock.patch.object(service,'ServiceManager',return_value=manager), mock.patch.object(provider_service,'OwningContinuity',return_value=inspector), mock.patch.object(provider_service,'configured',return_value=(b'',command)), mock.patch.object(provider_owner,'manifest',return_value=({'ImageSHA256':expected},None,None)):
            self.assertEqual(native.expected_generation_sha(registration,expected),expected)
            inspector.inspect.return_value={'process':{'pid':56}}
            with self.assertRaises(native.EvidenceError): native.expected_generation_sha(registration,expected)

class ActivationTests(unittest.TestCase):
    def test_normal_wrapper_profile_rollover_and_resume_after_partial(self):
        import sqlite3
        from contextlib import closing
        from agent_chat import provider_owner, provider_service, client_continuity
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        directory=root/'private';directory.mkdir(mode=0o700)
        wrapper=root/'client.sh';wrapper.write_text('#!/bin/sh\nexec python client bridge --codex-server ws://127.0.0.1:1234 --managed-provider-manifest /old/provider.plist --managed-provider-sha256 old\n')
        profile=directory/'profile.plist';profile.write_bytes(plistlib.dumps({'Manifest':'/old/provider.plist','SHA256':'old','Source':'fixture'}))
        new=directory/'provider-admitted-new.plist';new.write_bytes(b'manifest')
        (directory/'provider-admission-new.plist').write_bytes(plistlib.dumps({'ImageSHA256':'x','PreviousManifestSHA256':'old'}))
        old_manifest=root/'old-provider.plist';old_manifest.write_bytes(b'old-fixture')
        profile.write_bytes(plistlib.dumps({'Manifest':str(old_manifest),'SHA256':'old','Source':'fixture'}))
        wrapper.write_text('#!/bin/sh\nexec python client bridge --codex-server ws://127.0.0.1:1234 --managed-provider-manifest '+str(old_manifest)+' --managed-provider-sha256 old\n')
        ledger=directory/'owned-provider.sqlite3'
        with closing(sqlite3.connect(ledger)) as db:
            db.execute('create table owner_config(singleton INTEGER,pin,endpoint)');db.execute("insert into owner_config values(1,'old','ws://127.0.0.1:1234')");db.commit()
        manager=mock.Mock();manager._platform.return_value='launchd';manager.home=root;manager.paths.return_value={'wrapper':wrapper}
        command=['python','client','bridge','--codex-server','ws://127.0.0.1:1234','--managed-provider-manifest',str(old_manifest),'--managed-provider-sha256','old']
        class Ledger:
            def __init__(self,*a): pass
            def __enter__(self): return self
            def __exit__(self,*a): pass
            def rollover_manifest(self,*a):
                with closing(sqlite3.connect(ledger)) as db:
                    db.execute("update owner_config set pin='new'");db.commit()
        original=admission._replace_exact
        calls=[]
        def interrupted(path,before,after):
            calls.append(path)
            if path==profile: raise OSError('fixture crash')
            return original(path,before,after)
        with mock.patch.object(admission,'verify_manifest_transition'), mock.patch.object(provider_owner,'manifest'), mock.patch.object(provider_owner,'assert_listener_absent'), mock.patch.object(provider_owner,'LaunchLedger',Ledger), mock.patch.object(provider_service,'configured',return_value=(wrapper.read_bytes(),command)), mock.patch.object(client_continuity.ClientContinuity,'_run',return_value=mock.Mock(returncode=113,stdout='',stderr='Could not find service')):
            with mock.patch.object(admission,'_replace_exact',side_effect=interrupted):
                with self.assertRaisesRegex(OSError,'fixture crash'): admission.activate(manager,directory,new,'new','old','ws://127.0.0.1:1234')
            self.assertIn('--managed-provider-sha256 new',wrapper.read_text())
            self.assertEqual(plistlib.loads(profile.read_bytes())['SHA256'],'old')
            admission.activate(manager,directory,new,'new','old','ws://127.0.0.1:1234')
        self.assertEqual(plistlib.loads(profile.read_bytes())['SHA256'],'new')
        self.assertEqual(plistlib.loads(profile.read_bytes())['Manifest'],str(new))
        _,selected=provider_service.configured(manager)
        self.assertEqual(provider_service.option(selected,'--managed-provider-manifest'),str(new))
        self.assertEqual(provider_service.option(selected,'--managed-provider-sha256'),'new')

class OwnedStartTests(unittest.TestCase):
    def test_normal_owned_start_uses_exact_legacy_or_admitted_manifest(self):
        from agent_chat import provider_owner as owner, native_owner_evidence as native
        from types import SimpleNamespace
        for image_sha in (native.IMAGE_SHA,'a'*64):
            with self.subTest(image_sha=image_sha):
                config={'ImageSHA256':image_sha,'ProgramArguments':['/fixture/codex','app-server']}
                ledger=mock.Mock()
                child=SimpleNamespace(pid=123)
                with mock.patch.object(owner,'manifest',return_value=(config,{},'/tmp')) as verify, \
                     mock.patch.object(owner,'LaunchLedger',return_value=ledger) as ledger_type, \
                     mock.patch.object(owner.subprocess,'Popen',return_value=child):
                    provider=owner.OwnedProvider('/tmp/manifest','pin','ws://127.0.0.1:1')
                    self.assertIs(provider.start({},None),child)
                    self.assertEqual(verify.call_count,2)
                    ledger_type.assert_called_once_with(Path('/tmp/owned-provider.sqlite3'),'pin','ws://127.0.0.1:1',require_admitted_transition=(image_sha!=native.IMAGE_SHA))
                    ledger.claim.assert_called_once()
                    ledger.launched.assert_called_once_with(child)
                    ledger.release.assert_called_once()

class ManifestBindingTests(unittest.TestCase):
    def test_only_image_sha_can_change(self):
        before={'ImageSHA256':'old','ProgramArguments':['codex','app-server'],'EnvironmentVariables':{'PATH':'x'},'WorkingDirectory':'/a'}
        after=dict(before,ImageSHA256='new')
        raw_old=plistlib.dumps(before); raw_new=plistlib.dumps(after)
        old=hashlib.sha256(raw_old).hexdigest();new=hashlib.sha256(raw_new).hexdigest()
        admission.verify_manifest_transition(raw_old,raw_new,old,new)
        for key,value in (('ProgramArguments',['codex','other']),('EnvironmentVariables',{'PATH':'y'}),('WorkingDirectory','/b')):
            changed=dict(after,**{key:value});changed_raw=plistlib.dumps(changed)
            with self.assertRaises(admission.Blocked): admission.verify_manifest_transition(raw_old,changed_raw,old,hashlib.sha256(changed_raw).hexdigest())
        with self.assertRaises(admission.Blocked): admission.verify_manifest_transition(raw_old,raw_new,'0'*64,new)

    def test_upgrade_uses_profile_pins_without_manual_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp); old=directory/'old.plist'
            old.write_bytes(b'old');oldpin=hashlib.sha256(b'old').hexdigest()
            (directory/'profile.plist').write_bytes(plistlib.dumps({'Manifest':str(old),'SHA256':oldpin}))
            new=directory/'new.plist';new.write_bytes(b'new')
            manager=mock.Mock()
            with mock.patch.object(admission,'assert_stopped'), mock.patch.object(admission,'admit',return_value=(new,'newpin')) as admit, mock.patch.object(admission,'activate',return_value={'state':'activated_stopped'}) as activate:
                result=admission.upgrade(manager,directory,'ws://127.0.0.1:1')
            admit.assert_called_once_with(directory,old,oldpin,'ws://127.0.0.1:1')
            activate.assert_called_once_with(manager,directory,new,'newpin',oldpin,'ws://127.0.0.1:1')
            self.assertEqual(result['state'],'activated_stopped')

    def test_upgrade_resumes_same_plan_after_interruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp);old=directory/'old.plist';old.write_bytes(b'old')
            oldpin=hashlib.sha256(b'old').hexdigest()
            (directory/'profile.plist').write_bytes(plistlib.dumps({'Manifest':str(old),'SHA256':oldpin}))
            new=directory/'new.plist';new.write_bytes(b'new');manager=mock.Mock()
            with mock.patch.object(admission,'assert_stopped'), mock.patch.object(admission,'admit',return_value=(new,'newpin')) as admit, mock.patch.object(admission,'activate',side_effect=[OSError('interrupted'),{'state':'activated_stopped'}]) as activate:
                with self.assertRaisesRegex(OSError,'interrupted'): admission.upgrade(manager,directory,'ws://127.0.0.1:1')
                self.assertTrue((directory/'provider-upgrade-pending.plist').exists())
                self.assertEqual(admission.upgrade(manager,directory,'ws://127.0.0.1:1')['state'],'activated_stopped')
            self.assertEqual(admit.call_count,1)
            self.assertEqual(activate.call_count,2)
            self.assertFalse((directory/'provider-upgrade-pending.plist').exists())

    def test_service_parser_exposes_upgrade_without_hash_arguments(self):
        from agent_chat import service
        args=service.parser().parse_args(['provider-image-upgrade'])
        self.assertEqual(args.action,'provider-image-upgrade')
        self.assertIsNone(args.provider_manifest_sha256)
        self.assertIsNone(args.previous_provider_sha256)

    def test_same_installed_image_does_not_republish_receipt(self):
        from agent_chat import native_owner_evidence as native
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp); image=directory/'codex';image.write_bytes(b'same')
            digest=hashlib.sha256(b'same').hexdigest(); address='ws://127.0.0.1:1'
            cfg={'Image':str(image),'ImageSHA256':digest,'Endpoint':address,'ProgramArguments':[str(image),'app-server','--listen',address,'-c','sqlite_home="/tmp"']}
            template=directory/'old.plist';raw=plistlib.dumps(cfg);template.write_bytes(raw);oldpin=hashlib.sha256(raw).hexdigest()
            (directory/'provider-signing-baseline.plist').write_bytes(plistlib.dumps({'BaselineSHA256':digest,'Requirement':admission.REQUIREMENT}))
            with mock.patch.object(native,'IMAGE',image),mock.patch.object(native,'IMAGE_SHA',digest),mock.patch.object(admission,'verify_signature'):
                actual,pin=admission.admit(directory,template,oldpin,address)
            self.assertEqual((actual,pin),(template,oldpin))
            self.assertFalse(list(directory.glob('provider-admission-*')))
