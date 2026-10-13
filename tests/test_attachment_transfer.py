from __future__ import annotations

import base64
import contextlib
import hashlib
import http.client
import io
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest import mock
import uuid

from agent_chat import cli
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import Coordinator, CoordError, MAX_ATTACHMENT_SIZE
from agent_chat.remote import HttpClient, RemoteCoordError, _NoRedirect
from agent_chat.remote_service import dispatch
from test_remote_web import RemoteWebFixture, TOKEN
from test_root_orchestrator import ScopedRpc


class AttachmentDownloadServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="agent-chat-attachment-transfer-")
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / "state.sqlite3"
        for session, agent, host in (("home-root", "root", "root-host"),
                                     ("operator", "operator", "operator-host"),
                                     ("other-root", "root", "other-host")):
            self.call("register", session=session, host=host, agent=agent)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("INSERT INTO operator_identities(session_id) VALUES('operator')")
            coord.db.executescript("""
                CREATE TABLE root_projections (
                    session_id TEXT PRIMARY KEY, home_project TEXT NOT NULL,
                    home_session TEXT NOT NULL, thread_id TEXT NOT NULL,
                    cwd TEXT, paused INTEGER NOT NULL DEFAULT 0, model TEXT, effort TEXT);
                INSERT INTO root_projections VALUES
                    ('home-root','home','home-root','root-thread','/private/root',0,NULL,NULL),
                    ('other-root','home','other-root','other-thread','/private/other',0,NULL,NULL);
            """)
            coord.session = "operator"
            prepared = [coord._prepare_attachment("proof.png", b"\x89PNG\r\n\x1a\nprivate-proof")]
            self.sent = coord._send_prepared("home-root", "private", prepared)

    def call(self, op, *, session="home-root", host="root-host", project="home", **params):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dispatch(coord, {"op": op, "session": session, "host_id": host,
                                    "params": params}, canonical_project=project)

    def counts(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return (coord.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
                    coord.db.execute("SELECT COUNT(*) FROM attachments").fetchone()[0])

    def test_exact_canonical_home_private_attachment_returns_verified_metadata(self):
        attachment_id = self.sent["attachments"][0]["id"]
        result = self.call("attachment-download", source_project="home",
                           attachment_id=attachment_id)
        content = base64.b64decode(result.pop("content_base64"), validate=True)
        self.assertEqual(content, b"\x89PNG\r\n\x1a\nprivate-proof")
        self.assertEqual(result, {
            "attachment_id": attachment_id, "message_id": self.sent["id"],
            "name": "proof.png", "mime": "image/png", "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(), "source_project": "home",
            "project": "home", "recipient_session": "home-root", "host_id": "root-host",
        })

    def test_wrong_project_owner_id_and_private_pair_are_rejected_without_mutation(self):
        attachment_id = self.sent["attachments"][0]["id"]
        before = self.counts()
        attempts = [
            dict(source_project="elsewhere", attachment_id=attachment_id),
            dict(source_project="home", attachment_id="attachment_missing"),
        ]
        for params in attempts:
            with self.subTest(params=params), self.assertRaises(CoordError):
                self.call("attachment-download", **params)
        with self.assertRaisesRegex(CoordError, "caller private inbox"):
            self.call("attachment-download", session="other-root", host="other-host",
                      source_project="home", attachment_id=attachment_id)
        with self.assertRaisesRegex(CoordError, "different remote host"):
            self.call("attachment-download", host="wrong-host",
                      source_project="home", attachment_id=attachment_id)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.session = "operator"
            grouped = coord.send_group_prepared(
                ["home-root", "other-root"], "group",
                [coord._prepare_attachment("group.txt", b"group payload")])
        root_group = next(item for item in grouped if item["recipient_session"] == "home-root")
        with self.assertRaisesRegex(CoordError, "caller private inbox"):
            self.call("attachment-download", source_project="home",
                      attachment_id=root_group["attachments"][0]["id"])
        self.assertEqual(self.counts(), (before[0] + 2, before[1] + 2))

    def test_stored_size_and_mime_corruption_fail_closed(self):
        attachment_id = self.sent["attachments"][0]["id"]
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE attachments SET size=size+1 WHERE id=?", (attachment_id,))
        with self.assertRaisesRegex(CoordError, "size"):
            self.call("attachment-download", source_project="home", attachment_id=attachment_id)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE attachments SET size=length(content),mime='text/plain' WHERE id=?",
                             (attachment_id,))
        with self.assertRaisesRegex(CoordError, "metadata"):
            self.call("attachment-download", source_project="home", attachment_id=attachment_id)


class AttachmentDownloadClientTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="agent-chat-attachment-client-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.content = b"\x89PNG\r\n\x1a\nprivate-proof"

    def response(self):
        return {
            "attachment_id": "attachment_exact", "message_id": "message_exact",
            "name": "proof.png", "mime": "image/png", "size": len(self.content),
            "sha256": hashlib.sha256(self.content).hexdigest(), "source_project": "home",
            "content_base64": base64.b64encode(self.content).decode("ascii"), "project": "home",
            "recipient_session": "home-root", "host_id": "root-host",
        }

    def publish(self, result=None, name="proof.png"):
        destination = self.root / name
        output = cli._publish_attachment_download(
            self.response() if result is None else result, destination, "home", "attachment_exact",
            "home-root", "root-host")
        return destination, output

    def test_private_atomic_noclobber_download_and_normal_readback(self):
        destination, output = self.publish()
        self.assertEqual(destination.read_bytes(), self.content)
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o600)
        self.assertNotIn("content_base64", output)
        self.assertEqual((output["path"], output["sha256"], output["size"]),
                         (str(destination), hashlib.sha256(self.content).hexdigest(), len(self.content)))
        with self.assertRaisesRegex(CoordError, "already exists"):
            self.publish()
        self.assertEqual(destination.read_bytes(), self.content)
        self.assertEqual(list(self.root.glob(".*.agent-chat-*")), [])

    def test_redirected_parent_and_metadata_failures_publish_nothing(self):
        real = self.root / "real"
        real.mkdir()
        redirected = self.root / "redirected"
        redirected.symlink_to(real, target_is_directory=True)
        with self.assertRaisesRegex(CoordError, "symlink"):
            cli._publish_attachment_download(self.response(), redirected / "proof.png",
                                             "home", "attachment_exact", "home-root", "root-host")
        self.assertFalse((real / "proof.png").exists())
        variants = []
        for key, value in (("sha256", "0" * 64), ("size", len(self.content) + 1),
                           ("mime", "text/plain"), ("size", MAX_ATTACHMENT_SIZE + 1),
                           ("content_base64", "%%%"), ("project", "wrong-project"),
                           ("source_project", "wrong-project"), ("attachment_id", "attachment_wrong"),
                           ("message_id", None), ("recipient_session", "other-root"),
                           ("host_id", "other-host")):
            changed = dict(self.response())
            changed[key] = value
            variants.append((key, changed))
        extra = dict(self.response(), unexpected=True)
        variants.append(("fields", extra))
        for field in ("project", "source_project", "attachment_id", "message_id", "recipient_session", "host_id"):
            missing = dict(self.response())
            del missing[field]
            variants.append(("missing-" + field, missing))
        for index, (kind, value) in enumerate(variants):
            destination = self.root / f"failure-{index}.png"
            with self.subTest(kind=kind), self.assertRaises(CoordError):
                cli._publish_attachment_download(value, destination, "home", "attachment_exact",
                                                 "home-root", "root-host")
            self.assertFalse(destination.exists())

    def test_redirect_partial_response_and_uncertain_rpc_stop_before_publication(self):
        with self.assertRaisesRegex(RemoteCoordError, "redirect refused"):
            _NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://elsewhere.invalid")

        class PartialResponse:
            def __enter__(self): return self
            def __exit__(self, *unused): return False
            def read(self, *unused):
                raise http.client.IncompleteRead(b'{"attachment_id":', 200)

        client = HttpClient("http://127.0.0.1:8765", "token", project="home")
        with mock.patch.object(client._opener, "open", return_value=PartialResponse()):
            with self.assertRaisesRegex(RemoteCoordError, "unavailable"):
                client.call("/api/coord", {"op": "attachment-download", "params": {}})

        destination = self.root / "uncertain.png"
        with (mock.patch.object(HttpClient, "call", side_effect=RemoteCoordError("uncertain result")),
              mock.patch("agent_chat.remote.client_host_id", return_value="root-host"),
              self.assertRaisesRegex(RemoteCoordError, "uncertain result")):
            cli.main(["--server", "http://127.0.0.1:8765", "--project", "home",
                      "--session", "home-root", "attachment-download",
                      "--source-project", "home", "--attachment-id", "attachment_exact",
                      "--output", str(destination)])
        self.assertFalse(destination.exists())

    def test_root_destination_send_reuses_private_file_and_preserves_normal_result(self):
        downloaded, _ = self.publish()
        body = self.root / "body.md"
        body.write_text("forward", encoding="utf-8")
        calls = []

        def routed(client, path, payload, **unused):
            calls.append((client.project, path, payload))
            self.assertEqual(path, "/api/coord")
            self.assertEqual((client.project, payload["op"], payload["session"]),
                             ("destination", "send", "destination-root"))
            attachment = payload["params"]["attachments"]
            self.assertEqual(len(attachment), 1)
            self.assertEqual(base64.b64decode(attachment[0]["content_base64"], validate=True), self.content)
            return {"id": "message_sent", "project": "destination"}

        stdout = io.StringIO()
        with (mock.patch.object(HttpClient, "call", routed),
              mock.patch("agent_chat.remote.client_host_id", return_value="root-host"),
              contextlib.redirect_stdout(stdout)):
            result = cli.main(["--server", "http://127.0.0.1:8765", "--project", "destination",
                               "--session", "destination-root", "send",
                               "--to", "orchestrator", "--body-file", str(body),
                               "--attach", str(downloaded)])
        self.assertEqual(result, 0)
        self.assertEqual(stdout.getvalue(), '{"id": "message_sent", "project": "destination"}\n')
        self.assertEqual(len(calls), 1)

    def test_send_enforces_fifty_file_and_ten_mib_bounds_before_rpc(self):
        body = self.root / "body.md"
        body.write_text("forward", encoding="utf-8")
        small = self.root / "small.txt"
        small.write_text("payload", encoding="utf-8")
        captured = []

        def sent(client, path, payload, **unused):
            captured.append(payload)
            return {"id": "message_sent"}

        common = ["--server", "http://127.0.0.1:8765", "--project", "destination",
                  "--session", "destination-root", "send", "--to", "orchestrator",
                  "--body-file", str(body)]
        fifty = [value for _ in range(50) for value in ("--attach", str(small))]
        with (mock.patch.object(HttpClient, "call", sent),
              mock.patch("agent_chat.remote.client_host_id", return_value="root-host"),
              contextlib.redirect_stdout(io.StringIO())):
            self.assertEqual(cli.main([*common, *fifty]), 0)
        self.assertEqual(len(captured[0]["params"]["attachments"]), 50)

        with (mock.patch.object(HttpClient, "call") as rpc,
              mock.patch("agent_chat.remote.client_host_id", return_value="root-host"),
              self.assertRaisesRegex(CoordError, "at most 50")):
            cli.main([*common, *fifty, "--attach", str(small)])
        rpc.assert_not_called()

        oversized = self.root / "oversized.txt"
        with oversized.open("wb") as stream:
            stream.seek(MAX_ATTACHMENT_SIZE)
            stream.write(b"x")
        with (mock.patch.object(HttpClient, "call") as rpc,
              mock.patch("agent_chat.remote.client_host_id", return_value="root-host"),
              self.assertRaisesRegex(CoordError, "10 MiB")):
            cli.main([*common, "--attach", str(oversized)])
        rpc.assert_not_called()


class AttachmentTransferEndToEndTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call("register", agent="root")
        self.thread_id = str(uuid.uuid4())
        self.call("bind", thread=self.thread_id)
        neutral = Path(self.tmp.name).resolve() / "neutral"
        neutral.mkdir()
        self.destination = self.client.call(
            "/api/projects/rpc", {"op": "create", "name": "Destination"})["project"]["id"]
        self.call("context")
        rpc = ScopedRpc(self.thread_id, str(neutral))
        identity = {"owner": "attachment-transfer-fixture", "secret": "x" * 40, "host_id": "mac"}
        state = RemoteBridgeState(self.client, self.url, identity)
        state.call("acquire")
        bridge = Bridge(state, rpc)
        self.call("agent-status", refresh=True, expected_thread=self.thread_id,
                  request_id=str(uuid.uuid4()))
        bridge.tick()
        # Legitimate application-local setup, not a shared-bearer Root RPC.
        self.server.roots.enroll("default", "a", "mac", self.thread_id, True)
        self.home_route = self.server.roots.route("default", "a", "mac", "default")
        self.destination_route = self.server.roots.route("default", "a", "mac", self.destination)

    def test_shared_bearer_route_and_enroll_remain_denied_before_root_dispatch(self):
        with (mock.patch.object(self.server.roots, 'route') as route,
              mock.patch.object(self.server.roots, 'enroll') as enroll):
            for operation, params in (
                    ('route', {'project': self.destination}),
                    ('enroll', {'expected_thread': self.thread_id, 'confirm': True})):
                with self.subTest(operation=operation):
                    status, raw, _ = self.request('POST', '/api/root/rpc', {
                        'op': operation, 'session': 'a', 'host_id': 'mac', 'params': params})
                    self.assertEqual(status, 400, raw)
                    self.assertIn(b'shared bearer permits only', raw)
            route.assert_not_called()
            enroll.assert_not_called()

    def test_ordinary_download_refuses_wrong_recipient_project_and_host(self):
        self.call('register', agent='ordinary-worker', session='ordinary-worker')
        with contextlib.closing(Coordinator(self.db)) as coord:
            operator = coord.db.execute('SELECT session_id FROM operator_identities').fetchone()[0]
            coord.session = operator
            sent = coord._send_prepared('a', 'private', [coord._prepare_attachment('proof.png', b'\x89PNG\r\n\x1a\nprivate')])
        attachment_id = sent['attachments'][0]['id']
        for project, session, host in (
                ('default', 'ordinary-worker', 'mac'),
                (self.destination_route['project'], self.destination_route['session'], 'mac'),
                ('default', 'a', 'wrong-host')):
            with self.subTest(project=project, session=session, host=host):
                client = HttpClient(self.url, TOKEN, project=project)
                with self.assertRaises(RemoteCoordError):
                    client.call('/api/coord', {'op': 'attachment-download', 'session': session,
                        'host_id': host, 'params': {'source_project': 'default', 'attachment_id': attachment_id}})
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT content FROM attachments WHERE id=?', (attachment_id,)).fetchone()[0],
                             b'\x89PNG\r\n\x1a\nprivate')

    def test_root_downloads_from_own_home_then_sends_reference_in_destination(self):
        content = b"\x89PNG\r\n\x1a\nend-to-end-private-proof"
        with contextlib.closing(Coordinator(self.db)) as coord:
            operator = coord.db.execute("SELECT session_id FROM operator_identities").fetchone()[0]
            coord.session = operator
            sent = coord._send_prepared("a", "private", [coord._prepare_attachment("proof.png", content)])
        attachment_id = sent["attachments"][0]["id"]
        downloaded = Path(self.tmp.name).resolve() / "private-proof.png"
        body = Path(self.tmp.name).resolve() / "body.md"
        body.write_text("forward", encoding="utf-8")
        environment = {"AGENT_CHAT_HOST_ID": "mac"}
        with mock.patch.dict(os.environ, environment, clear=False), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main([
                "--server", self.url, "--api-token", TOKEN, "--project", self.home_route['project'],
                "--session", self.home_route['session'], "attachment-download",
                "--source-project", "default", "--attachment-id", attachment_id,
                "--output", str(downloaded),
            ]), 0)
            self.assertEqual(cli.main([
                "--server", self.url, "--api-token", TOKEN, "--project", self.destination_route['project'],
                "--session", self.destination_route['session'], "send",
                "--to", "operator", "--body-file", str(body), "--attach", str(downloaded),
            ]), 0)
        self.assertEqual(downloaded.read_bytes(), content)
        self.assertEqual(stat.S_IMODE(downloaded.stat().st_mode), 0o600)
        with contextlib.closing(Coordinator(self.server.projects.db_path(self.destination))) as coord:
            stored = coord.db.execute(
                "SELECT a.name,a.mime,a.size,a.content FROM attachments a "
                "JOIN messages m ON m.id=a.message_id WHERE m.sender_session=("
                "SELECT session_id FROM root_projections LIMIT 1)").fetchone()
        self.assertEqual(tuple(stored), ("private-proof.png", "image/png", len(content), content))


class ManagedOperatorAttachmentTests(AttachmentTransferEndToEndTests):
    def setUp(self):
        super().setUp()
        self.source_db = self.server.projects.db_path(self.destination)
        self.po = 'designated-po'
        client = HttpClient(self.url, TOKEN, project=self.destination)
        client.call('/api/coord', {'op': 'register', 'session': self.po,
                    'host_id': 'mac', 'params': {'agent': 'project-orchestrator'}})
        # Trusted application records in this neutral fixture, not caller claims.
        with self.server.projects.connection() as directory:
            directory.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?',
                              (self.po, self.destination))
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)',
                             (self.po, self.destination_route['session']))
            coord.session = coord.db.execute('SELECT session_id FROM operator_identities').fetchone()[0]
            coord._operator_message_origin = True
            self.content = b'\x89PNG\r\n\x1a\nneutral-managed-po-proof'
            self.sent = coord._send_prepared(self.po, 'neutral operator input',
                                            [coord._prepare_attachment('proof.png', self.content)])
        self.attachment = self.sent['attachments'][0]['id']
        self.params = dict(source_project=self.destination, message_id=self.sent['id'],
                           attachment_id=self.attachment)

    def download(self, **changes):
        return self.client.call('/api/coord', {'op': 'attachment-download',
            'session': self.home_route['session'], 'host_id': 'mac',
            'params': dict(self.params, **changes)})

    def test_managed_po_operator_download_server(self):
        before_source = self.source_db.read_bytes()
        before_directory = self.server.projects.path.read_bytes()
        result = self.download()
        self.assertEqual(base64.b64decode(result['content_base64'], validate=True), self.content)
        self.assertEqual((result['project'], result['source_project'], result['message_id'],
                          result['recipient_session']),
                         ('default', self.destination, self.sent['id'], self.home_route['session']))
        self.assertEqual(self.source_db.read_bytes(), before_source)
        self.assertEqual(self.server.projects.path.read_bytes(), before_directory)

    def test_authenticated_operator_ui_stamp_is_required_and_downloadable(self):
        metadata = subprocess.run(['jq', '-cn', '--arg', 'to', self.po,
            '{to:$to,body:"neutral authenticated UI input"}'], check=True, capture_output=True).stdout
        boundary = 'neutral-attachment-boundary'
        payload = (b'--' + boundary.encode() + b'\r\nContent-Disposition: form-data; name="message"\r\n\r\n'
                   + metadata + b'\r\n--' + boundary.encode()
                   + b'\r\nContent-Disposition: form-data; name="images"; filename="ui-auth.png"\r\n'
                     b'Content-Type: image/png\r\n\r\n' + self.content
                   + b'\r\n--' + boundary.encode() + b'--\r\n')
        basic = 'Basic ' + base64.b64encode(('operator:' + TOKEN).encode()).decode()
        before = self.source_db.read_bytes()
        for authorized, csrf, expected in ((False, True, 401), (True, False, 403), (True, True, 200)):
            connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
            headers = {'Content-Type': 'multipart/form-data; boundary=' + boundary,
                       'X-Agent-Chat-Project': self.destination}
            if authorized:
                headers['Authorization'] = basic
            if csrf:
                headers['X-Agent-Chat-CSRF'] = self.server.csrf_token
            try:
                connection.request('POST', '/api/messages', payload, headers)
                response = connection.getresponse()
                raw = response.read()
                self.assertEqual(response.status, expected, raw)
            finally:
                connection.close()
            if expected != 200:
                self.assertEqual(self.source_db.read_bytes(), before)
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            sent = coord.db.execute('''SELECT m.id,a.id AS attachment_id,o.channel FROM messages m
                JOIN attachments a ON a.message_id=m.id
                JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session
                WHERE m.recipient_session=? AND a.name='ui-auth.png' ''', (self.po,)).fetchone()
        self.assertEqual(sent['channel'], 'authenticated_operator_ui')
        result = self.download(message_id=sent['id'], attachment_id=sent['attachment_id'])
        self.assertEqual(base64.b64decode(result['content_base64'], validate=True), self.content)

    def test_managed_po_cli_publishes_verified_private_file(self):
        output = Path(self.tmp.name).resolve() / 'managed-proof.png'
        with mock.patch.dict(os.environ, {'AGENT_CHAT_HOST_ID': 'mac'}), contextlib.redirect_stdout(io.StringIO()):
            result = cli.main(['--server', self.url, '--api-token', TOKEN, '--project', 'default',
                '--session', self.home_route['session'], 'attachment-download',
                '--source-project', self.destination, '--message-id', self.sent['id'],
                '--attachment-id', self.attachment, '--output', str(output)])
        self.assertEqual(result, 0)
        self.assertEqual(output.read_bytes(), self.content)
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)

    def reject_readonly(self, **changes):
        source = self.source_db.read_bytes()
        directory = self.server.projects.path.read_bytes()
        with self.assertRaises(RemoteCoordError):
            self.download(**changes)
        self.assertEqual(self.source_db.read_bytes(), source)
        self.assertEqual(self.server.projects.path.read_bytes(), directory)

    def test_exact_source_attachment_and_message_are_required(self):
        for changes in (dict(source_project='project_' + '0' * 32),
                        dict(attachment_id='attachment_missing'), dict(message_id='message_missing')):
            with self.subTest(changes=changes):
                self.reject_readonly(**changes)
        with self.assertRaises(RemoteCoordError):
            self.client.call('/api/coord', {'op': 'attachment-download',
                'session': self.home_route['session'], 'host_id': 'mac',
                'params': {key: value for key, value in self.params.items() if key != 'message_id'}})
        with (mock.patch.object(HttpClient, 'call') as remote,
              self.assertRaisesRegex(CoordError, 'exact original message ID')):
            cli.main(['--server', self.url, '--api-token', TOKEN, '--project', 'default',
                '--session', self.home_route['session'], 'attachment-download',
                '--source-project', self.destination, '--attachment-id', self.attachment,
                '--output', str(Path(self.tmp.name).resolve() / 'missing-message.png')])
        remote.assert_not_called()

    def test_operator_origin_and_exact_direct_recipient_not_names_authorize(self):
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            operator = coord.db.execute('SELECT session_id FROM operator_identities').fetchone()[0]
            coord.session = operator
            unstamped = coord._send_prepared(self.po, 'not authenticated UI',
                [coord._prepare_attachment('unstamped.txt', b'unstamped')])
            coord._operator_message_origin = True
            wrong = coord._send_prepared(self.destination_route['session'], 'wrong recipient',
                [coord._prepare_attachment('wrong.txt', b'wrong')])
            group = coord.send_group_prepared([self.po, self.destination_route['session']], 'group',
                [coord._prepare_attachment('group.txt', b'group')])
            coord._operator_message_origin = False
            coord.session = self.po
            worker = coord._send_prepared(self.po, 'worker claiming operator',
                [coord._prepare_attachment('worker.txt', b'worker')])
            # Even an operator identity label without the immutable UI stamp fails.
            coord.db.execute('INSERT INTO operator_identities VALUES(?)', (self.po,))
        for sent in (unstamped, wrong, worker, next(item for item in group if item['recipient_session'] == self.po)):
            with self.subTest(message=sent['id']):
                self.reject_readonly(message_id=sent['id'], attachment_id=sent['attachments'][0]['id'])

    def test_current_enrollment_designation_and_management_are_required(self):
        for column, value in (('joined', 0), ('orchestrator_session', None),
                              ('orchestrator_session', self.destination_route['session'])):
            with self.subTest(column=column, value=value):
                with self.server.projects.connection() as directory:
                    directory.execute(f'UPDATE root_projects SET {column}=? WHERE project_id=?',
                                      (value, self.destination))
                self.reject_readonly()
                with self.server.projects.connection() as directory:
                    directory.execute('UPDATE root_projects SET joined=1,orchestrator_session=? WHERE project_id=?',
                                      (self.po, self.destination))
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?', ('unmanaged-root', self.po))
        self.reject_readonly()

    def test_missing_or_changed_trusted_bindings_fail_without_bootstrap(self):
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                             ('changed-thread', self.destination_route['session']))
        self.reject_readonly()
        # Missing source store must not be created by a denied read.
        retained = self.source_db.with_name('retained-state.sqlite3')
        self.source_db.rename(retained)
        try:
            with self.assertRaises(RemoteCoordError):
                self.download()
            self.assertFalse(self.source_db.exists())
        finally:
            retained.rename(self.source_db)

    def test_canonical_root_owner_host_projection_and_live_po_records_are_required(self):
        for session, host, project in ((self.home_route['session'], 'other-host', 'default'),
                                      (self.po, 'mac', 'default'),
                                      (self.destination_route['session'], 'mac', self.destination)):
            with self.subTest(session=session, host=host, project=project), self.assertRaises(RemoteCoordError):
                HttpClient(self.url, TOKEN, project=project).call('/api/coord', {
                    'op': 'attachment-download', 'session': session, 'host_id': host, 'params': self.params})
        with self.server.projects.connection() as directory:
            directory.execute('UPDATE root_directory_owner SET home_session=?', ('other-root',))
        self.reject_readonly()
        with self.server.projects.connection() as directory:
            directory.execute('UPDATE root_directory_owner SET home_session=?', (self.home_route['session'],))
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE root_projections SET home_session=?', ('other-root',))
        self.reject_readonly()
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE root_projections SET home_session=?', (self.home_route['session'],))
            coord.db.execute('DELETE FROM sessions WHERE id=?', (self.po,))
        self.reject_readonly()

    def test_metadata_and_missing_origin_associations_fail_readonly(self):
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE attachments SET size=size+1 WHERE id=?', (self.attachment,))
        self.reject_readonly()
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE attachments SET size=length(content),mime=? WHERE id=?', ('text/plain', self.attachment))
        self.reject_readonly()
        with contextlib.closing(Coordinator(self.source_db)) as coord:
            coord.db.execute('UPDATE attachments SET mime=? WHERE id=?', ('image/png', self.attachment))
            coord.db.execute('DELETE FROM operator_identities')
        self.reject_readonly()

    def quarantine(self, database, session):
        from agent_chat.root_team_reset import schema
        with contextlib.closing(Coordinator(database)) as coord:
            schema(coord.db)
            coord.db.execute('INSERT INTO root_team_reset_targets VALUES(?,?,?,?)',
                             (session, 'neutral-reset', None, 'mac'))

    def test_quarantined_root_cannot_use_new_route(self):
        self.quarantine(self.db, self.home_route['session'])
        self.reject_readonly()

    def test_quarantined_designated_po_cannot_use_new_route(self):
        self.quarantine(self.source_db, self.po)
        self.reject_readonly()

    def test_cross_project_publication_requires_home_and_exact_original_message(self):
        result = self.download()
        for field, value in (('project', self.destination), ('message_id', 'message_other'),
                             ('source_project', 'default'), ('recipient_session', self.po)):
            output = Path(self.tmp.name).resolve() / (field + '.png')
            with self.subTest(field=field), self.assertRaisesRegex(CoordError, 'identity'):
                cli._publish_attachment_download(dict(result, **{field: value}), output,
                    self.destination, self.attachment, self.home_route['session'], 'mac',
                    project='default', message_id=self.sent['id'])
            self.assertFalse(output.exists())

    def test_missing_directory_is_not_bootstrapped_by_rejected_download(self):
        directory = self.server.projects.path
        retained = directory.with_name('retained-directory.sqlite3')
        directory.rename(retained)
        try:
            with self.assertRaises(RemoteCoordError):
                self.download()
            self.assertFalse(directory.exists())
        finally:
            if directory.exists():
                directory.rename(directory.with_name('unexpected-created-directory.sqlite3'))
            retained.rename(directory)


if __name__ == "__main__":
    unittest.main()
