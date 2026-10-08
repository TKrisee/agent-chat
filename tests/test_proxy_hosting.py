"""Origin/auth contracts for public proxies and concurrent local coordination."""
from email.message import Message
import ipaddress
import unittest

from agent_chat.auth import RequestOrigins, proxy_networks
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture, TOKEN

PUBLIC = 'https://chat.example.test'
FORWARDED = {'X-Forwarded-Host': 'chat.example.test:443', 'X-Forwarded-Proto': 'https'}


class OriginPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = RequestOrigins('0.0.0.0', 8765, PUBLIC,
                                     proxy_networks(['127.0.0.1'], TOKEN, PUBLIC))

    def accepted(self, values, peer='127.0.0.1', policy=None):
        headers = Message()
        for key, value in values:
            headers[key] = value
        return (policy or self.policy).trusted(headers, peer)

    def test_direct_local_clients_work_even_when_their_peer_is_also_trusted(self):
        for host in ('127.0.0.1:8765', 'localhost:8765', '[::1]:8765'):
            with self.subTest(host=host):
                self.assertTrue(self.accepted([('Host', host)]))
                self.assertTrue(self.accepted([('Host', host), ('Origin', 'http://' + host)]))
                self.assertFalse(self.accepted([('Host', host), ('Origin', PUBLIC)]))
                self.assertFalse(self.accepted([('Host', host)], peer='192.0.2.20'))
        self.assertFalse(self.accepted([('Host', '0.0.0.0:8765')]))
        self.assertFalse(self.accepted([('Host', 'localhost:9999')]))

    def test_unconfigured_local_server_keeps_its_exact_listener_host_boundary(self):
        policy = RequestOrigins('127.0.0.1', 8765, None)
        self.assertTrue(self.accepted([('Host', '127.0.0.1:8765')], policy=policy))
        self.assertFalse(self.accepted([('Host', 'localhost:8765')], policy=policy))
        self.assertFalse(self.accepted([('Host', '[::1]:8765')], policy=policy))

    def test_forwarding_selects_only_public_origin_not_the_local_origin(self):
        headers = [('Host', '127.0.0.1:8765'), *FORWARDED.items()]
        self.assertTrue(self.accepted(headers))
        self.assertTrue(self.accepted(headers + [('Origin', PUBLIC)]))
        for wrong in ('http://127.0.0.1:8765', 'https://evil.test', 'http://chat.example.test'):
            self.assertFalse(self.accepted(headers + [('Origin', wrong)]))

    def test_untrusted_forwarding_cannot_select_a_public_or_local_route(self):
        headers = [('Host', '127.0.0.1:8765'), *FORWARDED.items()]
        self.assertFalse(self.accepted(headers, peer='192.0.2.20'))
        self.assertFalse(self.accepted(headers + [('Origin', PUBLIC)],
                                       policy=RequestOrigins('127.0.0.1', 8765, PUBLIC)))
        self.assertTrue(self.accepted([('Host', 'chat.example.test'), ('Origin', PUBLIC),
                                       ('X-Forwarded-Host', 'evil.test')], peer='192.0.2.20'))

    def test_host_preserving_tls_proxy_remains_supported_without_forwarding(self):
        self.assertTrue(self.accepted([('Host', 'CHAT.example.test:443'), ('Origin', PUBLIC)], peer='192.0.2.20'))
        self.assertFalse(self.accepted([('Host', 'evil.test'), ('Origin', PUBLIC)]))
        self.assertFalse(self.accepted([('Host', 'chat.example.test'), ('Origin', 'http://127.0.0.1:8765')]))

    def test_partial_duplicate_and_chained_forwarding_reject_without_fallback(self):
        base = [('Host', '127.0.0.1:8765')]
        invalid = [
            [('X-Forwarded-Host', 'chat.example.test')],
            [('X-Forwarded-Proto', 'https')],
            [*FORWARDED.items(), ('X-Forwarded-Host', 'chat.example.test')],
            [*FORWARDED.items(), ('X-Forwarded-Proto', 'https')],
            [('X-Forwarded-Host', 'chat.example.test, evil.test'), ('X-Forwarded-Proto', 'https')],
            [('X-Forwarded-Host', 'chat.example.test'), ('X-Forwarded-Proto', 'https, http')],
            [('X-Forwarded-Host', 'chat.example.test'), ('X-Forwarded-Proto', 'ftp')],
        ]
        for fields in invalid:
            with self.subTest(fields=fields):
                self.assertFalse(self.accepted(base + fields))

    def test_malformed_host_and_serialized_origin_reject(self):
        for host in ('', 'chat.example.test/', 'chat.example.test?', 'operator@chat.example.test',
                     'chat.example.test:', 'chat.example.test:99999', 'chat.example.test\\evil',
                     'chat.example.test\t', 'chat.example.test#', 'https://chat.example.test'):
            with self.subTest(host=host):
                self.assertFalse(self.accepted([('Host', host)]))
                self.assertFalse(self.accepted([('Host', '127.0.0.1:8765'),
                                               ('X-Forwarded-Host', host), ('X-Forwarded-Proto', 'https')]))
        for value in ('null', '', PUBLIC + '/', PUBLIC + '?', PUBLIC + '#', PUBLIC + '\t',
                      PUBLIC + ', https://evil.test', 'https://operator@chat.example.test'):
            with self.subTest(origin=value):
                self.assertFalse(self.accepted([('Host', 'chat.example.test'), ('Origin', value)]))
        self.assertFalse(self.accepted([('Host', 'chat.example.test'), ('Host', 'chat.example.test')]))
        self.assertFalse(self.accepted([('Host', 'chat.example.test'), ('Origin', PUBLIC), ('Origin', PUBLIC)]))

    def test_numeric_ipv6_and_mapped_peers_have_explicit_proxy_trust(self):
        headers = [('Host', '[::1]:8765'), *FORWARDED.items(), ('Origin', PUBLIC)]
        for value, peer in [('::1/128', '::1'), ('127.0.0.1/32', '::ffff:127.0.0.1'),
                            ('::ffff:127.0.0.1/128', '127.0.0.1'), ('::ffff:127.0.0.0/104', '::ffff:127.0.0.1'),
                            ('::/80', '::ffff:127.0.0.1')]:
            with self.subTest(value=value, peer=peer):
                policy = RequestOrigins('::', 8765, PUBLIC, proxy_networks([value], TOKEN, PUBLIC))
                self.assertTrue(self.accepted(headers, peer=peer, policy=policy))
        self.assertFalse(self.accepted(headers, peer='2001:db8::1'))
        self.assertEqual(proxy_networks(['::ffff:127.0.0.1'], TOKEN, PUBLIC),
                         (ipaddress.ip_network('127.0.0.1/32'),))

    def test_proxy_configuration_needs_public_origin_auth_and_numeric_network(self):
        for values, token, url in [(['127.0.0.1'], TOKEN, None), (['127.0.0.1'], None, PUBLIC),
                                   (['localhost'], TOKEN, PUBLIC), (['192.0.2.1/24'], TOKEN, PUBLIC)]:
            with self.subTest(values=values, token=token, url=url):
                with self.assertRaises(ValueError):
                    proxy_networks(values, token, url)


class ProxyHTTPTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.stop()
        self.start(public_url=PUBLIC, trusted_proxies=['127.0.0.1'])

    def test_local_machine_coordination_and_bridge_still_require_their_auth(self):
        registration = {'op': 'register', 'session': 'a', 'host_id': 'mac', 'params': {'agent': 'alpha'}}
        self.assertEqual(self.request('POST', '/api/coord', registration)[0], 200)
        status = {'op': 'status', 'session': 'a', 'host_id': 'mac', 'params': {}}
        self.assertEqual(self.request('POST', '/api/coord', status)[0], 200)
        bridge = {'owner': 'proxy-test-bridge', 'secret': 'proxy-test-bridge-secret-1234567890', 'host_id': 'mac'}
        self.assertEqual(self.request('POST', '/api/bridge/rpc', dict(bridge, op='acquire'))[0], 200)
        self.assertEqual(self.request('POST', '/api/bridge/rpc', dict(bridge, op='release'))[0], 200)
        self.assertEqual(self.request('POST', '/api/bridge/rpc', dict(bridge, op='acquire'), auth='basic')[0], 401)
        self.assertEqual(self.request('GET', '/api/config', auth='basic')[0], 200)

    def test_public_browser_write_retains_origin_auth_and_csrf_checks(self):
        registration = {'op': 'register', 'session': 'a', 'host_id': 'mac', 'params': {'agent': 'alpha'}}
        self.assertEqual(self.request('POST', '/api/coord', registration)[0], 200)
        headers = dict(FORWARDED, Origin=PUBLIC)
        payload = {'to': 'a', 'body': 'proxied browser write'}
        self.assertEqual(self.request('GET', '/api/config', auth=None, headers=headers)[0], 401)
        self.assertEqual(self.request('POST', '/api/messages', payload, auth='basic', headers=headers)[0], 403)
        headers['X-Agent-Chat-CSRF'] = self.server.csrf_token
        self.assertEqual(self.request('POST', '/api/messages', payload, auth='basic', headers=headers)[0], 200)
        self.assertEqual(self.request('POST', '/api/messages', payload, auth='basic',
                                      headers={**headers, 'Origin': 'https://evil.test'})[0], 403)
        self.assertEqual(self.request('POST', '/api/messages', payload, auth='basic',
                                      headers={**headers, 'Origin': self.url})[0], 403)
        coord = Coordinator(self.db)
        try:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM messages WHERE body=?',
                                              (payload['body'],)).fetchone()[0], 1)
        finally:
            coord.close()


if __name__ == '__main__':
    unittest.main()
