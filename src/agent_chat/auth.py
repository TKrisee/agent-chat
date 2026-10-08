"""Explicit HTTP origin configuration and shared-workspace authentication."""
import base64
import binascii
import ipaddress
import secrets
from urllib.parse import urlsplit


def origin(value):
    if not isinstance(value, str) or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise ValueError('origin must not contain whitespace or control characters')
    parsed = urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or
            parsed.username is not None or parsed.password is not None or
            parsed.path not in ('', '/') or parsed.query or parsed.fragment):
        raise ValueError('--public-url must be an HTTP(S) origin without credentials or a path')
    port = parsed.port
    host = '[' + parsed.hostname + ']' if ':' in parsed.hostname else parsed.hostname
    if port is not None and port != (443 if parsed.scheme == 'https' else 80):
        host += ':' + str(port)
    return parsed.scheme + '://' + host


def validate_config(host, api_token, public_url):
    if api_token is not None and (not isinstance(api_token, str) or not api_token.isascii() or len(api_token) < 24 or any(c.isspace() for c in api_token)):
        raise ValueError('API token must contain at least 24 non-whitespace ASCII characters')
    if host not in ('127.0.0.1', 'localhost', '::1') and (not api_token or not public_url):
        raise ValueError('non-loopback hosting requires --api-token and --public-url')
    if public_url:
        return origin(public_url)
    return None


def proxy_networks(values, api_token, public_url):
    """Trust only explicitly configured numeric proxy addresses/networks."""
    if values and (not api_token or not public_url):
        raise ValueError('--trusted-proxy requires an API token and --public-url')
    try:
        networks = []
        for value in values:
            network = ipaddress.ip_network(value)
            if isinstance(network, ipaddress.IPv6Network) and network.network_address.ipv4_mapped:
                network = ipaddress.ip_network((network.network_address.ipv4_mapped, network.prefixlen - 96))
            networks.append(network)
        return tuple(networks)
    except ValueError as error:
        raise ValueError('--trusted-proxy must be an IP address or canonical CIDR network') from error


def authority_origin(scheme, authority):
    """A Host header is an authority, not a URL or a forwarding chain."""
    if (scheme not in ('http', 'https') or not authority or not authority.isascii() or
            authority.endswith(':') or
            any(ord(c) <= 32 or ord(c) == 127 or c in '/\\?#@,' for c in authority)):
        raise ValueError('invalid request authority or scheme')
    return origin(scheme + '://' + authority)


def peer_address(value):
    address = ipaddress.ip_address(value)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        return address.ipv4_mapped
    return address


def browser_origin(value):
    scheme, separator, authority = value.partition('://')
    if not separator:
        raise ValueError('invalid browser Origin')
    return authority_origin(scheme.lower(), authority)


class RequestOrigins:
    """Select one request origin before checking the browser's Origin header."""

    def __init__(self, host, port, public_url, proxies=()):
        self.public_url = public_url
        self.proxies = proxies
        local_hosts = {'127.0.0.1', '[::1]', 'localhost'} if public_url else set()
        if host == 'localhost':
            local_hosts.add(host)
        try:
            if peer_address(host).is_loopback:
                local_hosts.add('[' + host + ']' if ':' in host else host)
        except ValueError:
            pass
        self.local_origins = {origin(f'http://{name}:{port}') for name in local_hosts}

    def trusted(self, headers, peer):
        try:
            hosts = headers.get_all('Host', [])
            origins = headers.get_all('Origin', [])
            if len(hosts) != 1 or len(origins) > 1:
                return False
            local = authority_origin('http', hosts[0])
            address = peer_address(peer)
            forwarded_hosts = headers.get_all('X-Forwarded-Host', [])
            forwarded_schemes = headers.get_all('X-Forwarded-Proto', [])
            proxy = any(address in network or ipaddress.ip_address(peer) in network for network in self.proxies)
            if proxy and (forwarded_hosts or forwarded_schemes):
                if len(forwarded_hosts) != 1 or len(forwarded_schemes) != 1:
                    return False
                effective = authority_origin(forwarded_schemes[0], forwarded_hosts[0])
                if effective != self.public_url:
                    return False
            elif self.public_url and authority_origin(urlsplit(self.public_url).scheme, hosts[0]) == self.public_url:
                effective = self.public_url
            elif address.is_loopback and local in self.local_origins:
                effective = local
            else:
                return False
            return not origins or browser_origin(origins[0]) == effective
        except ValueError:
            return False


def authorized(header, token, bearer_only=False):
    if not token:
        return not bearer_only
    if not isinstance(header, str) or not header.isascii():
        return False
    supplied = None
    if header.startswith('Bearer '):
        supplied = header[7:]
    elif not bearer_only and header.startswith('Basic '):
        try:
            username, supplied = base64.b64decode(header[6:], validate=True).decode('ascii').split(':', 1)
            if username != 'operator':
                return False
        except (ValueError, UnicodeError, binascii.Error):
            return False
    return supplied is not None and secrets.compare_digest(supplied, token)
