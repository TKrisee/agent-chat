"""Explicit HTTP origin configuration and shared-workspace authentication."""
import base64
import binascii
import secrets
from urllib.parse import urlsplit


def origin(value):
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
