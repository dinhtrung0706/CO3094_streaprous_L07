#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""
daemon.proxy
~~~~~~~~~~~~~

Reverse HTTP proxy for StreapRous virtual hosts.

The proxy reads one complete client request, selects a backend from the host's
proxy_pass list using the configured distribution policy, rewrites hop-by-hop
and proxy headers, forwards the request, and streams the backend response back
to the client.

Supported distribution policies:
- round-robin
- least-conn
- random
"""

import logging
import random
import re
import socket
import threading
from urllib.parse import urlsplit

from .errors import (
    BadGateway,
    BadRequest,
    GatewayTimeout,
    HTTPError,
    RequestTimeout,
)
from .http_utils import safe_header_value
from .httpadapter import CONTINUE_RESPONSE, BufferedReader, HttpAdapter
from .response import Response

logger = logging.getLogger(__name__)

PROXY_CONNECT_TIMEOUT = 5.0
PROXY_BACKEND_TIMEOUT = 15.0
PROXY_CLIENT_TIMEOUT = 30.0

_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_\x60|~0-9A-Za-z]+$")
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
    "expect",
}

#: Compatibility default map. Real deployments load config/proxy.conf.
PROXY_PASS = {
    "192.168.56.103:8080": ("192.168.56.103", 9000),
    "app1.local": ("192.168.56.103", 9001),
    "app2.local": ("192.168.56.103", 9002),
}

_state_lock = threading.Lock()
_round_robin_counters = {}
_active_backends = {}


def reset_proxy_state():
    """Clear round-robin counters and active backend counts (tests)."""
    with _state_lock:
        _round_robin_counters.clear()
        _active_backends.clear()


def canonical_backend(value):
    """Return a backend as 'host:port', accepting host:port or an HTTP URL."""
    text = str(value).strip()
    if not text:
        raise ValueError("empty backend address")
    if "://" not in text:
        text = "http://" + text
    parsed = urlsplit(text)
    host = parsed.hostname
    if not host:
        raise ValueError(f"backend has no host: {value!r}")
    port = parsed.port
    if port is None:
        port = 80
    if not 0 < port < 65536:
        raise ValueError(f"invalid backend port: {value!r}")
    if ":" in host and not host.startswith("["):
        host = "[" + host + "]"
    return f"{host}:{port}"


parse_backend_address = canonical_backend


def _split_backend(value):
    """Split a canonical backend into (host, port_string)."""
    text = str(value)
    if text.startswith("["):
        end = text.find("]")
        if end < 0:
            raise ValueError(f"invalid IPv6 backend: {value!r}")
        host = text[1:end]
        port = text[end + 1 :].lstrip(":")
    else:
        host, separator, port = text.rpartition(":")
        if not separator:
            host, port = text, "80"
    if not host:
        raise ValueError(f"backend has no host: {value!r}")
    try:
        port_number = int(port)
    except (TypeError, ValueError):
        raise ValueError(f"invalid backend port: {value!r}")
    if not 0 < port_number < 65536:
        raise ValueError(f"invalid backend port: {value!r}")
    return host, str(port_number)


def _backend_key(host, port):
    return (str(host), int(port))


def _backend_acquire(host, port):
    key = _backend_key(host, port)
    with _state_lock:
        _active_backends[key] = _active_backends.get(key, 0) + 1
    return key


def _backend_release(key):
    with _state_lock:
        count = _active_backends.get(key, 0)
        if count <= 1:
            _active_backends.pop(key, None)
        else:
            _active_backends[key] = count - 1


def _strip_host_port(hostname):
    """Return a Host header value without its port, preserving IPv6 brackets."""
    if not hostname:
        return ""
    value = str(hostname).strip()
    if value.startswith("["):
        end = value.find("]")
        if end >= 0:
            return value[1:end]
        return value
    if value.count(":") == 1:
        host, port = value.rsplit(":", 1)
        if port.isdigit():
            return host
    return value


def _route_entry(hostname, routes):
    if not routes:
        return None
    value = str(hostname or "")
    if value in routes:
        return routes[value]
    stripped = _strip_host_port(value)
    if stripped and stripped in routes:
        return routes[stripped]
    if "*" in routes:
        return routes["*"]
    return None


def _normalize_policy(policy):
    value = str(policy or "round-robin").strip().lower().replace("_", "-")
    if value in ("round-robin", "roundrobin"):
        return "round-robin"
    if value in ("least-conn", "leastconn", "least-connections"):
        return "least-conn"
    if value == "random":
        return "random"
    return "round-robin"


def _route_parts(route):
    """Return (target, policy, set_headers) from a route config value."""
    if isinstance(route, (tuple, list)):
        if len(route) >= 3:
            target, policy, headers = route[0], route[1], route[2]
        elif len(route) == 2:
            target, policy = route[0], route[1]
            headers = {}
        elif len(route) == 1:
            target, policy, headers = route[0], "round-robin", {}
        else:
            raise ValueError("empty proxy route")
    else:
        target, policy, headers = route, "round-robin", {}
    return target, _normalize_policy(policy), dict(headers or {})


def _backend_list(target):
    values = list(target) if isinstance(target, (list, tuple)) else [target]
    backends = []
    for value in values:
        backends.append(canonical_backend(value))
    return backends


def _select_backend(hostname, backends, policy):
    if not backends:
        return None
    policy = _normalize_policy(policy)
    with _state_lock:
        if policy == "random":
            return random.choice(backends)
        index = _round_robin_counters.get(hostname, 0)
        if policy == "least-conn":
            candidates = []
            lowest = None
            for backend in backends:
                host, port = _split_backend(backend)
                count = _active_backends.get(_backend_key(host, port), 0)
                if lowest is None or count < lowest:
                    lowest = count
                    candidates = [backend]
                elif count == lowest:
                    candidates.append(backend)
            selected = candidates[index % len(candidates)]
            _round_robin_counters[hostname] = index + 1
            return selected
        selected = backends[index % len(backends)]
        _round_robin_counters[hostname] = index + 1
        return selected


def resolve_routing_policy(hostname, routes):
    """Resolve a Host header to (backend_host, backend_port_string).

    Returns (None, None) for an unknown host. Policies:
    round-robin, least-conn, random.
    """
    route = _route_entry(hostname, routes)
    if route is None:
        return (None, None)
    target, policy, _headers = _route_parts(route)
    backends = _backend_list(target)
    selected = _select_backend(str(hostname or ""), backends, policy)
    if selected is None:
        return (None, None)
    return _split_backend(selected)


def route_headers(hostname, routes):
    """Return configured proxy_set_header values for a host."""
    route = _route_entry(hostname, routes)
    if route is None:
        return {}
    _target, _policy, headers = _route_parts(route)
    return dict(headers)


def _force_request_close(request):
    """Return request bytes with exactly one Connection: close header."""
    raw = request.encode("iso-8859-1") if isinstance(request, str) else bytes(request)
    head, separator, body = raw.partition(b"\r\n\r\n")
    lines = [
        line
        for line in head.split(b"\r\n")
        if not line.lower().startswith(b"connection:")
    ]
    if separator:
        return b"\r\n".join(lines + [b"Connection: close"]) + b"\r\n\r\n" + body
    return b"\r\n".join(lines + [b"Connection: close", b"", b""])


def _force_response_close(response):
    """Rewrite a backend response so the client connection can be closed."""
    head, separator, body = response.partition(b"\r\n\r\n")
    if not separator:
        return response
    lines = [
        line
        for line in head.split(b"\r\n")
        if not line.lower().startswith(b"connection:")
    ]
    return b"\r\n".join(lines + [b"Connection: close"]) + b"\r\n\r\n" + body


def forward_request(
    host,
    port,
    request,
    connect_timeout=None,
    read_timeout=None,
):
    """Forward a raw HTTP request to one backend and return response bytes.

    The upstream request is forced to Connection: close so the response stream
    ends predictably. Raises BadGateway for connection/protocol failures and
    GatewayTimeout for connect/send/receive timeouts.
    """
    connect_timeout = (
        PROXY_CONNECT_TIMEOUT if connect_timeout is None else connect_timeout
    )
    read_timeout = (
        PROXY_BACKEND_TIMEOUT if read_timeout is None else read_timeout
    )
    payload = _force_request_close(request)
    backend = None
    chunks = []
    try:
        try:
            backend = socket.create_connection(
                (host, int(port)), timeout=connect_timeout
            )
        except TimeoutError:
            raise GatewayTimeout("backend connect timed out")
        except OSError as exc:
            raise BadGateway(f"backend connection failed: {exc}")

        backend.settimeout(read_timeout)
        try:
            backend.sendall(payload)
        except TimeoutError:
            raise GatewayTimeout("backend send timed out")
        except OSError as exc:
            raise BadGateway(f"backend send failed: {exc}")

        while True:
            try:
                chunk = backend.recv(65536)
            except TimeoutError:
                raise GatewayTimeout("backend response timed out")
            except OSError as exc:
                raise BadGateway(f"backend receive failed: {exc}")
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        if backend is not None:
            try:
                backend.close()
            except OSError:
                pass

    response = b"".join(chunks)
    if not response:
        raise BadGateway("backend returned an empty response")
    if not response.startswith(b"HTTP/"):
        raise BadGateway("backend returned a non-HTTP response")
    return response


def _substitute_header_value(value, hostname, request, client_ip):
    original_host = request.headers.get("Host", hostname) or hostname
    replacements = (
        ("$host", hostname),
        ("$http_host", original_host),
        ("$remote_addr", client_ip),
        ("$scheme", "http"),
    )
    result = str(value)
    for token, replacement in replacements:
        result = result.replace(token, replacement)
    return result


def build_upstream_request(request, set_headers, hostname, client_ip):
    """Build the raw upstream request from a parsed Request."""
    body = request.body or b""
    version = request.version or "HTTP/1.1"
    target = request.target or request.path or "/"
    headers = []
    configured_host = any(
        str(name).lower() == "host" for name in set_headers
    )
    for name, value in request.headers.items():
        lowered = name.lower()
        if lowered in _HOP_BY_HOP or lowered == "content-length":
            continue
        if lowered == "host" and configured_host:
            continue
        headers.append((name, value))

    for name, value in set_headers.items():
        name = str(name)
        lowered = name.lower()
        if not _HEADER_NAME_RE.fullmatch(name):
            raise ValueError(f"invalid proxy_set_header name: {name!r}")
        if lowered in _HOP_BY_HOP or lowered == "content-length":
            continue
        value = _substitute_header_value(value, hostname, request, client_ip)
        value = safe_header_value(value)
        headers = [
            (existing_name, existing_value)
            for existing_name, existing_value in headers
            if existing_name.lower() != lowered
        ]
        headers.append((name, value))

    if not any(name.lower() == "host" for name, _value in headers):
        headers.append(("Host", request.headers.get("Host", hostname)))

    forwarded_for = [
        value
        for name, value in headers
        if name.lower() == "x-forwarded-for"
    ]
    headers = [
        (name, value)
        for name, value in headers
        if name.lower()
        not in ("x-forwarded-for", "x-forwarded-host", "x-forwarded-proto")
    ]
    if forwarded_for:
        chain = ", ".join(forwarded_for + [client_ip])
    else:
        chain = client_ip
    headers.append(("X-Forwarded-For", chain))
    headers.append(
        ("X-Forwarded-Host", request.headers.get("Host", hostname))
    )
    headers.append(("X-Forwarded-Proto", "http"))
    headers.append(("Content-Length", str(len(body))))
    headers.append(("Connection", "close"))

    lines = [f"{request.method} {target} {version}"]
    for name, value in headers:
        lines.append(f"{name}: {safe_header_value(value)}")
    head = "\r\n".join(lines) + "\r\n\r\n"
    return head.encode("iso-8859-1") + body


def _read_client_request(conn, addr, ip, port):
    reader = BufferedReader(lambda: conn.recv(65536))
    adapter = HttpAdapter(ip, port, conn, addr, {})
    return adapter.read_request(
        reader,
        lambda: conn.sendall(CONTINUE_RESPONSE),
    )


def _send_error(conn, status, reason=None, message=None):
    try:
        response = Response()
        response.text(message or reason or "Proxy error")
        response.set_status(status, reason)
        response.keep_alive = False
        conn.sendall(response.build_response(None))
    except OSError:
        pass


def handle_client(ip, port, conn, addr, routes):
    """Handle one client connection: read, route, forward, reply, close."""
    try:
        conn.settimeout(PROXY_CLIENT_TIMEOUT)
    except OSError:
        pass

    backend_key = None
    try:
        request = _read_client_request(conn, addr, ip, port)
        if request is None:
            return

        host_header = request.headers.get("Host", "") or ""
        hostname = _strip_host_port(host_header)
        if not hostname:
            _send_error(conn, 400, "Bad Request", "Missing Host header")
            return

        route = _route_entry(host_header, routes)
        if route is None:
            _send_error(
                conn,
                404,
                "Not Found",
                f"No proxy route for Host {host_header}",
            )
            return

        resolved_host, resolved_port = resolve_routing_policy(
            host_header, routes
        )
        if resolved_host is None:
            _send_error(
                conn,
                404,
                "Not Found",
                f"No backend for Host {host_header}",
            )
            return

        set_headers = route_headers(host_header, routes)
        upstream_request = build_upstream_request(
            request,
            set_headers,
            hostname,
            addr[0],
        )

        backend_key = _backend_acquire(resolved_host, resolved_port)
        response = forward_request(
            resolved_host,
            resolved_port,
            upstream_request,
        )
        conn.sendall(_force_response_close(response))
    except HTTPError as exc:
        _send_error(conn, exc.status_code, exc.reason, exc.message)
    except TimeoutError:
        _send_error(
            conn,
            408,
            "Request Timeout",
            "Client request timed out",
        )
    except OSError as exc:
        logger.warning("Proxy transport error from %r: %s", addr, exc)
        _send_error(conn, 502, "Bad Gateway", "Proxy transport error")
    except Exception:
        logger.exception("Unhandled proxy error from %r", addr)
        _send_error(conn, 500, "Internal Server Error", "Proxy failure")
    finally:
        if backend_key is not None:
            _backend_release(backend_key)
        try:
            conn.close()
        except OSError:
            pass


def run_proxy(ip, port, routes):
    """Accept proxy clients and handle each in its own daemon thread."""
    proxy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        proxy.bind((ip, port))
        proxy.listen(50)
        logger.info("Proxy listening on %s:%s", ip, port)
        while True:
            conn, addr = proxy.accept()
            thread = threading.Thread(
                target=handle_client,
                args=(ip, port, conn, addr, routes),
                daemon=True,
            )
            thread.start()
    except KeyboardInterrupt:
        logger.info("Proxy stopped")
    except OSError as exc:
        logger.error("Proxy socket error: %s", exc)
    finally:
        try:
            proxy.close()
        except OSError:
            pass


def create_proxy(ip, port, routes):
    """Entry point for launching the proxy server."""
    run_proxy(ip, port, routes)
