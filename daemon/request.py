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
daemon.request
~~~~~~~~~~~~~~~~~

This module provides a Request object that parses and stores one HTTP/1.0 or
HTTP/1.1 request.

The parser is deliberately independent of the transport: socket, asyncio and
raw-socket adapters all hand it a complete byte buffer (headers plus body) and
receive the same Request object back. It validates framing at the trust
boundary, rejects ambiguous or malformed messages, and exposes the route that
a StreapRous application registered for the request.
"""

import base64
import binascii
import json as _json
import re
import threading
import uuid
from urllib.parse import parse_qs, urlencode

from . import config
from .dictionary import CaseInsensitiveDict
from .errors import (
    BadRequest,
    PayloadTooLarge,
    RequestHeaderFieldsTooLarge,
    UnsupportedMediaType,
    URITooLong,
)
from .http_utils import normalize_path

_METHOD_RE = re.compile(r"^[!#$%&'*+\-.^_\x60|~0-9A-Za-z]+$")
_REQUEST_LINE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]* \S+ HTTP/\d\.\d$")
_CHARSET_RE = re.compile(r'charset\s*=\s*"?([^";\s]+)"?', re.IGNORECASE)
_ROUTE_TOKEN_RE = re.compile(
    r"\{([A-Za-z_][A-Za-z0-9_]*)\}"
    r"|<(?:(int|str|path):)?([A-Za-z_][A-Za-z0-9_]*)>"
)
_ROUTE_CACHE = {}
_ROUTE_CACHE_LOCK = threading.Lock()


def _parse_target(target):
    """Validate a request-target and return (path, query_string, query).

    The target is decoded exactly once. Percent-encoded control characters
    and NUL bytes are rejected after decoding, so they cannot reach routing or
    the static-file layer.
    """
    if not isinstance(target, str):
        raise BadRequest("Request target must be text")
    try:
        target_length = len(target.encode("iso-8859-1"))
    except UnicodeEncodeError as exc:
        raise BadRequest("Request target must be ASCII") from exc
    if target_length > config.MAX_URI_BYTES:
        raise URITooLong("Request target is too long")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in target):
        raise BadRequest("Request target contains control characters")
    if target == "*":
        return "*", "", {}

    if not target.startswith("/"):
        raise BadRequest("Request target must begin with '/'")

    path_part, separator, query_string = target.partition("?")
    try:
        path = normalize_path(path_part or "/")
    except (ValueError, UnicodeError) as exc:
        raise BadRequest("Invalid request path") from exc
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in path):
        raise BadRequest("Request path contains control characters")

    query = parse_qs(query_string, keep_blank_values=True) if separator else {}
    return path, (query_string if separator else ""), query


def _parse_request_line(line):
    """Validate one request line and return all of its parsed components."""
    if not isinstance(line, str):
        raise BadRequest("Request line must be text")
    line = line.rstrip("\r\n")
    if not line:
        raise BadRequest("Empty request line")
    if "\t" in line:
        raise BadRequest("Request line must use single spaces")
    parts = line.split(" ")
    if len(parts) != 3:
        raise BadRequest("Malformed request line")
    method, target, version = parts
    if not method or not _METHOD_RE.fullmatch(method):
        raise BadRequest("Invalid HTTP method")
    if version not in ("HTTP/1.0", "HTTP/1.1"):
        raise BadRequest("Unsupported HTTP version")
    method = method.upper()
    if target == "*" and method != "OPTIONS":
        raise BadRequest("'*' is only valid for OPTIONS")
    path, query_string, query = _parse_target(target)
    return method, target, path, query_string, query, version


def _build_route_regex(path):
    """Compile a StreapRous route path into a regex plus converter map."""
    parts = []
    converters = {}
    cursor = 0
    for token in _ROUTE_TOKEN_RE.finditer(path):
        parts.append(re.escape(path[cursor:token.start()]))
        if token.group(1) is not None:
            name = token.group(1)
            converter = "str"
        else:
            converter = token.group(2) or "str"
            name = token.group(3)
        if name in converters:
            raise ValueError(f"duplicate route parameter: {name}")
        converters[name] = converter
        if converter == "int":
            parts.append(rf"(?P<{name}>\d+)")
        elif converter == "path":
            parts.append(rf"(?P<{name}>.+)")
        else:
            parts.append(rf"(?P<{name}>[^/]+)")
        cursor = token.end()
    parts.append(re.escape(path[cursor:]))
    return re.compile("".join(parts)), converters


def _compile_route(path):
    """Return (regex, converters) for a route path, cached per process."""
    with _ROUTE_CACHE_LOCK:
        cached = _ROUTE_CACHE.get(path)
    if cached is not None:
        return cached
    try:
        compiled = _build_route_regex(path)
    except (re.error, ValueError):
        compiled = (None, {})
    with _ROUTE_CACHE_LOCK:
        _ROUTE_CACHE.setdefault(path, compiled)
    return compiled


def _route_specificity(path):
    """Sort key that puts static and more-specific routes first."""
    tokens = list(_ROUTE_TOKEN_RE.finditer(path))
    literal_characters = len(path) - sum(len(token.group(0)) for token in tokens)
    has_path_converter = any(
        (token.group(2) or "str") == "path" for token in tokens
    )
    return (int(has_path_converter), -literal_characters, len(tokens), -len(path))


class Request:
    """The mutable representation of one incoming HTTP request.

    Usage::

      >>> req = Request()
      >>> req.prepare(raw_http_message, routes)
      <Request GET /echo HTTP/1.1>

    The object is filled in one pass. prepare() accepts a complete message
    (headers and body) and prepare_from_parts() accepts the already-separated
    pieces used by streaming adapters.
    """

    __attrs__ = [
        "method",
        "target",
        "url",
        "path",
        "query_string",
        "query",
        "version",
        "headers",
        "body",
        "text",
        "charset",
        "cookies",
        "authorization",
        "auth",
        "remote_addr",
        "request_id",
        "keep_alive",
        "content_length",
        "chunked",
        "expect_continue",
        "routes",
        "hook",
        "path_params",
        "allowed_methods",
        "user",
        "remaining",
        "_raw_headers",
        "_raw_body",
    ]

    def __init__(self):
        #: Uppercase HTTP verb, e.g. GET.
        self.method = None
        #: Raw request-target exactly as received.
        self.target = None
        #: Compatibility alias for the request-target.
        self.url = None
        #: Decoded and normalised path, e.g. /channels/1.
        self.path = None
        #: Raw query string, without the leading '?'.
        self.query_string = ""
        #: Parsed query parameters: {name: [value, ...]}.
        self.query = {}
        #: HTTP/1.0 or HTTP/1.1.
        self.version = None
        #: Case-insensitive request headers, preserving display casing.
        self.headers = CaseInsensitiveDict()
        #: Raw request body bytes (after chunked decoding).
        self.body = b""
        #: Body decoded as text using the declared charset.
        self.text = ""
        #: Charset declared by Content-Type (defaults to UTF-8).
        self.charset = config.DEFAULT_CHARSET
        #: Parsed Cookie header.
        self.cookies = {}
        #: Raw Authorization header value.
        self.authorization = None
        #: Decoded Basic credentials as (username, password).
        self.auth = None
        #: (ip, port) of the client when known.
        self.remote_addr = None
        #: Short identifier used by the adapter and server logs.
        self.request_id = uuid.uuid4().hex[:12]
        #: Whether the connection should be reused after this request.
        self.keep_alive = False
        #: Declared or decoded body length.
        self.content_length = 0
        #: Whether the body was chunked.
        self.chunked = False
        #: Whether the client sent Expect: 100-continue.
        self.expect_continue = False
        #: Route table supplied by the application.
        self.routes = {}
        #: Handler selected for this request, or None.
        self.hook = None
        #: Values captured from a parameterised route.
        self.path_params = {}
        #: Methods registered for a path that did not match the request method.
        self.allowed_methods = []
        #: Filled by the authentication middleware in Phase 3.
        self.user = None
        #: Bytes that followed the framed body (pipelined data).
        self.remaining = b""
        #: Raw header section and body section.
        self._raw_headers = b""
        self._raw_body = b""

    # ------------------------------------------------------------------
    # Parsing entry points
    # ------------------------------------------------------------------
    def prepare(self, request, routes=None, remote_addr=None):
        """Parse a complete HTTP message and return this object."""
        self.request_id = uuid.uuid4().hex[:12]
        self.user = None
        raw = self._coerce_message(request)
        head, body = self.fetch_headers_body(raw)
        head_text = head.decode("iso-8859-1")
        request_line = head_text.split("\r\n", 1)[0]
        method, target, path, query_string, query, version = _parse_request_line(
            request_line
        )

        self.method = method
        self.target = target
        self.url = target
        self.path = path
        self.query_string = query_string
        self.query = query
        self.version = version
        self._raw_headers = head
        self._raw_body = body
        self.headers = self.prepare_headers(head)
        self._parse_body(body)
        self._finalize(routes, remote_addr)
        return self

    def prepare_from_parts(
        self,
        method,
        target,
        version,
        headers,
        body=b"",
        routes=None,
        remote_addr=None,
    ):
        """Parse a request whose head and body were read separately.

        Streaming adapters use this after framing: headers is a mapping,
        sequence of pairs, or CaseInsensitiveDict, and body is the framed body
        (still chunk-encoded when Transfer-Encoding says so).
        """
        if not isinstance(method, str) or not _METHOD_RE.fullmatch(method):
            raise BadRequest("Invalid HTTP method")
        method = method.upper()
        if version not in ("HTTP/1.0", "HTTP/1.1"):
            raise BadRequest("Unsupported HTTP version")
        if target == "*" and method != "OPTIONS":
            raise BadRequest("'*' is only valid for OPTIONS")

        self.request_id = uuid.uuid4().hex[:12]
        self.user = None
        self.method = method
        self.target = target
        self.url = target
        self.version = version
        self.path, self.query_string, self.query = _parse_target(target)
        self.headers = self._coerce_headers(headers)
        self._raw_headers = b""
        self._raw_body = self._coerce_body(body)
        self._parse_body(self._raw_body)
        self._finalize(routes, remote_addr)
        return self

    # ------------------------------------------------------------------
    # Request line and headers
    # ------------------------------------------------------------------
    def extract_request_line(self, request):
        """Return (method, path, version) or raise BadRequest."""
        text = self._coerce_text(request)
        first_line = text.split("\r\n", 1)[0]
        method, _target, path, _query_string, _query, version = _parse_request_line(
            first_line
        )
        return method, path, version

    def prepare_headers(self, request):
        """Parse the header block into a CaseInsensitiveDict.

        request may be either the complete head (request line included) or
        just the header lines. Duplicate headers are combined with ', ';
        Cookie is combined with '; ' so the cookie parser sees every value.
        Obsolete line folding and control characters are rejected.
        """
        raw = self._coerce_raw(request)
        if len(raw) > config.MAX_HEADER_BYTES:
            raise RequestHeaderFieldsTooLarge("HTTP headers are too large")
        lines = raw.decode("iso-8859-1").split("\r\n")
        if lines and _REQUEST_LINE_RE.fullmatch(lines[0]):
            lines = lines[1:]

        headers = CaseInsensitiveDict()
        header_count = 0
        for line in lines:
            if line == "":
                continue
            if line.startswith((" ", "\t")):
                raise BadRequest("Obsolete line folding is not allowed")
            if len(line.encode("iso-8859-1")) > config.MAX_HEADER_LINE_BYTES:
                raise RequestHeaderFieldsTooLarge("HTTP header line is too long")
            if ":" not in line:
                raise BadRequest("Malformed HTTP header line")

            raw_name, value = line.split(":", 1)
            name = raw_name.strip()
            if not name or not _METHOD_RE.fullmatch(name):
                raise BadRequest("Invalid HTTP header name")
            if name != raw_name:
                raise BadRequest("Whitespace before header colon")
            value = value.strip(" \t")
            self._validate_header_value(value)

            header_count += 1
            if header_count > config.MAX_HEADERS:
                raise RequestHeaderFieldsTooLarge("Too many HTTP headers")
            separator = "; " if name.lower() == "cookie" else ", "
            if name in headers:
                headers[name] = f"{headers[name]}{separator}{value}"
            else:
                headers[name] = value
        return headers

    def fetch_headers_body(self, request):
        """Split a complete message into (head_bytes, body_bytes)."""
        raw = self._coerce_message(request)
        boundary = raw.find(b"\r\n\r\n")
        if boundary < 0:
            raise BadRequest("Incomplete HTTP headers")
        head = raw[:boundary]
        body = raw[boundary + 4:]
        if len(head) > config.MAX_HEADER_BYTES:
            raise RequestHeaderFieldsTooLarge("HTTP headers are too large")
        return head, body

    # ------------------------------------------------------------------
    # Body framing
    # ------------------------------------------------------------------
    def _parse_body(self, body):
        self._raw_body = body
        self.remaining = b""
        self.chunked = False
        if len(body) > config.MAX_BODY_BYTES + config.MAX_HEADER_BYTES:
            raise PayloadTooLarge("Request body is too large")

        transfer_encoding = self.headers.get("Transfer-Encoding")
        content_length = self.headers.get("Content-Length")
        content_encoding = self.headers.get("Content-Encoding")

        if content_encoding and content_encoding.strip().lower() not in (
            "identity",
            "",
        ):
            raise UnsupportedMediaType("Unsupported Content-Encoding")

        if transfer_encoding is not None and content_length is not None:
            raise BadRequest(
                "Content-Length and Transfer-Encoding must not both be present"
            )

        if transfer_encoding is not None:
            codings = [
                coding.strip().lower()
                for coding in transfer_encoding.split(",")
                if coding.strip()
            ]
            if codings != ["chunked"]:
                raise UnsupportedMediaType("Unsupported Transfer-Encoding")
            self.chunked = True
            decoded = self._decode_chunked(body)
            self.body = decoded
            self.content_length = len(decoded)
            return

        if content_length is not None:
            parts = [part.strip() for part in content_length.split(",")]
            if not parts or any(not part or not part.isdigit() for part in parts):
                raise BadRequest("Invalid Content-Length")
            lengths = {int(part) for part in parts}
            if len(lengths) != 1:
                raise BadRequest("Conflicting Content-Length values")
            length = lengths.pop()
            if length > config.MAX_BODY_BYTES:
                raise PayloadTooLarge("Request body is too large")
            if len(body) < length:
                raise BadRequest("Incomplete request body")
            self.content_length = length
            self.body = body[:length]
            self.remaining = body[length:]
            return

        self.content_length = 0
        self.body = b""
        self.remaining = body

    def _decode_chunked(self, body):
        position = 0
        chunks = []
        decoded_length = 0

        while True:
            line_end = body.find(b"\r\n", position)
            if line_end < 0:
                raise BadRequest("Malformed chunked body")
            if line_end - position > config.MAX_CHUNK_LINE_BYTES:
                raise BadRequest("Chunk size line is too long")
            size_field = body[position:line_end].split(b";", 1)[0].strip()
            if not size_field:
                raise BadRequest("Missing chunk size")
            if any(
                char not in b"0123456789abcdefABCDEF" for char in size_field
            ):
                raise BadRequest("Invalid chunk size")
            try:
                chunk_size = int(size_field, 16)
            except ValueError as exc:
                raise BadRequest("Invalid chunk size") from exc
            if chunk_size < 0:
                raise BadRequest("Invalid chunk size")
            position = line_end + 2

            if chunk_size == 0:
                while True:
                    trailer_end = body.find(b"\r\n", position)
                    if trailer_end < 0:
                        raise BadRequest("Malformed chunked trailer")
                    if trailer_end - position > config.MAX_HEADER_LINE_BYTES:
                        raise BadRequest("Chunked trailer line is too long")
                    trailer = body[position:trailer_end]
                    position = trailer_end + 2
                    if trailer == b"":
                        break
                self.remaining = body[position:]
                break

            if decoded_length + chunk_size > config.MAX_BODY_BYTES:
                raise PayloadTooLarge("Request body is too large")
            if position + chunk_size > len(body):
                raise BadRequest("Incomplete chunk data")
            chunks.append(body[position : position + chunk_size])
            decoded_length += chunk_size
            position += chunk_size
            if body[position : position + 2] != b"\r\n":
                raise BadRequest("Chunk is not terminated by CRLF")
            position += 2

        return b"".join(chunks)

    # ------------------------------------------------------------------
    # Headers and body convenience accessors
    # ------------------------------------------------------------------
    @property
    def content_type(self):
        return self.headers.get("Content-Type", "")

    @property
    def mimetype(self):
        return self.content_type.split(";", 1)[0].strip().lower()

    @property
    def is_json(self):
        mimetype = self.mimetype
        return mimetype == "application/json" or mimetype.endswith("+json")

    @property
    def json(self):
        """Decode the body as JSON, raising BadRequest on failure."""
        try:
            return _json.loads(self.text_body())
        except (ValueError, UnicodeDecodeError) as exc:
            raise BadRequest("Request body is not valid JSON") from exc

    @property
    def form(self):
        """Decode an application/x-www-form-urlencoded body."""
        if not self.body:
            return {}
        if self.mimetype not in ("", "application/x-www-form-urlencoded"):
            return {}
        return parse_qs(self.text_body(), keep_blank_values=True)

    def text_body(self, charset=None):
        """Decode the body with charset or the declared/default charset."""
        selected = charset or self.charset or config.DEFAULT_CHARSET
        try:
            return self.body.decode(selected, errors="replace")
        except LookupError:
            return self.body.decode(config.DEFAULT_CHARSET, errors="replace")

    # ------------------------------------------------------------------
    # Cookies, authentication and connection state
    # ------------------------------------------------------------------
    def parse_cookies(self):
        """Parse the Cookie header according to RFC 6265 syntax."""
        raw = self.headers.get("Cookie", "")
        cookies = {}
        for part in raw.split(";"):
            part = part.strip()
            if not part or part.startswith("$") or "=" not in part:
                continue
            name, value = part.split("=", 1)
            name = name.strip()
            value = value.strip()
            if not name:
                continue
            if len(value) >= 2 and value[0] == value[-1] == '"':
                value = value[1:-1]
            cookies[name] = value
        return cookies

    def parse_authorization(self):
        """Decode a Basic Authorization header, or return None."""
        raw = self.headers.get("Authorization")
        if not raw:
            return None
        parts = raw.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "basic":
            return None
        try:
            decoded = base64.b64decode(parts[1].strip(), validate=True)
        except (binascii.Error, ValueError):
            return None
        try:
            text = decoded.decode("utf-8")
        except UnicodeDecodeError:
            text = decoded.decode("iso-8859-1", errors="replace")
        if ":" not in text:
            return None
        username, password = text.split(":", 1)
        return (username, password)

    def _parse_content_type(self):
        value = self.headers.get("Content-Type", "")
        match = _CHARSET_RE.search(value)
        self.charset = match.group(1) if match else config.DEFAULT_CHARSET

    def _parse_keep_alive(self):
        connection = self.headers.get("Connection", "")
        tokens = {
            token.strip().lower()
            for token in connection.split(",")
            if token.strip()
        }
        if self.version == "HTTP/1.1":
            self.keep_alive = "close" not in tokens
        else:
            self.keep_alive = (
                "keep-alive" in tokens and "close" not in tokens
            )

    def _parse_expect(self):
        expect = self.headers.get("Expect", "")
        tokens = {
            token.strip().lower() for token in expect.split(",") if token.strip()
        }
        self.expect_continue = (
            self.version == "HTTP/1.1" and "100-continue" in tokens
        )

    # ------------------------------------------------------------------
    # Route matching
    # ------------------------------------------------------------------
    def match_route(self, routes):
        """Select the handler for this request and capture path parameters.

        The route table maps (METHOD, path) to a callable. Paths may use
        {name} / <name>, <int:name> or <path:name> placeholders. Exact routes
        win; otherwise the most specific matching pattern wins. When the path
        matches but the method does not, the registered methods are recorded
        in allowed_methods so the adapter can emit 405.
        """
        self.routes = routes if isinstance(routes, dict) else {}
        self.hook = None
        self.path_params = {}
        self.allowed_methods = []
        if not self.routes:
            return None

        method = (self.method or "").upper()
        path = self.path or ""

        exact = self.routes.get((method, path))
        if exact is None and method == "HEAD":
            exact = self.routes.get(("GET", path))
        if exact is not None:
            self.hook = exact
            return exact

        allowed = set()
        matches = []
        for key, handler in self.routes.items():
            if not isinstance(key, tuple) or len(key) != 2:
                continue
            route_method, route_path = key
            if not isinstance(route_method, str) or not isinstance(route_path, str):
                continue
            regex, converters = _compile_route(route_path)
            if regex is None:
                continue
            match = regex.fullmatch(path)
            if match is None:
                continue

            route_method = route_method.upper()
            if route_method == "GET":
                allowed.update(("GET", "HEAD"))
            else:
                allowed.add(route_method)

            if route_method != method and not (
                method == "HEAD" and route_method == "GET"
            ):
                continue

            params = {}
            valid = True
            for name, converter in converters.items():
                value = match.group(name)
                if converter == "int":
                    try:
                        params[name] = int(value)
                    except (TypeError, ValueError):
                        valid = False
                        break
                else:
                    params[name] = value
            if not valid:
                continue
            matches.append((_route_specificity(route_path), handler, params))

        if matches:
            matches.sort(key=lambda item: item[0])
            _specificity, handler, params = matches[0]
            self.hook = handler
            self.path_params = params
            self.allowed_methods = sorted(allowed)
            return handler

        self.allowed_methods = sorted(allowed)
        return None

    # ------------------------------------------------------------------
    # Completion and coercion helpers
    # ------------------------------------------------------------------
    def _finalize(self, routes, remote_addr):
        self.remote_addr = remote_addr
        if self.version == "HTTP/1.1" and not self.headers.get("Host"):
            raise BadRequest("HTTP/1.1 requests require a Host header")
        self._parse_content_type()
        self.cookies = self.parse_cookies()
        self.authorization = self.headers.get("Authorization")
        self.auth = self.parse_authorization()
        self._parse_keep_alive()
        self._parse_expect()
        self.text = self.text_body()
        self.match_route(routes or {})
        # Keep the historical static-server behaviour: an unmatched '/' serves
        # www/index.html. A registered GET '/' route still wins above.
        if self.hook is None and not self.allowed_methods and self.path == "/":
            self.path = "/index.html"

    @staticmethod
    def _validate_header_value(value):
        for char in value:
            code = ord(char)
            if code == 0x09:
                continue
            if code < 0x20 or code == 0x7F:
                raise BadRequest("HTTP header value contains control characters")

    @classmethod
    def _coerce_message(cls, value):
        """Coerce a complete message, encoding a text body as UTF-8.

        Header octets stay ISO-8859-1 (the HTTP wire encoding), while a
        string body is UTF-8 so its Content-Length still describes the
        bytes a UTF-8 client sent.
        """
        if isinstance(value, str):
            head, separator, body = value.partition("\r\n\r\n")
            if separator:
                try:
                    head_bytes = head.encode("iso-8859-1")
                except UnicodeEncodeError:
                    head_bytes = head.encode("utf-8")
                return head_bytes + b"\r\n\r\n" + body.encode("utf-8")
        return cls._coerce_raw(value)

    @staticmethod
    def _coerce_raw(value):
        if isinstance(value, str):
            # Headers are ISO-8859-1, but callers may hand us a string that
            # already contains a non-Latin-1 body. Prefer the wire-faithful
            # Latin-1 encoding and fall back to UTF-8 only for such bodies.
            try:
                return value.encode("iso-8859-1")
            except UnicodeEncodeError:
                return value.encode("utf-8")
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        raise BadRequest("HTTP message must be str or bytes")

    @staticmethod
    def _coerce_text(value):
        if isinstance(value, str):
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).decode("iso-8859-1")
        raise BadRequest("HTTP message must be str or bytes")

    @staticmethod
    def _coerce_headers(headers):
        if headers is None:
            return CaseInsensitiveDict()
        if isinstance(headers, CaseInsensitiveDict):
            return headers.copy()
        if isinstance(headers, dict):
            items = headers.items()
        else:
            try:
                items = list(headers)
            except TypeError as exc:
                raise BadRequest("Invalid request headers") from exc
        try:
            return CaseInsensitiveDict(
                (str(name), str(value)) for name, value in items
            )
        except (TypeError, ValueError) as exc:
            raise BadRequest("Invalid request headers") from exc

    @staticmethod
    def _coerce_body(body):
        if body is None:
            return b""
        if isinstance(body, (bytes, bytearray, memoryview)):
            return bytes(body)
        if isinstance(body, str):
            return body.encode("utf-8")
        raise BadRequest("Request body must be bytes")

    # ------------------------------------------------------------------
    # Outgoing-request compatibility helpers (used by proxy/client code)
    # ------------------------------------------------------------------
    def prepare_body(self, data, files=None, json=None):
        """Prepare an outgoing body and its framing headers."""
        if files:
            raise UnsupportedMediaType("Multipart uploads are not supported")
        if json is not None:
            payload = _json.dumps(json).encode("utf-8")
            self.headers["Content-Type"] = "application/json"
        elif data is None:
            payload = b""
        elif isinstance(data, dict):
            payload = urlencode(data, doseq=True).encode("utf-8")
            self.headers.setdefault(
                "Content-Type", "application/x-www-form-urlencoded"
            )
        elif isinstance(data, str):
            payload = data.encode("utf-8")
        elif isinstance(data, (bytes, bytearray, memoryview)):
            payload = bytes(data)
        else:
            raise TypeError("data must be a mapping, str, bytes or None")

        self.body = payload
        self.text = self.text_body()
        self.prepare_content_length(payload)
        return payload

    def prepare_content_length(self, body):
        """Set Content-Length for an outgoing body and return the length."""
        if isinstance(body, str):
            body = body.encode("utf-8")
        length = len(body or b"")
        self.headers["Content-Length"] = str(length)
        return length

    def prepare_auth(self, auth, url=""):
        """Set a Basic Authorization header from a credential tuple."""
        if not auth:
            return
        if not isinstance(auth, (tuple, list)) or len(auth) != 2:
            raise ValueError("auth must be a (username, password) pair")
        username, password = auth
        token = base64.b64encode(
            f"{username}:{password}".encode()
        ).decode("ascii")
        self.headers["Authorization"] = f"Basic {token}"
        self.authorization = self.headers["Authorization"]
        self.auth = (str(username), str(password))

    def prepare_cookies(self, cookies):
        if isinstance(cookies, dict):
            cookies = "; ".join(
                f"{name}={value}" for name, value in cookies.items()
            )
        elif not isinstance(cookies, str):
            cookies = str(cookies)
        self.headers["Cookie"] = cookies
        self.cookies = self.parse_cookies()

    def __repr__(self):
        return "<Request {} {} {}>".format(
            self.method or "?", self.path or "?", self.version or "?"
        )
