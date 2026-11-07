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
daemon.response
~~~~~~~~~~~~~~~~~

This module provides a Response object that builds HTTP/1.1 responses,
including status lines, headers, cookies, JSON/text/HTML bodies, static files,
conditional GET, and consistent error pages.

The builder is transport independent: socket, asyncio and raw-socket adapters
call build_response() and send the returned bytes. Static-file access is
confined to www/ and static/ and rejects traversal, symlink escapes, hidden
files, control characters and oversized files.
"""

import datetime
import html
import json as _json
import mimetypes
import os
import re
import time
from http import HTTPStatus
from urllib.parse import unquote

from . import config
from .dictionary import CaseInsensitiveDict
from .http_utils import (
    format_http_date,
    is_json_request,
    parse_http_date,
    safe_header_value,
    sniff_content_type,
    to_bytes,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/"
WWW_DIR = os.path.join(BASE_DIR, "www")
STATIC_DIR = os.path.join(BASE_DIR, "static")

_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_\x60|~0-9A-Za-z]+$")
_COOKIE_OCTET_RE = re.compile(r"^[\x21\x23-\x2B\x2D-\x3A\x3C-\x5B\x5D-\x7E]*$")
_HEADER_CONTROL_RE = re.compile(r"[\x00-\x1F\x7F]")

_TEXT_CHARSET_TYPES = {
    "application/json",
    "application/javascript",
    "application/xml",
    "application/xhtml+xml",
    "image/svg+xml",
}

_MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".htm": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".mjs": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".map": "application/json; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
    ".md": "text/plain; charset=utf-8",
    ".csv": "text/csv; charset=utf-8",
    ".xml": "application/xml; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".ico": "image/x-icon",
    ".bmp": "image/bmp",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
    ".wasm": "application/wasm",
    ".pdf": "application/pdf",
    ".zip": "application/zip",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
}

_STATIC_SUFFIXES = set(_MIME_TYPES) - {".html", ".htm"}
_MANAGED_HEADERS = {
    "date",
    "server",
    "content-type",
    "content-length",
    "connection",
    "set-cookie",
    "x-content-type-options",
    "referrer-policy",
}


def _reason_for(status_code):
    """Return the standard reason phrase for a status code."""
    try:
        return HTTPStatus(int(status_code)).phrase
    except (TypeError, ValueError):
        return "Unknown"


def _with_charset(content_type):
    """Append a UTF-8 charset to text-like content types when missing."""
    if not content_type:
        return content_type
    lowered = content_type.lower()
    if "charset=" in lowered:
        return content_type
    main_type = lowered.split("/", 1)[0].strip()
    if main_type == "text" or lowered in _TEXT_CHARSET_TYPES:
        return content_type + "; charset=utf-8"
    return content_type


def _content_type_for_path(path):
    """Return the full Content-Type for a static path."""
    suffix = os.path.splitext(path or "")[1].lower()
    content_type = _MIME_TYPES.get(suffix)
    if content_type is None:
        guessed, _encoding = mimetypes.guess_type(path or "")
        content_type = guessed or "application/octet-stream"
    return _with_charset(content_type)


def _request_header(request, name):
    """Read a header from a Request-like object without assuming its type."""
    headers = getattr(request, "headers", None)
    if not headers:
        return None
    try:
        return headers.get(name)
    except (AttributeError, TypeError):
        return None


def _static_target(path):
    """Map a URL path to (base_directory, relative_file_path) or None."""
    if not path:
        path = "/"
    try:
        path = unquote(path, encoding="utf-8", errors="strict")
    except (UnicodeError, ValueError):
        return None
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in path):
        return None

    if path in ("", "/"):
        return WWW_DIR, "index.html"
    if path == "/favicon.ico":
        return os.path.join(STATIC_DIR, "images"), "favicon.ico"
    if path.startswith("/static/"):
        return STATIC_DIR, path[len("/static/") :]
    if path.startswith("/www/"):
        return WWW_DIR, path[len("/www/") :]

    suffix = os.path.splitext(path)[1].lower()
    if suffix in (".html", ".htm"):
        return WWW_DIR, path.lstrip("/")
    if suffix in _STATIC_SUFFIXES:
        return STATIC_DIR, path.lstrip("/")
    return None


class Response:
    """The server's response to one HTTP request.

    The object is mutable: handlers can set a status, headers, cookies and a
    body, then call build_response() to obtain the complete wire bytes. When no
    body is supplied, build_response() serves the matching asset from www/ or
    static/.
    """

    __attrs__ = [  # noqa: RUF012
        "_content",
        "_header",
        "status_code",
        "reason",
        "headers",
        "cookies",
        "body",
        "content_type",
        "charset",
        "encoding",
        "http_version",
        "keep_alive",
        "url",
        "history",
        "elapsed",
        "request",
    ]

    def __init__(self, request=None):
        self._content = False
        self._content_consumed = False
        self._body_set = False
        self._next = None

        #: Integer HTTP status code (defaults to 200 when built).
        self.status_code = None
        #: Textual reason phrase, derived from the status code when unset.
        self.reason = None
        #: Case-insensitive response headers.
        self.headers = CaseInsensitiveDict()
        #: Serialized Set-Cookie header values (one per cookie).
        self.cookies = []
        #: Response body bytes.
        self.body = b""
        #: Full Content-Type selected by a helper or static file.
        self.content_type = None
        #: Encoding used when a string body is supplied.
        self.charset = "utf-8"
        self.encoding = "utf-8"
        #: Response protocol version.
        self.http_version = "HTTP/1.1"
        #: None derives from the request; True/False overrides it.
        self.keep_alive = None
        #: URL/target associated with the response.
        self.url = None
        self.history = []
        self.elapsed = datetime.timedelta(0)
        self.request = request

    # ------------------------------------------------------------------
    # Status, headers and cookies
    # ------------------------------------------------------------------
    def set_status(self, code, reason=None):
        """Set the status code and optional reason phrase."""
        self.status_code = int(code)
        self.reason = reason if reason is not None else _reason_for(code)
        return self

    def set_header(self, name, value):
        """Set a response header, replacing an existing value."""
        if name is None:
            raise ValueError("header name is required")
        name = str(name)
        if not _HEADER_NAME_RE.fullmatch(name):
            raise ValueError(f"invalid header name: {name!r}")
        if isinstance(value, (list, tuple)):
            if name.lower() == "set-cookie":
                self.cookies = [safe_header_value(item) for item in value]
                return self
            value = ", ".join(str(item) for item in value)
        value = safe_header_value(value)
        if name.lower() == "set-cookie":
            self.cookies = [value]
        else:
            self.headers[name] = value
        return self

    def add_header(self, name, value):
        """Add a header value, combining repeated non-cookie headers."""
        if name is None:
            raise ValueError("header name is required")
        name = str(name)
        if not _HEADER_NAME_RE.fullmatch(name):
            raise ValueError(f"invalid header name: {name!r}")
        if isinstance(value, (list, tuple)):
            if name.lower() == "set-cookie":
                for item in value:
                    self.add_header(name, item)
                return self
            value = ", ".join(str(item) for item in value)
        value = safe_header_value(value)
        if name.lower() == "set-cookie":
            if not isinstance(self.cookies, list):
                self.cookies = []
            self.cookies.append(value)
        elif name in self.headers:
            self.headers[name] = f"{self.headers[name]}, {value}"
        else:
            self.headers[name] = value
        return self

    def set_cookie(
        self,
        name,
        value,
        *,
        max_age=None,
        path="/",
        domain=None,
        expires=None,
        http_only=True,
        secure=False,
        same_site="Lax",
    ):
        """Append an RFC 6265 Set-Cookie value."""
        if not _HEADER_NAME_RE.fullmatch(str(name or "")):
            raise ValueError(f"invalid cookie name: {name!r}")
        raw_value = "" if value is None else str(value)
        if not _COOKIE_OCTET_RE.fullmatch(raw_value):
            raise ValueError("invalid cookie value")
        parts = [f"{name}={raw_value}"]

        if expires is not None:
            if isinstance(expires, datetime.datetime):
                expires = expires.timestamp()
            if isinstance(expires, (int, float)):
                parts.append(f"Expires={format_http_date(float(expires))}")
            else:
                parts.append(f"Expires={safe_header_value(expires)}")
        if max_age is not None:
            parts.append(f"Max-Age={int(max_age)}")
        if path:
            path = str(path)
            if _HEADER_CONTROL_RE.search(path) or ";" in path or "," in path:
                raise ValueError("invalid cookie path")
            parts.append(f"Path={path}")
        if domain:
            domain = str(domain)
            if _HEADER_CONTROL_RE.search(domain) or ";" in domain or "," in domain:
                raise ValueError("invalid cookie domain")
            parts.append(f"Domain={domain}")
        if secure:
            parts.append("Secure")
        if http_only:
            parts.append("HttpOnly")
        if same_site:
            same_site = str(same_site).capitalize()
            if same_site not in ("Lax", "Strict", "None"):
                raise ValueError("invalid SameSite value")
            parts.append(f"SameSite={same_site}")

        if not isinstance(self.cookies, list):
            self.cookies = []
        self.cookies.append("; ".join(parts))
        return self

    def delete_cookie(self, name, path="/"):
        """Emit an expired cookie that removes the client-side value."""
        return self.set_cookie(
            name,
            "",
            max_age=0,
            path=path,
            expires=0,
            http_only=True,
        )

    # ------------------------------------------------------------------
    # Body helpers
    # ------------------------------------------------------------------
    def set_body(self, value):
        """Set the body from bytes, text, a mapping or a list."""
        if value is None:
            payload = b""
        elif isinstance(value, (dict, list)):
            payload = _json.dumps(value, ensure_ascii=False).encode("utf-8")
            if self.content_type is None:
                self.content_type = "application/json; charset=utf-8"
        elif isinstance(value, str):
            payload = value.encode(self.encoding or "utf-8")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            payload = bytes(value)
        else:
            raise TypeError("response body must be bytes, str, mapping or list")
        self.body = payload
        self._content = payload
        self._body_set = True
        return self

    def json(self, data, status=200):
        """Set a JSON body and its content type."""
        self.set_status(status)
        self.content_type = "application/json; charset=utf-8"
        self.set_header("Content-Type", self.content_type)
        return self.set_body(_json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def text(self, data, status=200, content_type="text/plain; charset=utf-8"):
        """Set a text body and its content type."""
        self.set_status(status)
        self.content_type = content_type
        self.set_header("Content-Type", content_type)
        if isinstance(data, bytes):
            return self.set_body(data)
        if not isinstance(data, str):
            data = str(data)
        return self.set_body(data)

    def html(self, data, status=200):
        """Set an HTML body and its content type."""
        return self.text(data, status, "text/html; charset=utf-8")

    def redirect(self, location, status=302):
        """Create a redirect response."""
        self.set_status(status)
        self.set_header("Location", location)
        return self.set_body(b"")

    def no_content(self):
        """Create a 204 response with no body."""
        self.set_status(204)
        return self.set_body(b"")

    def not_modified(self):
        """Create a 304 response with no body."""
        self.set_status(304)
        return self.set_body(b"")

    def _body_bytes(self):
        body = self.body
        if not self._body_set and isinstance(
            self._content, (bytes, bytearray, memoryview, str)
        ):
            body = self._content
        if body is None:
            return b""
        if isinstance(body, str):
            return body.encode(self.encoding or "utf-8")
        if isinstance(body, (bytes, bytearray, memoryview)):
            return bytes(body)
        raise TypeError("response body must be bytes or str")

    # ------------------------------------------------------------------
    # MIME and static-content helpers
    # ------------------------------------------------------------------
    def get_mime_type(self, path):
        """Return the bare MIME type for a path."""
        return _content_type_for_path(path).split(";", 1)[0].strip()

    def prepare_content_type(self, mime_type="text/html"):
        """Set Content-Type and return the base directory for that type."""
        mime_type = str(mime_type).split(";", 1)[0].strip()
        main_type, separator, sub_type = mime_type.partition("/")
        if not separator or not main_type or not sub_type:
            raise ValueError(f"invalid MIME type: {mime_type!r}")
        main_type = main_type.lower()
        sub_type = sub_type.lower()
        if main_type == "text" and sub_type in ("html", "htm"):
            base_dir = WWW_DIR
        elif main_type in (
            "text",
            "image",
            "application",
            "font",
            "audio",
            "video",
        ):
            base_dir = STATIC_DIR
        else:
            raise ValueError(f"unsupported MIME type: {mime_type!r}")
        self.content_type = _with_charset(str(mime_type))
        self.set_header("Content-Type", self.content_type)
        return base_dir + os.sep

    def build_content(self, path, base_dir):
        """Load a static file safely, returning (length, bytes) or (-1, b"")."""
        if not base_dir:
            return -1, b""
        try:
            decoded = unquote(path or "", encoding="utf-8", errors="strict")
        except (UnicodeError, ValueError):
            return -1, b""
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in decoded):
            return -1, b""
        if "\\" in decoded:
            return -1, b""

        relative = decoded.lstrip("/")
        if not relative:
            return -1, b""
        segments = relative.split("/")
        if any(
            segment in ("", ".", "..") or segment.startswith(".")
            for segment in segments
        ):
            return -1, b""

        base = os.path.realpath(base_dir)
        candidate = os.path.realpath(os.path.join(base, *segments))
        try:
            if os.path.commonpath([base, candidate]) != base:
                return -1, b""
        except ValueError:
            return -1, b""
        if not os.path.isfile(candidate):
            return -1, b""

        try:
            if os.path.getsize(candidate) > config.MAX_STATIC_BYTES:
                return -1, b""
            with open(candidate, "rb") as handle:
                content = handle.read()
        except OSError:
            return -1, b""
        if len(content) > config.MAX_STATIC_BYTES:
            return -1, b""
        return len(content), content

    # ------------------------------------------------------------------
    # Response construction
    # ------------------------------------------------------------------
    def _suppress_body(self, request):
        status = self.status_code or 200
        method = str(getattr(request, "method", "") or "").upper()
        if method == "HEAD":
            return True
        return status in (204, 304) or 100 <= status < 200

    def _is_not_modified(self, request, etag, mtime):
        if request is None:
            return False
        if_none_match = _request_header(request, "If-None-Match")
        if if_none_match is not None:
            tags = [tag.strip() for tag in str(if_none_match).split(",")]
            return "*" in tags or etag in tags
        if_modified_since = _request_header(request, "If-Modified-Since")
        if if_modified_since is None:
            return False
        parsed = parse_http_date(str(if_modified_since))
        if parsed is None:
            return False
        return int(mtime) <= int(parsed)

    def _build_static(self, request):
        """Serve a static file into this response. Return True when handled."""
        if request is None:
            return False
        method = (getattr(request, "method", None) or "GET").upper()
        if method not in ("GET", "HEAD"):
            self.set_status(405)
            self.set_header("Allow", "GET, HEAD")
            return False

        path = getattr(request, "path", None) or "/"
        target = _static_target(path)
        if target is None:
            return False
        base_dir, relative = target
        size, content = self.build_content(relative, base_dir)
        if size < 0:
            return False

        self.set_status(200)
        self.set_body(content)
        self.content_type = _content_type_for_path(relative)
        self.set_header("Content-Type", self.content_type)
        self.set_header("Cache-Control", "max-age=0, must-revalidate")

        file_path = os.path.realpath(os.path.join(base_dir, relative.lstrip("/")))
        try:
            stat = os.stat(file_path)
        except OSError:
            stat = None
        if stat is not None:
            etag = f'W/"{stat.st_mtime_ns}-{size}"'
            self.set_header("ETag", etag)
            self.set_header("Last-Modified", format_http_date(stat.st_mtime))
            if self._is_not_modified(request, etag, stat.st_mtime):
                self.set_status(304)
                self.set_body(b"")
        return True

    def _build_error_response(self, status, request=None, reason=None, headers=None):
        """Build a complete error response in HTML or JSON form."""
        response = Response(request)
        response.status_code = int(status)
        response.reason = reason if reason is not None else _reason_for(status)
        if headers is not None:
            for name, value in headers.items():
                response.set_header(name, value)
        response.set_header("Cache-Control", "no-store")

        path = getattr(request, "path", "") if request is not None else ""
        json_mode = is_json_request(request) if request is not None else False
        phrase = response.reason

        if json_mode:
            payload = {
                "error": phrase,
                "status": response.status_code,
                "path": path,
            }
            body = _json.dumps(payload, ensure_ascii=False).encode("utf-8")
            response.set_header("Content-Type", "application/json; charset=utf-8")
        else:
            body = (
                (
                    f"<!doctype html><html><head><title>{response.status_code} {html.escape(str(phrase))}</title></head>"
                    f"<body><h1>{response.status_code} {html.escape(str(phrase))}</h1><p>{html.escape(str(path))}</p></body></html>"
                )
                
                .encode()
            )
            response.set_header("Content-Type", "text/html; charset=utf-8")
        response.set_body(body)

        header_bytes = response.build_response_header(request)
        method = str(getattr(request, "method", "") or "").upper()
        if method == "HEAD":
            return header_bytes
        return header_bytes + body

    def build_response_header(self, request):
        """Construct the status line and response headers as bytes."""
        body = self._body_bytes()
        status = int(self.status_code or 200)
        self.status_code = status
        self.reason = self.reason or _reason_for(status)
        reason = _HEADER_CONTROL_RE.sub(" ", str(self.reason))

        no_body_status = status in (204, 304) or 100 <= status < 200
        content_type = self.headers.get("Content-Type") or self.content_type
        if no_body_status:
            content_type = None
        elif not content_type:
            if body:
                content_type = sniff_content_type(body)
                content_type = _with_charset(content_type)
            else:
                content_type = "text/plain; charset=utf-8"

        keep_alive = self.keep_alive
        if keep_alive is None:
            keep_alive = bool(getattr(request, "keep_alive", False))
        connection = "keep-alive" if keep_alive else "close"

        lines = [
            "{} {} {}".format(self.http_version or "HTTP/1.1", status, reason),
            f"Date: {format_http_date(time.time())}",
            f"Server: {config.SERVER_NAME}",
        ]
        if content_type:
            lines.append(f"Content-Type: {safe_header_value(content_type)}")
        if not no_body_status:
            lines.append(f"Content-Length: {len(body)}")
        lines.append(f"Connection: {connection}")
        lines.append("X-Content-Type-Options: nosniff")
        lines.append("Referrer-Policy: same-origin")

        if "Cache-Control" not in self.headers:
            lines.append(
                "Cache-Control: {}".format("no-store" if status >= 400 else "no-cache")
            )

        for name, value in self.headers.items():
            lowered = name.lower()
            if lowered in _MANAGED_HEADERS:
                continue
            if not _HEADER_NAME_RE.fullmatch(name):
                raise ValueError(f"invalid header name: {name!r}")
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                value = ", ".join(str(item) for item in value)
            lines.append(f"{name}: {safe_header_value(value)}")

        for cookie in self._iter_cookies():
            lines.append(f"Set-Cookie: {safe_header_value(cookie)}")

        return to_bytes("\r\n".join(lines) + "\r\n\r\n", "utf-8")

    def _iter_cookies(self):
        cookies = self.cookies
        if cookies is None:
            return
        if isinstance(cookies, (list, tuple)):
            for cookie in cookies:
                yield str(cookie)
            return
        if hasattr(cookies, "items"):
            for name, value in cookies.items():
                yield f"{name}={value}"

    def build_response(self, request=None, envelop_content=None):
        """Build the complete response bytes for this response and request."""
        if request is not None:
            self.request = request
            self.url = getattr(request, "target", None) or getattr(
                request, "path", None
            )

        if envelop_content is not None:
            self.set_body(envelop_content)
        elif not self._body_set and self.body:
            self._body_set = True
        elif (
            not self._body_set
            and isinstance(self._content, (bytes, bytearray, memoryview, str))
            and self._content not in (b"", "")
        ):
            self.set_body(self._content)

        if not self._body_set:
            if (
                self.status_code in (None, 200)
                and not self._build_static(request)
                and self.status_code in (None, 200)
            ):
                self.status_code = 404
            if self.status_code is None:
                self.status_code = 200
            if self.status_code >= 400:
                return self._build_error_response(
                    self.status_code,
                    request,
                    reason=self.reason,
                    headers=self.headers,
                )
            if not self._body_set:
                self.set_body(b"")

        if self.status_code is None:
            self.status_code = 200
        if self.status_code >= 400 and not self._body_set:
            return self._build_error_response(
                self.status_code,
                request,
                reason=self.reason,
                headers=self.headers,
            )

        header_bytes = self.build_response_header(request)
        body = self._body_bytes()
        if self._suppress_body(request):
            body = b""
        return header_bytes + body

    def build_notfound(self):
        """Return a complete 404 response."""
        return self._build_error_response(404, None)

    def __repr__(self):
        status = self.status_code or 200
        return f"<Response {status} {self.reason or _reason_for(status)}>"
