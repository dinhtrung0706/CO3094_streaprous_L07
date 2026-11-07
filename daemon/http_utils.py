"""Small, shared helpers for HTTP request and response handling."""

import email.utils
import json
import re
from datetime import timezone
from urllib.parse import unquote


def format_http_date(ts):
    """Format a Unix timestamp as an RFC-compliant HTTP date."""
    return email.utils.formatdate(ts, usegmt=True)


def parse_http_date(value):
    """Parse an HTTP date to a Unix timestamp, or return ``None`` if invalid."""
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def safe_header_value(value):
    """Return a header value as text, rejecting control characters."""
    value = str(value)
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("HTTP header values must not contain control characters")
    return value


def is_json_request(request):
    """Return whether a request targets the API or accepts JSON responses."""
    path = getattr(request, "path", "") or ""
    headers = getattr(request, "headers", {}) or {}
    accept = headers.get("Accept", headers.get("accept", "")) or ""
    return (
        path == "/api"
        or path.startswith("/api/")
        or "json" in str(accept).lower()
    )


def to_bytes(value, charset="utf-8"):
    """Normalize text or bytes to bytes, leaving existing bytes unchanged."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode(charset)
    raise TypeError("value must be str or bytes")


def sniff_content_type(body):
    """Guess JSON, UTF-8 text, or binary content type from a response body."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    elif not isinstance(body, bytes):
        raise TypeError("body must be str or bytes")

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return "application/octet-stream"

    try:
        json.loads(text)
        return "application/json"
    except (json.JSONDecodeError, ValueError):
        if any(ord(char) < 32 and char not in "\t\n\r\f\v" for char in text):
            return "application/octet-stream"
        return "text/plain; charset=utf-8"


def normalize_path(path):
    """Decode a URL path once, reject NUL, and collapse repeated slashes."""
    decoded = unquote(path, encoding="utf-8", errors="strict")
    if "\x00" in decoded:
        raise ValueError("URL paths must not contain NUL")
    return re.sub(r"/{2,}", "/", decoded)
