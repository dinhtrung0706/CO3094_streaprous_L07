"""HTTP error types shared by the request parser and the server."""


class HTTPError(Exception):
    """Base class for errors that map directly to an HTTP status code."""

    status_code = 500
    reason = "Internal Server Error"

    def __init__(self, message=None, *, status_code=None, headers=None):
        self.message = message if message is not None else self.reason
        self.headers = dict(headers or {})
        if status_code is not None:
            self.status_code = int(status_code)
        super().__init__(self.message)

    def __str__(self):
        return str(self.message)


class BadRequest(HTTPError, ValueError):
    """The client sent a malformed or unsupported request."""

    status_code = 400
    reason = "Bad Request"


class Unauthorized(HTTPError):
    status_code = 401
    reason = "Unauthorized"


class Forbidden(HTTPError):
    status_code = 403
    reason = "Forbidden"


class NotFound(HTTPError):
    status_code = 404
    reason = "Not Found"


class MethodNotAllowed(HTTPError):
    status_code = 405
    reason = "Method Not Allowed"

    def __init__(self, message=None, *, allowed=None, **kwargs):
        self.allowed = sorted({method.upper() for method in (allowed or [])})
        headers = dict(kwargs.pop("headers", {}) or {})
        if self.allowed:
            headers.setdefault("Allow", ", ".join(self.allowed))
        super().__init__(message, headers=headers, **kwargs)

class ProxyAuthenticationRequired(HTTPError):
    status_code = 407
    reason = "Proxy Authentication Required"

class RequestTimeout(HTTPError):
    status_code = 408
    reason = "Request Timeout"


class PayloadTooLarge(HTTPError):
    status_code = 413
    reason = "Payload Too Large"


class URITooLong(HTTPError):
    status_code = 414
    reason = "URI Too Long"


class UnsupportedMediaType(HTTPError):
    status_code = 415
    reason = "Unsupported Media Type"


class RequestHeaderFieldsTooLarge(HTTPError):
    status_code = 431
    reason = "Request Header Fields Too Large"


class BadGateway(HTTPError):
    status_code = 502
    reason = "Bad Gateway"


class GatewayTimeout(HTTPError):
    status_code = 504
    reason = "Gateway Timeout"
