class HTTPError(Exception):
    status_code = 500
    reason = "Internal Server Error"

    def __init__(self, message=None, *, status_code=None, headers=None): ...


class BadRequest(HTTPError):
    status_code = 400


class Unauthorized(HTTPError):
    status_code = 401


class Forbidden(HTTPError):
    status_code = 403


class NotFound(HTTPError):
    status_code = 404


class MethodNotAllowed(HTTPError):
    status_code = 405


class RequestTimeout(HTTPError):
    status_code = 408


class PayloadTooLarge(HTTPError):
    status_code = 413


class URITooLong(HTTPError):
    status_code = 414


class UnsupportedMediaType(HTTPError):
    status_code = 415


class RequestHeaderFieldsTooLarge(HTTPError):
    status_code = 431


class BadGateway(HTTPError):
    status_code = 502


class GatewayTimeout(HTTPError):
    status_code = 504
