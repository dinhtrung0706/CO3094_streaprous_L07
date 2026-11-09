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
daemon.httpadapter
~~~~~~~~~~~~~~~~~

This module provides the transport-facing HTTP adapter. It reads complete
HTTP/1.1 messages, dispatches registered route handlers (sync or async),
normalizes their return values, builds a Response, and sends the bytes over a
standard socket, an asyncio stream, or the raw streaprous transport.

The adapter owns framing and connection lifecycle. Request parsing lives in
daemon.request and response construction lives in daemon.response.
"""

import asyncio
import inspect
import logging
import re
import socket

from streaprous_socket.streaprous_raw_connection import (
    close_connection,
    run_conn_recv,
    run_conn_send,
)

from . import config
from .errors import (
    BadRequest,
    HTTPError,
    MethodNotAllowed,
    PayloadTooLarge,
    RequestHeaderFieldsTooLarge,
    RequestTimeout,
    Unauthorized,
    UnsupportedMediaType,
)
from .request import Request
from .response import Response

logger = logging.getLogger(__name__)

_CONTINUE_RESPONSE = b"HTTP/1.1 100 Continue\r\n\r\n"
_FALLBACK_500 = (
    b"HTTP/1.1 500 Internal Server Error\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"Content-Length: 21\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Internal Server Error"
)
_CHUNK_HEX = set(b"0123456789abcdefABCDEF")


class _BufferedReader:
    """Incremental reader shared by standard sockets and the raw transport."""

    def __init__(self, recv_fn):
        self._recv = recv_fn
        self.buffer = b""
        self.started = False

    def _receive(self):
        chunk = self._recv()
        if not chunk:
            return False
        self.started = True
        self.buffer += chunk
        return True

    def read_until(self, delimiter, limit, limit_error=RequestHeaderFieldsTooLarge):
        while True:
            index = self.buffer.find(delimiter)
            if index >= 0:
                if index > limit:
                    raise limit_error("HTTP line is too long")
                data = self.buffer[:index]
                self.buffer = self.buffer[index + len(delimiter) :]
                return data, True
            if len(self.buffer) >= limit + len(delimiter):
                raise limit_error("HTTP line is too long")
            if not self._receive():
                if not self.buffer:
                    return None, False
                raise BadRequest("Incomplete HTTP message")

    def read_exact(self, length):
        if length < 0:
            raise BadRequest("Invalid body length")
        while len(self.buffer) < length:
            if not self._receive():
                raise BadRequest("Incomplete HTTP body")
        data = self.buffer[:length]
        self.buffer = self.buffer[length:]
        return data


class HttpAdapter:
    """Transport adapter that turns requests into routed HTTP responses."""

    __attrs__ = [  # noqa: RUF012
        "ip",
        "port",
        "conn",
        "connaddr",
        "routes",
        "request",
        "response",
        "conn_info",
        "raw_socket",
    ]

    def __init__(self, ip, port, conn, connaddr, routes):
        self.ip = ip
        self.port = port
        self.conn = conn
        self.connaddr = connaddr
        self.routes = routes if routes is not None else {}
        self.request = Request()
        self.response = Response()
        self.conn_info = None
        self.raw_socket = None

    # ------------------------------------------------------------------
    # Standard socket transport
    # ------------------------------------------------------------------
    def handle_client(self, conn, addr, routes):
        """Serve one standard TCP client, supporting keep-alive."""
        self.conn = conn
        self.connaddr = addr
        self.routes = routes if routes is not None else {}
        try:
            conn.settimeout(config.SOCKET_TIMEOUT)
        except OSError:
            pass

        reader = _BufferedReader(lambda: conn.recv(65536))
        try:
            for _request_number in range(config.MAX_KEEPALIVE_REQUESTS):
                reader.started = bool(reader.buffer)
                try:
                    request = self.read_request(
                        reader,
                        lambda: conn.sendall(_CONTINUE_RESPONSE),
                    )
                except TimeoutError:
                    if reader.started or reader.buffer:
                        self._safe_send(
                            conn,
                            self._error_payload(
                                None,
                                RequestTimeout("Request timed out"),
                            ),
                        )
                    break
                except HTTPError as exc:
                    self._safe_send(conn, self._error_payload(None, exc))
                    break
                except (ConnectionResetError, BrokenPipeError, OSError):
                    break
                except Exception as exc:
                    logger.exception(
                        "Unhandled request read error from %r", addr
                    )
                    self._safe_send(conn, self._error_payload(None, exc))
                    break

                if request is None:
                    break
                self.request = request

                try:
                    response = self.dispatch(request)
                    payload = response.build_response(request)
                except HTTPError as exc:
                    self._safe_send(
                        conn, self._error_payload(request, exc)
                    )
                    break
                except (ConnectionResetError, BrokenPipeError):
                    break
                except Exception as exc:
                    logger.exception(
                        "Unhandled dispatch error for %s %s",
                        request.method,
                        request.path,
                    )
                    self._safe_send(
                        conn, self._error_payload(request, exc)
                    )
                    break

                self.response = response
                try:
                    conn.sendall(payload)
                except (ConnectionResetError, BrokenPipeError, OSError):
                    break

                if not request.keep_alive:
                    break
                if response.keep_alive is False:
                    break
                if (
                    response.status_code is not None
                    and response.status_code >= 400
                ):
                    break
                try:
                    conn.settimeout(config.KEEP_ALIVE_TIMEOUT)
                except OSError:
                    break
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def read_request(self, reader, send_continue=None):
        """Read and parse one complete HTTP message from a reader."""
        head, found = reader.read_until(
            b"\r\n\r\n", config.MAX_HEADER_BYTES
        )
        if not found:
            return None
        if len(head) > config.MAX_HEADER_BYTES:
            raise RequestHeaderFieldsTooLarge("HTTP headers are too large")

        headers = Request().prepare_headers(head)
        if (
            self._expects_continue(headers)
            and self._body_expected(headers)
            and send_continue is not None
        ):
            send_continue()

        body = self._read_body(reader, headers)
        raw = head + b"\r\n\r\n" + body
        return Request().prepare(
            raw,
            self.routes,
            remote_addr=self.connaddr,
        )

    # ------------------------------------------------------------------
    # Raw streaprous transport
    # ------------------------------------------------------------------
    def handle_client_raw(self, raw_socket, conn_info, routes):
        """Serve one raw-socket client and close the connection afterwards."""
        self.raw_socket = raw_socket
        self.conn_info = conn_info
        self.routes = routes if routes is not None else {}
        self.connaddr = (conn_info.client_ip, conn_info.client_port)

        reader = _BufferedReader(
            lambda: run_conn_recv(raw_socket, conn_info)
        )
        response = None
        try:
            try:
                request = self.read_request(
                    reader,
                    lambda: run_conn_send(
                        raw_socket, conn_info, _CONTINUE_RESPONSE
                    ),
                )
            except HTTPError as exc:
                run_conn_send(
                    raw_socket, conn_info, self._error_payload(None, exc)
                )
                return
            except Exception as exc:
                logger.exception("Unhandled raw request read error")
                run_conn_send(
                    raw_socket,
                    conn_info,
                    self._error_payload(None, exc),
                )
                return

            if request is None:
                return
            self.request = request

            try:
                response = self.dispatch(request)
                response.keep_alive = False
                payload = response.build_response(request)
            except HTTPError as exc:
                payload = self._error_payload(request, exc)
            except Exception:
                logger.exception(
                    "Unhandled raw dispatch error for %s %s",
                    request.method,
                    request.path,
                )
                payload = self._error_payload(request, RuntimeError())

            self.response = response
            if payload:
                run_conn_send(raw_socket, conn_info, payload)
        finally:
            close_connection(raw_socket, conn_info)

    # ------------------------------------------------------------------
    # Asyncio transport
    # ------------------------------------------------------------------
    async def handle_client_coroutine(self, reader, writer):
        """Serve one asyncio client, supporting keep-alive."""
        addr = writer.get_extra_info("peername")
        self.connaddr = addr
        try:
            for _request_number in range(config.MAX_KEEPALIVE_REQUESTS):
                try:
                    request = await self.read_request_async(reader, writer)
                except HTTPError as exc:
                    await self._send_async(
                        writer, self._error_payload(None, exc)
                    )
                    break
                except asyncio.TimeoutError:
                    await self._send_async(
                        writer,
                        self._error_payload(
                            None, RequestTimeout("Request timed out")
                        ),
                    )
                    break
                except (ConnectionResetError, BrokenPipeError, OSError):
                    break
                except Exception as exc:
                    logger.exception(
                        "Unhandled async request read error from %r", addr
                    )
                    await self._send_async(
                        writer, self._error_payload(None, exc)
                    )
                    break

                if request is None:
                    break
                self.request = request

                try:
                    response = await self.dispatch_async(request)
                    payload = response.build_response(request)
                except HTTPError as exc:
                    await self._send_async(
                        writer, self._error_payload(request, exc)
                    )
                    break
                except (ConnectionResetError, BrokenPipeError):
                    break
                except Exception as exc:
                    logger.exception(
                        "Unhandled async dispatch error for %s %s",
                        request.method,
                        request.path,
                    )
                    await self._send_async(
                        writer, self._error_payload(request, exc)
                    )
                    break

                self.response = response
                try:
                    writer.write(payload)
                    await writer.drain()
                except (ConnectionResetError, BrokenPipeError, OSError):
                    break

                if not request.keep_alive:
                    break
                if response.keep_alive is False:
                    break
                if (
                    response.status_code is not None
                    and response.status_code >= 400
                ):
                    break
        finally:
            try:
                writer.close()
                if hasattr(writer, "wait_closed"):
                    await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass

    async def read_request_async(self, reader, writer):
        """Read and parse one complete HTTP message from an asyncio reader."""
        try:
            head_with_delimiter = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=config.SOCKET_TIMEOUT,
            )
        except asyncio.TimeoutError:
            if getattr(reader, "_buffer", b""):
                raise RequestTimeout("Request timed out")
            return None
        except asyncio.IncompleteReadError as exc:
            if not exc.partial:
                return None
            raise BadRequest("Incomplete HTTP headers")
        except asyncio.LimitOverrunError:
            raise RequestHeaderFieldsTooLarge("HTTP headers are too large")
        except (ConnectionResetError, BrokenPipeError):
            raise

        head = head_with_delimiter[:-4]
        if len(head) > config.MAX_HEADER_BYTES:
            raise RequestHeaderFieldsTooLarge("HTTP headers are too large")
        headers = Request().prepare_headers(head)
        if self._expects_continue(headers) and self._body_expected(headers):
            writer.write(_CONTINUE_RESPONSE)
            await writer.drain()

        body = await self._read_body_async(reader, headers)
        raw = head + b"\r\n\r\n" + body
        return Request().prepare(
            raw,
            self.routes,
            remote_addr=self.connaddr,
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------
    def dispatch(self, request):
        """Dispatch a request synchronously and return a Response."""
        self.request = request
        before = self._run_before_hooks_sync(request)
        if before is not None:
            response = self._normalize_result(before, request)
            return self._run_after_hooks_sync(request, response)

        if request.hook is None:
            if request.allowed_methods:
                raise MethodNotAllowed(allowed=request.allowed_methods)
            response = Response()
        else:
            self._check_route_auth(request)
            result = self._call_handler_sync(request.hook, request)
            response = self._normalize_result(result, request)
        return self._run_after_hooks_sync(request, response)

    async def dispatch_async(self, request):
        """Dispatch a request from an event loop and return a Response."""
        self.request = request
        before = await self._run_before_hooks_async(request)
        if before is not None:
            response = self._normalize_result(before, request)
            return await self._run_after_hooks_async(request, response)

        if request.hook is None:
            if request.allowed_methods:
                raise MethodNotAllowed(allowed=request.allowed_methods)
            response = Response()
        else:
            self._check_route_auth(request)
            result = await self._call_handler_async(request.hook, request)
            response = self._normalize_result(result, request)
        return await self._run_after_hooks_async(request, response)

    def _hooks(self, name):
        return list(getattr(self.routes, name, None) or [])

    def _call_hook_sync(self, hook, *args):
        result = hook(*args)
        if inspect.isawaitable(result):
            result = self._run_awaitable_sync(result)
        return result

    def _run_awaitable_sync(self, awaitable):
        try:
            previous = asyncio.get_event_loop()
        except RuntimeError:
            previous = None
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(awaitable)
        finally:
            asyncio.set_event_loop(previous)
            loop.close()

    async def _call_hook_async(self, hook, *args):
        result = hook(*args)
        if inspect.isawaitable(result):
            result = await result
        return result

    def _run_before_hooks_sync(self, request):
        for hook in self._hooks("before_request_hooks"):
            result = self._call_hook_sync(hook, request)
            if result is not None:
                return result
        return None

    async def _run_before_hooks_async(self, request):
        for hook in self._hooks("before_request_hooks"):
            result = await self._call_hook_async(hook, request)
            if result is not None:
                return result
        return None

    def _run_after_hooks_sync(self, request, response):
        current = response
        for hook in self._hooks("after_request_hooks"):
            result = self._call_hook_sync(hook, request, current)
            if result is not None:
                if isinstance(result, Response):
                    current = result
                else:
                    current = self._normalize_result(result, request)
        return current

    async def _run_after_hooks_async(self, request, response):
        current = response
        for hook in self._hooks("after_request_hooks"):
            result = await self._call_hook_async(hook, request, current)
            if result is not None:
                if isinstance(result, Response):
                    current = result
                else:
                    current = self._normalize_result(result, request)
        return current

    def _check_route_auth(self, request):
        if getattr(request.hook, "_route_auth", False) and request.user is None:
            raise Unauthorized("Authentication required")

    def _handler_target(self, handler):
        return getattr(handler, "_route_handler", handler)

    def _handler_call(self, target, request):
        """Build positional/keyword arguments for a route handler."""
        try:
            signature = inspect.signature(target)
        except (TypeError, ValueError):
            return (request.headers, request.text), {}

        parameters = signature.parameters
        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        accepts_varargs = any(
            parameter.kind == inspect.Parameter.VAR_POSITIONAL
            for parameter in parameters.values()
        )
        path_params = getattr(request, "path_params", {}) or {}
        args = []
        kwargs = {}

        def place(name, value):
            parameter = parameters.get(name)
            if (
                parameter is not None
                and parameter.kind == inspect.Parameter.POSITIONAL_ONLY
            ):
                args.append(value)
            else:
                kwargs[name] = value

        def place_path_params():
            for name, value in path_params.items():
                if name in parameters or accepts_kwargs:
                    kwargs[name] = value

        has_request = "request" in parameters or "req" in parameters
        has_headers = "headers" in parameters
        has_body = "body" in parameters

        if has_request:
            request_name = "request" if "request" in parameters else "req"
            place(request_name, request)
            if has_headers:
                place("headers", request.headers)
            if has_body:
                place("body", request.text)
            place_path_params()
            return tuple(args), kwargs

        if has_headers or has_body:
            if has_headers:
                place("headers", request.headers)
            if has_body:
                place("body", request.text)
            place_path_params()
            return tuple(args), kwargs

        path_kwargs = {
            name: value
            for name, value in path_params.items()
            if name in parameters or accepts_kwargs
        }
        if path_kwargs and not accepts_kwargs:
            return (), path_kwargs
        if accepts_kwargs:
            kwargs.update(path_kwargs)
            kwargs.setdefault("headers", request.headers)
            kwargs.setdefault("body", request.text)
            return (), kwargs
        if not parameters:
            return (), {}
        if accepts_varargs:
            return (request.headers, request.text), path_kwargs
        positional = [
            parameter
            for parameter in parameters.values()
            if parameter.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        if len(positional) == 1:
            return (request,), path_kwargs
        return (request.headers, request.text), path_kwargs

    def _call_handler_sync(self, handler, request):
        target = self._handler_target(handler)
        args, kwargs = self._handler_call(target, request)
        result = target(*args, **kwargs)
        if inspect.isawaitable(result):
            result = self._run_awaitable_sync(result)
        return result

    async def _call_handler_async(self, handler, request):
        target = self._handler_target(handler)
        args, kwargs = self._handler_call(target, request)
        result = target(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    def _normalize_result(self, result, request):
        if isinstance(result, Response):
            response = result
            if (
                response.status_code is None
                and not response._body_set
                and not response._content
            ):
                response.set_body(b"")
            response.request = request
            if response.url is None:
                response.url = getattr(request, "target", None) or getattr(
                    request, "path", None
                )
            return response

        if isinstance(result, tuple):
            if len(result) == 2:
                return self._response_from_parts(result[0], result[1], None, request)
            if len(result) == 3:
                return self._response_from_parts(
                    result[0], result[1], result[2], request
                )
            raise TypeError(
                "handler tuple must be (status, body) or (status, body, headers)"
            )

        if result is None:
            response = Response().no_content()
            response.request = request
            response.url = getattr(request, "target", None) or getattr(
                request, "path", None
            )
            return response

        return self._response_from_parts(None, result, None, request)

    def _response_from_parts(self, status, body, headers, request):
        if isinstance(body, Response):
            response = body
            if status is not None:
                response.set_status(status)
            if (
                response.status_code is None
                and not response._body_set
                and not response._content
            ):
                response.set_body(b"")
        else:
            response = Response()
            if status is not None:
                response.set_status(status)
            if body is None:
                if status is None:
                    response.no_content()
                elif int(status) < 400:
                    response.set_body(b"")
            elif isinstance(body, (dict, list)):
                response.json(body)
                if status is not None:
                    response.set_status(status)
            elif isinstance(body, (bytes, bytearray, memoryview)):
                response.set_body(bytes(body))
                if status is not None:
                    response.set_status(status)
            elif isinstance(body, str):
                if body.lstrip().startswith("<"):
                    response.html(body)
                else:
                    response.text(body)
                if status is not None:
                    response.set_status(status)
            elif isinstance(body, (int, float)):
                response.text(str(body))
                if status is not None:
                    response.set_status(status)
            else:
                raise TypeError(
                    f"unsupported handler return type: {type(body).__name__}"
                )

        if headers:
            items = headers.items() if hasattr(headers, "items") else headers
            for name, value in items:
                response.set_header(name, value)

        response.request = request
        if response.url is None:
            response.url = getattr(request, "target", None) or getattr(
                request, "path", None
            )
        return response

    # ------------------------------------------------------------------
    # Body framing
    # ------------------------------------------------------------------
    @staticmethod
    def _expects_continue(headers):
        value = headers.get("Expect", "")
        tokens = {
            token.strip().lower() for token in value.split(",") if token.strip()
        }
        return "100-continue" in tokens

    @staticmethod
    def _body_expected(headers):
        transfer_encoding = headers.get("Transfer-Encoding")
        content_length = headers.get("Content-Length")
        if transfer_encoding is not None:
            return True
        if content_length is None:
            return False
        return content_length.strip() not in ("", "0")

    @staticmethod
    def _transfer_codings(headers):
        value = headers.get("Transfer-Encoding")
        if value is None:
            return None
        codings = [
            coding.strip().lower()
            for coding in value.split(",")
            if coding.strip()
        ]
        if codings != ["chunked"]:
            raise UnsupportedMediaType("Unsupported Transfer-Encoding")
        return codings

    @staticmethod
    def _content_length(headers):
        value = headers.get("Content-Length")
        if value is None:
            return None
        parts = [part.strip() for part in value.split(",")]
        if not parts or any(not re.fullmatch(r"[0-9]+", part) for part in parts):
            raise BadRequest("Invalid Content-Length")
        lengths = {int(part) for part in parts}
        if len(lengths) != 1:
            raise BadRequest("Conflicting Content-Length values")
        length = lengths.pop()
        if length > config.MAX_BODY_BYTES:
            raise PayloadTooLarge("Request body is too large")
        return length

    def _read_body(self, reader, headers):
        transfer_encoding = headers.get("Transfer-Encoding")
        content_length = headers.get("Content-Length")
        if transfer_encoding is not None and content_length is not None:
            raise BadRequest(
                "Content-Length and Transfer-Encoding must not both be present"
            )
        codings = self._transfer_codings(headers)
        if codings is not None:
            return self._read_chunked(reader)
        length = self._content_length(headers)
        if length is not None:
            return reader.read_exact(length)
        return b""

    def _read_chunked(self, reader):
        parts = []
        decoded_length = 0
        while True:
            line, found = reader.read_until(
                b"\r\n",
                config.MAX_CHUNK_LINE_BYTES,
                BadRequest,
            )
            if not found:
                raise BadRequest("Malformed chunked body")
            parts.append(line + b"\r\n")
            size_field = line.split(b";", 1)[0].strip()
            if not size_field:
                raise BadRequest("Missing chunk size")
            if any(char not in _CHUNK_HEX for char in size_field):
                raise BadRequest("Invalid chunk size")
            chunk_size = int(size_field, 16)

            if chunk_size == 0:
                while True:
                    trailer, found = reader.read_until(
                        b"\r\n",
                        config.MAX_HEADER_LINE_BYTES,
                        BadRequest,
                    )
                    if not found:
                        raise BadRequest("Malformed chunked trailer")
                    parts.append(trailer + b"\r\n")
                    if trailer == b"":
                        break
                return b"".join(parts)

            if decoded_length + chunk_size > config.MAX_BODY_BYTES:
                raise PayloadTooLarge("Request body is too large")
            data = reader.read_exact(chunk_size)
            parts.append(data)
            terminator = reader.read_exact(2)
            if terminator != b"\r\n":
                raise BadRequest("Chunk is not terminated by CRLF")
            parts.append(terminator)
            decoded_length += chunk_size

    async def _read_body_async(self, reader, headers):
        transfer_encoding = headers.get("Transfer-Encoding")
        content_length = headers.get("Content-Length")
        if transfer_encoding is not None and content_length is not None:
            raise BadRequest(
                "Content-Length and Transfer-Encoding must not both be present"
            )
        codings = self._transfer_codings(headers)
        if codings is not None:
            return await self._read_chunked_async(reader)
        length = self._content_length(headers)
        if length is not None:
            try:
                return await asyncio.wait_for(
                    reader.readexactly(length),
                    timeout=config.SOCKET_TIMEOUT,
                )
            except asyncio.TimeoutError:
                raise RequestTimeout("Request body timed out")
            except asyncio.IncompleteReadError:
                raise BadRequest("Incomplete HTTP body")
        return b""

    async def _read_chunked_async(self, reader):
        parts = []
        decoded_length = 0
        while True:
            try:
                line = await asyncio.wait_for(
                    reader.readuntil(b"\r\n"),
                    timeout=config.SOCKET_TIMEOUT,
                )
            except asyncio.TimeoutError:
                raise RequestTimeout("Chunked body timed out")
            except asyncio.IncompleteReadError:
                raise BadRequest("Malformed chunked body")
            except asyncio.LimitOverrunError:
                raise BadRequest("Chunk size line is too long")
            if len(line) > config.MAX_CHUNK_LINE_BYTES + 2:
                raise BadRequest("Chunk size line is too long")
            parts.append(line)
            size_field = line[:-2].split(b";", 1)[0].strip()
            if not size_field:
                raise BadRequest("Missing chunk size")
            if any(char not in _CHUNK_HEX for char in size_field):
                raise BadRequest("Invalid chunk size")
            chunk_size = int(size_field, 16)

            if chunk_size == 0:
                while True:
                    try:
                        trailer = await asyncio.wait_for(
                            reader.readuntil(b"\r\n"),
                            timeout=config.SOCKET_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        raise RequestTimeout("Chunked trailer timed out")
                    except asyncio.IncompleteReadError:
                        raise BadRequest("Malformed chunked trailer")
                    except asyncio.LimitOverrunError:
                        raise BadRequest("Chunked trailer line is too long")
                    if len(trailer) > config.MAX_HEADER_LINE_BYTES + 2:
                        raise BadRequest("Chunked trailer line is too long")
                    parts.append(trailer)
                    if trailer == b"\r\n":
                        return b"".join(parts)

            if decoded_length + chunk_size > config.MAX_BODY_BYTES:
                raise PayloadTooLarge("Request body is too large")
            try:
                data = await asyncio.wait_for(
                    reader.readexactly(chunk_size),
                    timeout=config.SOCKET_TIMEOUT,
                )
            except asyncio.TimeoutError:
                raise RequestTimeout("Chunk data timed out")
            except asyncio.IncompleteReadError:
                raise BadRequest("Incomplete chunk data")
            parts.append(data)
            try:
                terminator = await asyncio.wait_for(
                    reader.readexactly(2),
                    timeout=config.SOCKET_TIMEOUT,
                )
            except asyncio.TimeoutError:
                raise RequestTimeout("Chunk terminator timed out")
            except asyncio.IncompleteReadError:
                raise BadRequest("Chunk terminator is missing")
            if terminator != b"\r\n":
                raise BadRequest("Chunk is not terminated by CRLF")
            parts.append(terminator)
            decoded_length += chunk_size

    # ------------------------------------------------------------------
    # Error responses and small helpers
    # ------------------------------------------------------------------
    def _error_response(self, exc):
        if isinstance(exc, HTTPError):
            status = exc.status_code
            reason = exc.reason
            headers = getattr(exc, "headers", {}) or {}
        elif isinstance(exc, (ValueError, KeyError)):
            status = 400
            reason = "Bad Request"
            headers = {}
        elif isinstance(
            exc, (TimeoutError, asyncio.TimeoutError, socket.timeout)
        ):
            status = 408
            reason = "Request Timeout"
            headers = {}
        else:
            status = 500
            reason = "Internal Server Error"
            headers = {}

        response = Response()
        response.set_status(status, reason)
        for name, value in headers.items():
            response.set_header(name, value)
        if status == 401 and "WWW-Authenticate" not in response.headers:
            response.set_header(
                "WWW-Authenticate", 'Basic realm="StreapRous"'
            )
        response.keep_alive = False
        return response

    def _error_payload(self, request, exc):
        try:
            response = self._error_response(exc)
            return response.build_response(request)
        except Exception:
            logger.exception("Failed to build an error response")
            return _FALLBACK_500

    @staticmethod
    def _safe_send(conn, payload):
        try:
            conn.sendall(payload)
        except OSError:
            pass

    @staticmethod
    async def _send_async(writer, payload):
        try:
            writer.write(payload)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass

    # ------------------------------------------------------------------
    # Compatibility helpers
    # ------------------------------------------------------------------
    def extract_cookies(self, request):
        """Return the parsed cookies from a Request."""
        return dict(getattr(request, "cookies", None) or {})

    def build_response(self, req, resp):
        """Compatibility wrapper returning a Response for a request."""
        if isinstance(resp, Response):
            response = resp
        else:
            response = Response(req)
        response.request = req
        if response.url is None:
            response.url = getattr(req, "url", None)
        return response

    def build_json_response(self, req, resp):
        """Compatibility wrapper for a JSON response."""
        if isinstance(resp, Response):
            response = resp
        else:
            response = Response(req)
            if isinstance(resp, (dict, list)):
                response.json(resp)
            else:
                response.set_body(resp)
        response.request = req
        if response.url is None:
            response.url = getattr(req, "url", None)
        return response

    def add_headers(self, request):
        """Hook for subclasses; does nothing by default."""
        pass

    def build_proxy_headers(self, proxy):
        """Returns a dictionary of the headers to add to any request sent
        through a proxy. 

        :class:`HttpAdapter <HttpAdapter>`.

        :param proxy: The url of the proxy being used for this request.
        :rtype: dict
        """
        headers = {}
        #
        # TODO: build your authentication here
        #       username, password =...
        # we provide dummy auth here
        #
        
        username, password = ("user1", "password")

        if username:
            headers["Proxy-Authorization"] = (username, password)
            
        return headers
