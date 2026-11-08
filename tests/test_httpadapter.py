import asyncio
import functools
import json
import logging
import socket
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from daemon import config
from daemon.backend import handle_client_coroutine as backend_async_handler
from daemon.errors import BadRequest, MethodNotAllowed, Unauthorized
from daemon.httpadapter import HttpAdapter, _BufferedReader
from daemon.request import Request
from daemon.response import Response
from daemon.streaprous import StreapRous

logging.getLogger("daemon.httpadapter").disabled = True


def make_request(path="/", method="GET", headers=None, body=b"", routes=None):
    headers = dict(headers or {})
    names = {name.lower() for name in headers}
    if "host" not in names:
        headers["Host"] = "example.test"
    if isinstance(body, str):
        body = body.encode("utf-8")
    if body and "content-length" not in names and "transfer-encoding" not in names:
        headers["Content-Length"] = str(len(body))
    lines = [f"{method} {path} HTTP/1.1"]
    for name, value in headers.items():
        lines.append(f"{name}: {value}")
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1") + body
    return Request().prepare(raw, routes)


def split_response(payload):
    head, _, body = payload.partition(b"\r\n\r\n")
    headers = head.decode("iso-8859-1").split("\r\n")
    return headers, body


def header_values(headers, name):
    values = []
    for line in headers[1:]:
        key, separator, value = line.partition(":")
        if separator and key.strip().lower() == name.lower():
            values.append(value.strip())
    return values


def header_value(headers, name):
    values = header_values(headers, name)
    return values[0] if values else None


def status_of(headers):
    return headers[0]


def parse_responses(payload):
    responses = []
    remaining = payload
    while remaining:
        head, separator, rest = remaining.partition(b"\r\n\r\n")
        if not separator:
            break
        headers = head.decode("iso-8859-1").split("\r\n")
        length = 0
        for line in headers[1:]:
            key, separator, value = line.partition(":")
            if separator and key.strip().lower() == "content-length":
                try:
                    length = int(value.strip())
                except ValueError:
                    length = 0
        body = rest[:length]
        remaining = rest[length:]
        responses.append((headers, body))
    return responses


def call_adapter(routes, raw, timeout=3.0):
    server, client = socket.socketpair()
    adapter = HttpAdapter(
        "127.0.0.1", 9000, server, ("127.0.0.1", 1), routes
    )
    thread = threading.Thread(
        target=adapter.handle_client,
        args=(server, ("127.0.0.1", 1), routes),
        daemon=True,
    )
    thread.start()
    client.sendall(raw)
    try:
        client.shutdown(socket.SHUT_WR)
    except OSError:
        pass
    data = b""
    client.settimeout(timeout)
    while True:
        try:
            chunk = client.recv(65536)
        except TimeoutError:
            break
        if not chunk:
            break
        data += chunk
    thread.join(timeout=timeout)
    client.close()
    return data


def make_reader(*chunks):
    remaining = list(chunks)

    def recv():
        if remaining:
            return remaining.pop(0)
        return b""

    return _BufferedReader(recv)


class DispatchTests(unittest.TestCase):
    def setUp(self):
        self.adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})

    def _payload(self, result, path="/x", method="GET", body=b""):
        handler = lambda *args, **kwargs: result
        routes = {(method, path): handler}
        request = make_request(path, method, body=body, routes=routes)
        response = self.adapter.dispatch(request)
        return response, response.build_response(request)

    def test_static_dispatch(self):
        request = make_request("/", routes={})
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 200 OK")
        self.assertIn(b"bksysnet@hcmut", body)

    def test_dict_result_is_json(self):
        _response, payload = self._payload({"ok": True})
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "application/json; charset=utf-8",
        )
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def test_list_result_is_json(self):
        _response, payload = self._payload([1, 2, 3])
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "application/json; charset=utf-8",
        )
        self.assertEqual(json.loads(body.decode("utf-8")), [1, 2, 3])

    def test_bytes_result_keeps_body_and_sniffs_json(self):
        _response, payload = self._payload(b'{"ok": true}')
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "application/json; charset=utf-8",
        )
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def test_html_string_result(self):
        _response, payload = self._payload("<h1>Hi</h1>")
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "text/html; charset=utf-8",
        )
        self.assertEqual(body, b"<h1>Hi</h1>")

    def test_plain_string_result(self):
        _response, payload = self._payload("hello")
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "text/plain; charset=utf-8",
        )
        self.assertEqual(body, b"hello")

    def test_none_result_is_204(self):
        _response, payload = self._payload(None)
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 204 No Content")
        self.assertEqual(body, b"")
        self.assertIsNone(header_value(headers, "Content-Length"))

    def test_two_tuple_result(self):
        _response, payload = self._payload((201, {"id": 1}))
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 201 Created")
        self.assertEqual(json.loads(body.decode("utf-8")), {"id": 1})

    def test_three_tuple_result_headers(self):
        _response, payload = self._payload((200, "ok", {"X-Test": "yes"}))
        headers, body = split_response(payload)
        self.assertEqual(header_value(headers, "X-Test"), "yes")
        self.assertEqual(body, b"ok")

    def test_response_result(self):
        handler = lambda request: Response().json({"ok": True})
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        self.assertIn(b'{"ok": true}', payload)

    def test_int_result_is_text(self):
        _response, payload = self._payload(7)
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "text/plain; charset=utf-8",
        )
        self.assertEqual(body, b"7")

    def test_method_not_allowed(self):
        routes = {("POST", "/x"): lambda request: "x"}
        request = make_request("/x", routes=routes)
        with self.assertRaises(MethodNotAllowed) as context:
            self.adapter.dispatch(request)
        self.assertEqual(context.exception.allowed, ["POST"])

    def test_auth_required(self):
        handler = lambda request: {"ok": True}
        handler._route_auth = True
        routes = {("GET", "/secure"): handler}
        request = make_request("/secure", routes=routes)
        with self.assertRaises(Unauthorized):
            self.adapter.dispatch(request)

    def test_auth_with_user_passes(self):
        handler = lambda request: {"ok": True}
        handler._route_auth = True
        routes = {("GET", "/secure"): handler}
        request = make_request("/secure", routes=routes)
        request.user = object()
        response = self.adapter.dispatch(request)
        self.assertIn(b'{"ok": true}', response.build_response(request))

    def test_legacy_headers_body_handler(self):
        handler = lambda headers, body: body
        routes = {("POST", "/echo"): handler}
        request = make_request("/echo", "POST", body=b"hello", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        _headers, body = split_response(payload)
        self.assertEqual(body, b"hello")

    def test_request_handler(self):
        handler = lambda request: {"path": request.path}
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"path": "/x"})

    def test_path_parameters(self):
        handler = lambda request, user_id: {"id": user_id}
        routes = {("GET", "/users/<int:user_id>"): handler}
        request = make_request("/users/42", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"id": 42})

    def test_kwargs_handler(self):
        handler = lambda **kwargs: {"keys": sorted(kwargs)}
        routes = {("POST", "/users/<int:user_id>"): handler}
        request = make_request(
            "/users/42", "POST", body=b"x", routes=routes
        )
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        _headers, body = split_response(payload)
        self.assertEqual(
            json.loads(body.decode("utf-8")),
            {"keys": ["body", "headers", "user_id"]},
        )

    def test_async_legacy_handler(self):
        async def handler(headers, body):
            return {"async": True}

        routes = {("GET", "/async"): handler}
        request = make_request("/async", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"async": True})

    def test_async_request_handler(self):
        async def handler(request):
            return {"method": request.method}

        routes = {("GET", "/async"): handler}
        request = make_request("/async", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"method": "GET"})

    def test_handler_bad_request_is_400(self):
        handler = lambda request: (_ for _ in ()).throw(BadRequest("bad"))
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        payload = self.adapter._error_payload(request, BadRequest("bad"))
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 400 Bad Request")

    def test_handler_value_error_is_400(self):
        payload = self.adapter._error_payload(make_request("/x"), ValueError("bad"))
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 400 Bad Request")

    def test_handler_key_error_is_400(self):
        payload = self.adapter._error_payload(make_request("/x"), KeyError("bad"))
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 400 Bad Request")

    def test_handler_runtime_error_is_500(self):
        payload = self.adapter._error_payload(
            make_request("/x"), RuntimeError("boom")
        )
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 500 Internal Server Error")
        self.assertNotIn(b"boom", body)

    def test_error_payload_405_has_allow(self):
        request = make_request("/x", routes={("POST", "/x"): lambda r: "x"})
        payload = self.adapter._error_payload(
            request, MethodNotAllowed(allowed=["POST"])
        )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 405 Method Not Allowed")
        self.assertEqual(header_value(headers, "Allow"), "POST")

    def test_error_payload_401_has_challenge(self):
        request = make_request("/secure")
        payload = self.adapter._error_payload(
            request, Unauthorized("auth required")
        )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 401 Unauthorized")
        self.assertIn("StreapRous", header_value(headers, "WWW-Authenticate"))

    def test_two_tuple_404_none_builds_error_page(self):
        handler = lambda request: (404, None)
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 404 Not Found")
        self.assertIn(b"404 Not Found", body)

    def test_two_tuple_201_none_is_empty_body(self):
        handler = lambda request: (201, None)
        routes = {("POST", "/x"): handler}
        request = make_request("/x", "POST", body=b"", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 201 Created")
        self.assertEqual(body, b"")
        self.assertEqual(header_value(headers, "Content-Length"), "0")

    def test_two_tuple_204_none_has_no_entity_headers(self):
        handler = lambda request: (204, None)
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        response = self.adapter.dispatch(request)
        payload = response.build_response(request)
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 204 No Content")
        self.assertEqual(body, b"")
        self.assertIsNone(header_value(headers, "Content-Length"))

    def test_bad_tuple_length_is_500(self):
        handler = lambda request: (200,)
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        with self.assertRaises(TypeError):
            self.adapter.dispatch(request)

    def test_unsupported_result_type_is_500(self):
        handler = lambda request: object()
        routes = {("GET", "/x"): handler}
        request = make_request("/x", routes=routes)
        payload = self.adapter._error_payload(
            request, RuntimeError("unsupported")
        )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 500 Internal Server Error")


class SocketTransportTests(unittest.TestCase):
    def test_static_get(self):
        payload = call_adapter({}, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 200 OK")
        self.assertIn(b"bksysnet@hcmut", body)

    def test_route_json(self):
        routes = {("GET", "/hello"): lambda request: {"ok": True}}
        payload = call_adapter(routes, b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        headers, body = split_response(payload)
        self.assertEqual(
            header_value(headers, "Content-Type"),
            "application/json; charset=utf-8",
        )
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def test_legacy_echo(self):
        routes = {("POST", "/echo"): lambda headers, body: body}
        payload = call_adapter(
            routes,
            b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello",
        )
        _headers, body = split_response(payload)
        self.assertEqual(body, b"hello")

    def test_async_route(self):
        async def handler(headers, body):
            return {"async": True}

        routes = {("GET", "/async"): handler}
        payload = call_adapter(routes, b"GET /async HTTP/1.1\r\nHost: x\r\n\r\n")
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"async": True})

    def test_path_parameter_route(self):
        routes = {
            ("GET", "/users/<int:user_id>"): lambda request, user_id: {
                "id": user_id
            }
        }
        payload = call_adapter(
            routes, b"GET /users/42 HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"id": 42})

    def test_chunked_request(self):
        routes = {("POST", "/echo"): lambda headers, body: body}
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: x\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"\r\n"
            b"5\r\nhello\r\n"
            b"6\r\n world\r\n"
            b"0\r\n\r\n"
        )
        payload = call_adapter(routes, raw)
        _headers, body = split_response(payload)
        self.assertEqual(body, b"hello world")

    def test_keep_alive_two_requests(self):
        routes = {("GET", "/hello"): lambda request: {"ok": True}}
        raw = (
            b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"
            b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
        )
        payload = call_adapter(routes, raw)
        responses = parse_responses(payload)
        self.assertEqual(len(responses), 2)
        self.assertEqual(status_of(responses[0][0]), "HTTP/1.1 200 OK")
        self.assertEqual(status_of(responses[1][0]), "HTTP/1.1 200 OK")

    def test_expect_100_continue(self):
        routes = {("POST", "/echo"): lambda headers, body: body}
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: x\r\n"
            b"Expect: 100-continue\r\n"
            b"Content-Length: 5\r\n"
            b"\r\n"
            b"hello"
        )
        payload = call_adapter(routes, raw)
        responses = parse_responses(payload)
        self.assertEqual(len(responses), 2)
        self.assertEqual(status_of(responses[0][0]), "HTTP/1.1 100 Continue")
        self.assertEqual(responses[1][1], b"hello")

    def test_method_not_allowed(self):
        routes = {("POST", "/hello"): lambda request: "ok"}
        payload = call_adapter(
            routes, b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 405 Method Not Allowed")
        self.assertEqual(header_value(headers, "Allow"), "POST")

    def test_auth_required(self):
        handler = lambda request: {"ok": True}
        handler._route_auth = True
        routes = {("GET", "/secure"): handler}
        payload = call_adapter(
            routes, b"GET /secure HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 401 Unauthorized")
        self.assertIn("StreapRous", header_value(headers, "WWW-Authenticate"))

    def test_handler_error_is_500(self):
        def handler(request):
            raise RuntimeError("boom")

        routes = {("GET", "/boom"): handler}
        payload = call_adapter(
            routes, b"GET /boom HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 500 Internal Server Error")
        self.assertNotIn(b"RuntimeError", body)
        self.assertNotIn(b"Traceback", body)

    def test_malformed_request_is_400(self):
        payload = call_adapter({}, b"GET\r\n\r\n")
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 400 Bad Request")

    def test_missing_host_is_400(self):
        payload = call_adapter({}, b"GET / HTTP/1.1\r\n\r\n")
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 400 Bad Request")

    def test_payload_too_large(self):
        with patch.object(config, "MAX_BODY_BYTES", 4):
            payload = call_adapter(
                {},
                b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello",
            )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 413 Payload Too Large")

    def test_header_too_large(self):
        with patch.object(config, "MAX_HEADER_BYTES", 32):
            payload = call_adapter(
                {},
                (
                    b"GET / HTTP/1.1\r\nHost: x\r\n"
                    b"X-Long: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n"
                ),
            )
        headers, _body = split_response(payload)
        self.assertEqual(
            status_of(headers),
            "HTTP/1.1 431 Request Header Fields Too Large",
        )

    def test_uri_too_long(self):
        with patch.object(config, "MAX_URI_BYTES", 8):
            payload = call_adapter(
                {},
                b"GET /123456789 HTTP/1.1\r\nHost: x\r\n\r\n",
            )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 414 URI Too Long")

    def test_unsupported_content_encoding(self):
        payload = call_adapter(
            {},
            (
                b"POST /x HTTP/1.1\r\nHost: x\r\n"
                b"Content-Encoding: gzip\r\nContent-Length: 0\r\n\r\n"
            ),
        )
        headers, _body = split_response(payload)
        self.assertEqual(
            status_of(headers), "HTTP/1.1 415 Unsupported Media Type"
        )

    def test_head_static_has_no_body(self):
        payload = call_adapter(
            {}, b"HEAD /static/css/styles.css HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 200 OK")
        self.assertEqual(body, b"")
        self.assertGreater(int(header_value(headers, "Content-Length")), 0)

    def test_missing_static_is_404(self):
        payload = call_adapter(
            {}, b"GET /static/no-such.css HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 404 Not Found")
        self.assertIn(b"404 Not Found", body)

    def test_keepalive_limit(self):
        with patch.object(config, "MAX_KEEPALIVE_REQUESTS", 1):
            payload = call_adapter(
                {("GET", "/hello"): lambda request: "ok"},
                b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"
                b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n",
            )
        responses = parse_responses(payload)
        self.assertEqual(len(responses), 1)

    def test_connection_close(self):
        payload = call_adapter(
            {("GET", "/hello"): lambda request: "ok"},
            b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        headers, _body = split_response(payload)
        self.assertEqual(header_value(headers, "Connection"), "close")


class FramingTests(unittest.TestCase):
    def setUp(self):
        self.adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})

    def test_read_content_length(self):
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhe",
            b"llo",
        )
        request = self.adapter.read_request(reader)
        self.assertEqual(request.body, b"hello")

    def test_read_no_body(self):
        reader = make_reader(b"GET /x HTTP/1.1\r\nHost: x\r\n\r\n")
        request = self.adapter.read_request(reader)
        self.assertEqual(request.body, b"")

    def test_read_chunked(self):
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5\r\nhello\r\n",
            b"0\r\n\r\n",
        )
        request = self.adapter.read_request(reader)
        self.assertEqual(request.body, b"hello")
        self.assertTrue(request.chunked)

    def test_read_chunk_extensions_and_trailers(self):
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5;foo=bar\r\nhello\r\n"
            b"0\r\nX-Trailer: yes\r\n\r\n"
        )
        request = self.adapter.read_request(reader)
        self.assertEqual(request.body, b"hello")

    def test_read_returns_none_on_clean_eof(self):
        reader = make_reader(b"")
        self.assertIsNone(self.adapter.read_request(reader))

    def test_incomplete_header_is_bad_request(self):
        reader = make_reader(b"GET /x HTTP/1.1\r\nHost: x")
        with self.assertRaises(BadRequest):
            self.adapter.read_request(reader)

    def test_header_limit(self):
        with patch.object(config, "MAX_HEADER_BYTES", 10):
            reader = make_reader(b"GET /x HTTP/1.1\r\nHost: x\r\n\r\n")
            with self.assertRaises(Exception):
                self.adapter.read_request(reader)

    def test_content_length_and_transfer_encoding_conflict(self):
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
        )
        with self.assertRaises(BadRequest):
            self.adapter.read_request(reader)

    def test_invalid_content_length(self):
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n"
        )
        with self.assertRaises(BadRequest):
            self.adapter.read_request(reader)

    def test_body_too_large(self):
        with patch.object(config, "MAX_BODY_BYTES", 4):
            reader = make_reader(
                b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"
            )
            with self.assertRaises(Exception):
                self.adapter.read_request(reader)

    def test_invalid_chunk_size(self):
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"Z\r\nhello\r\n0\r\n\r\n"
        )
        with self.assertRaises(BadRequest):
            self.adapter.read_request(reader)

    def test_chunked_too_large(self):
        with patch.object(config, "MAX_BODY_BYTES", 4):
            reader = make_reader(
                b"POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
                b"5\r\nhello\r\n0\r\n\r\n"
            )
            with self.assertRaises(Exception):
                self.adapter.read_request(reader)

    def test_expect_continue_calls_sender(self):
        calls = []
        reader = make_reader(
            b"POST /x HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n"
            b"Content-Length: 5\r\n\r\nhello"
        )
        request = self.adapter.read_request(reader, lambda: calls.append(True))
        self.assertEqual(calls, [True])
        self.assertEqual(request.body, b"hello")

    def test_raw_reader_until_limit(self):
        reader = make_reader(b"a" * 20)
        with self.assertRaises(Exception):
            reader.read_until(b"\r\n", 5)


class RawTransportTests(unittest.TestCase):
    def setUp(self):
        self.conn_info = SimpleNamespace(
            client_ip="127.0.0.1",
            client_port=1234,
            server_ip="127.0.0.1",
            server_port=9000,
        )

    def test_raw_route_response(self):
        sent = []
        closed = []
        raw = b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"
        chunks = [raw]

        def recv(_socket, _info):
            return chunks.pop(0) if chunks else b""

        def send(_socket, _info, payload):
            sent.append(payload)
            return len(payload)

        routes = {("GET", "/hello"): lambda request: {"ok": True}}
        adapter = HttpAdapter("127.0.0.1", 9000, None, None, routes)
        with patch("daemon.httpadapter.run_conn_recv", recv), patch(
            "daemon.httpadapter.run_conn_send", send
        ), patch(
            "daemon.httpadapter.close_connection",
            lambda _socket, _info: closed.append(True),
        ):
            adapter.handle_client_raw(object(), self.conn_info, routes)
        self.assertTrue(closed)
        self.assertTrue(sent)
        headers, body = split_response(sent[-1])
        self.assertEqual(status_of(headers), "HTTP/1.1 200 OK")
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def test_raw_malformed_request_sends_400(self):
        sent = []
        chunks = [b"GET\r\n\r\n"]

        def recv(_socket, _info):
            return chunks.pop(0) if chunks else b""

        def send(_socket, _info, payload):
            sent.append(payload)
            return len(payload)

        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        with patch("daemon.httpadapter.run_conn_recv", recv), patch(
            "daemon.httpadapter.run_conn_send", send
        ), patch("daemon.httpadapter.close_connection", lambda *_: None):
            adapter.handle_client_raw(object(), self.conn_info, {})
        headers, _body = split_response(sent[-1])
        self.assertEqual(status_of(headers), "HTTP/1.1 400 Bad Request")

    def test_raw_no_data_closes(self):
        sent = []
        closed = []

        def recv(_socket, _info):
            return b""

        def send(_socket, _info, payload):
            sent.append(payload)
            return len(payload)

        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        with patch("daemon.httpadapter.run_conn_recv", recv), patch(
            "daemon.httpadapter.run_conn_send", send
        ), patch(
            "daemon.httpadapter.close_connection",
            lambda *_: closed.append(True),
        ):
            adapter.handle_client_raw(object(), self.conn_info, {})
        self.assertEqual(sent, [])
        self.assertTrue(closed)


class AsyncTransportTests(unittest.TestCase):
    def _call(self, routes, raw):
        async def run():
            adapter = HttpAdapter(None, None, None, None, routes)
            server = await asyncio.start_server(
                lambda reader, writer: adapter.handle_client_coroutine(
                    reader, writer
                ),
                "127.0.0.1",
                0,
            )
            port = server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(raw)
            await writer.drain()
            writer.write_eof()
            try:
                data = await asyncio.wait_for(reader.read(), timeout=3)
            finally:
                writer.close()
                await writer.wait_closed()
                server.close()
                await server.wait_closed()
            return data

        return asyncio.run(run())

    def test_async_static(self):
        payload = self._call({}, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 200 OK")
        self.assertIn(b"bksysnet@hcmut", body)

    def test_async_route(self):
        routes = {("GET", "/hello"): lambda request: {"ok": True}}
        payload = self._call(routes, b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def test_async_handler(self):
        async def handler(request):
            return {"async": True}

        routes = {("GET", "/async"): handler}
        payload = self._call(routes, b"GET /async HTTP/1.1\r\nHost: x\r\n\r\n")
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"async": True})

    def test_async_keep_alive(self):
        routes = {("GET", "/hello"): lambda request: "ok"}
        raw = (
            b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"
            b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
        )
        payload = self._call(routes, raw)
        responses = parse_responses(payload)
        self.assertEqual(len(responses), 2)

    def test_async_405(self):
        routes = {("POST", "/hello"): lambda request: "ok"}
        payload = self._call(routes, b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 405 Method Not Allowed")

    def test_async_500(self):
        def handler(request):
            raise RuntimeError("boom")

        routes = {("GET", "/boom"): handler}
        payload = self._call(routes, b"GET /boom HTTP/1.1\r\nHost: x\r\n\r\n")
        headers, body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 500 Internal Server Error")
        self.assertNotIn(b"RuntimeError", body)
        self.assertNotIn(b"Traceback", body)

    def test_backend_async_handler_with_routes(self):
        routes = {("GET", "/hello"): lambda request: {"ok": True}}

        async def run():
            server = await asyncio.start_server(
                functools.partial(backend_async_handler, routes=routes),
                "127.0.0.1",
                0,
            )
            port = server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            writer.write_eof()
            try:
                data = await asyncio.wait_for(reader.read(), timeout=3)
            finally:
                writer.close()
                await writer.wait_closed()
                server.close()
                await server.wait_closed()
            return data

        payload = asyncio.run(run())
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})


class RouterIntegrationTests(unittest.TestCase):
    def test_streaprous_route_metadata(self):
        app = StreapRous()

        @app.route("/hello")
        def hello(request):
            return {"ok": True}

        payload = call_adapter(
            app.routes, b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"ok": True})

    def test_streaprous_auth_route(self):
        app = StreapRous()

        @app.route("/secure", auth=True)
        def secure(request):
            return {"ok": True}

        payload = call_adapter(
            app.routes, b"GET /secure HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        headers, _body = split_response(payload)
        self.assertEqual(status_of(headers), "HTTP/1.1 401 Unauthorized")

    def test_streaprous_async_route(self):
        app = StreapRous()

        @app.route("/async")
        async def async_route(request):
            return {"async": True}

        payload = call_adapter(
            app.routes, b"GET /async HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"async": True})

    def test_streaprous_path_parameter(self):
        app = StreapRous()

        @app.route("/users/<int:user_id>")
        def user(request, user_id):
            return {"id": user_id}

        payload = call_adapter(
            app.routes, b"GET /users/42 HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        _headers, body = split_response(payload)
        self.assertEqual(json.loads(body.decode("utf-8")), {"id": 42})


class CompatibilityTests(unittest.TestCase):
    def test_extract_cookies(self):
        request = make_request("/x", headers={"Cookie": "a=b; c=d"})
        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        self.assertEqual(adapter.extract_cookies(request), {"a": "b", "c": "d"})

    def test_build_response_wrapper(self):
        request = make_request("/x")
        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        response = adapter.build_response(request, Response())
        self.assertIs(response.request, request)

    def test_build_json_response_wrapper(self):
        request = make_request("/x")
        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        response = adapter.build_json_response(request, {"ok": True})
        self.assertIn(b'{"ok": true}', response.build_response(request))

    def test_add_headers_is_noop(self):
        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        self.assertIsNone(adapter.add_headers(make_request("/x")))

    def test_build_proxy_headers(self):
        adapter = HttpAdapter("127.0.0.1", 9000, None, None, {})
        headers = adapter.build_proxy_headers("http://proxy")
        self.assertIn("Proxy-Authorization", headers)


if __name__ == "__main__":
    unittest.main()
