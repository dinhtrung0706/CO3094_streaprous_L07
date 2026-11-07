import base64
import json
import unittest
from unittest.mock import patch

from daemon import config
from daemon.errors import (
    BadRequest,
    PayloadTooLarge,
    RequestHeaderFieldsTooLarge,
    UnsupportedMediaType,
    URITooLong,
)
from daemon.request import Request


def build_request(
    method="GET",
    target="/",
    version="HTTP/1.1",
    headers=None,
    body=b"",
    host="example.test",
):
    """Build a complete HTTP message for parser tests."""
    headers = dict(headers or {})
    names = {name.lower() for name in headers}
    if version == "HTTP/1.1" and host is not None and "host" not in names:
        headers["Host"] = host
    if isinstance(body, str):
        body = body.encode("utf-8")
    if body and "content-length" not in names and "transfer-encoding" not in names:
        headers["Content-Length"] = str(len(body))
    head = f"{method} {target} {version}\r\n"
    for name, value in headers.items():
        head += f"{name}: {value}\r\n"
    head += "\r\n"
    return head.encode("iso-8859-1") + body


def parse(*args, routes=None, **kwargs):
    return Request().prepare(build_request(*args, **kwargs), routes)


class RequestLineTests(unittest.TestCase):
    def test_basic_get(self):
        request = parse("GET", "/hello?x=1&x=2&blank=")
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.target, "/hello?x=1&x=2&blank=")
        self.assertEqual(request.url, request.target)
        self.assertEqual(request.path, "/hello")
        self.assertEqual(request.query_string, "x=1&x=2&blank=")
        self.assertEqual(
            request.query, {"x": ["1", "2"], "blank": [""]}
        )
        self.assertEqual(request.version, "HTTP/1.1")
        self.assertTrue(request.keep_alive)
        self.assertEqual(len(request.request_id), 12)

    def test_method_is_uppercased(self):
        request = parse("post", "/echo", body=b"hi")
        self.assertEqual(request.method, "POST")

    def test_each_parse_gets_a_new_request_id_and_clears_user(self):
        request = Request()
        first = request.prepare(build_request("GET", "/x")).request_id
        request.user = object()
        second = request.prepare(build_request("GET", "/x")).request_id
        self.assertNotEqual(first, second)
        self.assertIsNone(request.user)

    def test_root_maps_to_index_when_unrouted(self):
        request = parse("GET", "/")
        self.assertEqual(request.path, "/index.html")
        self.assertIsNone(request.hook)

    def test_http10_defaults_to_close(self):
        request = parse("GET", "/", version="HTTP/1.0", host=None)
        self.assertFalse(request.keep_alive)
        self.assertEqual(request.path, "/index.html")

    def test_http10_keep_alive(self):
        request = parse(
            "GET", "/", version="HTTP/1.0", host=None,
            headers={"Connection": "keep-alive"},
        )
        self.assertTrue(request.keep_alive)

    def test_http11_close(self):
        request = parse("GET", "/", headers={"Connection": "close"})
        self.assertFalse(request.keep_alive)

    def test_http10_close_wins_over_keep_alive(self):
        request = parse(
            "GET", "/", version="HTTP/1.0", host=None,
            headers={"Connection": "keep-alive, close"},
        )
        self.assertFalse(request.keep_alive)

    def test_http11_requires_host(self):
        with self.assertRaises(BadRequest):
            Request().prepare(b"GET / HTTP/1.1\r\n\r\n")

    def test_options_star(self):
        request = parse("OPTIONS", "*")
        self.assertEqual(request.method, "OPTIONS")
        self.assertEqual(request.target, "*")
        self.assertEqual(request.path, "*")

    def test_star_rejected_for_get(self):
        with self.assertRaises(BadRequest):
            parse("GET", "*")

    def test_extract_request_line_does_not_map_root(self):
        self.assertEqual(
            Request().extract_request_line("GET / HTTP/1.1"),
            ("GET", "/", "HTTP/1.1"),
        )

    def test_malformed_request_lines(self):
        cases = [
            b"",
            b"GET / HTTP/1.1",
            b"GET / HTTP/2.0\r\nHost: x\r\n\r\n",
            b"GET  / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"GET /\r\nHost: x\r\n\r\n",
            b"G@T / HTTP/1.1\r\nHost: x\r\n\r\n",
            b"GET http://example.test/ HTTP/1.1\r\nHost: x\r\n\r\n",
            b"GET /a\x00b HTTP/1.1\r\nHost: x\r\n\r\n",
            b"GET /a%00b HTTP/1.1\r\nHost: x\r\n\r\n",
            b"GET\t/ HTTP/1.1\r\nHost: x\r\n\r\n",
        ]
        for raw in cases:
            with self.subTest(raw=raw), self.assertRaises(BadRequest):
                Request().prepare(raw)

    def test_uri_too_long(self):
        with patch.object(config, "MAX_URI_BYTES", 8), self.assertRaises(URITooLong):
                parse("GET", "/123456789")


class HeaderTests(unittest.TestCase):
    def test_case_insensitive_lookup_and_display(self):
        request = parse(
            "GET", "/x", headers={"X-Custom": "value", "Accept": "*/*"}
        )
        self.assertEqual(request.headers["x-custom"], "value")
        self.assertIn("X-Custom", request.headers)
        self.assertIn("x-custom", request.headers)

    def test_duplicate_headers_are_joined(self):
        raw = (
            b"GET /x HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"X-Test: one\r\n"
            b"X-Test: two\r\n"
            b"Cookie: a=1\r\n"
            b"Cookie: b=2\r\n"
            b"\r\n"
        )
        request = Request().prepare(raw)
        self.assertEqual(request.headers["X-Test"], "one, two")
        self.assertEqual(request.headers["Cookie"], "a=1; b=2")
        self.assertEqual(request.cookies, {"a": "1", "b": "2"})

    def test_obsolete_line_folding_is_rejected(self):
        raw = (
            b"GET /x HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"X-Test: one\r\n"
            b" continued\r\n"
            b"\r\n"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_whitespace_before_colon_is_rejected(self):
        raw = (
            b"GET /x HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"X-Test : one\r\n"
            b"\r\n"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_control_characters_in_value_are_rejected(self):
        raw = (
            b"GET /x HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"X-Test: a\x00b\r\n"
            b"\r\n"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_invalid_header_name_is_rejected(self):
        raw = (
            b"GET /x HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Bad Header: value\r\n"
            b"\r\n"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_too_many_headers(self):
        with patch.object(config, "MAX_HEADERS", 2), self.assertRaises(RequestHeaderFieldsTooLarge):
                parse("GET", "/x", headers={"X-A": "1", "X-B": "2"})

    def test_duplicate_headers_count_toward_limit(self):
        raw = (
            b"GET /x HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"X-Test: one\r\n"
            b"X-Test: two\r\n"
            b"\r\n"
        )
        with patch.object(config, "MAX_HEADERS", 2), self.assertRaises(RequestHeaderFieldsTooLarge):
                Request().prepare(raw)

    def test_header_line_too_long(self):
        with patch.object(config, "MAX_HEADER_LINE_BYTES", 8), self.assertRaises(RequestHeaderFieldsTooLarge):
                parse("GET", "/x", headers={"X-Test": "123456789"})

    def test_header_block_too_large(self):
        with patch.object(config, "MAX_HEADER_BYTES", 32), self.assertRaises(RequestHeaderFieldsTooLarge):
                parse("GET", "/x", headers={"X-Long": "a" * 50})

    def test_prepare_headers_accepts_header_only_text(self):
        headers = Request().prepare_headers("A: 1\r\nB: 2\r\n")
        self.assertEqual(headers["a"], "1")
        self.assertEqual(headers["b"], "2")

    def test_prepare_headers_skips_request_line(self):
        headers = Request().prepare_headers(
            "GET /x HTTP/1.1\r\nHost: example.test\r\n"
        )
        self.assertEqual(headers["Host"], "example.test")
        self.assertNotIn("GET /x HTTP/1.1", list(headers))


class BodyTests(unittest.TestCase):
    def test_content_length_body(self):
        request = parse("POST", "/echo", body=b"hello")
        self.assertEqual(request.body, b"hello")
        self.assertEqual(request.text, "hello")
        self.assertEqual(request.content_length, 5)
        self.assertFalse(request.chunked)
        self.assertEqual(request.remaining, b"")

    def test_zero_content_length(self):
        request = parse(
            "POST", "/echo",
            headers={"Content-Length": "0"},
            body=b"",
        )
        self.assertEqual(request.body, b"")
        self.assertEqual(request.content_length, 0)

    def test_extra_bytes_after_content_length_are_remaining(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Length: 5\r\n"
            b"\r\n"
            b"helloEXTRA"
        )
        request = Request().prepare(raw)
        self.assertEqual(request.body, b"hello")
        self.assertEqual(request.remaining, b"EXTRA")

    def test_conflicting_content_lengths(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Length: 5, 6\r\n"
            b"\r\n"
            b"hello"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_duplicate_equal_content_lengths(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Length: 5\r\n"
            b"Content-Length: 5\r\n"
            b"\r\n"
            b"hello"
        )
        request = Request().prepare(raw)
        self.assertEqual(request.body, b"hello")

    def test_invalid_content_length(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Length: abc\r\n"
            b"\r\n"
            b"hello"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_incomplete_body(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Length: 5\r\n"
            b"\r\n"
            b"hi"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_payload_too_large(self):
        with patch.object(config, "MAX_BODY_BYTES", 4), self.assertRaises(PayloadTooLarge):
                parse("POST", "/echo", body=b"hello")

    def test_chunked_body(self):
        body = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
        request = parse(
            "POST", "/echo",
            headers={"Transfer-Encoding": "chunked"},
            body=body,
        )
        self.assertTrue(request.chunked)
        self.assertEqual(request.body, b"hello world")
        self.assertEqual(request.content_length, 11)

    def test_chunk_extensions(self):
        body = b"5;foo=bar\r\nhello\r\n0\r\n\r\n"
        request = parse(
            "POST", "/echo",
            headers={"Transfer-Encoding": "chunked"},
            body=body,
        )
        self.assertEqual(request.body, b"hello")

    def test_chunk_trailers(self):
        body = b"5\r\nhello\r\n0\r\nX-Trailer: yes\r\n\r\n"
        request = parse(
            "POST", "/echo",
            headers={"Transfer-Encoding": "chunked"},
            body=body,
        )
        self.assertEqual(request.body, b"hello")
        self.assertEqual(request.remaining, b"")

    def test_chunked_remaining(self):
        body = b"0\r\n\r\nNEXT"
        request = parse(
            "POST", "/echo",
            headers={"Transfer-Encoding": "chunked"},
            body=body,
        )
        self.assertEqual(request.body, b"")
        self.assertEqual(request.remaining, b"NEXT")

    def test_malformed_chunk_size(self):
        body = b"Z\r\nhello\r\n0\r\n\r\n"
        with self.assertRaises(BadRequest):
            parse(
                "POST", "/echo",
                headers={"Transfer-Encoding": "chunked"},
                body=body,
            )

    def test_chunk_size_must_be_hex(self):
        for size in (b"+5", b"0x5", b"-1"):
            with self.subTest(size=size), self.assertRaises(BadRequest):
                    parse(
                        "POST", "/echo",
                        headers={"Transfer-Encoding": "chunked"},
                        body=size + b"\r\nhello\r\n0\r\n\r\n",
                    )

    def test_incomplete_chunk_data(self):
        body = b"5\r\nhel"
        with self.assertRaises(BadRequest):
            parse(
                "POST", "/echo",
                headers={"Transfer-Encoding": "chunked"},
                body=body,
            )

    def test_chunk_missing_crlf(self):
        body = b"1\r\naX\r\n0\r\n\r\n"
        with self.assertRaises(BadRequest):
            parse(
                "POST", "/echo",
                headers={"Transfer-Encoding": "chunked"},
                body=body,
            )

    def test_chunked_too_large(self):
        with patch.object(config, "MAX_BODY_BYTES", 4), self.assertRaises(PayloadTooLarge):
                parse(
                    "POST", "/echo",
                    headers={"Transfer-Encoding": "chunked"},
                    body=b"5\r\nhello\r\n0\r\n\r\n",
                )

    def test_content_length_and_transfer_encoding_conflict(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Length: 5\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"\r\n"
            b"5\r\nhello\r\n0\r\n\r\n"
        )
        with self.assertRaises(BadRequest):
            Request().prepare(raw)

    def test_unsupported_transfer_encoding(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Transfer-Encoding: gzip\r\n"
            b"\r\n"
        )
        with self.assertRaises(UnsupportedMediaType):
            Request().prepare(raw)

    def test_unsupported_content_encoding(self):
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: example.test\r\n"
            b"Content-Encoding: gzip\r\n"
            b"Content-Length: 0\r\n"
            b"\r\n"
        )
        with self.assertRaises(UnsupportedMediaType):
            Request().prepare(raw)


class CookieAndAuthTests(unittest.TestCase):
    def test_cookies(self):
        request = parse(
            "GET", "/x",
            headers={"Cookie": 'a=1; b="two"; $Version=1; empty=; eq=x=y'},
        )
        self.assertEqual(
            request.cookies,
            {"a": "1", "b": "two", "empty": "", "eq": "x=y"},
        )

    def test_no_cookie(self):
        self.assertEqual(parse("GET", "/x").cookies, {})

    def test_basic_auth(self):
        token = base64.b64encode(b"alice:secret").decode("ascii")
        request = parse(
            "GET", "/x",
            headers={"Authorization": f"Basic {token}"},
        )
        self.assertEqual(request.auth, ("alice", "secret"))
        self.assertEqual(
            request.authorization, f"Basic {token}"
        )

    def test_malformed_basic_auth(self):
        request = parse(
            "GET", "/x", headers={"Authorization": "Basic !!!"}
        )
        self.assertIsNone(request.auth)

    def test_non_basic_auth(self):
        request = parse(
            "GET", "/x", headers={"Authorization": "Bearer token"}
        )
        self.assertIsNone(request.auth)

    def test_basic_auth_without_colon(self):
        token = base64.b64encode(b"alice").decode("ascii")
        request = parse(
            "GET", "/x",
            headers={"Authorization": f"Basic {token}"},
        )
        self.assertIsNone(request.auth)


class BodyAccessorTests(unittest.TestCase):
    def test_json_body(self):
        request = parse(
            "POST", "/echo",
            headers={"Content-Type": "application/json; charset=utf-8"},
            body=b'{"ok": true, "n": 2}',
        )
        self.assertTrue(request.is_json)
        self.assertEqual(request.json, {"ok": True, "n": 2})
        self.assertEqual(request.mimetype, "application/json")

    def test_invalid_json_body(self):
        request = parse(
            "POST", "/echo",
            headers={"Content-Type": "application/json"},
            body=b"{not json",
        )
        with self.assertRaises(BadRequest):
            _ = request.json

    def test_form_body(self):
        request = parse(
            "POST", "/login",
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            body=b"username=alice&password=s%20ecret",
        )
        self.assertEqual(
            request.form,
            {"username": ["alice"], "password": ["s ecret"]},
        )

    def test_declared_charset(self):
        request = parse(
            "POST", "/echo",
            headers={"Content-Type": "text/plain; charset=iso-8859-1"},
            body=b"caf\xe9",
        )
        self.assertEqual(request.text, "caf\xe9")

    def test_utf8_string_body(self):
        body = '{"text": "h\u00e9llo"}'
        body_bytes = body.encode("utf-8")
        raw = (
            "POST /echo HTTP/1.1\r\n"
            "Host: example.test\r\n"
            "Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body_bytes)}\r\n"
            "\r\n"
            f"{body}"
        )
        request = Request().prepare(raw)
        self.assertEqual(request.json, {"text": "h\u00e9llo"})

    def test_unknown_charset_falls_back(self):
        request = parse(
            "POST", "/echo",
            headers={"Content-Type": "text/plain; charset=no-such"},
            body=b"ok",
        )
        self.assertEqual(request.text, "ok")


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.handler = lambda request: None
        self.other = lambda request: None

    def test_exact_route(self):
        routes = {("GET", "/hello"): self.handler}
        request = parse("GET", "/hello", routes=routes)
        self.assertIs(request.hook, self.handler)
        self.assertEqual(request.path_params, {})

    def test_head_falls_back_to_get(self):
        routes = {("GET", "/hello"): self.handler}
        request = parse("HEAD", "/hello", routes=routes)
        self.assertIs(request.hook, self.handler)

    def test_string_parameter(self):
        routes = {("GET", "/users/{user_id}"): self.handler}
        request = parse("GET", "/users/42", routes=routes)
        self.assertIs(request.hook, self.handler)
        self.assertEqual(request.path_params, {"user_id": "42"})

    def test_angle_parameter(self):
        routes = {("GET", "/users/<user_id>"): self.handler}
        request = parse("GET", "/users/42", routes=routes)
        self.assertEqual(request.path_params, {"user_id": "42"})

    def test_int_parameter(self):
        routes = {("GET", "/users/<int:user_id>"): self.handler}
        request = parse("GET", "/users/42", routes=routes)
        self.assertEqual(request.path_params, {"user_id": 42})

    def test_path_parameter(self):
        routes = {("GET", "/files/<path:name>"): self.handler}
        request = parse("GET", "/files/a/b.txt", routes=routes)
        self.assertEqual(request.path_params, {"name": "a/b.txt"})

    def test_method_not_allowed_reports_allowed_method(self):
        routes = {("POST", "/hello"): self.handler}
        request = parse("GET", "/hello", routes=routes)
        self.assertIsNone(request.hook)
        self.assertEqual(request.allowed_methods, ["POST"])

    def test_get_path_reports_get_and_head(self):
        routes = {("GET", "/hello"): self.handler}
        request = parse("POST", "/hello", routes=routes)
        self.assertIsNone(request.hook)
        self.assertEqual(request.allowed_methods, ["GET", "HEAD"])

    def test_static_route_beats_parameter_route(self):
        routes = {
            ("GET", "/files/<name>"): self.handler,
            ("GET", "/files/static"): self.other,
        }
        request = parse("GET", "/files/static", routes=routes)
        self.assertIs(request.hook, self.other)

    def test_parameter_route_beats_path_converter(self):
        routes = {
            ("GET", "/files/<path:name>"): self.handler,
            ("GET", "/files/<name>"): self.other,
        }
        request = parse("GET", "/files/a", routes=routes)
        self.assertIs(request.hook, self.other)
        request = parse("GET", "/files/a/b", routes=routes)
        self.assertIs(request.hook, self.handler)

    def test_root_route_wins_over_index_mapping(self):
        routes = {("GET", "/"): self.handler}
        request = parse("GET", "/", routes=routes)
        self.assertIs(request.hook, self.handler)
        self.assertEqual(request.path, "/")

    def test_root_without_route_maps_to_index(self):
        request = parse("GET", "/", routes={})
        self.assertIsNone(request.hook)
        self.assertEqual(request.path, "/index.html")

    def test_lowercase_route_method(self):
        routes = {("get", "/hello"): self.handler}
        request = parse("GET", "/hello", routes=routes)
        self.assertIs(request.hook, self.handler)

    def test_no_match(self):
        routes = {("GET", "/hello"): self.handler}
        request = parse("GET", "/other", routes=routes)
        self.assertIsNone(request.hook)
        self.assertEqual(request.allowed_methods, [])


class PrepareFromPartsTests(unittest.TestCase):
    def setUp(self):
        self.handler = lambda request: None

    def test_content_length(self):
        routes = {("POST", "/echo"): self.handler}
        request = Request().prepare_from_parts(
            "POST",
            "/echo?x=1",
            "HTTP/1.1",
            {"Host": "example.test", "Content-Length": "5"},
            b"hello",
            routes,
            ("127.0.0.1", 1234),
        )
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.query, {"x": ["1"]})
        self.assertEqual(request.body, b"hello")
        self.assertEqual(request.content_length, 5)
        self.assertEqual(request.remote_addr, ("127.0.0.1", 1234))
        self.assertIs(request.hook, self.handler)

    def test_chunked(self):
        request = Request().prepare_from_parts(
            "POST",
            "/echo",
            "HTTP/1.1",
            [("Host", "example.test"), ("Transfer-Encoding", "chunked")],
            b"5\r\nhello\r\n0\r\n\r\n",
        )
        self.assertEqual(request.body, b"hello")
        self.assertTrue(request.chunked)

    def test_missing_host(self):
        with self.assertRaises(BadRequest):
            Request().prepare_from_parts(
                "GET", "/x", "HTTP/1.1", {}, b""
            )

    def test_invalid_method(self):
        with self.assertRaises(BadRequest):
            Request().prepare_from_parts(
                "G@T", "/x", "HTTP/1.1", {"Host": "x"}, b""
            )

    def test_invalid_version(self):
        with self.assertRaises(BadRequest):
            Request().prepare_from_parts(
                "GET", "/x", "HTTP/2.0", {"Host": "x"}, b""
            )


class CompatibilityTests(unittest.TestCase):
    def test_fetch_headers_body_returns_bytes(self):
        head, body = Request().fetch_headers_body(
            b"GET /x HTTP/1.1\r\nHost: example.test\r\n\r\nhello"
        )
        self.assertEqual(head, b"GET /x HTTP/1.1\r\nHost: example.test")
        self.assertEqual(body, b"hello")

    def test_prepare_body_from_mapping(self):
        request = Request()
        payload = request.prepare_body({"a": "1", "b": ["2", "3"]})
        self.assertEqual(payload, b"a=1&b=2&b=3")
        self.assertEqual(request.headers["Content-Length"], "11")
        self.assertEqual(
            request.headers["Content-Type"],
            "application/x-www-form-urlencoded",
        )

    def test_prepare_body_from_json(self):
        request = Request()
        payload = request.prepare_body(None, json={"ok": True})
        self.assertEqual(json.loads(payload.decode("utf-8")), {"ok": True})
        self.assertEqual(request.headers["Content-Type"], "application/json")

    def test_prepare_auth_and_cookies(self):
        request = Request()
        request.prepare_auth(("alice", "secret"))
        self.assertEqual(request.auth, ("alice", "secret"))
        self.assertTrue(
            request.headers["Authorization"].startswith("Basic ")
        )
        self.assertEqual(
            request.authorization, request.headers["Authorization"]
        )
        request.prepare_cookies("a=b; c=d")
        self.assertEqual(request.cookies, {"a": "b", "c": "d"})
        self.assertEqual(request.parse_cookies(), {"a": "b", "c": "d"})
        request.prepare_cookies({"x": "1", "y": "2"})
        self.assertEqual(request.cookies, {"x": "1", "y": "2"})

    def test_repr(self):
        request = parse("GET", "/hello")
        self.assertEqual(repr(request), "<Request GET /hello HTTP/1.1>")


if __name__ == "__main__":
    unittest.main()
