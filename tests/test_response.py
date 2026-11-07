import json
import os
import tempfile
import unittest
from unittest.mock import patch

from daemon import config
from daemon.request import Request
from daemon.response import (
    STATIC_DIR,
    WWW_DIR,
    Response,
)


def make_request(path="/", method="GET", headers=None, version="HTTP/1.1"):
    """Build and parse a request for response tests."""
    headers = dict(headers or {})
    lines = [f"{method} {path} {version}", "Host: example.test"]
    for name, value in headers.items():
        lines.append(f"{name}: {value}")
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1")
    return Request().prepare(raw)


def split_response(payload):
    head, _, body = payload.partition(b"\r\n\r\n")
    return head.decode("utf-8").split("\r\n"), body


def header_values(payload, name):
    lines, _body = split_response(payload)
    values = []
    for line in lines[1:]:
        key, separator, value = line.partition(":")
        if separator and key.strip().lower() == name.lower():
            values.append(value.strip())
    return values


def header_value(payload, name):
    values = header_values(payload, name)
    return values[0] if values else None


def body_of(payload):
    _lines, body = split_response(payload)
    return body


class StatusAndHeaderTests(unittest.TestCase):
    def test_default_status_and_headers(self):
        payload = Response().build_response_header(None)
        lines, _body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 200 OK")
        self.assertEqual(header_value(payload, "Content-Type"), "text/plain; charset=utf-8")
        self.assertEqual(header_value(payload, "Content-Length"), "0")
        self.assertEqual(header_value(payload, "Connection"), "close")
        self.assertEqual(header_value(payload, "Server"), "StreapRous/1.0")
        self.assertIsNotNone(header_value(payload, "Date"))
        self.assertEqual(header_value(payload, "X-Content-Type-Options"), "nosniff")
        self.assertEqual(header_value(payload, "Referrer-Policy"), "same-origin")

    def test_set_status_and_reason(self):
        response = Response().set_status(404)
        lines, _body = split_response(response.build_response_header(None))
        self.assertEqual(lines[0], "HTTP/1.1 404 Not Found")

    def test_custom_reason(self):
        response = Response().set_status(418, "Short and stout")
        lines, _body = split_response(response.build_response_header(None))
        self.assertEqual(lines[0], "HTTP/1.1 418 Short and stout")

    def test_unknown_status_uses_unknown_reason(self):
        response = Response().set_status(599)
        lines, _body = split_response(response.build_response_header(None))
        self.assertEqual(lines[0], "HTTP/1.1 599 Unknown")

    def test_header_names_are_case_insensitive(self):
        response = Response()
        response.set_header("X-Test", "yes")
        self.assertEqual(response.headers["x-test"], "yes")
        self.assertEqual(header_value(response.build_response_header(None), "X-Test"), "yes")

    def test_header_injection_is_rejected(self):
        response = Response()
        with self.assertRaises(ValueError):
            response.set_header("X-Test", "a\r\nX-Injected: yes")
        with self.assertRaises(ValueError):
            response.set_header("Bad Name", "value")
        with self.assertRaises(ValueError):
            response.set_header("X-Test", "a\x00b")

    def test_add_header_combines_values(self):
        response = Response()
        response.add_header("X-Test", "one")
        response.add_header("X-Test", "two")
        self.assertEqual(header_values(response.build_response_header(None), "X-Test"), ["one, two"])

    def test_add_set_cookie_keeps_separate_headers(self):
        response = Response()
        response.add_header("Set-Cookie", "a=1")
        response.add_header("Set-Cookie", "b=2")
        payload = response.build_response_header(None)
        self.assertEqual(header_values(payload, "Set-Cookie"), ["a=1", "b=2"])

    def test_managed_headers_are_computed(self):
        response = Response()
        response.set_header("Content-Length", "999")
        response.set_header("Date", "custom")
        response.set_header("Server", "custom")
        response.set_header("Connection", "custom")
        response.set_body(b"hi")
        payload = response.build_response_header(None)
        self.assertEqual(header_value(payload, "Content-Length"), "2")
        self.assertEqual(header_value(payload, "Server"), "StreapRous/1.0")
        self.assertEqual(header_value(payload, "Connection"), "close")
        self.assertNotEqual(header_value(payload, "Date"), "custom")
        self.assertNotIn("custom", payload.decode("utf-8"))

    def test_keep_alive_from_request(self):
        response = Response()
        response.set_body(b"ok")
        payload = response.build_response(make_request("/", headers={"Connection": "close"}))
        self.assertEqual(header_value(payload, "Connection"), "close")
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_value(payload, "Connection"), "keep-alive")

    def test_response_keep_alive_override(self):
        response = Response()
        response.keep_alive = False
        response.set_body(b"ok")
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_value(payload, "Connection"), "close")

    def test_204_has_no_entity_headers(self):
        response = Response().no_content()
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_value(payload, "Content-Type"), None)
        self.assertEqual(header_value(payload, "Content-Length"), None)
        self.assertEqual(body_of(payload), b"")

    def test_304_has_no_entity_headers(self):
        response = Response().not_modified()
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_value(payload, "Content-Type"), None)
        self.assertEqual(header_value(payload, "Content-Length"), None)
        self.assertEqual(body_of(payload), b"")

    def test_1xx_has_no_entity_headers(self):
        response = Response().set_status(100)
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_value(payload, "Content-Type"), None)
        self.assertEqual(header_value(payload, "Content-Length"), None)
        self.assertEqual(body_of(payload), b"")

    def test_head_keeps_content_length_but_sends_no_body(self):
        response = Response().html("hello")
        payload = response.build_response(make_request("/", method="HEAD"))
        self.assertEqual(header_value(payload, "Content-Length"), "5")
        self.assertEqual(header_value(payload, "Content-Type"), "text/html; charset=utf-8")
        self.assertEqual(body_of(payload), b"")

    def test_http_version_can_be_chosen(self):
        response = Response()
        response.http_version = "HTTP/1.0"
        lines, _body = split_response(response.build_response_header(None))
        self.assertEqual(lines[0], "HTTP/1.0 200 OK")

    def test_cache_control_default_and_custom(self):
        response = Response()
        response.set_body(b"ok")
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_values(payload, "Cache-Control"), ["no-cache"])

        response = Response()
        response.set_header("Cache-Control", "max-age=5")
        response.set_body(b"ok")
        payload = response.build_response(make_request("/"))
        self.assertEqual(header_values(payload, "Cache-Control"), ["max-age=5"])

    def test_error_cache_control_is_no_store(self):
        response = Response().set_status(500)
        payload = response.build_response(make_request("/missing"))
        self.assertEqual(header_values(payload, "Cache-Control"), ["no-store"])


class BodyTests(unittest.TestCase):
    def test_json_body(self):
        payload = Response().json({"ok": True}).build_response(None)
        self.assertEqual(header_value(payload, "Content-Type"), "application/json; charset=utf-8")
        self.assertEqual(json.loads(body_of(payload).decode("utf-8")), {"ok": True})

    def test_json_status(self):
        payload = Response().json({"created": True}, status=201).build_response(None)
        lines, _body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 201 Created")

    def test_text_body(self):
        payload = Response().text("hello").build_response(None)
        self.assertEqual(header_value(payload, "Content-Type"), "text/plain; charset=utf-8")
        self.assertEqual(body_of(payload), b"hello")

    def test_html_body(self):
        payload = Response().html("<h1>Hi</h1>").build_response(None)
        self.assertEqual(header_value(payload, "Content-Type"), "text/html; charset=utf-8")
        self.assertEqual(body_of(payload), b"<h1>Hi</h1>")

    def test_set_body_types(self):
        response = Response().set_body(b"bytes")
        self.assertEqual(body_of(response.build_response(None)), b"bytes")

        response = Response().set_body("text")
        self.assertEqual(body_of(response.build_response(None)), b"text")

        response = Response().set_body({"a": 1})
        self.assertEqual(header_value(response.build_response(None), "Content-Type"), "application/json; charset=utf-8")

    def test_envelop_content_override(self):
        payload = Response().build_response(make_request("/"), b"hello")
        self.assertEqual(body_of(payload), b"hello")
        self.assertEqual(header_value(payload, "Content-Length"), "5")

    def test_redirect(self):
        payload = Response().redirect("/login").build_response(None)
        lines, body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 302 Found")
        self.assertEqual(header_value(payload, "Location"), "/login")
        self.assertEqual(header_value(payload, "Content-Length"), "0")
        self.assertEqual(body, b"")

    def test_no_content(self):
        payload = Response().no_content().build_response(None)
        self.assertEqual(header_value(payload, "Content-Length"), None)
        self.assertEqual(body_of(payload), b"")

    def test_not_modified(self):
        payload = Response().not_modified().build_response(None)
        self.assertEqual(header_value(payload, "Content-Length"), None)
        self.assertEqual(body_of(payload), b"")

    def test_error_status_without_body_builds_page(self):
        payload = Response().set_status(500).build_response(make_request("/missing"))
        lines, body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 500 Internal Server Error")
        self.assertEqual(header_value(payload, "Content-Type"), "text/html; charset=utf-8")
        self.assertIn(b"500 Internal Server Error", body)

    def test_error_status_with_body_uses_body(self):
        response = Response().set_status(404)
        response.set_body(b"missing")
        payload = response.build_response(make_request("/missing"))
        self.assertEqual(header_value(payload, "Content-Length"), "7")
        self.assertEqual(body_of(payload), b"missing")

    def test_head_error_has_no_body(self):
        payload = Response().build_response(make_request("/missing", method="HEAD"))
        lines, body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 404 Not Found")
        self.assertEqual(body, b"")
        self.assertGreater(int(header_value(payload, "Content-Length")), 0)

    def test_api_error_is_json(self):
        payload = Response().build_response(make_request("/api/missing"))
        self.assertEqual(header_value(payload, "Content-Type"), "application/json; charset=utf-8")
        data = json.loads(body_of(payload).decode("utf-8"))
        self.assertEqual(data["status"], 404)
        self.assertEqual(data["path"], "/api/missing")

    def test_accept_json_produces_json_error(self):
        payload = Response().build_response(
            make_request("/missing", headers={"Accept": "application/json"})
        )
        self.assertEqual(header_value(payload, "Content-Type"), "application/json; charset=utf-8")
        self.assertEqual(json.loads(body_of(payload).decode("utf-8"))["status"], 404)

    def test_error_page_escapes_path(self):
        payload = Response().build_response(make_request("/<script>alert(1)</script>"))
        body = body_of(payload)
        self.assertIn(b"&lt;script&gt;", body)
        self.assertNotIn(b"<script>alert(1)</script>", body)

    def test_build_notfound(self):
        payload = Response().build_notfound()
        self.assertIn(b"HTTP/1.1 404 Not Found", payload)
        self.assertIn(b"404 Not Found", payload)


class CookieTests(unittest.TestCase):
    def test_set_cookie(self):
        response = Response().set_cookie("SR_SESSION", "abc", max_age=60)
        payload = response.build_response_header(None)
        self.assertEqual(
            header_values(payload, "Set-Cookie"),
            ["SR_SESSION=abc; Max-Age=60; Path=/; HttpOnly; SameSite=Lax"],
        )

    def test_multiple_cookies(self):
        response = Response()
        response.set_cookie("a", "1")
        response.set_cookie("b", "2")
        values = header_values(response.build_response_header(None), "Set-Cookie")
        self.assertEqual(len(values), 2)

    def test_delete_cookie(self):
        response = Response().delete_cookie("SR_SESSION")
        value = header_values(response.build_response_header(None), "Set-Cookie")[0]
        self.assertIn("Max-Age=0", value)
        self.assertIn("Expires=Thu, 01 Jan 1970 00:00:00 GMT", value)

    def test_secure_and_samesite_none(self):
        response = Response().set_cookie(
            "token", "abc", secure=True, same_site="None"
        )
        value = header_values(response.build_response_header(None), "Set-Cookie")[0]
        self.assertIn("Secure", value)
        self.assertIn("SameSite=None", value)

    def test_cookie_domain_and_path(self):
        response = Response().set_cookie(
            "token", "abc", domain="example.test", path="/app"
        )
        value = header_values(response.build_response_header(None), "Set-Cookie")[0]
        self.assertIn("Path=/app", value)
        self.assertIn("Domain=example.test", value)

    def test_invalid_cookie_name(self):
        response = Response()
        with self.assertRaises(ValueError):
            response.set_cookie("bad name", "x")
        with self.assertRaises(ValueError):
            response.set_cookie("bad;name", "x")

    def test_invalid_cookie_value(self):
        response = Response()
        for value in ("a b", "a;b", "a,b", 'a"b', "a\\b", "café"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                response.set_cookie("token", value)

    def test_invalid_same_site(self):
        with self.assertRaises(ValueError):
            Response().set_cookie("token", "abc", same_site="sometimes")

    def test_legacy_cookie_mapping(self):
        response = Response()
        response.cookies = {"a": "b"}
        response.set_body(b"ok")
        payload = response.build_response(None)
        self.assertEqual(header_values(payload, "Set-Cookie"), ["a=b"])


class StaticTests(unittest.TestCase):
    def test_root_serves_index(self):
        payload = Response().build_response(make_request("/"))
        lines, body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 200 OK")
        self.assertEqual(header_value(payload, "Content-Type"), "text/html; charset=utf-8")
        self.assertIn(b"bksysnet@hcmut", body)
        self.assertEqual(int(header_value(payload, "Content-Length")), len(body))

    def test_index_html_direct(self):
        payload = Response().build_response(make_request("/index.html"))
        self.assertIn(b"bksysnet@hcmut", body_of(payload))

    def test_static_css(self):
        payload = Response().build_response(make_request("/static/css/styles.css"))
        self.assertEqual(header_value(payload, "Content-Type"), "text/css; charset=utf-8")
        with open(os.path.join(STATIC_DIR, "css", "styles.css"), "rb") as handle:
            self.assertEqual(body_of(payload), handle.read())

    def test_static_image(self):
        payload = Response().build_response(make_request("/static/images/welcome.png"))
        self.assertEqual(header_value(payload, "Content-Type"), "image/png")
        self.assertTrue(body_of(payload).startswith(b"\x89PNG"))

    def test_favicon(self):
        payload = Response().build_response(make_request("/favicon.ico"))
        self.assertEqual(header_value(payload, "Content-Type"), "image/x-icon")
        self.assertGreater(len(body_of(payload)), 0)

    def test_missing_file_is_404(self):
        payload = Response().build_response(make_request("/static/no-such-file.css"))
        lines, body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 404 Not Found")
        self.assertIn(b"404 Not Found", body)

    def test_api_missing_is_json_404(self):
        payload = Response().build_response(make_request("/api/missing"))
        self.assertEqual(header_value(payload, "Content-Type"), "application/json; charset=utf-8")

    def test_post_to_static_is_405(self):
        payload = Response().build_response(make_request("/static/css/styles.css", method="POST"))
        lines, _body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 405 Method Not Allowed")
        self.assertEqual(header_value(payload, "Allow"), "GET, HEAD")

    def test_head_static_has_no_body(self):
        payload = Response().build_response(make_request("/static/css/styles.css", method="HEAD"))
        self.assertEqual(body_of(payload), b"")
        with open(os.path.join(STATIC_DIR, "css", "styles.css"), "rb") as handle:
            self.assertEqual(int(header_value(payload, "Content-Length")), len(handle.read()))

    def test_traversal_paths_are_404(self):
        paths = [
            "/../config/proxy.conf",
            "/%2e%2e/config/proxy.conf",
            "/static/../config/proxy.conf",
            "/static/%2e%2e/config/proxy.conf",
            "/static/css/../../config/proxy.conf",
            "/static/.hidden",
        ]
        for path in paths:
            with self.subTest(path=path):
                payload = Response().build_response(make_request(path))
                self.assertIn(b"404 Not Found", payload)

    def test_build_content_rejects_unsafe_paths(self):
        response = Response()
        cases = [
            "",
            "/",
            "../config/proxy.conf",
            "%2e%2e/config/proxy.conf",
            "css/../../config/proxy.conf",
            "css\\..\\config",
            "css/%00",
            "css/.hidden",
            "css//styles.css",
            "/etc/passwd",
        ]
        for path in cases:
            with self.subTest(path=path):
                self.assertEqual(response.build_content(path, STATIC_DIR), (-1, b""))

    def test_build_content_accepts_safe_path(self):
        size, body = Response().build_content("css/styles.css", STATIC_DIR)
        self.assertGreater(size, 0)
        self.assertEqual(len(body), size)

    def test_build_content_rejects_empty_base(self):
        self.assertEqual(Response().build_content("css/styles.css", ""), (-1, b""))

    def test_build_content_rejects_oversize_file(self):
        with patch.object(config, "MAX_STATIC_BYTES", 10):
            self.assertEqual(
                Response().build_content("css/styles.css", STATIC_DIR),
                (-1, b""),
            )

    def test_build_content_rejects_symlink_escape(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = os.path.join(temporary, "base")
            os.makedirs(base)
            secret = os.path.join(temporary, "secret.txt")
            with open(secret, "w", encoding="utf-8") as handle:
                handle.write("secret")
            link = os.path.join(base, "link.txt")
            try:
                os.symlink(secret, link)
            except OSError:
                self.skipTest("symlinks are not available")
            self.assertEqual(Response().build_content("link.txt", base), (-1, b""))

    def test_static_cache_control(self):
        payload = Response().build_response(make_request("/static/css/styles.css"))
        self.assertEqual(header_values(payload, "Cache-Control"), ["max-age=0, must-revalidate"])

    def test_static_etag_and_last_modified(self):
        payload = Response().build_response(make_request("/static/css/styles.css"))
        self.assertIsNotNone(header_value(payload, "ETag"))
        self.assertIsNotNone(header_value(payload, "Last-Modified"))

    def test_if_none_match_returns_304(self):
        payload = Response().build_response(make_request("/static/css/styles.css"))
        etag = header_value(payload, "ETag")
        payload = Response().build_response(
            make_request("/static/css/styles.css", headers={"If-None-Match": etag})
        )
        lines, body = split_response(payload)
        self.assertEqual(lines[0], "HTTP/1.1 304 Not Modified")
        self.assertEqual(body, b"")
        self.assertIsNone(header_value(payload, "Content-Length"))
        self.assertEqual(header_value(payload, "ETag"), etag)

    def test_if_none_match_star_returns_304(self):
        payload = Response().build_response(
            make_request("/static/css/styles.css", headers={"If-None-Match": "*"})
        )
        self.assertIn(b"304 Not Modified", payload)

    def test_if_none_match_mismatch_returns_200(self):
        payload = Response().build_response(
            make_request("/static/css/styles.css", headers={"If-None-Match": 'W/"nope"'})
        )
        self.assertIn(b"200 OK", payload)
        self.assertGreater(len(body_of(payload)), 0)

    def test_if_modified_since_returns_304(self):
        payload = Response().build_response(make_request("/static/css/styles.css"))
        last_modified = header_value(payload, "Last-Modified")
        payload = Response().build_response(
            make_request("/static/css/styles.css", headers={"If-Modified-Since": last_modified})
        )
        self.assertIn(b"304 Not Modified", payload)

    def test_invalid_if_modified_since_returns_200(self):
        payload = Response().build_response(
            make_request("/static/css/styles.css", headers={"If-Modified-Since": "not a date"})
        )
        self.assertIn(b"200 OK", payload)

    def test_if_none_match_takes_precedence(self):
        payload = Response().build_response(make_request("/static/css/styles.css"))
        last_modified = header_value(payload, "Last-Modified")
        payload = Response().build_response(
            make_request(
                "/static/css/styles.css",
                headers={
                    "If-None-Match": 'W/"nope"',
                    "If-Modified-Since": last_modified,
                },
            )
        )
        self.assertIn(b"200 OK", payload)


class MimeTests(unittest.TestCase):
    def test_get_mime_type(self):
        response = Response()
        self.assertEqual(response.get_mime_type("index.html"), "text/html")
        self.assertEqual(response.get_mime_type("styles.css"), "text/css")
        self.assertEqual(response.get_mime_type("app.js"), "application/javascript")
        self.assertEqual(response.get_mime_type("data.json"), "application/json")
        self.assertEqual(response.get_mime_type("image.png"), "image/png")
        self.assertEqual(response.get_mime_type("icon.svg"), "image/svg+xml")
        self.assertEqual(response.get_mime_type("font.woff2"), "font/woff2")
        self.assertEqual(response.get_mime_type("file.unknown"), "application/octet-stream")

    def test_prepare_content_type(self):
        response = Response()
        self.assertTrue(response.prepare_content_type("text/html").startswith(WWW_DIR))
        self.assertEqual(
            response.headers["Content-Type"], "text/html; charset=utf-8"
        )
        self.assertTrue(response.prepare_content_type("text/css").startswith(STATIC_DIR))
        self.assertEqual(
            response.headers["Content-Type"], "text/css; charset=utf-8"
        )
        self.assertTrue(response.prepare_content_type("application/json").startswith(STATIC_DIR))
        self.assertEqual(
            response.headers["Content-Type"], "application/json; charset=utf-8"
        )
        self.assertTrue(response.prepare_content_type("image/png").startswith(STATIC_DIR))
        self.assertEqual(response.headers["Content-Type"], "image/png")

    def test_prepare_content_type_strips_parameters(self):
        response = Response()
        self.assertTrue(
            response.prepare_content_type("text/html; charset=utf-8").startswith(WWW_DIR)
        )

    def test_prepare_content_type_rejects_invalid(self):
        with self.assertRaises(ValueError):
            Response().prepare_content_type("text")
        with self.assertRaises(ValueError):
            Response().prepare_content_type("unknown/type")


class CompatibilityTests(unittest.TestCase):
    def test_build_content_returns_length_and_bytes(self):
        size, body = Response().build_content("index.html", WWW_DIR)
        self.assertGreater(size, 0)
        self.assertEqual(len(body), size)

    def test_build_response_stores_request(self):
        request = make_request("/")
        response = Response()
        response.set_body(b"ok")
        response.build_response(request)
        self.assertIs(response.request, request)

    def test_repr(self):
        self.assertEqual(repr(Response()), "<Response 200 OK>")
        self.assertEqual(repr(Response().set_status(404)), "<Response 404 Not Found>")


if __name__ == "__main__":
    unittest.main()
