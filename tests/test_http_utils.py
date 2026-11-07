import unittest
from datetime import datetime, timezone

from daemon.dictionary import CaseInsensitiveDict
from daemon.http_utils import (
    format_http_date,
    is_json_request,
    normalize_path,
    parse_http_date,
    safe_header_value,
    sniff_content_type,
    to_bytes,
)
from daemon.request import Request
from daemon.response import Response


class HttpUtilsTests(unittest.TestCase):
    def test_http_date_round_trip(self):
        timestamp = datetime(2026, 11, 7, 12, 30, tzinfo=timezone.utc).timestamp()
        value = format_http_date(timestamp)
        self.assertEqual(parse_http_date(value), timestamp)

    def test_invalid_http_date_returns_none(self):
        self.assertIsNone(parse_http_date("not a date"))

    def test_header_value_rejects_controls(self):
        with self.assertRaises(ValueError):
            safe_header_value("safe\r\nInjected: yes")
        with self.assertRaises(ValueError):
            safe_header_value("bad\x00value")

    def test_json_request_detection(self):
        self.assertTrue(is_json_request(type("Request", (), {"path": "/api/items", "headers": {}})()))
        self.assertTrue(is_json_request(type("Request", (), {"path": "/page", "headers": {"accept": "text/html, application/json"}})()))
        self.assertFalse(is_json_request(type("Request", (), {"path": "/page", "headers": {}})()))

    def test_to_bytes_and_content_sniffing(self):
        self.assertEqual(to_bytes("héllo", "utf-8"), "héllo".encode())
        self.assertEqual(to_bytes(b"raw", "utf-8"), b"raw")
        self.assertEqual(sniff_content_type(b'{"ok": true}'), "application/json")
        self.assertEqual(sniff_content_type(b"plain text\n"), "text/plain; charset=utf-8")
        self.assertEqual(sniff_content_type(b"\x00\xff"), "application/octet-stream")

    def test_normalize_path_decodes_once_and_rejects_nul(self):
        self.assertEqual(normalize_path("/a//b%20c/%252F"), "/a/b c/%2F")
        with self.assertRaises(ValueError):
            normalize_path("/a%00b")

    def test_request_line_does_not_swallow_rejected_path(self):
        request = Request()
        with self.assertRaises(ValueError):
            request.extract_request_line("GET /a%00b HTTP/1.1")


class CaseInsensitiveDictTests(unittest.TestCase):
    def test_lookup_keeps_original_header_names_and_overwrites(self):
        headers = CaseInsensitiveDict({"Content-Type": "text/plain"})
        headers["content-TYPE"] = "application/json"
        self.assertEqual(headers["CONTENT-type"], "application/json")
        self.assertEqual(list(headers.items()), [("Content-Type", "application/json")])
        self.assertEqual(list(headers), ["Content-Type"])

    def test_copy_is_independent_and_repr_uses_display_names(self):
        headers = CaseInsensitiveDict({"Content-Type": "text/html"})
        duplicate = headers.copy()
        duplicate["content-type"] = "application/json"
        duplicate["X-Test"] = "yes"
        self.assertEqual(headers["content-type"], "text/html")
        self.assertIn("'Content-Type': 'text/html'", repr(headers))
        self.assertIn("'X-Test': 'yes'", repr(duplicate))

    def test_mapping_update_and_setdefault_are_case_insensitive(self):
        headers = CaseInsensitiveDict()
        headers.setdefault("Content-Type", "text/plain")
        headers.setdefault("content-type", "ignored")
        headers.update({"CONTENT-TYPE": "application/json", "Accept": "*/*"})
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(list(headers), ["Content-Type", "Accept"])


class ResponseTests(unittest.TestCase):
    def test_empty_response_has_a_text_content_type(self):
        response = Response()
        header = response.build_response_header(None)
        self.assertIn(b"Content-Type: text/plain; charset=utf-8", header)


if __name__ == "__main__":
    unittest.main()
