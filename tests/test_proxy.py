import http.client
import json
import os
import random
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from daemon.errors import BadGateway, GatewayTimeout
from daemon.httpadapter import HttpAdapter
from daemon.proxy import (
    _active_backends,
    canonical_backend,
    forward_request,
    handle_client,
    reset_proxy_state,
    resolve_routing_policy,
    route_headers,
)
from start_proxy import parse_virtual_hosts


class BackendServer:
    def __init__(self, routes):
        self.routes = routes
        self._stop = threading.Event()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(50)
        self.port = self.socket.getsockname()[1]
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, addr = self.socket.accept()
            except OSError:
                break
            adapter = HttpAdapter(
                "127.0.0.1", self.port, conn, addr, self.routes
            )
            worker = threading.Thread(
                target=adapter.handle_client,
                args=(conn, addr, self.routes),
                daemon=True,
            )
            worker.start()

    def close(self):
        self._stop.set()
        try:
            self.socket.close()
        except OSError:
            pass
        self.thread.join(timeout=2)


class ProxyServer:
    def __init__(self, routes):
        self.routes = routes
        self._stop = threading.Event()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(50)
        self.port = self.socket.getsockname()[1]
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, addr = self.socket.accept()
            except OSError:
                break
            worker = threading.Thread(
                target=handle_client,
                args=("127.0.0.1", self.port, conn, addr, self.routes),
                daemon=True,
            )
            worker.start()

    def close(self):
        self._stop.set()
        try:
            self.socket.close()
        except OSError:
            pass
        self.thread.join(timeout=2)


def proxy_call(port, method, path, host="app.local", body=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    request_headers = {"Host": host}
    request_headers.update(headers or {})
    try:
        connection.request(
            method, path, body=body, headers=request_headers
        )
        response = connection.getresponse()
        data = response.read()
        return response.status, dict(response.getheaders()), data
    finally:
        connection.close()


def header_value(headers, name):
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


class ResolvePolicyTests(unittest.TestCase):
    def setUp(self):
        reset_proxy_state()

    def test_single_string_route(self):
        routes = {"a.local": ("10.0.0.1:9001", "round-robin")}
        self.assertEqual(
            resolve_routing_policy("a.local", routes),
            ("10.0.0.1", "9001"),
        )

    def test_single_item_list_route(self):
        routes = {"b.local": (["10.0.0.2:9002"], "round-robin")}
        self.assertEqual(
            resolve_routing_policy("b.local", routes),
            ("10.0.0.2", "9002"),
        )

    def test_round_robin_two_backends(self):
        routes = {
            "a.local": (
                ["10.0.0.1:9001", "10.0.0.2:9002"],
                "round-robin",
            )
        }
        first = resolve_routing_policy("a.local", routes)
        second = resolve_routing_policy("a.local", routes)
        third = resolve_routing_policy("a.local", routes)
        self.assertEqual(first, ("10.0.0.1", "9001"))
        self.assertEqual(second, ("10.0.0.2", "9002"))
        self.assertEqual(third, ("10.0.0.1", "9001"))

    def test_random_policy(self):
        routes = {
            "a.local": (
                ["10.0.0.1:9001", "10.0.0.2:9002"],
                "random",
            )
        }
        with patch(
            "daemon.proxy.random.choice", return_value="10.0.0.2:9002"
        ):
            self.assertEqual(
                resolve_routing_policy("a.local", routes),
                ("10.0.0.2", "9002"),
            )

    def test_least_conn_policy(self):
        routes = {
            "a.local": (
                ["10.0.0.1:9001", "10.0.0.2:9002"],
                "least-conn",
            )
        }
        _active_backends[("10.0.0.1", 9001)] = 2
        _active_backends[("10.0.0.2", 9002)] = 0
        self.assertEqual(
            resolve_routing_policy("a.local", routes),
            ("10.0.0.2", "9002"),
        )

    def test_host_with_port_is_normalized(self):
        routes = {"app.local": ("10.0.0.1:9001", "round-robin")}
        self.assertEqual(
            resolve_routing_policy("app.local:8080", routes),
            ("10.0.0.1", "9001"),
        )

    def test_unknown_host(self):
        self.assertEqual(resolve_routing_policy("other.local", {}), (None, None))

    def test_route_headers(self):
        routes = {
            "app.local": (
                "10.0.0.1:9001",
                "round-robin",
                {"Host": "$host", "X-Test": "yes"},
            )
        }
        self.assertEqual(
            route_headers("app.local", routes),
            {"Host": "$host", "X-Test": "yes"},
        )

    def test_canonical_backend(self):
        self.assertEqual(
            canonical_backend("http://127.0.0.1:9000"), "127.0.0.1:9000"
        )
        self.assertEqual(
            canonical_backend("127.0.0.1:9000"), "127.0.0.1:9000"
        )
        with self.assertRaises(ValueError):
            canonical_backend("")


class ConfigParsingTests(unittest.TestCase):
    def _write(self, text):
        handle = tempfile.NamedTemporaryFile(
            "w", delete=False, suffix=".conf"
        )
        try:
            handle.write(text)
            handle.close()
            return handle.name
        except Exception:
            handle.close()
            os.unlink(handle.name)
            raise

    def test_parse_multiple_backends_and_headers(self):
        path = self._write(
            'host "app.local" {\n'
            "  proxy_set_header Host $host;\n"
            "  proxy_set_header X-Test yes;\n"
            "  proxy_pass http://127.0.0.1:9001;\n"
            "  proxy_pass http://127.0.0.1:9002;\n"
            "  dist_policy least-conn;\n"
            "}\n"
        )
        try:
            routes = parse_virtual_hosts(path)
        finally:
            os.unlink(path)
        target, policy, headers = routes["app.local"]
        self.assertEqual(target, ["127.0.0.1:9001", "127.0.0.1:9002"])
        self.assertEqual(policy, "least-conn")
        self.assertEqual(headers["Host"], "$host")
        self.assertEqual(headers["X-Test"], "yes")

    def test_parse_single_backend(self):
        path = self._write(
            'host "a.local" {\n'
            "  proxy_pass http://127.0.0.1:9001;\n"
            "}\n"
        )
        try:
            routes = parse_virtual_hosts(path)
        finally:
            os.unlink(path)
        self.assertEqual(routes["a.local"][0], "127.0.0.1:9001")
        self.assertEqual(routes["a.local"][1], "round-robin")

    def test_parse_rejects_missing_proxy_pass(self):
        path = self._write('host "a.local" {\n  dist_policy round-robin;\n}\n')
        try:
            with self.assertRaises(ValueError):
                parse_virtual_hosts(path)
        finally:
            os.unlink(path)

    def test_parse_rejects_invalid_policy(self):
        path = self._write(
            'host "a.local" {\n'
            "  proxy_pass http://127.0.0.1:9001;\n"
            "  dist_policy nope;\n"
            "}\n"
        )
        try:
            with self.assertRaises(ValueError):
                parse_virtual_hosts(path)
        finally:
            os.unlink(path)

    def test_parse_merges_duplicate_host_blocks(self):
        path = self._write(
            'host "a.local" {\n'
            "  proxy_pass http://127.0.0.1:9001;\n"
            "}\n"
            'host "a.local" {\n'
            "  proxy_pass http://127.0.0.1:9002;\n"
            "}\n"
        )
        try:
            routes = parse_virtual_hosts(path)
        finally:
            os.unlink(path)
        self.assertEqual(
            routes["a.local"][0],
            ["127.0.0.1:9001", "127.0.0.1:9002"],
        )


class ForwardRequestTests(unittest.TestCase):
    def test_forces_connection_close_upstream(self):
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        received = []

        def backend():
            conn, _ = server.accept()
            try:
                received.append(conn.recv(4096).decode())
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
                )
            finally:
                conn.close()

        thread = threading.Thread(target=backend, daemon=True)
        thread.start()
        request = (
            "GET / HTTP/1.1\r\nHost: x\r\n"
            "Connection: keep-alive\r\n\r\n"
        )
        response = forward_request(
            "127.0.0.1", server.getsockname()[1], request
        )
        thread.join(2)
        server.close()

        self.assertTrue(response.endswith(b"ok"))
        self.assertIn("Connection: close\r\n\r\n", received[0])
        self.assertNotIn("keep-alive", received[0])

    def test_connection_failure_is_502(self):
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        port = server.getsockname()[1]
        server.close()
        with self.assertRaises(BadGateway):
            forward_request(
                "127.0.0.1",
                port,
                "GET / HTTP/1.1\r\nHost: x\r\n\r\n",
                connect_timeout=0.5,
            )

    def test_connect_timeout_is_504(self):
        with patch(
            "daemon.proxy.socket.create_connection",
            side_effect=socket.timeout(),
        ):
            with self.assertRaises(GatewayTimeout):
                forward_request(
                    "127.0.0.1",
                    9000,
                    "GET / HTTP/1.1\r\nHost: x\r\n\r\n",
                )

    def test_non_http_response_is_502(self):
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(1)

        def backend():
            conn, _ = server.accept()
            try:
                conn.recv(4096)
                conn.sendall(b"not http")
            finally:
                conn.close()

        thread = threading.Thread(target=backend, daemon=True)
        thread.start()
        with self.assertRaises(BadGateway):
            forward_request(
                "127.0.0.1",
                server.getsockname()[1],
                "GET / HTTP/1.1\r\nHost: x\r\n\r\n",
            )
        thread.join(2)
        server.close()


class EndToEndProxyTests(unittest.TestCase):
    def setUp(self):
        reset_proxy_state()
        self.backend_a = BackendServer(
            {
                ("GET", "/info"): lambda request: {
                    "backend": "a",
                    "host": request.headers.get("Host"),
                    "rewritten": request.headers.get("X-Rewritten"),
                },
                ("POST", "/echo"): lambda request: {
                    "body": request.text,
                    "backend": "a",
                },
            }
        )
        self.backend_b = BackendServer(
            {("GET", "/info"): lambda request: {"backend": "b"}}
        )
        self.routes = {
            "app.local": (
                [
                    "127.0.0.1:{}".format(self.backend_a.port),
                    "127.0.0.1:{}".format(self.backend_b.port),
                ],
                "round-robin",
                {"Host": "$host", "X-Rewritten": "$host"},
            )
        }
        self.proxy = ProxyServer(self.routes)
        self.addCleanup(self._close)

    def _close(self):
        self.proxy.close()
        self.backend_a.close()
        self.backend_b.close()

    def test_round_robin_proxy(self):
        first = proxy_call(self.proxy.port, "GET", "/info")
        second = proxy_call(self.proxy.port, "GET", "/info")
        third = proxy_call(self.proxy.port, "GET", "/info")
        self.assertEqual(first[0], 200)
        self.assertEqual(json.loads(first[2].decode("utf-8"))["backend"], "a")
        self.assertEqual(json.loads(second[2].decode("utf-8"))["backend"], "b")
        self.assertEqual(json.loads(third[2].decode("utf-8"))["backend"], "a")

    def test_header_rewrite_and_host(self):
        status, _headers, body = proxy_call(
            self.proxy.port, "GET", "/info"
        )
        self.assertEqual(status, 200)
        data = json.loads(body.decode("utf-8"))
        self.assertEqual(data["host"], "app.local")
        self.assertEqual(data["rewritten"], "app.local")

    def test_body_forwarding(self):
        status, _headers, body = proxy_call(
            self.proxy.port,
            "POST",
            "/echo",
            body=b"hello",
            headers={"Content-Type": "text/plain"},
        )
        self.assertEqual(status, 200)
        data = json.loads(body.decode("utf-8"))
        self.assertEqual(data["body"], "hello")

    def test_host_with_port(self):
        status, _headers, body = proxy_call(
            self.proxy.port, "GET", "/info", host="app.local:8080"
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body.decode("utf-8"))["backend"], "a")

    def test_unknown_host_is_404(self):
        status, _headers, _body = proxy_call(
            self.proxy.port, "GET", "/info", host="other.local"
        )
        self.assertEqual(status, 404)

    def test_dead_backend_is_502(self):
        dead = socket.socket()
        dead.bind(("127.0.0.1", 0))
        dead_port = dead.getsockname()[1]
        dead.close()
        routes = {
            "dead.local": (
                "127.0.0.1:{}".format(dead_port),
                "round-robin",
            )
        }
        proxy = ProxyServer(routes)
        self.addCleanup(proxy.close)
        status, _headers, _body = proxy_call(
            proxy.port, "GET", "/x", host="dead.local"
        )
        self.assertEqual(status, 502)

    def test_response_is_connection_close(self):
        status, headers, _body = proxy_call(
            self.proxy.port, "GET", "/info"
        )
        self.assertEqual(status, 200)
        self.assertEqual(header_value(headers, "Connection"), "close")


if __name__ == "__main__":
    unittest.main()
