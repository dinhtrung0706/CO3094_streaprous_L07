import http.client
import logging
import socket
import threading
import unittest

from daemon.httpadapter import HttpAdapter
from daemon.response import Response
from daemon.streaprous import StreapRous

logging.getLogger("daemon.httpadapter").disabled = True


class Phase1Server:
    """A tiny real HTTP server around the Phase 1 adapter."""

    def __init__(self, app):
        self.app = app
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
                "127.0.0.1", self.port, conn, addr, self.app.routes
            )
            worker = threading.Thread(
                target=adapter.handle_client,
                args=(conn, addr, self.app.routes),
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


def build_app():
    app = StreapRous()

    @app.before_request
    def identify(request):
        if request.path == "/blocked":
            return Response().text("blocked", status=403)
        user = request.headers.get("X-User")
        if user:
            request.user = user
        return None

    @app.after_request
    def add_marker(request, response):
        response.set_header("X-After", "yes")
        return response

    @app.route("/hello")
    def hello(request):
        return {"message": "hello"}

    @app.route("/echo", methods=["POST"])
    def echo(headers, body):
        return body

    @app.route("/async")
    async def async_route(request):
        return {"async": True}

    @app.route("/users/<int:user_id>")
    def user(request, user_id):
        return {"id": user_id}

    @app.route("/login", methods=["POST"])
    def login(request):
        response = Response().json({"ok": True})
        response.set_cookie("SR_SESSION", "abc", max_age=60)
        return response

    @app.route("/secure", auth=True)
    def secure(request):
        return {"user": request.user}

    @app.route("/boom")
    def boom(request):
        raise RuntimeError("boom")

    return app


class Phase1IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.app = build_app()
        self.server = Phase1Server(self.app)
        self.addCleanup(self.server.close)

    def connection(self):
        return http.client.HTTPConnection(
            "127.0.0.1", self.server.port, timeout=3
        )

    def call(self, method, path, body=None, headers=None):
        connection = self.connection()
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            data = response.read()
            return response.status, dict(response.getheaders()), data
        finally:
            connection.close()

    def header(self, headers, name):
        for key, value in headers.items():
            if key.lower() == name.lower():
                return value
        return None

    def test_full_request_path(self):
        status, headers, body = self.call("GET", "/hello")
        self.assertEqual(status, 200)
        self.assertEqual(self.header(headers, "X-After"), "yes")
        self.assertEqual(self.header(headers, "Server"), "StreapRous/1.0")
        self.assertIn(b'"message": "hello"', body)

    def test_static_root_and_css(self):
        status, headers, body = self.call("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(b"bksysnet@hcmut", body)
        self.assertEqual(self.header(headers, "X-After"), "yes")
        status, headers, body = self.call("GET", "/static/css/styles.css")
        self.assertEqual(status, 200)
        self.assertEqual(
            self.header(headers, "Content-Type"), "text/css; charset=utf-8"
        )
        self.assertGreater(len(body), 0)

    def test_legacy_handler(self):
        body = b'{"text": "hi"}'
        status, _headers, data = self.call(
            "POST",
            "/echo",
            body=body,
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data, body)

    def test_async_handler(self):
        status, _headers, body = self.call("GET", "/async")
        self.assertEqual(status, 200)
        self.assertIn(b'"async": true', body)

    def test_path_parameter(self):
        status, _headers, body = self.call("GET", "/users/42")
        self.assertEqual(status, 200)
        self.assertIn(b'"id": 42', body)

    def test_method_not_allowed(self):
        status, headers, _body = self.call("GET", "/echo")
        self.assertEqual(status, 405)
        self.assertEqual(self.header(headers, "Allow"), "POST")

    def test_not_found(self):
        status, _headers, body = self.call("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertIn(b"404 Not Found", body)

    def test_head_static(self):
        status, headers, body = self.call(
            "HEAD", "/static/css/styles.css"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertGreater(
            int(self.header(headers, "Content-Length")), 0
        )

    def test_keep_alive(self):
        connection = self.connection()
        try:
            connection.request("GET", "/hello")
            first = connection.getresponse()
            first.read()
            self.assertEqual(first.status, 200)
            self.assertEqual(
                first.getheader("Connection"), "keep-alive"
            )
            connection.request("GET", "/hello")
            second = connection.getresponse()
            second.read()
            self.assertEqual(second.status, 200)
        finally:
            connection.close()

    def test_auth_required_and_authenticated(self):
        status, headers, _body = self.call("GET", "/secure")
        self.assertEqual(status, 401)
        self.assertIn("StreapRous", self.header(headers, "WWW-Authenticate"))

        status, _headers, body = self.call(
            "GET", "/secure", headers={"X-User": "alice"}
        )
        self.assertEqual(status, 200)
        self.assertIn(b"alice", body)

    def test_before_hook_short_circuit(self):
        status, headers, body = self.call("GET", "/blocked")
        self.assertEqual(status, 403)
        self.assertEqual(body, b"blocked")
        self.assertEqual(self.header(headers, "X-After"), "yes")

    def test_cookie_response(self):
        status, headers, body = self.call("POST", "/login")
        self.assertEqual(status, 200)
        self.assertIn("SR_SESSION=abc", self.header(headers, "Set-Cookie"))
        self.assertIn(b'"ok": true', body)

    def test_500_response(self):
        status, _headers, body = self.call("GET", "/boom")
        self.assertEqual(status, 500)
        self.assertNotIn(b"RuntimeError", body)
        self.assertNotIn(b"Traceback", body)


if __name__ == "__main__":
    unittest.main()
