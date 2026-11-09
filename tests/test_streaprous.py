import unittest
from unittest.mock import MagicMock, patch

try:
    from unittest.mock import AsyncMock
except ImportError:  # Python < 3.8
    class AsyncMock(MagicMock):
        async def __call__(self, *args, **kwargs):
            return super().__call__(*args, **kwargs)

from daemon.backend import _run_coroutine
from daemon.httpadapter import HttpAdapter
from daemon.request import Request
from daemon.response import Response
from daemon.streaprous import Route, RouteTable, StreapRous


def make_request(path="/", method="GET", headers=None, body=b"", routes=None):
    headers = dict(headers or {})
    names = {name.lower() for name in headers}
    if "host" not in names:
        headers["Host"] = "example.test"
    if isinstance(body, str):
        body = body.encode("utf-8")
    if body and "content-length" not in names:
        headers["Content-Length"] = str(len(body))
    lines = [f"{method} {path} HTTP/1.1"]
    for name, value in headers.items():
        lines.append(f"{name}: {value}")
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1") + body
    return Request().prepare(raw, routes)


def response_parts(payload):
    head, _, body = payload.partition(b"\r\n\r\n")
    return head.decode("iso-8859-1").split("\r\n"), body


def header_value(headers, name):
    for line in headers[1:]:
        key, separator, value = line.partition(":")
        if separator and key.strip().lower() == name.lower():
            return value.strip()
    return None


class RouteRegistrationTests(unittest.TestCase):
    def test_route_returns_original_and_registers_methods(self):
        app = StreapRous()

        def handler(request):
            return {"ok": True}

        decorated = app.route("/x", methods=["get", "POST", "get"])(handler)
        self.assertIs(decorated, handler)
        self.assertIs(app.routes[("GET", "/x")], handler)
        self.assertIs(app.routes[("POST", "/x")], handler)
        self.assertFalse(hasattr(handler, "_route_handler"))
        self.assertEqual(handler._route_methods, ("GET", "POST"))
        self.assertFalse(handler._route_auth)

    def test_route_defaults_to_get(self):
        app = StreapRous()
        handler = app.route("/x")(lambda request: "ok")
        self.assertIs(app.routes[("GET", "/x")], handler)

    def test_route_methods_as_string(self):
        app = StreapRous()
        handler = app.route("/x", methods="POST")(lambda request: "ok")
        self.assertIs(app.routes[("POST", "/x")], handler)

    def test_route_spec_metadata(self):
        app = StreapRous()

        def handler(request):
            return "ok"

        app.route("/x", methods=["GET"], auth=True)(handler)
        self.assertEqual(len(app._route_specs), 1)
        spec = app._route_specs[0]
        self.assertIsInstance(spec, Route)
        self.assertEqual(spec.path, "/x")
        self.assertEqual(spec.methods, ("GET",))
        self.assertIs(spec.handler, handler)
        self.assertTrue(spec.auth)

    def test_route_rejects_invalid_path(self):
        app = StreapRous()
        with self.assertRaises(ValueError):
            app.route("x")(lambda request: "ok")

    def test_route_rejects_empty_methods(self):
        app = StreapRous()
        with self.assertRaises(ValueError):
            app.route("/x", methods=[])(lambda request: "ok")

    def test_add_route(self):
        app = StreapRous()

        def handler(request):
            return "ok"

        self.assertIs(app.add_route("/x", handler, methods=["PUT"]), handler)
        self.assertIs(app.routes[("PUT", "/x")], handler)

    def test_require_auth_decorator(self):
        app = StreapRous()

        @app.route("/secure")
        @StreapRous.require_auth
        def secure(request):
            return "ok"

        self.assertTrue(app.routes[("GET", "/secure")]._route_auth)
        self.assertTrue(app._route_specs[0].auth)

    def test_before_and_after_hooks_return_functions(self):
        app = StreapRous()

        def before(request):
            return None

        def after(request, response):
            return response

        self.assertIs(app.before_request(before), before)
        self.assertIs(app.after_request(after), after)
        self.assertEqual(app.routes.before_request_hooks, [before])
        self.assertEqual(app.routes.after_request_hooks, [after])

    def test_route_table_hook_lists_are_independent(self):
        first = RouteTable()
        second = RouteTable()
        first.before_request_hooks.append(lambda request: None)
        self.assertEqual(second.before_request_hooks, [])


class MatchTests(unittest.TestCase):
    def setUp(self):
        self.app = StreapRous()

        @self.app.route("/hello")
        def hello(request):
            return "ok"

        @self.app.route("/users/<int:user_id>")
        def user(request, user_id):
            return user_id

    def test_exact_match(self):
        handler, params, allowed = self.app.match("GET", "/hello")
        self.assertIs(handler, self.app.routes[("GET", "/hello")])
        self.assertEqual(params, {})
        self.assertEqual(allowed, [])

    def test_parameter_match(self):
        handler, params, _allowed = self.app.match("GET", "/users/42")
        self.assertIs(handler, self.app.routes[("GET", "/users/<int:user_id>")])
        self.assertEqual(params, {"user_id": 42})

    def test_method_not_allowed(self):
        handler, _params, allowed = self.app.match("POST", "/hello")
        self.assertIsNone(handler)
        self.assertEqual(allowed, ["GET", "HEAD"])


class RunTests(unittest.TestCase):
    def test_run_requires_address(self):
        with self.assertRaises(RuntimeError):
            StreapRous().run()

    def test_run_standard(self):
        app = StreapRous()
        app.prepare_address("127.0.0.1", 9000)
        with patch("daemon.streaprous.run_backend_standard") as start:
            app.run(transport="standard")
        start.assert_called_once_with("127.0.0.1", 9000, app.routes)

    def test_run_raw(self):
        app = StreapRous()
        app.prepare_address("127.0.0.1", 9000)
        with patch("daemon.streaprous.create_backend") as start:
            app.run(transport="raw", interface="lo0")
        start.assert_called_once_with(
            "127.0.0.1", 9000, app.routes, interface="lo0"
        )

    def test_run_async(self):
        app = StreapRous()
        app.prepare_address("127.0.0.1", 9000)
        with patch(
            "daemon.streaprous.async_server", new_callable=AsyncMock
        ) as start:
            app.run(transport="async")
        start.assert_called_once_with("127.0.0.1", 9000, app.routes)

    def test_run_unknown_transport(self):
        app = StreapRous()
        app.prepare_address("127.0.0.1", 9000)
        with self.assertRaises(ValueError):
            app.run(transport="teleport")


class HookDispatchTests(unittest.TestCase):
    def _adapter(self, routes):
        return HttpAdapter("127.0.0.1", 9000, None, None, routes)

    def test_before_hook_short_circuits(self):
        app = StreapRous()

        @app.before_request
        def before(request):
            return Response().text("blocked", status=403)

        @app.route("/x")
        def handler(request):
            return "ok"

        request = make_request("/x", routes=app.routes)
        response = self._adapter(app.routes).dispatch(request)
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 403 Forbidden")
        self.assertEqual(body, b"blocked")

    def test_before_hook_can_authenticate_request(self):
        app = StreapRous()

        @app.before_request
        def identify(request):
            if request.headers.get("X-User"):
                request.user = request.headers["X-User"]

        @app.route("/secure", auth=True)
        def secure(request):
            return {"user": request.user}

        request = make_request(
            "/secure", headers={"X-User": "alice"}, routes=app.routes
        )
        response = self._adapter(app.routes).dispatch(request)
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 200 OK")
        self.assertIn(b"alice", body)

    def test_after_hook_can_add_header(self):
        app = StreapRous()

        @app.after_request
        def after(request, response):
            response.set_header("X-After", "yes")
            return response

        @app.route("/x")
        def handler(request):
            return "ok"

        request = make_request("/x", routes=app.routes)
        response = self._adapter(app.routes).dispatch(request)
        headers, _body = response_parts(response.build_response(request))
        self.assertEqual(header_value(headers, "X-After"), "yes")

    def test_after_hook_can_replace_response(self):
        app = StreapRous()

        @app.after_request
        def after(request, response):
            return Response().text("replaced", status=418)

        @app.route("/x")
        def handler(request):
            return "ok"

        request = make_request("/x", routes=app.routes)
        response = self._adapter(app.routes).dispatch(request)
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 418 I'm a Teapot")
        self.assertEqual(body, b"replaced")

    def test_async_hooks_in_async_dispatch(self):
        app = StreapRous()

        @app.before_request
        async def before(request):
            request.user = "async-user"

        @app.after_request
        async def after(request, response):
            response.set_header("X-Async-Hook", "yes")
            return response

        @app.route("/secure", auth=True)
        def secure(request):
            return {"user": request.user}

        request = make_request("/secure", routes=app.routes)
        response = _run_coroutine(
            self._adapter(app.routes).dispatch_async(request)
        )
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 200 OK")
        self.assertEqual(header_value(headers, "X-Async-Hook"), "yes")
        self.assertIn(b"async-user", body)

    def test_async_hooks_in_sync_dispatch(self):
        app = StreapRous()

        @app.before_request
        async def before(request):
            request.user = "sync-run-user"

        @app.route("/secure", auth=True)
        def secure(request):
            return {"user": request.user}

        request = make_request("/secure", routes=app.routes)
        response = self._adapter(app.routes).dispatch(request)
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 200 OK")
        self.assertIn(b"sync-run-user", body)

    def test_hooks_apply_to_static_responses(self):
        app = StreapRous()

        @app.after_request
        def after(request, response):
            response.set_header("X-Static-Hook", "yes")
            return response

        request = make_request("/", routes=app.routes)
        response = self._adapter(app.routes).dispatch(request)
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 200 OK")
        self.assertEqual(header_value(headers, "X-Static-Hook"), "yes")
        self.assertIn(b"bksysnet@hcmut", body)

    def test_empty_route_table_hooks_are_preserved(self):
        app = StreapRous()

        @app.before_request
        def before(request):
            return Response().text("hooked", status=403)

        request = make_request("/", routes=app.routes)
        adapter = self._adapter(app.routes)
        response = adapter.dispatch(request)
        headers, body = response_parts(response.build_response(request))
        self.assertEqual(headers[0], "HTTP/1.1 403 Forbidden")
        self.assertEqual(body, b"hooked")


if __name__ == "__main__":
    unittest.main()
