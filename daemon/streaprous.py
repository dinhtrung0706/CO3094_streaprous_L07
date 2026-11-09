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
daemon.streaprous
~~~~~~~~~~~~~~~~~

This module provides a StreapRous application router. Handlers are registered
under (HTTP method, path) keys; the request parser selects a handler and the
HTTP adapter executes it.

The router also owns application-level before/after hooks. The route table is a
dict subclass so existing backend and adapter code can keep treating it as a
plain route mapping, while the adapter can discover hooks through attributes.
"""
from dataclasses import dataclass

from .backend import (
    _run_coroutine,
    async_server,
    create_backend,
    run_backend_standard,
)
from .request import Request


class RouteTable(dict):
    """A route mapping that also carries application hooks.

    The dict keys are (METHOD, path) pairs. Hook lists are consumed by the HTTP
    adapter; keeping them on the table lets the backend pass one object through
    without changing the route representation.
    """

    def __init__(self):
        super().__init__()
        self.before_request_hooks = []
        self.after_request_hooks = []


@dataclass(frozen=True)
class Route:
    """A registered route and its adapter-facing metadata."""

    path: str
    methods: tuple
    handler: object
    auth: bool


class StreapRous:
    """The fully mutable :class:`StreapRous <StreapRous>` object, which is a lightweight,
    mutable web application router for deploying RESTful URL endpoints.

    The `StreapRous` class provides a decorator-based routing system for building simple
    RESTful web applications.  The class allows developers to register route handlers
    using decorators and launch a TCP-based backend server to serve RESTful requests.
    Each route is mapped to a handler function based on HTTP method and path. It mappings
    supports tracking the combined HTTP methods and path route mappings internally.

    Usage::
      >>> import daemon.streaprous
      >>> app = StreapRous()
      >>> @app.route('/login', methods=['POST'])
      >>> def login(headers="guest", body="anonymous"):
      >>>     return {'message': 'Logged in'}

      >>> @app.route('/hello', methods=['GET'])
      >>> def hello(headers, body):
      >>>     return {'message': 'Hello, world!'}

      >>> app.run()
    """

    def __init__(self):
        self.routes = RouteTable()
        self._route_specs = []
        self.ip = None
        self.port = None

    def prepare_address(self, ip, port):
        """Configure the IP address and port for the backend server."""
        self.ip = ip
        self.port = port
        return self

    def route(self, path, methods=None, auth=False):
        """Register a handler for one or more HTTP methods.

        :param path (str): URL path beginning with '/'.
        :param methods (list): HTTP methods; defaults to ['GET'].
        :param auth (bool): Require an authenticated request.user.
        :rtype: function - decorator returning the original handler.
        """
        def decorator(func):
            if not callable(func):
                raise TypeError("route handler must be callable")
            if not isinstance(path, str) or not path.startswith("/"):
                raise ValueError("route path must begin with '/'")
            selected = ["GET"] if methods is None else methods
            if isinstance(selected, str):
                selected = [selected]
            normalized = []
            for method in selected:
                method = str(method).upper()
                if method and method not in normalized:
                    normalized.append(method)
            if not normalized:
                raise ValueError("route must declare at least one method")

            route_auth = bool(auth or getattr(func, "_route_auth", False))
            func._route_path = path
            func._route_methods = tuple(normalized)
            func._route_auth = route_auth

            for method in normalized:
                self.routes[(method, path)] = func
            self._route_specs.append(
                Route(path, tuple(normalized), func, route_auth)
            )
            return func

        return decorator

    def add_route(self, path, handler, methods=None, auth=False):
        """Register a handler without decorator syntax."""
        self.route(path, methods=methods, auth=auth)(handler)
        return handler

    def before_request(self, func):
        """Register a hook called as func(request) before dispatch."""
        if not callable(func):
            raise TypeError("before_request hook must be callable")
        self.routes.before_request_hooks.append(func)
        return func

    def after_request(self, func):
        """Register a hook called as func(request, response)."""
        if not callable(func):
            raise TypeError("after_request hook must be callable")
        self.routes.after_request_hooks.append(func)
        return func

    @staticmethod
    def require_auth(func):
        """Mark a handler as requiring an authenticated request.user."""
        func._route_auth = True
        return func

    def match(self, method, path):
        """Return (handler, path_params, allowed_methods) for a path.

        This uses the same matcher as the request parser, so router and parser
        cannot drift on parameter syntax or method fallback.
        """
        request = Request()
        request.method = str(method).upper()
        request.path = path
        request.version = "HTTP/1.1"
        handler = request.match_route(self.routes)
        return handler, request.path_params, request.allowed_methods

    def run(self, transport=None, interface=None):
        """Start the backend server using the configured address.

        :param transport: None/'raw', 'standard', or 'async'/'asyncio'.
        :param interface: Optional network interface for the raw transport.
        """
        if not self.ip or not self.port:
            raise RuntimeError(
                "call app.prepare_address(ip, port) before app.run()"
            )

        selected = (transport or "raw").lower()
        if selected == "standard":
            run_backend_standard(self.ip, self.port, self.routes)
        elif selected in ("async", "asyncio", "coroutine"):
            _run_coroutine(async_server(self.ip, self.port, self.routes))
        elif selected == "raw":
            create_backend(
                self.ip,
                self.port,
                self.routes,
                interface=interface,
            )
        else:
            raise ValueError(f"unknown transport: {transport!r}")
