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
deamon.streaprous
~~~~~~~~~~~~~~~~~

This module provides a StreapRous object to deploy RESTful url web app with routing
"""

import asyncio
import inspect

from .backend import create_backend


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
        """
        Initialize a new StreapRous instance.

        Sets up an empty route registry and prepares placeholders for IP and port.
        """
        self.routes = {}
        self.ip = None
        self.port = None
        return

    def prepare_address(self, ip, port):
        """
        Configure the IP address and port for the backend server.

        :param ip (str): The IP address to bind the server.
        :param port (str): The port number to listen on.
        """
        self.ip = ip
        self.port = port

    def route(self, path, methods=['GET']):
        """
        Decorator to register a route handler for a specific path and HTTP methods.

        :param path (str): The URL path to route.
        :param methods (list): A list of HTTP methods (e.g., ['GET', 'POST']) to bind.

        :rtype: function - A decorator that registers the handler function.
        """
        def decorator(func):
            for method in methods:
                self.routes[(method.upper(), path)] = func

            # Optional attach route metadata to the function
            func._route_path = path
            func._route_methods = methods

            def sync_wrapper(*args, **kwargs):
               print("[StreapRous] running sync function...  [{}] {}".format(methods, path))
               result = func(*args, **kwargs)
               return result

            async def async_wrapper(*args, **kwargs):
               print("[StreapRous] running Async function... [{}] {}".format(methods, path))
               result = await func(*args, **kwargs)
               return result

            if inspect.iscoroutinefunction(func):
               return async_wrapper
            else:
               return sync_wrapper
        return decorator

    def run(self):
        """
        Start the backend server and begin handling requests.

        This method launches the TCP server using the configured IP and port,
        and dispatches incoming requests to the registered route handlers.

        :raise: Error if IP or port has not been configured.
        """
        if not self.ip or not self.port:
            print("Rous app need to preapre address"
                  "by calling app.prepare_address(ip,port)")

        create_backend(self.ip, self.port, self.routes)
        
