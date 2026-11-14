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
daemon.backend
~~~~~~~~~~~~~~~~~

This module provides a backend object to manage and persist backend daemon.
It implements a basic backend server using streaprous_raw_connection for raw socket handling.
It supports handling multiple client connections concurrently and routing requests using a
custom HTTP adapter.

Requirements:
--------------
- streaprous_raw_connection: provides raw socket networking interface.
- threading: Enables concurrent client handling via threads.
- response: response utilities.
- httpadapter: the class for handling HTTP requests.
- CaseInsensitiveDict: provides dictionary for managing headers or routes.


Notes:
------
- The server create daemon threads for client handling.
- The current implementation error handling is minimal, socket errors are printed to the console.
- The actual request processing is delegated to the HttpAdapter class.

Usage Example:
--------------
>>> create_backend("127.0.0.1", 9000, routes={})

"""

import argparse
import asyncio
import functools
import inspect
import selectors
import socket
import threading

# Import streaprous raw connection
from streaprous_socket.streaprous_raw_connection import (
    DEFAULT_INTERFACE_NAME,
    accept,
    close_connection,
    create_raw_socket,
    run_conn_recv,
    run_conn_send,
)

from .dictionary import CaseInsensitiveDict
from .httpadapter import HttpAdapter
from .response import *

sel = selectors.DefaultSelector()

mode_async = "callback"
#mode_async = "coroutine"
mode_async = "threading"

def handle_client(ip, port, conn, addr, routes):
    """
    Initializes an HttpAdapter instance and delegates the client handling logic to it.

    :param ip (str): IP address of the server.
    :param port (int): Port number the server is listening on.
    :param conn (socket.socket): Client connection socket.
    :param addr (tuple): client address (IP, port).
    :param routes (dict): Dictionary of route handlers.
    """
    print(f"[Backend] Invoke handle_client accepted connection from {addr}")
    daemon = HttpAdapter(ip, port, conn, addr, routes)

    # Handle client
    daemon.handle_client(conn, addr, routes)


# Callback for handling new client (itself run in sync mode)
def handle_client_callback(server, ip, port,conn, addr, routes):
    """
    Initialize connection instance and delegates the client handling logic to it.

    :param ip (str): IP address of the server.
    :param port (int): Port number the server is listening on.
    :param routes (dict): Dictionary of route handlers.
    """
    print(f"[Backend] Invoke handle_client_callback accepted connection from {addr}")

    daemon = HttpAdapter(ip, port, conn, addr, routes)

    # Handle client
    daemon.handle_client(conn, addr, routes)


# Handle client using streaprous raw connection
def handle_client_raw(server, conn_info, routes):
    """
    Handle client connection using streaprous raw connection.
    
    :param server: Raw socket
    :param conn_info: conn_struct tuple for the established connection
    :param routes (dict): Dictionary of route handlers.
    """
    print(f"[Backend] Invoke handle_client_raw for {conn_info.client_ip}:{conn_info.client_port}")
    
    # Create HttpAdapter with connection info
    daemon = HttpAdapter(
        conn_info.server_ip,
        conn_info.server_port,
        None,  # conn not used with raw sockets
        (conn_info.client_ip, conn_info.client_port),
        routes
    )
    
    # Store connection info for use in HttpAdapter
    daemon.conn_info = conn_info
    daemon.raw_socket = server
    
    # Handle client using raw connection
    daemon.handle_client_raw(server, conn_info, routes)


# Coroutine async/await for handling new client
async def handle_client_coroutine(reader, writer, routes=None):
    """
    Coroutine in async communication to initialize connection instance
    then delegates the client handling logic to it.

    :param reader (StreamReader): Stream reader wrapper.
    :param writer (StreamWriter): Stream writer wrapper.
    :param routes (dict): Dictionary of route handlers.
    """
    addr = writer.get_extra_info("peername")
    print(f"[Backend] Invoke handle_client_coroutine accepted connection from {addr}")

    daemon = HttpAdapter(None, None, None, None, routes if routes is not None else {})
    await daemon.handle_client_coroutine(reader, writer)

def _run_coroutine(coro):
    """Run an async coroutine cleanly on Python 3.6+."""
    if hasattr(asyncio, "run"):
        return asyncio.run(coro)
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        loop.close()


async def async_server(ip="0.0.0.0", port=7000, routes=None):
    if routes is None:
        routes = {}
    print(f"[Backend] async_server **ASYNC** listening on port {port}")
    if routes != {}:
        print("[Backend] route settings")
        for key, value in routes.items():
            isCoFunc = ""
            if inspect.iscoroutinefunction(value):
               isCoFunc += "**ASYNC** "
            print(f"   + ('{key[0]}', '{key[1]}'): {isCoFunc}{value!s}")

    client_handler = functools.partial(
        handle_client_coroutine, routes=routes
    )
    server = await asyncio.start_server(client_handler, ip, port)
    try:
        if hasattr(server, "serve_forever"):
            await server.serve_forever()
        else:
            await asyncio.Future()
    finally:
        server.close()
        wait_closed = getattr(server, "wait_closed", None)
        if wait_closed is not None:
            await wait_closed()
    return


def run_backend(ip, port, routes, interface=None, transport=None):
    """
    Starts the backend server using an explicit or automatic transport.

    transport:
      - None / "auto": standard TCP on loopback, raw sockets elsewhere
      - "standard": standard TCP
      - "raw": raw streaprous transport
      - "async" / "asyncio" / "coroutine": asyncio server

    :param ip (str): IP address to bind the server.
    :param port (int): Port number to listen on.
    :param routes (dict): Dictionary of route handlers.
    :param interface (str, optional): Network interface for the raw transport.
    :param transport (str, optional): Backend transport selection.
    """
    global mode_async

    selected = (transport or "auto").strip().lower()
    if selected not in (
        "auto",
        "standard",
        "raw",
        "async",
        "asyncio",
        "coroutine",
    ):
        raise ValueError(f"unknown backend transport: {transport!r}")

    print(f"[Backend] run_backend transport={selected} routes={routes}")
    is_localhost = (
        ip == "127.0.0.1"
        or ip == "::1"
        or ip.startswith("127.")
        or ip == "localhost"
    )

    if selected in ("async", "asyncio", "coroutine") or (
        selected == "auto" and mode_async == "coroutine"
    ):
        _run_coroutine(async_server(ip, port, routes))
        return

    if selected == "standard":
        run_backend_standard(ip, port, routes)
        return

    if selected == "auto" and is_localhost:
        run_backend_standard(ip, port, routes)
        return

    # Auto-detect interface if not provided.
    if interface is None:
        from start_backend import detect_interface
        interface = detect_interface(ip)

    # Process socket object using streaprous_raw_connection
    try:
        # Create raw socket
        server = create_raw_socket()
        
        print(f"[Backend] Listening on port {port}")
        if routes != {}:
            print("[Backend] route settings")
            for key, value in routes.items():
               isCoFunc = ""
               if inspect.iscoroutinefunction(value):
                  isCoFunc += "**ASYNC** "
               print(f"   + ('{key[0]}', '{key[1]}'): {isCoFunc}{value!s}")

        # Main accept loop using streaprous raw connection
        while True:
            # Accept connection using raw socket handshake
            conn_info = accept(server, ip, port, interface)
            
            if conn_info is None:
                continue
                
            print(f"[Backend] Accepted connection from {conn_info.client_ip}:{conn_info.client_port}")
            
            # Handle client in a separate thread
            client_thread = threading.Thread(
                target=handle_client_raw,
                args=(server, conn_info, routes),
                daemon=True
            )
            client_thread.start()

    except PermissionError:
      print("Permission error: Raw sockets require root privileges (sudo)")
    except OSError as e:
      print(f"Socket error: {e}")
    except Exception as e:
      print(f"Error: {e}")


def run_backend_standard(ip, port, routes):
    """
    Starts the backend server using standard sockets for localhost connections.
    
    :param ip (str): IP address to bind the server.
    :param port (int): Port number to listen on.
    :param routes (dict): Dictionary of route handlers.
    """
    global mode_async
    
    print(f"[Backend] run_backend_standard with routes={routes}")
    
    # Process async stream for registering the service and terminate
    if mode_async == "coroutine":
        _run_coroutine(async_server(ip, port, routes))
        return

    # Process socket object using standard socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server.bind((ip, port))
        server.listen(50)

        print(f"[Backend] Listening on port {port}")
        if routes != {}:
            print("[Backend] route settings")
            for key, value in routes.items():
               isCoFunc = ""
               if inspect.iscoroutinefunction(value):
                  isCoFunc += "**ASYNC** "
               print(f"   + ('{key[0]}', '{key[1]}'): {isCoFunc}{value!s}")

        if mode_async == "callback":
            sel.register(server, selectors.EVENT_READ, (handle_client_callback, ip, port, routes))

        while True:
            # Accept connection
            conn, addr = server.accept()
            print(f"[Backend] Accepted connection from {addr[0]}:{addr[1]}")
            
            # Handle client in a separate thread
            client_thread = threading.Thread(
                target=handle_client,
                args=(ip, port, conn, addr, routes),
                daemon=True
            )
            client_thread.start()

    except OSError as e:
      print(f"Socket error: {e}")
    except Exception as e:
      print(f"Error: {e}")

def create_backend(ip, port, routes=None, interface=None, transport=None):
    """
    Entry point for creating and running the backend server.

    :param ip (str): IP address to bind the server.
    :param port (int): Port number to listen on.
    :param routes (dict, optional): Dictionary of route handlers.
    :param interface (str, optional): Network interface for the raw transport.
    :param transport (str, optional): auto, standard, raw, or async.
    """

    if routes is None:
        routes = {}
    run_backend(ip, port, routes, interface, transport)
