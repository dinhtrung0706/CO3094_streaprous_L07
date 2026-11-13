#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the LICENSE. Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""Queue-based raw TCP packet dispatcher for StreapRous connections.

Reads raw IPv4/TCP packets from a shared raw socket on a background
thread and routes each validated packet to either a per-connection queue
keyed by ``(client_ip, client_port)`` or the listener SYN queue.
"""

import queue
import socket
import struct
import threading

DEFAULT_QUEUE_MAXSIZE = 1024
_TIMEOUT_ERRORS = (socket.timeout, TimeoutError)

_packet_queues = {}
_packet_dispatcher_thread = None
_packet_dispatcher_running = False
packet_dispatcher_lock = threading.Lock()
_current_listen_port = None
_current_raw_socket = None

_SYN_KEY = ("SYN", None)


def _packet_dispatcher(s, listen_port):
    """Read raw IPv4/TCP packets and route them to connection queues.

    Args:
        s: Raw socket object supporting ``recvfrom``.
        listen_port: Local TCP port number to filter incoming packets.
    """
    global _packet_dispatcher_running

    with packet_dispatcher_lock:
        if _packet_dispatcher_thread is None:
            _packet_dispatcher_running = True

    if hasattr(s, "settimeout"):
        try:
            s.settimeout(0.2)
        except OSError:
            pass

    try:
        while _packet_dispatcher_running:
            try:
                packet, addr = s.recvfrom(65535)
            except _TIMEOUT_ERRORS:
                continue
            except OSError:
                break

            try:
                if not packet or len(packet) < 40:
                    continue

                version = packet[0] >> 4
                ihl = (packet[0] & 0x0F) * 4
                if version != 4 or ihl < 20 or len(packet) < ihl + 20:
                    continue
                if packet[9] != socket.IPPROTO_TCP:
                    continue

                total_len = struct.unpack("!H", packet[2:4])[0]
                if total_len < ihl + 20 or total_len > len(packet):
                    continue

                tcp = packet[ihl:total_len]
                if len(tcp) < 20:
                    continue

                data_offset = ((tcp[12] >> 4) & 0x0F) * 4
                if data_offset < 20 or len(tcp) < data_offset:
                    continue

                src_port, dest_port = struct.unpack("!HH", tcp[:4])
                if dest_port != listen_port:
                    continue

                flags = tcp[13]
                is_syn = bool(flags & 0x02) and not bool(flags & 0x10)
                is_ack = bool(flags & 0x10)
                key = (socket.inet_ntoa(packet[12:16]), src_port)

                with packet_dispatcher_lock:
                    client_queue = _packet_queues.get(key)
                    if client_queue is not None:
                        target = client_queue
                        print(
                            "[Sock_dispatcher] Route packet to existing "
                            f"queue for {key} (flags=0x{flags:02x}, "
                            f"SYN={is_syn}, ACK={is_ack})"
                        )
                    elif is_syn:
                        target = _packet_queues.setdefault(
                            _SYN_KEY,
                            queue.Queue(maxsize=DEFAULT_QUEUE_MAXSIZE),
                        )
                        print(
                            "[Sock_dispatcher] Route SYN packet to SYN "
                            f"queue for {key}"
                        )
                    else:
                        print(
                            f"[Sock_dispatcher] Drop packet for {key} - no "
                            f"queue (flags=0x{flags:02x}, SYN={is_syn}, "
                            f"ACK={is_ack})"
                        )
                        continue

                try:
                    target.put_nowait((packet, addr))
                except queue.Full:
                    pass
            except (IndexError, OSError, struct.error, ValueError) as exc:
                print(
                    f"[Sock_dispatcher] Dispatcher error: {exc}"
                )
    finally:
        with packet_dispatcher_lock:
            if (
                _packet_dispatcher_thread is None
                or _packet_dispatcher_thread is threading.current_thread()
            ):
                _packet_dispatcher_running = False


def start_packet_dispatcher(s, listen_port):
    """Start or restart the background packet dispatcher thread.

    Args:
        s: Raw socket used to receive incoming packets.
        listen_port: Local TCP port number to filter incoming packets.
    """
    global _packet_dispatcher_thread
    global _packet_dispatcher_running
    global _current_listen_port
    global _current_raw_socket

    old_thread = None
    with packet_dispatcher_lock:
        if (
            _packet_dispatcher_running
            and _packet_dispatcher_thread is not None
            and _packet_dispatcher_thread.is_alive()
        ):
            if (
                s is _current_raw_socket
                and listen_port == _current_listen_port
            ):
                return
            _packet_dispatcher_running = False
            old_thread = _packet_dispatcher_thread
            _packet_dispatcher_thread = None

    if (
        old_thread is not None
        and old_thread.is_alive()
        and old_thread is not threading.current_thread()
    ):
        old_thread.join(timeout=2.0)

    with packet_dispatcher_lock:
        _packet_dispatcher_running = True
        _current_raw_socket = s
        _current_listen_port = listen_port
        _packet_dispatcher_thread = threading.Thread(
            target=_packet_dispatcher,
            args=(s, listen_port),
            daemon=True,
            name=f"packet-dispatcher-{listen_port}",
        )
        _packet_dispatcher_thread.start()
        print(
            "[Sock_dispatcher] Start dispatching packet for port "
            f"{listen_port}"
        )


def stop_packet_dispatcher():
    """Stop the running packet dispatcher thread and reset state."""
    global _packet_dispatcher_running
    global _packet_dispatcher_thread
    global _current_listen_port
    global _current_raw_socket

    with packet_dispatcher_lock:
        _packet_dispatcher_running = False
        thread = _packet_dispatcher_thread
        _packet_dispatcher_thread = None
        _current_raw_socket = None
        _current_listen_port = None

    if (
        thread is not None
        and thread.is_alive()
        and thread is not threading.current_thread()
    ):
        thread.join(timeout=2.0)


def get_packet_queue(client_key):
    """Return or create the packet queue for a connection key.

    Args:
        client_key: ``(client_ip, client_port)`` or ``_SYN_KEY`` tuple.

    Returns:
        A bounded ``queue.Queue`` instance for ``client_key``.
    """
    with packet_dispatcher_lock:
        if client_key not in _packet_queues:
            _packet_queues[client_key] = queue.Queue(
                maxsize=DEFAULT_QUEUE_MAXSIZE
            )
        return _packet_queues[client_key]


def remove_packet_queue(client_key):
    """Remove the packet queue associated with ``client_key``.

    Args:
        client_key: ``(client_ip, client_port)`` or ``_SYN_KEY`` tuple.
    """
    with packet_dispatcher_lock:
        _packet_queues.pop(client_key, None)
