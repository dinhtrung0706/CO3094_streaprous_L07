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


"""
Streaprous packet dispatcher module based on queue. For each connection, a single thread that
reads from raw socket and dispatches packets to it associated queue.
Each queue is identified by its key includes (client_ip, client_port).
This module supports multiple processing flows (denoted by coonections) would call recvfrom() on the same socket.
"""

import queue
import socket
import struct
import threading
import time

# Global structure for packet dispatcher
# The multi connections are managed by client_key map to queue.Queue
_packet_queues = {}
_packet_dispatcher_thread = None
_packet_dispatcher_running = False
_packet_dispatcher_lock = threading.Lock()
_current_listen_port = None
_current_raw_socket = None


_SYN_KEY = ("SYN", None)

def _packet_dispatcher(s, listen_port):
    global _packet_dispatcher_running
    s.settimeout(0.2)                      # lets the loop notice a stop request
    while _packet_dispatcher_running:
        try:
            packet, addr = s.recvfrom(65535)
        except socket.timeout:
            continue
        except OSError:
            # The exception in the case socket closed
            break                         
        try:
            ihl = (packet[0] & 0x0F) * 4
            tcp = packet[ihl:]
            if len(tcp) < 20:
                continue
            src_port, dest_port = struct.unpack('!HH', tcp[:4])

            # Verify the received packet matching the listen port
            if dest_port != listen_port:   
                continue
            flags = tcp[13]
            is_syn = (flags & 0x02) and not (flags & 0x10)
            is_ack = (flags & 0x10)
            key = (socket.inet_ntoa(packet[12:16]), src_port)

            # Concurrently handle multiple packets
            with _packet_dispatcher_lock:
                client_key = _packet_queues.get(key)

                if client_key is not None:
                    # known client_key connection exist in queues
                    target = client_key                                   
                    print(f"[Sock_dispatcher] Route packet to existing queue for {key} (flags=0x{flags:02x}, SYN={is_syn}, ACK={is_ack})")
                elif is_syn:
                    # Non-established connection with syn handshaking
                    target = _packet_queues.setdefault(_SYN_KEY, queue.Queue(maxsize=100))
                    print(f"[Sock_dispatcher] Route SYN packet to SYN queue for {key}")
                else:
                    # No-managed packet will be dropped
                    print(f"[Sock_dispatcher] Drop packet for {key} - no queue (flags=0x{flags:02x}, SYN={is_syn}, ACK={is_ack})")
                    continue
            try:
                target.put_nowait((packet, addr))
            except queue.Full:
                pass
        except Exception as e:
            print(f"[Sock_dispatcher] Dispatcher error: {e}")


def start_packet_dispatcher(s: socket.socket, listen_port: int):
    """Start the packet dispatcher thread.

    Args:
         socket.socket - raw socket
         listen_port 
    """
    global _packet_dispatcher_thread, _packet_dispatcher_running

    # Concurrently handle multiple connections
    with _packet_dispatcher_lock:
        if _packet_dispatcher_running:
            return
        _packet_dispatcher_running = True
        _packet_dispatcher_thread = threading.Thread(
            target=_packet_dispatcher, 
            args=(s, listen_port),
            daemon=True,
            name=f"packet-dispatcher-{listen_port}"
        )
        _packet_dispatcher_thread.start()
        print(f"[Sock_dispatcher] Start dispatching packet for port {listen_port}")


def stop_packet_dispatcher():
    """Stop the running packet.

    Args: None
    """
    global _packet_dispatcher_running, _packet_dispatcher_thread
    with _packet_dispatcher_lock:
        _packet_dispatcher_running = False
    if _packet_dispatcher_thread:
        _packet_dispatcher_thread.join(timeout=2)


def get_packet_queue(client_key):
    """Get or create packet queue for a connection.

    Args:
         client_key - (client_ip, client_port) key for given connection.
    """

    # Concurrently handle multiple connections
    with _packet_dispatcher_lock:
        if client_key not in _packet_queues:
            _packet_queues[client_key] = queue.Queue(maxsize=100)
        return _packet_queues[client_key]


def remove_packet_queue(client_key):
    """Remove packet queue.

    Args: None
    """

    # Concurrently handle multiple connections
    with _packet_dispatcher_lock:
        if client_key in _packet_queues:
            del _packet_queues[client_key]