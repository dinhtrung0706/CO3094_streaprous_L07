"""
Provide a socket-free Connection implementation for transport tests.
"""

import queue
import sys
from types import ModuleType
from unittest.mock import patch

try:
    from transport.connection import Connection
except ModuleNotFoundError as exc:
    if exc.name != "netifaces":
        raise
    with patch.dict(sys.modules, {"netifaces": ModuleType("netifaces")}):
        from transport.connection import Connection

from transport.packet import Packet


class FakeConnection(Connection):
    def __init__(self, rwnd=65535, cwnd=10000):
        super().__init__(
            socket=None, client_ip="127.0.0.1", client_port=30001,
            server_ip="127.0.0.1", server_port=30002,
            state="ESTABLISHED", server_seq=0, client_seq=0,
            expected_final_ack=0, last_activity=0,
            cwnd=cwnd, rwnd=rwnd, handshake_rtt=0.1,
        )
        self.sent = []
        self.received = queue.Queue()
        self.handlers = set()

    def register_disconnect_handler(self, handler):
        self.handlers.add(handler)

    def unregister_disconnect_handler(self, handler):
        self.handlers.discard(handler)

    def send_data_segment(self, payload):
        self.sent.append(Packet.decode(payload))
        return len(payload)

    def recv_data_segment(self):
        return self.received.get()

    def stop_receive(self):
        self._receive_stop.set()
        self.received.put(b"")
