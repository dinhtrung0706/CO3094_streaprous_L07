"""Exchange HTTP messages using transport sessions."""

import socket
from collections import deque

from transport.connection_interface import ConnectionInterface
from transport.constant import MAX_RECEIVE_BUFFER
from . import config
from transport.packet import parse_buffer
from transport.stream_buffer import SendBuffer

REQUEST_ID = 1
RESPONSE_ID = 0x80000000
MESSAGE_SIZE = MAX_RECEIVE_BUFFER // 2
MAX_HTTP_MESSAGE = max(config.MAX_BODY_BYTES, config.MAX_STATIC_BYTES) + config.MAX_HEADER_BYTES
SESSION_HTTP_MAX_REQUESTS = 16
TRANSFER_ID_STRIDE = (MAX_HTTP_MESSAGE + 8 + MESSAGE_SIZE - 1) // MESSAGE_SIZE


class TCPConnection(ConnectionInterface):
    """Provide the Session connection interface over a TCP socket."""

    def __init__(self, sock):
        self._socket = sock
        self._buffer = b""
        self._handshake_rtt = config.SESSION_RTT
        self._cwnd = config.SESSION_CWND
        self._rwnd = config.SESSION_RWND

    @property
    def handshake_rtt(self):
        return self._handshake_rtt

    @property
    def cwnd(self):
        return self._cwnd

    @property
    def rwnd(self):
        return self._rwnd

    def register_disconnect_handler(self, handler):
        pass

    def unregister_disconnect_handler(self, handler):
        pass

    def stop_receive(self):
        """
        Interrupt the TCP socket's blocked receive operation.
        """
        try:
            self._socket.shutdown(socket.SHUT_RD)
        except OSError:
            pass

    def send_data_segment(self, payload):
        self._socket.sendall(payload)
        return len(payload)

    def recv_data_segment(self):
        while True:
            packet, remaining = parse_buffer(self._buffer)
            if packet is not None:
                frame = self._buffer[:len(self._buffer) - len(remaining)]
                self._buffer = remaining
                return frame
            chunk = self._socket.recv(4096)
            if not chunk:
                return b""
            self._buffer += chunk


def _send_chunk(session, payload, transfer_id):
    """
    Send a transfer with multiple chunks in flight.
    """
    buffer = SendBuffer(payload)
    outstanding = deque()
    while True:
        acquired = buffer.acquire_send_data()
        if acquired is None:
            break

        seq, _ = acquired
        if session.send_next_chunk(buffer, transfer_id):
            outstanding.append(seq)
            continue

        if not outstanding:
            print("[SessionHTTP] Warning: Send blocked")
            return False
        if not session.wait_for_ack(transfer_id, outstanding.popleft()):
            return False

    while outstanding:
        if not session.wait_for_ack(transfer_id, outstanding.popleft()):
            return False
    return True


def send_message(session, payload, transfer_id):
    """Send a length-prefixed message across bounded transfers."""
    if len(payload) > MAX_HTTP_MESSAGE:
        print("[SessionHTTP] Warning: Message too large")
        return False
    print("[SessionHTTP] TX data_size={}".format(len(payload)))
    data = len(payload).to_bytes(8, "big") + payload
    for index, offset in enumerate(range(0, len(data), MESSAGE_SIZE)):
        chunk = data[offset:offset + MESSAGE_SIZE]
        if not _send_chunk(session, chunk, transfer_id + index):
            return False
    return True


def _receive_chunk(session, transfer_id):
    return session.receive_transfer(transfer_id, wait=True)


def receive_message(session, transfer_id):
    """Receive a length-prefixed message assembled from bounded transfers."""
    first = _receive_chunk(session, transfer_id)
    if first is None:
        return None
    if len(first) < 8:
        print("[SessionHTTP] Warning: Missing message length")
        return None

    length = int.from_bytes(first[:8], "big")
    if length > MAX_HTTP_MESSAGE:
        print("[SessionHTTP] Warning: Message too large")
        return None
    data = bytearray(first[8:])
    index = 1
    while len(data) < length:
        chunk = _receive_chunk(session, transfer_id + index)
        if chunk is None:
            return None
        data.extend(chunk)
        index += 1
    if len(data) != length:
        print("[SessionHTTP] Warning: Invalid message length")
        return None
    print("[SessionHTTP] RX data_size={}".format(length))
    return bytes(data)
