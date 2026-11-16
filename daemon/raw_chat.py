"""Chat frames over the custom raw TCP Connection and Session."""

import logging
import queue
import threading

from . import config, protocol
from streaprous_socket import streaprous_raw_connection as raw
from streaprous_socket.streaprous_packet_dispatcher import stop_packet_dispatcher
from transport.connection import Connection
from transport.session import Session
from transport.stream_buffer import SendBuffer

logger = logging.getLogger(__name__)
TRANSFER_ID = 1


class _RawLink:
    """A bidirectional Session and its outgoing message buffer."""

    def __init__(self, info, session):
        self.info = info
        self.session = session
        self.buffer = SendBuffer(b"")
        self.send_lock = threading.Lock()


class RawFrameNode:
    """Exchange chat frames over persistent raw TCP Sessions."""

    def __init__(self, on_frame, listen_ip, port, public_ip):
        if not 1 <= port <= 65535:
            raise ValueError("raw TCP port must be between 1 and 65535")
        self.on_frame = on_frame
        self.listen_ip = listen_ip
        self.port = int(port)
        self.public_ip = public_ip
        self.address = (public_ip, self.port)
        self._stop = threading.Event()
        self._queue = queue.Queue(maxsize=128)
        self._links = {}
        self._lock = threading.Lock()
        self._workers = []
        self._socket = None

    def start(self):
        self._socket = raw.create_raw_socket()
        self._spawn(self._accept_loop, "raw-chat-accept")
        self._spawn(self._send_loop, "raw-chat-send")
        return self

    def _spawn(self, target, name, args=()):
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        thread.start()
        self._workers.append(thread)

    def send(self, frame, address):
        payload = (tuple(address), frame.encode())
        while not self._stop.is_set():
            try:
                self._queue.put(payload, timeout=0.2)
                return
            except queue.Full:
                continue

    def _connection(self, info):
        return Connection(
            socket=self._socket, **info._asdict(),
            cwnd=config.SESSION_CWND,
            rwnd=config.SESSION_RWND,
            handshake_rtt=config.SESSION_RTT,
        )

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                info = raw.accept(self._socket, self.listen_ip, self.port)
                if info is not None:
                    self._open(info)
            except OSError:
                if not self._stop.is_set():
                    logger.exception("raw listener failed")
                break
            except Exception:
                if not self._stop.is_set():
                    logger.exception("raw listener failed")
                    self._stop.wait(0.5)

    def _open(self, info):
        address = (info.client_ip, info.client_port)
        with self._lock:
            existing = self._links.get(address)
        if existing is not None:
            return existing

        try:
            session = Session(self._connection(info), max_retries=config.SESSION_MAX_RETRIES)
        except Exception:
            raw.close_connection(self._socket, info)
            raise
        link = _RawLink(info, session)
        with self._lock:
            self._links[address] = link
        self._spawn(self._receive, "raw-chat-recv", (address, link))
        return link

    def _receive(self, address, link):
        try:
            while not self._stop.is_set():
                payload = link.session.receive_transfer(TRANSFER_ID, wait=True)
                if payload is None:
                    break
                try:
                    frame = protocol.decode_frame(payload)
                except protocol.ProtocolError:
                    logger.warning("invalid raw peer frame from %s", address)
                    continue
                self.on_frame(frame, address)
        except Exception:
            logger.exception("raw chat receive failed from %s:%s", *address)
        finally:
            self._finish(address, link)

    def _send_loop(self):
        while not self._stop.is_set():
            try:
                address, payload = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            while not self._stop.is_set() and not self._send_one(address, payload):
                self._stop.wait(1.0)
            self._queue.task_done()

    def _send_one(self, address, payload):
        with self._lock:
            link = self._links.get(address)
        if link is None:
            try:
                info = raw.connect(self._socket, self.public_ip, self.port,
                                   address[0], address[1], timeout=1.0, retries=2)
                if info is None:
                    return False
                link = self._open(info)
            except Exception:
                logger.exception("raw chat connect failed to %s:%s", *address)
                return False

        with link.send_lock:
            if link.session.wait_termination(0):
                self._finish(address, link)
                return False
            try:
                link.buffer.register_send_data(payload)
                last_seq = None
                while not self._stop.is_set():
                    pending = link.buffer.acquire_send_data()
                    if pending is None:
                        break
                    last_seq = pending[0]
                    if not link.session.send_next_chunk(link.buffer, TRANSFER_ID):
                        if link.session.wait_termination(0):
                            break
                        self._stop.wait(0.05)
                if last_seq is not None and link.session.wait_for_ack(TRANSFER_ID, last_seq):
                    return True
            except Exception:
                logger.exception("raw chat send failed to %s:%s", *address)
        self._finish(address, link)
        return False

    def _finish(self, address, link):
        with self._lock:
            if self._links.get(address) is not link:
                return
            del self._links[address]
        link.session.abort()
        try:
            raw.close_connection(self._socket, link.info)
        except OSError:
            pass

    def stop(self, timeout=3.0):
        self._stop.set()
        with self._lock:
            links = list(self._links.items())
        for address, link in links:
            self._finish(address, link)
        if self._socket is not None:
            stop_packet_dispatcher()
            self._socket.close()
        for worker in self._workers:
            if worker.is_alive():
                worker.join(timeout=1.0)
