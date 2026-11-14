"""Peer node: discovery, heartbeat and message delivery over UDP."""

import http.client
import json
import logging
import socket
import sqlite3
import threading
import time
from collections import deque

from . import protocol
from .udp_transport import DEFAULT_CAPACITY, DEFAULT_RTO, ReliableChannel, UdpNode

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 5.0
TICK_SECONDS = 0.05


def _detect_local_ip():
    """Best-effort local IPv4 address without third-party packages."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 53))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()


class PeerNode:
    """Background UDP peer with heartbeat, retransmission and relay support."""

    def __init__(
        self,
        chat,
        *,
        username,
        token,
        verifier,
        role="peer",
        listen_ip="0.0.0.0",
        udp_port=0,
        public_ip=None,
        tracker=None,
        heartbeat=HEARTBEAT_SECONDS,
        tick=TICK_SECONDS,
        capacity=DEFAULT_CAPACITY,
        rto=DEFAULT_RTO,
        http_port=0,
        auth=None,
    ):
        if role not in ("peer", "tracker"):
            raise ValueError("role must be 'peer' or 'tracker'")
        self.chat = chat
        self.username = username
        self.token = token
        self.verifier = verifier
        self.role = role
        self.tracker = tuple(tracker) if tracker else None
        self.heartbeat_seconds = float(heartbeat)
        self.tick_seconds = float(tick)
        self.capacity = int(capacity)
        self.rto = float(rto)
        self.http_port = int(http_port or 0)
        self.auth = auth
        self._fallback = deque()
        self._fallback_lock = threading.Lock()
        self._fallback_running = False
        self._pending_udp = {}

        self.node = UdpNode(self._on_frame, listen_ip, udp_port)
        if public_ip:
            self.public_ip = public_ip
        elif listen_ip in ("0.0.0.0", "::", ""):
            self.public_ip = _detect_local_ip()
        else:
            self.public_ip = listen_ip
        self.public_port = int(self.node.port)
        self.address = (self.public_ip, self.public_port)

        self._channels = {}
        self._outbox = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads = []
        self.started = False
        self.stats = {
            "hellos": 0,
            "pings": 0,
            "messages_sent": 0,
            "messages_received": 0,
            "duplicates": 0,
            "rejected": 0,
            "relayed": 0,
            "malformed": 0,
        }

    def start(self):
        if self.started:
            return self
        self.node.start()
        self.started = True
        if self.tracker is not None:
            self.send_hello()
        self._spawn(self._tick_loop, "peer-tick")
        if self.tracker is not None and self.role == "peer":
            self._spawn(self._heartbeat_loop, "peer-heartbeat")
        return self

    def stop(self, timeout=3.0):
        self._stop.set()
        self.node.stop(timeout)
        for thread in self._threads:
            thread.join(timeout)
        self._threads.clear()
        self.started = False

    def _spawn(self, target, name):
        thread = threading.Thread(target=target, name=f"{name}-{self.username}", daemon=True)
        thread.start()
        self._threads.append(thread)

    def _tick_loop(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("peer tick failed")
            self._stop.wait(self.tick_seconds)

    def _heartbeat_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self.heartbeat_seconds)
            if self._stop.is_set():
                break
            try:
                self.send_ping()
            except Exception:
                logger.exception("heartbeat failed")

    def tick(self):
        now = time.monotonic()
        for address, channel in list(self._channels.items()):
            retransmissions = channel.poll(now)
            for frame in retransmissions:
                self._send(frame, address)
                message_id = self._message_id(frame)
                pending = self._pending_udp.get(message_id) if message_id else None
                if pending is not None:
                    self._pending_udp[message_id] = (
                        address, pending[1], frame.seq
                    )
            if channel.dead:
                logger.warning("peer %s stopped responding", address)
                for message_id, pending in list(self._pending_udp.items()):
                    if pending[0] == address:
                        with self._fallback_lock:
                            self._fallback.append((
                                "/internal/p2p/message", pending[1]
                            ))
                        self._pending_udp.pop(message_id, None)
                with self._lock:
                    self._channels.pop(address, None)
        for address in list(self._outbox):
            self._flush(address)
        self._flush_fallback()
        self.chat.prune_peers()

    def _flush(self, address):
        with self._lock:
            channel = self._channel(address)
            queue = self._outbox.get(address)
            if queue is None:
                return
            while queue:
                frame_type, payload = queue[0]
                frame = channel.queue(frame_type, payload)
                if frame is None:
                    break
                queue.popleft()
                message_id = self._message_id(frame)
                if message_id:
                    pending = self._pending_udp.get(message_id)
                    if pending is not None:
                        self._pending_udp[message_id] = (
                            address, pending[1], frame.seq
                        )
                self._send(frame, address)
                self.stats["messages_sent"] += 1
            if not queue:
                self._outbox.pop(address, None)

    def _send(self, frame, address):
        try:
            self.node.send(frame, address)
        except OSError:
            logger.warning("failed to send to %s", address)

    def _channel(self, address):
        channel = self._channels.get(address)
        if channel is None:
            channel = ReliableChannel(
                address, capacity=self.capacity, rto=self.rto
            )
            self._channels[address] = channel
        return channel

    def _enqueue(self, address, payload):
        try:
            envelope, _token = protocol.decode_message(payload)
        except protocol.ProtocolError:
            envelope = None
        with self._lock:
            if envelope is not None:
                self._pending_udp[envelope["id"]] = (
                    address, envelope, time.monotonic()
                )
            self._outbox.setdefault(address, deque()).append(
                (protocol.MSG, payload)
            )
        self._flush(address)

    def send_hello(self):
        if self.tracker is None:
            return
        frame = protocol.Frame.with_json(protocol.HELLO, {
            "username": self.username, "ip": self.public_ip,
            "port": self.public_port, "http_port": self.http_port,
            "token": self.token,
        })
        self._send(frame, self.tracker)

    def send_ping(self):
        if self.tracker is None:
            return
        self._send(
            protocol.ping(
                self.username, self.token, ip=self.public_ip,
                port=self.public_port
            ),
            self.tracker,
        )

    def _broadcast_envelope(self, envelope):
        targets = self.chat.peers(exclude=self.username)
        channel = self.chat.database.get_channel(envelope["channel"])
        if channel is not None and channel["is_private"]:
            targets = [peer for peer in targets if
                       self.chat.database.is_channel_member(
                           channel["id"], peer["username"]
                       )]
        if not targets:
            return 0
        payload = self._encode_payload(envelope)
        for peer in targets:
            self._enqueue((peer["ip"], int(peer["port"])), payload)
        return len(targets)

    def _relay(self, envelope):
        # Chat messages are never relayed through the tracker.
        return None

    def broadcast(self, channel_id, body, *, message_id=None, sender=None):
        """Store a message locally and fan it out over UDP."""
        sender = sender or self.username
        if sender != self.username:
            raise PermissionError("this node can only send as its own user")
        owner = self.chat.database.get_user(sender)
        if owner is None:
            raise PermissionError("authenticated local user is required")
        self.chat.require_member(channel_id, owner)
        message, created = self.chat.post_message(
            channel_id, sender, body, message_id=message_id
        )
        if created:
            envelope = self._channel_envelope(message)
            if not self._broadcast_envelope(envelope):
                with self._fallback_lock:
                    self._fallback.append(("/internal/p2p/message", envelope))
        return message

    def send_direct(self, channel_id, recipient, body, *, message_id=None,
                    sender=None):
        """Store a channel message addressed to one peer."""
        sender = sender or self.username
        if sender != self.username:
            raise PermissionError("this node can only send as its own user")
        owner = self.chat.database.get_user(sender)
        if owner is None:
            raise PermissionError("authenticated local user is required")
        self.chat.require_member(channel_id, owner)
        message, created = self.chat.post_message(
            channel_id, sender, body, message_id=message_id, recipient=recipient
        )
        if not created:
            return message
        address = self.chat.peer_address(recipient)
        envelope = self._channel_envelope(message)
        envelope["to"] = recipient
        if address is None:
            with self._fallback_lock:
                self._fallback.append(("/internal/p2p/message", envelope))
        else:
            self._enqueue(address, self._encode_payload(envelope))
        return message

    def invite(self, channel_id, username):
        channel = self.chat.require_channel(channel_id)
        envelope = {
            "v": protocol.VERSION, "kind": "invite",
            "id": protocol.new_message_id(), "channel": channel_id,
            "channel_name": channel["name"], "is_private": bool(channel["is_private"]),
            "from": channel["owner_name"], "to": username, "body": "",
            "ts": int(time.time()),
        }
        address = self.chat.peer_address(username)
        if address is None:
            with self._fallback_lock:
                self._fallback.append(("/internal/p2p/message", envelope))
        else:
            self._enqueue(address, self._encode_payload(envelope))

    def send_direct_message(self, message):
        envelope = {
            "v": protocol.VERSION, "kind": "direct", "id": message["id"],
            "channel": "direct", "from": message["sender"],
            "to": message["recipient"], "body": message["body"],
            "ts": message["created_at"],
        }
        address = self.chat.peer_address(message["recipient"])
        if address is None:
            with self._fallback_lock:
                self._fallback.append(("/internal/p2p/message", envelope))
        else:
            self._enqueue(address, self._encode_payload(envelope))

    def send_direct_chat(self, recipient, body, *, message_id=None, sender=None):
        sender = sender or self.username
        if sender != self.username:
            raise PermissionError("this node can only send as its own user")
        message, created = self.chat.post_direct_message(
            sender, recipient, body, message_id=message_id
        )
        if created:
            self.send_direct_message(message)
        return message

    def _http_request(self, username, port, path, envelope):
        record = self.chat.peer_record(username)
        if record is None or not port or self.auth is None:
            return None
        body = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
        timestamp = int(time.time())
        signature = self.auth.sign_peer_request(self.username, timestamp, body)
        headers = {
            "Content-Type": "application/json",
            "X-StreapRous-Peer": self.username,
            "X-StreapRous-Time": str(timestamp),
            "X-StreapRous-Signature": signature,
        }
        try:
            connection = http.client.HTTPConnection(
                record["ip"], int(port), timeout=2.0
            )
            connection.request("POST", path, body=body, headers=headers)
            response = connection.getresponse()
            if response.length is not None and response.length > 1024 * 1024:
                connection.close()
                return None
            payload = response.read(1024 * 1024 + 1)
            status = response.status
            connection.close()
            if status < 200 or status >= 300:
                return None
            return json.loads(payload.decode("utf-8")) if payload else {}
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        except http.client.HTTPException:
            return None

    def _sync_from_peer(self, username):
        record = self.chat.peer_record(username)
        if record is None or not record.get("http_port"):
            return
        user = self.chat.database.get_user(self.username)
        if user is None:
            return
        for channel in self.chat.list_channels(user["id"]):
            if not channel["joined"] or (
                channel["is_private"] and not channel["owner_name"]
            ):
                continue
            response = self._http_request(
                username, record["http_port"], "/internal/p2p/sync",
                {"channel": channel["id"], "since": 0, "limit": 500},
            )
            if not isinstance(response, dict):
                continue
            for message in response.get("messages", []):
                try:
                    if channel["is_private"] and not self.chat.database.is_channel_member(
                        channel["id"], message["sender"]
                    ):
                        continue
                    self.chat.post_message(
                        message["channel_id"], message["sender"],
                        message["body"], message_id=message["id"],
                        ts=message["created_at"],
                    )
                except (KeyError, ValueError):
                    self.stats["malformed"] += 1

    def _flush_fallback(self):
        if self.auth is None or self._fallback_running:
            return
        with self._fallback_lock:
            pending = list(self._fallback)
            self._fallback.clear()
            if pending:
                self._fallback_running = True
        if pending:
            threading.Thread(
                target=self._deliver_fallback, args=(pending,),
                name=f"peer-fallback-{self.username}", daemon=True,
            ).start()

    def _deliver_fallback(self, pending):
        try:
            for path, envelope in pending:
                targets = self.chat.peers(exclude=self.username)
                if envelope.get("kind") in ("direct", "invite"):
                    targets = [record for record in targets if
                               record["username"].casefold() ==
                               envelope.get("to", "").casefold()]
                delivered = False
                for record in targets:
                    if record.get("http_port") and self._http_request(
                        record["username"], record["http_port"], path, envelope
                    ) is not None:
                        delivered = True
                        break
                if not delivered:
                    with self._fallback_lock:
                        self._fallback.append((path, envelope))
        finally:
            with self._fallback_lock:
                self._fallback_running = False

    @staticmethod
    def _envelope(message, recipient=None, kind="channel"):
        if recipient is None:
            recipient = message.get("recipient") or "*"
        return {
            "v": protocol.VERSION,
            "kind": kind,
            "id": message["id"],
            "channel": message["channel_id"],
            "from": message["sender"],
            "to": recipient,
            "body": message["body"],
            "ts": message["created_at"],
            "owner": message.get("owner", ""),
            "channel_name": message.get("channel_name", ""),
            "is_private": bool(message.get("is_private", False)),
        }

    def _channel_envelope(self, message):
        channel = self.chat.database.get_channel(message["channel_id"])
        envelope = self._envelope(message)
        if channel is not None:
            envelope.update({
                "channel_name": channel["name"],
                "is_private": bool(channel["is_private"]),
                "owner": channel["owner_name"],
            })
        return envelope

    def _encode_payload(self, envelope):
        return protocol.encode_message(envelope, self.token)

    def _decode_payload(self, payload):
        return protocol.decode_message(payload)

    def _on_frame(self, frame, address):
        if address == self.node.address:
            return
        if frame.seq:
            with self._lock:
                channel = self._channel(address)
            delivered = channel.receive(frame)
            self._send(channel.acknowledgement(), address)
            for inner in delivered:
                self._dispatch(inner, address)
            return
        if frame.type == protocol.MSG_ACK:
            with self._lock:
                channel = self._channels.get(address)
            if channel is not None:
                channel.receive(frame)
                for message_id, pending in list(self._pending_udp.items()):
                    if pending[0] == address and pending[2] < frame.ack:
                        self._pending_udp.pop(message_id, None)
            return
        if frame.type in (protocol.HELLO_ACK, protocol.PONG):
            self._dispatch(frame, address)
            if frame.type == protocol.HELLO_ACK:
                try:
                    peer_username = next(
                        peer["username"] for peer in self.chat.peers()
                        if (peer["ip"], peer["port"]) == address
                    )
                except StopIteration:
                    return
                self._sync_from_peer(peer_username)
            return
        self._dispatch(frame, address)

    @staticmethod
    def _message_id(frame):
        try:
            envelope, _token = protocol.decode_message(frame.payload)
        except protocol.ProtocolError:
            return None
        return envelope["id"]

    def _dispatch(self, frame, address):
        handler = {
            protocol.HELLO: self._handle_hello,
            protocol.PING: self._handle_ping,
            protocol.HELLO_ACK: self._handle_peer_list,
            protocol.PONG: self._handle_peer_list,
            protocol.MSG: self._handle_message,
            protocol.BYE: self._handle_bye,
            protocol.AUTH_ERR: self._handle_auth_error,
        }.get(frame.type)
        if handler is None:
            return
        try:
            handler(frame, address)
        except ValueError:
            self.stats["malformed"] += 1
        except Exception:
            logger.exception("failed to handle %s from %s", frame.name, address)

    def _authenticate(self, token):
        username = self.verifier(token) if token else None
        if not username:
            self.stats["rejected"] += 1
        return username

    def _handle_hello(self, frame, address):
        payload = frame.json_payload()
        username = self._authenticate(payload.get("token"))
        if username is None:
            self._send(protocol.Frame.with_json(protocol.AUTH_ERR, {"error": "bad token"}), address)
            return
        if payload.get("username") != username:
            self.stats["rejected"] += 1
            return
        ip = payload.get("ip") or address[0]
        port = payload.get("port")
        self.chat.register_peer(
            username, str(ip), port, payload.get("http_port", 0)
        )
        self.stats["hellos"] += 1
        self._send(self._peer_list_frame(protocol.HELLO_ACK), address)

    def _handle_ping(self, frame, address):
        payload = frame.json_payload()
        username = self._authenticate(payload.get("token"))
        if username is None:
            return
        # Prefer the advertised address: the source address is often 127.0.0.1
        # for a local send and would create a second row for the same peer.
        ip = payload.get("ip") or address[0]
        port = payload.get("port") or self._port_for(username)
        if port:
            self.chat.register_peer(
                username, str(ip), port, payload.get("http_port", 0)
            )
        self.stats["pings"] += 1
        self._send(self._peer_list_frame(protocol.PONG), address)

    def _port_for(self, username):
        for peer in self.chat.peers():
            if peer["username"].lower() == str(username).lower():
                return int(peer["port"])
        return 0

    def _peer_list_frame(self, frame_type):
        peers = [
            {"username": peer["username"], "ip": peer["ip"],
             "port": peer["port"], "http_port": peer["http_port"]}
            for peer in self.chat.peers(exclude=self.username)
        ]
        return protocol.Frame.with_json(frame_type, {"peers": peers})

    def _handle_peer_list(self, frame, address):
        payload = frame.json_payload()
        for peer in payload.get("peers") or []:
            if not isinstance(peer, dict):
                continue
            username = peer.get("username")
            if not username or username.lower() == self.username.lower():
                continue
            try:
                self.chat.register_peer(
                    username, peer.get("ip", ""), peer.get("port"),
                    peer.get("http_port", 0),
                )
            except Exception:
                continue

    def _handle_message(self, frame, address):
        envelope, token = self._decode_payload(frame.payload)
        username = self._authenticate(token)
        if username is None or envelope["from"].casefold() != username.casefold():
            self.stats["rejected"] += 1
            return
        recipient = envelope.get("to") or "*"
        kind = envelope.get("kind", "channel")
        if recipient != "*" and recipient.casefold() != self.username.casefold():
            return
        if kind == "direct":
            _message, created = self.chat.receive_direct_message(
                envelope["from"], recipient, envelope["body"],
                message_id=envelope["id"], ts=envelope["ts"],
            )
            self.stats["messages_received" if created else "duplicates"] += 1
            return
        if kind == "invite":
            channel = self.chat.database.get_channel(envelope["channel"])
            if channel is None:
                try:
                    channel = self.chat.database.create_channel(
                        envelope["channel"], envelope["channel_name"], None,
                        int(envelope["ts"]),
                        is_private=envelope.get("is_private", True),
                        owner_name=envelope.get("owner") or envelope["from"],
                    )
                except sqlite3.IntegrityError:
                    channel = self.chat.database.get_channel(envelope["channel"])
            if channel is None or channel["owner_name"].casefold() != \
                    (envelope.get("owner") or envelope["from"]).casefold():
                self.stats["rejected"] += 1
                return
            self.chat.database.add_channel_member(
                channel["id"], self.username, int(time.time())
            )
            return

        channel = self.chat.database.get_channel(envelope["channel"])
        if channel is None:
            try:
                self.chat.database.create_channel(
                    envelope["channel"], envelope.get("channel_name") or
                    "channel-" + envelope["channel"][:8], None,
                    int(envelope["ts"]),
                    is_private=envelope.get("is_private", False),
                    owner_name=envelope.get("owner") or envelope["from"],
                )
            except sqlite3.IntegrityError:
                pass
            channel = self.chat.database.get_channel(envelope["channel"])
        if channel is None:
            self.stats["rejected"] += 1
            return
        if envelope.get("owner") and channel["owner_name"].casefold() != \
                envelope["owner"].casefold():
            self.stats["rejected"] += 1
            return
        if channel["is_private"] and not self.chat.database.is_channel_member(
            channel["id"], self.username
        ):
            self.stats["rejected"] += 1
            return
        if channel["is_private"] and not self.chat.database.is_channel_member(
            channel["id"], envelope["from"]
        ):
            self.stats["rejected"] += 1
            return
        _message, created = self.chat.post_message(
            envelope["channel"], envelope["from"], envelope["body"],
            message_id=envelope["id"], ts=envelope["ts"], recipient=recipient,
        )
        self.stats["messages_received" if created else "duplicates"] += 1

    def _handle_bye(self, frame, address):
        with self._lock:
            self._channels.pop(address, None)

    def _handle_auth_error(self, frame, address):
        logger.warning("peer %s rejected our token", address)

    def status(self):
        return {
            "username": self.username,
            "role": self.role,
            "address": self.address,
            "tracker": self.tracker,
            "peers": len(self.chat.peers()),
        }
