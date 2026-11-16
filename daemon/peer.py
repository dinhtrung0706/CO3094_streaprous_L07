"""Peer node: use one selected transport for discovery and chat."""

import logging
import secrets
import socket
import sqlite3
import threading
import time
from collections import deque

from . import protocol
from .peer_crypto import canonical, decode_key, encode_key
from .chat import CHAT_USERNAME_RE
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
    """Use UDP or raw Session for both discovery and peer messages."""

    def __init__(
        self,
        chat,
        *,
        username,
        role="peer",
        listen_ip="0.0.0.0",
        udp_port=0,
        peer_transport="raw",
        raw_port=9200,
        public_ip=None,
        tracker=None,
        heartbeat=HEARTBEAT_SECONDS,
        tick=TICK_SECONDS,
        capacity=DEFAULT_CAPACITY,
        rto=DEFAULT_RTO,
        http_port=0,
        auth=None,
        follow_tracker_keys=False,
        require_claimed_identity=False,
        file_store=None,
    ):
        if role not in ("peer", "tracker"):
            raise ValueError("role must be 'peer' or 'tracker'")
        if peer_transport not in ("udp", "raw"):
            raise ValueError("peer_transport must be udp or raw")
        if peer_transport == "raw" and not 1 <= raw_port <= 65535:
            raise ValueError("raw_port must be between 1 and 65535")
        self.chat = chat
        self.username = username
        self.role = role
        self.tracker = tuple(tracker) if tracker else None
        self.heartbeat_seconds = float(heartbeat)
        self.tick_seconds = float(tick)
        self.capacity = int(capacity)
        self.rto = float(rto)
        self.http_port = int(http_port or 0)
        self.peer_transport = peer_transport
        self.raw_port = int(raw_port) if peer_transport == "raw" else 0
        if auth is None:
            raise ValueError("X25519 peer identity is required")
        self.auth = auth
        self.file_store = file_store
        self.follow_tracker_keys = bool(follow_tracker_keys)
        self.require_claimed_identity = bool(require_claimed_identity)
        self.crypto = auth.peer_crypto
        self._challenges = {}
        self._pending_tracker_key = None
        self._last_hello = None
        self._announced_peers = set()
        if public_ip:
            self.public_ip = public_ip
        elif listen_ip not in ("0.0.0.0", "::", ""):
            self.public_ip = listen_ip
        elif peer_transport == "raw":
            raise ValueError("raw mode requires --public-ip for the raw socket")
        else:
            self.public_ip = _detect_local_ip()

        if peer_transport == "raw":
            from .raw_chat import RawFrameNode
            self.node = RawFrameNode(self._on_frame, self.public_ip,
                                     self.raw_port, self.public_ip)
        else:
            self.node = UdpNode(self._on_frame, listen_ip, udp_port)
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
        if self.peer_transport == "raw":
            self.chat.prune_peers()
            return
        now = time.monotonic()
        for address, channel in list(self._channels.items()):
            retransmissions = channel.poll(now)
            for frame in retransmissions:
                self._send(frame, address)
            if channel.dead:
                logger.warning("peer %s stopped responding", address)
                with self._lock:
                    self._channels.pop(address, None)
        for address in list(self._outbox):
            self._flush(address)
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
                # Only independently authenticated ACK frames may acknowledge
                # traffic; do not trust the mutable piggyback header.
                frame.ack = 0
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
        if self.peer_transport == "raw":
            self._send(protocol.Frame(protocol.MSG, payload=payload), address)
            self.stats["messages_sent"] += 1
            return
        while not self._stop.is_set():
            with self._lock:
                queue = self._outbox.setdefault(address, deque())
                if len(queue) < 128:
                    queue.append((protocol.MSG, payload))
                    self._flush(address)
                    return
            self._stop.wait(self.tick_seconds)
        raise OSError("P2P client stopped during file transfer")

    def send_hello(self):
        if self.tracker is None:
            return
        self._last_hello = {
            "username": self.username, "ip": self.public_ip,
            "port": self.public_port, "http_port": self.http_port,
            "public_key": self.crypto.public_text,
        }
        self._send(protocol.Frame.with_json(protocol.HELLO, self._last_hello),
                   self.tracker)

    def send_ping(self):
        if self.tracker is None:
            return
        tracker_key = self.crypto.pinned("tracker")
        if tracker_key is None:
            self.send_hello()
            return
        payload = {
            "username": self.username, "ip": self.public_ip,
            "port": self.public_port, "http_port": self.http_port,
            "timestamp": int(time.time()), "nonce": secrets.token_hex(8),
        }
        payload["mac"] = self.crypto.mac(tracker_key, "ping", canonical(payload))
        self._send(protocol.Frame.with_json(protocol.PING, payload), self.tracker)

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
        sent = 0
        for peer in targets:
            if self.crypto.pinned(peer["username"]) is None:
                continue
            payload = self._encode_payload(envelope, peer["username"])
            self._enqueue((peer["ip"], int(peer["port"])), payload)
            sent += 1
        return sent

    def _send_to_peer(self, username, envelope):
        address = self.chat.peer_address(username)
        if address is not None and self.crypto.pinned(username) is not None:
            self._enqueue(address, self._encode_payload(envelope, username))

    def announce_channel(self, channel, recipient=None):
        """Share a public channel with discovered peers."""
        if channel["is_private"] or channel["owner_name"].casefold() != self.username.casefold():
            return
        envelope = {
            "v": protocol.VERSION, "kind": "announce",
            "id": protocol.new_message_id(), "channel": channel["id"],
            "channel_name": channel["name"], "is_private": False,
            "owner": channel["owner_name"], "from": self.username,
            "to": recipient or "*", "body": "", "ts": channel["created_at"],
        }
        if recipient is None:
            self._broadcast_envelope(envelope)
        else:
            self._send_to_peer(recipient, envelope)

    def _announce_channels_to(self, username):
        owner = self.chat.database.get_user(self.username)
        if owner is None:
            return
        for channel in self.chat.list_channels(owner["id"]):
            if channel["owner_name"].casefold() == self.username.casefold():
                self.announce_channel(channel, recipient=username)

    def _relay(self, envelope):
        # Chat messages are never relayed through the tracker.
        return None

    def _post_channel_message(self, channel_id, sender, body, **kwargs):
        owner = self.chat.database.get_user(sender)
        if owner is None:
            raise PermissionError("authenticated local user is required")
        self.chat.require_member(channel_id, owner)
        return self.chat.post_message(channel_id, sender, body, **kwargs)

    def broadcast(self, channel_id, body, *, message_id=None, sender=None):
        """Store a message locally and fan it out to discovered peers."""
        sender = sender or self.username
        if sender != self.username:
            raise PermissionError("this node can only send as its own user")
        message, created = self._post_channel_message(
            channel_id, sender, body, message_id=message_id
        )
        if created:
            envelope = self._channel_envelope(message)
            self._broadcast_envelope(envelope)
        return message

    def send_direct(self, channel_id, recipient, body, *, message_id=None,
                    sender=None):
        """Store a channel message addressed to one peer."""
        sender = sender or self.username
        if sender != self.username:
            raise PermissionError("this node can only send as its own user")
        message, created = self._post_channel_message(
            channel_id, sender, body, message_id=message_id, recipient=recipient
        )
        if not created:
            return message
        envelope = self._channel_envelope(message)
        envelope["to"] = recipient
        self._send_to_peer(recipient, envelope)
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
        self._send_to_peer(username, envelope)

    def send_direct_message(self, message):
        envelope = {
            "v": protocol.VERSION, "kind": "direct", "id": message["id"],
            "channel": "direct", "from": message["sender"],
            "to": message["recipient"], "body": message["body"],
            "ts": message["created_at"],
        }
        self._send_to_peer(message["recipient"], envelope)

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

    def send_file(self, recipient, filename, contents):
        """Store a byte payload and send it using the existing P2P file frames."""
        if self.file_store is None:
            raise ValueError("file transfer is unavailable")
        transfer_id = protocol.new_message_id()
        self.file_store.save_sent(transfer_id, self.username, recipient,
                                  filename, contents)
        self.send_saved_file(recipient, transfer_id)
        return {"id": transfer_id, "name": self.file_store.clean_name(filename),
                "size": len(contents), "recipient": recipient, "queued": True}

    def send_saved_file(self, recipient, transfer_id):
        """Read file chunks from disk, never queue the whole file in memory."""
        import base64
        import json

        if self.file_store is None:
            raise ValueError("file transfer is unavailable")
        metadata = self.file_store.get(transfer_id, self.username)
        if metadata is None or metadata["sender"] != self.username or metadata["recipient"] != recipient:
            raise ValueError("file transfer not found")
        address = self.chat.peer_address(recipient)
        if address is None or self.crypto.pinned(recipient) is None:
            raise ValueError("recipient is not online or trusted")
        progress = -1
        self.file_store.update_status(transfer_id, "sending")
        for index, total, chunk in self.file_store.iter_send_chunks(
                transfer_id, metadata["size"]):
            if self._stop.is_set():
                raise OSError("P2P client stopped during file transfer")
            body = json.dumps({
                "transfer": transfer_id, "name": metadata["name"],
                "size": metadata["size"], "total": total, "index": index,
                "sha256": metadata["sha256"],
                "data": base64.b64encode(chunk).decode("ascii"),
            }, separators=(",", ":"))
            envelope = {
                "v": protocol.VERSION, "kind": "file", "id": protocol.new_message_id(),
                "channel": "direct", "from": self.username, "to": recipient,
                "body": body, "ts": int(time.time()),
            }
            self._enqueue(address, self._encode_payload(envelope, recipient))
            current = (index + 1) * 100 // total
            if current != progress:
                self.file_store.set_progress(transfer_id, current)
                progress = current
        self.file_store.update_status(transfer_id, "queued")
        return metadata

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

    def _encode_payload(self, envelope, recipient):
        sealed = self.crypto.seal(recipient, envelope)
        return protocol.encode_message(self.username, envelope["id"], sealed)

    def _decode_payload(self, payload):
        sender, message_id, sealed = protocol.decode_message(payload)
        try:
            envelope = protocol.validate_envelope(
                self.crypto.open(sender, self.username, sealed)
            )
        except (ValueError, TypeError) as exc:
            raise protocol.ProtocolError("message authentication failed") from exc
        if (envelope["from"].casefold() != sender.casefold() or
                envelope["id"] != message_id):
            raise protocol.ProtocolError("sender or message id mismatch")
        return envelope, sender

    def _matches_peer(self, username, address):
        record = self.chat.peer_record(username)
        return (record is not None and
                (record["ip"], int(record["port"])) == address)

    @staticmethod
    def _ack_body(frame, sender):
        return canonical({"ack": frame.ack, "window": frame.window,
                          "sender": sender})

    def _ack_frame(self, address, channel):
        username = next((peer["username"] for peer in self.chat.peers()
                         if (peer["ip"], int(peer["port"])) == address), None)
        if username is None:
            return None
        frame = channel.acknowledgement()
        body = self._ack_body(frame, self.username)
        public = self.crypto.pinned(username)
        if public is None:
            return None
        frame.payload = canonical({
            "sender": self.username,
            "mac": self.crypto.mac(public, "ack", body),
        })
        return frame

    def _valid_ack(self, frame, address):
        try:
            data = frame.json_payload()
            sender = data["sender"]
            if not self._matches_peer(sender, address):
                return False
            public = self.crypto.pinned(sender)
            body = self._ack_body(frame, sender)
            return public is not None and self.crypto.verify_mac(
                public, "ack", body, data.get("mac")
            )
        except (ValueError, KeyError, TypeError):
            return False

    def _on_frame(self, frame, address):
        if address == self.node.address:
            return
        if self.peer_transport == "raw":
            if frame.seq or frame.type == protocol.MSG_ACK:
                self.stats["rejected"] += 1
                return
            self._dispatch(frame, address)
            return
        if frame.seq:
            if frame.type != protocol.MSG:
                self.stats["rejected"] += 1
                return
            try:
                _envelope, sender = self._decode_payload(frame.payload)
            except protocol.ProtocolError:
                self.stats["rejected"] += 1
                return
            if not self._matches_peer(sender, address):
                self.stats["rejected"] += 1
                return
            # An authenticated ciphertext does not authenticate mutable
            # header ACKs and advertised window sizes.
            frame.ack = 0
            frame.window = 0
            with self._lock:
                channel = self._channel(address)
                delivered = channel.receive(frame)
                acknowledgement = self._ack_frame(address, channel)
            if acknowledgement is not None:
                self._send(acknowledgement, address)
            for inner in delivered:
                self._dispatch(inner, address)
            return
        if frame.type == protocol.MSG_ACK:
            if not self._valid_ack(frame, address):
                self.stats["rejected"] += 1
                return
            with self._lock:
                channel = self._channels.get(address)
                if channel is not None:
                    channel.receive(frame)
            return
        self._dispatch(frame, address)

    def _dispatch(self, frame, address):
        handler = {
            protocol.HELLO: self._handle_hello,
            protocol.HELLO_CHALLENGE: self._handle_hello_challenge,
            protocol.HELLO_PROOF: self._handle_hello_proof,
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

    def _handle_hello(self, frame, address):
        if self.role != "tracker":
            return
        payload = frame.json_payload()
        username = payload.get("username")
        if (not isinstance(username, str) or
                not CHAT_USERNAME_RE.fullmatch(username) or
                username.casefold() == self.username.casefold()):
            self.stats["rejected"] += 1
            return
        public = decode_key(payload.get("public_key"), 32)
        pinned = self.crypto.pinned(username)
        if self.require_claimed_identity and (
                self.chat.database.get_user(username) is None or pinned is None):
            self.stats["rejected"] += 1
            return
        if pinned is not None and pinned != public:
            self.stats["rejected"] += 1
            self._send(protocol.Frame.with_json(
                protocol.AUTH_ERR, {"error": "peer key changed"}
            ), address)
            return
        challenge = secrets.token_hex(16)
        with self._lock:
            now = time.monotonic()
            self._challenges = {
                addr: value for addr, value in self._challenges.items()
                if now - value[3] < 30.0
            }
            self._challenges[address] = (username, public, payload, now, challenge)
        self._send(protocol.Frame.with_json(protocol.HELLO_CHALLENGE, {
            "public_key": self.crypto.public_text, "challenge": challenge,
        }), address)

    def _handle_hello_challenge(self, frame, address):
        if self.role != "peer" or address != self.tracker:
            return
        payload = frame.json_payload()
        public = decode_key(payload.get("public_key"), 32)
        pinned = self.crypto.pinned("tracker")
        if pinned is not None and pinned != public:
            self.stats["rejected"] += 1
            return
        challenge = payload.get("challenge")
        if not isinstance(challenge, str) or len(challenge) != 32:
            self.stats["rejected"] += 1
            return
        if self._last_hello is None:
            self.stats["rejected"] += 1
            return
        self._pending_tracker_key = public
        self._send(protocol.Frame.with_json(protocol.HELLO_PROOF, {
            "username": self.username,
            "challenge": challenge,
            "mac": self.crypto.mac(public, "hello-proof",
                                   challenge.encode() + canonical(self._last_hello)),
        }), address)

    def _handle_hello_proof(self, frame, address):
        if self.role != "tracker":
            return
        with self._lock:
            pending = self._challenges.pop(address, None)
        if pending is None:
            self.stats["rejected"] += 1
            return
        username, public, hello, created, challenge = pending
        data = frame.json_payload()
        if (time.monotonic() - created > 30.0 or
                data.get("username") != username or
                data.get("challenge") != challenge or
                not self.crypto.verify_mac(
                    public, "hello-proof", challenge.encode() + canonical(hello),
                    data.get("mac")
                )):
            self.stats["rejected"] += 1
            return
        if not self.crypto.pin(username, public):
            self.stats["rejected"] += 1
            return
        ip = hello.get("ip") or address[0]
        self.chat.register_peer(username, str(ip), hello.get("port"),
                                hello.get("http_port", 0))
        self.stats["hellos"] += 1
        self._send(self._peer_list_frame(protocol.HELLO_ACK, public), address)

    def _handle_ping(self, frame, address):
        if self.role != "tracker":
            return
        payload = frame.json_payload()
        username = payload.get("username")
        public = self.crypto.pinned(username)
        try:
            timestamp = int(payload.get("timestamp"))
        except (ValueError, TypeError):
            self.stats["rejected"] += 1
            return
        nonce = payload.get("nonce")
        if (public is None or not isinstance(nonce, str) or len(nonce) != 16 or
                abs(int(time.time()) - timestamp) > 60):
            self.stats["rejected"] += 1
            return
        body = dict(payload)
        mac = body.pop("mac", None)
        if not self.crypto.verify_mac(public, "ping", canonical(body), mac):
            self.stats["rejected"] += 1
            return
        ip = payload.get("ip") or address[0]
        port = payload.get("port") or self._port_for(username)
        if port:
            self.chat.register_peer(username, str(ip), port,
                                    payload.get("http_port", 0))
        self.stats["pings"] += 1
        self._send(self._peer_list_frame(protocol.PONG, public), address)

    def _port_for(self, username):
        for peer in self.chat.peers():
            if peer["username"].lower() == str(username).lower():
                return int(peer["port"])
        return 0

    def _peer_list_frame(self, frame_type, recipient_public):
        peers = []
        for peer in self.chat.peers(exclude=self.username):
            key = self.crypto.pinned(peer["username"])
            if key is not None:
                peers.append({
                    "username": peer["username"], "ip": peer["ip"],
                    "port": peer["port"], "http_port": peer["http_port"],
                    "public_key": encode_key(key),
                })
        payload = {"peers": peers, "public_key": self.crypto.public_text,
                   "timestamp": int(time.time()), "nonce": secrets.token_hex(8)}
        payload["mac"] = self.crypto.mac(recipient_public, "peer-list",
                                         canonical(payload))
        return protocol.Frame.with_json(frame_type, payload)

    def _handle_peer_list(self, frame, address):
        if self.role != "peer" or address != self.tracker:
            return
        payload = frame.json_payload()
        public = decode_key(payload.get("public_key"), 32)
        expected = self.crypto.pinned("tracker") or self._pending_tracker_key
        if expected is None or public != expected:
            self.stats["rejected"] += 1
            return
        try:
            timestamp = int(payload["timestamp"])
        except (TypeError, ValueError, KeyError):
            self.stats["rejected"] += 1
            return
        body = dict(payload)
        mac = body.pop("mac", None)
        if (abs(int(time.time()) - timestamp) > 60 or
                not self.crypto.verify_mac(public, "peer-list", canonical(body), mac)):
            self.stats["rejected"] += 1
            return
        if not self.crypto.pin("tracker", public):
            self.stats["rejected"] += 1
            return
        self._pending_tracker_key = None
        for peer in payload.get("peers") or []:
            if not isinstance(peer, dict):
                continue
            username = peer.get("username")
            if not isinstance(username, str) or username.casefold() == self.username.casefold():
                continue
            try:
                key = decode_key(peer.get("public_key"), 32)
                if not self.crypto.pin(username, key):
                    if self.follow_tracker_keys:
                        self.chat.database.authorize_peer_key(username, key)
                    else:
                        self.stats["rejected"] += 1
                        continue
                self.chat.register_peer(username, peer.get("ip", ""),
                                        peer.get("port"), peer.get("http_port", 0))
                if username.casefold() not in self._announced_peers:
                    self._announced_peers.add(username.casefold())
                    self._announce_channels_to(username)
            except (ValueError, TypeError):
                self.stats["malformed"] += 1

    def _handle_message(self, frame, address):
        try:
            envelope, username = self._decode_payload(frame.payload)
        except protocol.ProtocolError:
            self.stats["rejected"] += 1
            return
        if not self._matches_peer(username, address):
            self.stats["rejected"] += 1
            return
        recipient = envelope.get("to") or "*"
        if recipient != "*" and recipient.casefold() != self.username.casefold():
            return

        kind = envelope.get("kind", "channel")
        if kind == "file":
            self._receive_file(envelope)
        elif kind == "direct":
            _message, created = self.chat.receive_direct_message(
                envelope["from"], recipient, envelope["body"],
                message_id=envelope["id"], ts=envelope["ts"],
            )
            self.stats["messages_received" if created else "duplicates"] += 1
        elif kind == "announce":
            self._receive_announcement(envelope, username)
        elif kind == "invite":
            self._receive_invite(envelope)
        else:
            self._receive_channel_message(envelope, recipient)

    def _receive_file(self, envelope):
        if self.file_store is None:
            self.stats["rejected"] += 1
            return
        try:
            if self.file_store.receive_chunk(envelope):
                self.stats["messages_received"] += 1
        except (ValueError, OSError) as exc:
            logger.warning("file chunk rejected: %s", exc)
            self.stats["rejected"] += 1

    def _receive_announcement(self, envelope, username):
        name = envelope.get("channel_name")
        owner = envelope.get("owner")
        if (envelope.get("is_private") or not isinstance(name, str)
                or not name or not isinstance(owner, str)
                or owner.casefold() != username.casefold()):
            self.stats["rejected"] += 1
            return
        channel = self.chat.database.get_channel(envelope["channel"])
        if channel is None:
            try:
                channel = self.chat.database.create_channel(
                    envelope["channel"], name, None, envelope["ts"],
                    is_private=False, owner_name=owner,
                )
            except sqlite3.IntegrityError:
                self.stats["rejected"] += 1
                return
        if (channel["name"] != name or channel["is_private"]
                or channel["owner_name"].casefold() != owner.casefold()):
            self.stats["rejected"] += 1

    def _receive_invite(self, envelope):
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

    def _receive_channel_message(self, envelope, recipient):
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
        # An unauthenticated BYE must not reset an established channel.
        return

    def _handle_auth_error(self, frame, address):
        logger.warning("peer %s rejected our identity", address)

    def status(self):
        return {
            "username": self.username,
            "role": self.role,
            "address": self.address,
            "tracker": self.tracker,
            "peer_transport": self.peer_transport,
            "raw_port": self.raw_port,
            "peers": len(self.chat.peers()),
        }
