"""Loopback API for a user's direct P2P chat client."""

import base64
import json
import os
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import Request, urlopen

from .auth import AuthService
from .chat import ChatService
from .database import Database
from .peer import PeerNode
from .file_transfer import FileStore, MAX_FILE_BYTES, UPLOAD_CHUNK_BYTES


class PeerClient:
    """Authenticate a local browser user and operate their P2P connection."""

    def __init__(self, central_url, tracker, db_path, public_ip,
                 sar_mode, client_port, heartbeat):
        self.central_url = central_url.rstrip("/")
        parsed = urlsplit(self.central_url)
        if parsed.scheme != "http" or not parsed.netloc or parsed.path:
            raise ValueError("--central-url must be an HTTP origin")
        if tracker is None:
            raise ValueError("--tracker is required for the peer agent")
        self.origin = self.central_url
        self.tracker = tracker
        self.database = Database(db_path)
        self.auth = AuthService(self.database)
        self.chat = ChatService(self.database)
        self.files = FileStore(db_path)
        self.public_ip = public_ip
        self.peer_transport = sar_mode
        self.client_port = client_port
        self.heartbeat = heartbeat
        self.username = None
        self.token = None
        self.peer = None
        self.lock = threading.RLock()

    def attach(self, ticket):
        """Redeem a central login ticket before enabling a local peer."""
        if not isinstance(ticket, str) or not ticket:
            raise ValueError("missing login ticket")
        payload = json.dumps({
            "ticket": ticket, "public_key": self.auth.peer_crypto.public_text,
        }).encode("utf-8")
        request = Request(
            self.central_url + "/api/agent/claim", data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urlopen(request, timeout=6.0) as response:
            username = json.loads(response.read().decode("utf-8"))["username"]
        with self.lock:
            if self.peer is not None and self.username != username:
                self.peer.stop()
                self.peer = None
            if self.database.get_user(username) is None:
                self.database.create_user(
                    username, os.urandom(32), os.urandom(16), 1, int(time.time())
                )
            if self.peer is None:
                self.peer = PeerNode(
                    self.chat, username=username, role="peer",
                    tracker=self.tracker, peer_transport=self.peer_transport,
                    raw_port=self.client_port if self.peer_transport == "raw" else 9200,
                    udp_port=self.client_port if self.peer_transport == "udp" else 0,
                    public_ip=self.public_ip, heartbeat=self.heartbeat,
                    auth=self.auth, follow_tracker_keys=True,
                    file_store=self.files,
                )
                self.peer.start()
            self.username = username
            self.token = secrets.token_urlsafe(32)
            return {"username": username, "token": self.token}

    def send_file(self, recipient, name, encoded):
        if not isinstance(encoded, str) or len(encoded) > (4 * 1024 * 1024 * 4 // 3 + 32):
            raise ValueError("use chunked uploads for files over 4 MiB")
        try:
            contents = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as exc:
            raise ValueError("invalid file encoding") from exc
        if len(contents) > 4 * 1024 * 1024:
            raise ValueError("use chunked uploads for files over 4 MiB")
        with self.lock:
            if self.peer is None:
                raise ValueError("P2P client is not attached")
            if not isinstance(recipient, str) or recipient.casefold() == self.username.casefold():
                raise ValueError("invalid file recipient")
            return self.peer.send_file(recipient, name, contents)

    def begin_file(self, recipient, name, size):
        with self.lock:
            if self.peer is None:
                raise ValueError("P2P client is not attached")
            if not isinstance(recipient, str) or recipient.casefold() == self.username.casefold():
                raise ValueError("invalid file recipient")
            if (self.chat.peer_address(recipient) is None or
                    self.peer.crypto.pinned(recipient) is None):
                raise ValueError("recipient is not online or trusted")
            return self.files.start_upload(self.username, recipient, name, size)

    def append_file(self, recipient, transfer_id, index, encoded):
        return self.files.append_upload(transfer_id, self.username,
                                        recipient, index, encoded)

    def finish_file(self, recipient, transfer_id):
        record = self.files.finish_upload(transfer_id, self.username, recipient)
        peer = self.peer
        thread = threading.Thread(
            target=self._transmit_file, args=(peer, recipient, transfer_id),
            name="peer-file-send", daemon=True,
        )
        thread.start()
        return record

    def _transmit_file(self, peer, recipient, transfer_id):
        try:
            peer.send_saved_file(recipient, transfer_id)
        except Exception as exc:
            print("[PeerClient] Warning: file send failed: {}".format(exc))
            self.files.update_status(transfer_id, "failed")

    def authorized(self, token):
        with self.lock:
            return (isinstance(token, str) and self.token is not None
                    and secrets.compare_digest(token, self.token))

    def list_messages(self, recipient, since, limit):
        return self.chat.direct_messages(self.username, recipient, since, limit)

    def send_message(self, recipient, body, message_id):
        with self.lock:
            if recipient.casefold() == self.username.casefold():
                raise ValueError("cannot message yourself")
            if self.chat.peer_address(recipient) is None:
                raise ValueError("recipient is not currently discovered")
            if self.peer.crypto.pinned(recipient) is None:
                raise ValueError("recipient identity is not yet trusted")
            return self.peer.send_direct_chat(
                recipient, body, message_id=message_id, sender=self.username
            )


class _ThreadedServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class AgentHandler(BaseHTTPRequestHandler):
    """JSON endpoints, never exposed beyond the local loopback interface."""

    def _origin_valid(self):
        return self.headers.get("Origin") == self.server.agent.origin

    def _reply(self, status, value):
        body = json.dumps(value).encode("utf-8")
        self.send_response(status)
        if self._origin_valid():
            self.send_header("Access-Control-Allow-Origin", self.server.agent.origin)
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.send_header("Vary", "Origin")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        if not self._origin_valid():
            self._reply(403, {"error": "origin not allowed"})
            return
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", self.server.agent.origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Agent-Token")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Vary", "Origin")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _authorized(self):
        if not self._origin_valid():
            self._reply(403, {"error": "origin not allowed"})
            return False
        if not self.server.agent.authorized(self.headers.get("X-Agent-Token")):
            self._reply(401, {"error": "local peer agent is not attached"})
            return False
        return True

    def _data(self):
        length = int(self.headers.get("Content-Length", "0"))
        if self.path.startswith("/api/files/uploads/"):
            maximum = UPLOAD_CHUNK_BYTES * 4 // 3 + 4096
        elif self.path.startswith("/api/files/"):
            maximum = 4 * 1024 * 1024 * 4 // 3 + 4096
        else:
            maximum = 65536
        if length <= 0 or length > maximum:
            raise ValueError("invalid request body length")
        value = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON object required")
        return value

    def do_GET(self):
        path = urlsplit(self.path)
        if path.path == "/identity":
            if not self._origin_valid():
                self._reply(403, {"error": "origin not allowed"})
                return
            self._reply(200, {"public_key": self.server.agent.auth.peer_crypto.public_text})
            return
        if not self._authorized():
            return
        agent = self.server.agent
        if path.path == "/status":
            self._reply(200, {"username": agent.username,
                              "peer_transport": agent.peer_transport,
                              "peers": agent.chat.peers(exclude=agent.username)})
            return
        if path.path == "/api/files":
            peer = parse_qs(path.query).get("peer", [""])[0]
            if not peer:
                self._reply(400, {"error": "peer is required"})
                return
            self._reply(200, {"files": agent.files.list(agent.username, peer)})
            return
        if path.path.startswith("/api/files/"):
            transfer_id = path.path[len("/api/files/"):]
            try:
                record = agent.files.get(transfer_id, agent.username)
            except ValueError:
                record = None
            if record is None:
                self._reply(404, {"error": "file not found"})
                return
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", agent.origin)
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.send_header("Vary", "Origin")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", 'attachment; filename="{}"'.format(record["name"]))
            self.send_header("Content-Length", str(record["size"]))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            with open(agent.files.download_path(transfer_id), "rb") as source:
                for block in iter(lambda: source.read(65536), b""):
                    self.wfile.write(block)
            return
        if path.path.startswith("/api/direct-messages/"):
            username = unquote(path.path[len("/api/direct-messages/"):])
            args = parse_qs(path.query)
            try:
                rows = agent.list_messages(
                    username, max(0, int(args.get("since", ["0"])[0])),
                    max(1, min(500, int(args.get("limit", ["100"])[0]))),
                )
                since = rows[-1]["seq"] if rows else int(args.get("since", ["0"])[0])
                self._reply(200, {"messages": rows, "next_since": since})
            except (ValueError, TypeError) as exc:
                self._reply(400, {"error": str(exc)})
            return
        self._reply(404, {"error": "not found"})

    def do_POST(self):
        if not self._origin_valid():
            self._reply(403, {"error": "origin not allowed"})
            return
        if self.path != "/attach" and not self._authorized():
            return
        try:
            data = self._data()
            if self.path == "/attach":
                self._reply(200, self.server.agent.attach(data.get("ticket")))
                return
            if self.path.startswith("/api/files/uploads/"):
                parts = self.path[len("/api/files/uploads/"):].split("/")
                if len(parts) == 1:
                    result = self.server.agent.begin_file(
                        unquote(parts[0]), data.get("name"), data.get("size"))
                    self._reply(201, {"file": result})
                    return
                if len(parts) == 3 and parts[2] == "complete":
                    result = self.server.agent.finish_file(unquote(parts[0]), parts[1])
                    self._reply(202, {"file": result})
                    return
                raise ValueError("invalid upload URL")
            if self.path.startswith("/api/files/"):
                recipient = unquote(self.path[len("/api/files/"):])
                result = self.server.agent.send_file(
                    recipient, data.get("name"), data.get("data"))
                self._reply(202, {"file": result})
                return
            if self.path.startswith("/api/direct-messages/"):
                recipient = unquote(self.path[len("/api/direct-messages/"):])
                message = self.server.agent.send_message(
                    recipient, data.get("body", ""), data.get("client_id")
                )
                self._reply(201, {"message": message})
                return
            self._reply(404, {"error": "not found"})
        except (ValueError, TypeError) as exc:
            self._reply(400, {"error": str(exc)})
        except (HTTPError, URLError, OSError) as exc:
            self._reply(503, {"error": "central registration failed: " + str(exc)})

    def do_PUT(self):
        if not self._authorized():
            return
        try:
            prefix = "/api/files/uploads/"
            if not self.path.startswith(prefix):
                self._reply(404, {"error": "not found"})
                return
            parts = self.path[len(prefix):].split("/")
            if len(parts) != 2:
                raise ValueError("invalid upload URL")
            data = self._data()
            written = self.server.agent.append_file(
                unquote(parts[0]), parts[1], data.get("index"), data.get("data"))
            self._reply(200, {"received": written})
        except (ValueError, TypeError) as exc:
            self._reply(400, {"error": str(exc)})
        except OSError as exc:
            self._reply(503, {"error": str(exc)})

    def log_message(self, format, *args):
        pass


def run_client(central_url, tracker, db_path, public_ip, sar_mode,
               client_port, http_port, heartbeat):
    """Serve local browser requests and establish direct peer connections."""
    agent = PeerClient(central_url, tracker, db_path, public_ip,
                       sar_mode, client_port, heartbeat)
    server = _ThreadedServer(("127.0.0.1", http_port), AgentHandler)
    server.agent = agent
    print("[PeerClient] Listening at 127.0.0.1:{}".format(http_port))
    try:
        server.serve_forever()
    finally:
        if agent.peer is not None:
            agent.peer.stop()
        server.server_close()
