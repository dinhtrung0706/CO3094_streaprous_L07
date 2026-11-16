"""Chat app: JSON control plane plus the UDP peer node."""

import base64
import functools
import json
import os
import secrets
import threading
import time

from daemon.auth import install_auth
from daemon.chat import ChatService
from daemon.file_transfer import FileStore, UPLOAD_CHUNK_BYTES
from daemon.errors import BadRequest, HTTPError
from daemon.peer import PeerNode
from daemon.peer_crypto import decode_key
from daemon.response import Response
from daemon.streaprous import StreapRous

DEFAULT_DB = os.path.join("data", "chat.sqlite3")


def _api(view):
    """Convert domain errors into JSON responses instead of tracebacks."""

    @functools.wraps(view)
    def wrapper(request):
        try:
            return view(request)
        except HTTPError as exc:
            return Response().json({"error": exc.message}, status=exc.status_code)
        except ValueError as exc:
            return Response().json({"error": str(exc)}, status=400)

    return wrapper


def _json_object(request):
    if not request.body:
        return {}
    data = request.json
    if not isinstance(data, dict):
        raise BadRequest("request body must be a JSON object")
    return data


def _since(request):
    raw = (request.query.get("since") or ["0"])[0]
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise BadRequest("since must be an integer") from exc
    if value < 0:
        raise BadRequest("since must not be negative")
    return value


def _limit(request):
    raw = (request.query.get("limit") or ["100"])[0]
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise BadRequest("limit must be an integer") from exc
    return max(1, min(value, 500))


def _peer_authorized(auth, request):
    username = request.headers.get("X-StreapRous-Peer", "")
    if not auth.verify_peer_request(
        username,
        request.headers.get("X-StreapRous-Time", ""),
        request.headers.get("X-StreapRous-Signature", ""),
        request.body,
        request.path,
    ):
        return False
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    claimed_username = payload.get("from") or payload.get("username")
    return claimed_username is None or (
        isinstance(claimed_username, str)
        and claimed_username.casefold() == username.casefold()
    )


def build_chatapp(
    *,
    mode="peer",
    tracker=None,
    db_path=None,
    udp_port=0,
    peer_transport="raw",
    raw_port=9200,
    public_ip=None,
    username=None,
    heartbeat=5.0,
    install_peer=True,
    password_iterations=None,
    http_port=0,
):
    """Create the app, wire services and register every chat route."""
    mode = (mode or "peer").lower()
    if mode not in ("peer", "tracker", "local", "server"):
        raise ValueError("mode must be 'peer', 'tracker', 'local' or 'server'")
    app = StreapRous()
    auth = install_auth(
        app,
        db_path or DEFAULT_DB,
        password_iterations=password_iterations,
    )
    chat = ChatService(auth.database)
    channel_files = FileStore(db_path or DEFAULT_DB) if mode == "server" else None
    app._streaprous_chat = chat
    app._streaprous_auth = auth

    peer = None
    if install_peer and mode != "local":
        node_user = "tracker" if mode in ("tracker", "server") else username
        if not node_user:
            raise ValueError("username is required for peer mode")
        peer = PeerNode(
            chat,
            username=node_user,
            role="tracker" if mode in ("tracker", "server") else "peer",
            udp_port=udp_port,
            peer_transport=peer_transport,
            raw_port=raw_port,
            public_ip=public_ip,
            tracker=tracker,
            heartbeat=heartbeat,
            http_port=http_port,
            auth=auth,
            require_claimed_identity=(mode == "server"),
        )
        peer.start()
    app._streaprous_peer = peer
    app._streaprous_mode = mode
    app._streaprous_peer_transport = peer_transport

    if mode == "server":
        @app.route("/", methods=["GET"])
        def home(request):
            return Response().redirect("/login.html")

    @app.route("/api/status", methods=["GET"])
    def status(request):
        return {"mode": mode, "peer_transport": peer_transport if peer else None}

    agent_tickets = {}
    agent_ticket_lock = threading.Lock()

    @app.route("/api/agent/ticket", methods=["POST"], auth=True)
    @_api
    def issue_agent_ticket(request):
        if mode != "server":
            raise BadRequest("agent tickets require central server mode")
        data = _json_object(request)
        try:
            public = decode_key(data.get("public_key"), 32)
        except ValueError:
            raise BadRequest("invalid peer public key")
        ticket = secrets.token_urlsafe(32)
        with agent_ticket_lock:
            agent_tickets[ticket] = (request.user["username"], time.time() + 60,
                                     public)
        return {"ticket": ticket}

    @app.route("/api/agent/claim", methods=["POST"])
    @_api
    def claim_agent_ticket(request):
        if mode != "server":
            raise BadRequest("agent registration requires central server mode")
        data = _json_object(request)
        ticket = data.get("ticket")
        if not isinstance(ticket, str):
            raise BadRequest("invalid agent ticket")
        try:
            public = decode_key(data.get("public_key"), 32)
        except ValueError:
            raise BadRequest("invalid peer public key")
        with agent_ticket_lock:
            identity = agent_tickets.pop(ticket, None)
        if identity is None or identity[1] < time.time() or identity[2] != public:
            return Response().json({"error": "expired or invalid ticket"}, status=401)
        username = identity[0]
        if auth.database.get_user(username) is None:
            return Response().json({"error": "user no longer exists"}, status=401)
        auth.database.authorize_peer_key(username, public)
        return {"username": username}

    @app.route("/api/channels", methods=["GET"], auth=True)
    @_api
    def list_channels(request):
        return {"channels": chat.list_channels(request.user["id"])}

    @app.route("/api/channels", methods=["POST"], auth=True)
    @_api
    def create_channel(request):
        data = _json_object(request)
        try:
            channel = chat.create_channel(
                data.get("name", ""),
                request.user,
                is_private=data.get("is_private", False),
            )
        except BadRequest:
            raise
        except ValueError:
            return Response().json(
                {"error": "channel already exists"}, status=409
            )
        if peer is not None and mode != "server":
            peer.announce_channel(channel)
        return Response().json({"channel": channel}, status=201)

    @app.route("/api/channels/{channel_id}/join", methods=["POST"], auth=True)
    @_api
    def join_channel(request):
        channel = chat.require_channel(request.path_params["channel_id"])
        chat.join(channel["id"], request.user)
        return Response().no_content()

    @app.route("/api/channels/{channel_id}/leave", methods=["POST"], auth=True)
    @_api
    def leave_channel(request):
        channel = chat.require_channel(request.path_params["channel_id"])
        chat.leave(channel["id"], request.user)
        return Response().no_content()

    @app.route(
        "/api/channels/{channel_id}/messages", methods=["GET"], auth=True
    )
    @_api
    def list_messages(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        rows = chat.messages(
            channel_id,
            since=_since(request),
            limit=_limit(request),
            viewer=request.user["username"],
        )
        next_since = rows[-1]["seq"] if rows else _since(request)
        return {"messages": rows, "next_since": next_since}

    @app.route(
        "/api/channels/{channel_id}/messages", methods=["POST"], auth=True
    )
    @_api
    def post_message(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        data = _json_object(request)
        body = data.get("body", "")
        client_id = data.get("client_id")
        recipient = data.get("to") or "*"
        sender = request.user["username"]

        if mode == "server" and recipient != "*":
            raise BadRequest("direct messages must use the local peer agent")
        if peer is not None and mode != "server":
            try:
                if recipient == "*":
                    message = peer.broadcast(
                        channel_id, body, message_id=client_id, sender=sender
                    )
                else:
                    message = peer.send_direct(
                        channel_id,
                        recipient,
                        body,
                        message_id=client_id,
                        sender=sender,
                    )
            except PermissionError:
                message, _created = chat.post_message(
                    channel_id, sender, body, message_id=client_id,
                    recipient=recipient,
                )
        else:
            message, _created = chat.post_message(
                channel_id, sender, body, message_id=client_id,
                recipient=recipient,
            )
        return Response().json({"message": message}, status=201)

    @app.route("/api/channels/{channel_id}/files", methods=["GET"], auth=True)
    @_api
    def list_channel_files(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        if channel_files is None:
            raise BadRequest("central channel file storage is unavailable")
        return {"files": channel_files.list_channel(channel_id)}

    @app.route("/api/channels/{channel_id}/files", methods=["POST"], auth=True)
    @_api
    def begin_channel_file(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        if channel_files is None:
            raise BadRequest("central channel file storage is unavailable")
        data = _json_object(request)
        record = channel_files.start_upload(request.user["username"], channel_id,
                                            data.get("name"), data.get("size"))
        return Response().json({"file": record}, status=201)

    @app.route("/api/channels/{channel_id}/files/{transfer_id}/chunks",
               methods=["PUT"], auth=True)
    @_api
    def upload_channel_chunk(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        if channel_files is None:
            raise BadRequest("central channel file storage is unavailable")
        data = _json_object(request)
        written = channel_files.append_upload(
            request.path_params["transfer_id"], request.user["username"],
            channel_id, data.get("index"), data.get("data"))
        return {"received": written}

    @app.route("/api/channels/{channel_id}/files/{transfer_id}/complete",
               methods=["POST"], auth=True)
    @_api
    def finish_channel_file(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        if channel_files is None:
            raise BadRequest("central channel file storage is unavailable")
        record = channel_files.finish_upload(
            request.path_params["transfer_id"], request.user["username"],
            channel_id, status="received")
        return {"file": record}

    @app.route("/api/channels/{channel_id}/files/{transfer_id}/chunks",
               methods=["GET"], auth=True)
    @_api
    def download_channel_chunk(request):
        channel_id = request.path_params["channel_id"]
        chat.require_member(channel_id, request.user)
        if channel_files is None:
            raise BadRequest("central channel file storage is unavailable")
        transfer_id = request.path_params["transfer_id"]
        record = channel_files.get_channel(transfer_id, channel_id)
        if record is None:
            return Response().json({"error": "file not found"}, status=404)
        raw = (request.query.get("index") or ["0"])[0]
        try:
            index = int(raw)
        except (ValueError, TypeError) as exc:
            raise BadRequest("invalid chunk index") from exc
        total = max(1, (record["size"] + UPLOAD_CHUNK_BYTES - 1) // UPLOAD_CHUNK_BYTES)
        if index < 0 or index >= total:
            raise BadRequest("invalid chunk index")
        chunk = channel_files.read_chunk(transfer_id, index)
        return {"data": base64.b64encode(chunk).decode("ascii"), "index": index}

    @app.route("/api/peers/heartbeat", methods=["POST"], auth=True)
    @_api
    def peer_heartbeat(request):
        if mode == "server":
            raise BadRequest("central peers must register over the P2P transport")
        data = _json_object(request)
        chat.register_peer(
            request.user["username"],
            data.get("ip") or (request.remote_addr[0] if request.remote_addr else "127.0.0.1"),
            data.get("port") or 0,
            data.get("http_port") or 0,
        )
        return Response().no_content()

    @app.route(
        "/api/channels/{channel_id}/members", methods=["POST"], auth=True
    )
    @_api
    def invite_channel_member(request):
        data = _json_object(request)
        invited = data.get("username")
        if (mode == "server" and isinstance(invited, str) and
                auth.database.get_user(invited) is None):
            return Response().json({"error": "user not found"}, status=404)
        result = chat.invite_member(
            request.path_params["channel_id"], request.user,
            data.get("username"),
        )
        if peer is not None and mode != "server":
            peer.invite(result["channel_id"], result["username"])
        return Response().json({"invitation": result}, status=201)

    @app.route(
        "/api/direct-messages/{username}", methods=["GET"], auth=True
    )
    @_api
    def list_direct_messages(request):
        other = request.path_params["username"]
        if request.user["username"].casefold() not in (
            request.user["username"].casefold(), other.casefold()
        ):
            return Response().json({"error": "forbidden"}, status=403)
        if mode == "server":
            raise BadRequest("direct messages require the local peer agent")
        rows = chat.direct_messages(
            request.user["username"], other, _since(request), _limit(request)
        )
        return {"messages": rows,
                "next_since": rows[-1]["seq"] if rows else _since(request)}

    @app.route(
        "/api/direct-messages/{username}", methods=["POST"], auth=True
    )
    @_api
    def post_direct_message(request):
        other = request.path_params["username"]
        data = _json_object(request)
        if mode == "server":
            raise BadRequest("direct messages require the local peer agent")
        if peer is not None:
            message = peer.send_direct_chat(
                other, data.get("body", ""),
                message_id=data.get("client_id"),
                sender=request.user["username"],
            )
        else:
            message, _created = chat.post_direct_message(
                request.user["username"], other, data.get("body", ""),
                message_id=data.get("client_id"),
            )
        return Response().json({"message": message}, status=201)

    def peer_route(method, path):
        def route(request):
            if not _peer_authorized(auth, request):
                return Response().json({"error": "invalid peer signature"},
                                       status=401)
            data = _json_object(request)
            return method(request, data)
        app.route(path, methods=["POST"])(route)

    def peer_register(request, data):
        record = chat.register_peer(
            data.get("username"), data.get("ip"), data.get("port"),
            data.get("http_port", 0),
        )
        return Response().json({"peer": record}, status=201)

    def peer_connect(request, data):
        username_value = data.get("username")
        record = chat.peer_record(username_value)
        if record is None:
            return Response().json({"error": "peer not found"}, status=404)
        return {"peer": record}

    def peer_message(request, data):
        kind = data.get("kind", "channel")
        if kind == "direct":
            message, created = chat.receive_direct_message(
                data.get("from"), data.get("to"), data.get("body", ""),
                message_id=data.get("id"), ts=data.get("ts"),
            )
        elif kind == "invite":
            channel = chat.database.get_channel(data.get("channel"))
            if channel is None:
                channel = chat.database.create_channel(
                    data["channel"], data.get("channel_name"), None,
                    int(data.get("ts", time.time())),
                    is_private=data.get("is_private", True),
                    owner_name=data.get("owner") or data.get("from"),
                )
            owner_name = data.get("owner") or data.get("from")
            if channel["owner_name"].casefold() != owner_name.casefold():
                return Response().json({"error": "invalid channel owner"},
                                       status=403)
            chat.database.add_channel_member(
                channel["id"], data.get("to"), int(time.time())
            )
            message, created = {"channel_id": channel["id"],
                                "invitee": data.get("to")}, True
        else:
            channel = chat.database.get_channel(data.get("channel"))
            if channel is None:
                channel = chat.database.create_channel(
                    data["channel"],
                    data.get("channel_name") or "channel-" + data["channel"][:8],
                    None, int(data.get("ts", time.time())),
                    is_private=data.get("is_private", False),
                    owner_name=data.get("owner") or data.get("from"),
                )
            if data.get("is_private") and (
                channel["owner_name"].casefold() !=
                (data.get("owner") or "").casefold()
            ):
                return Response().json({"error": "private owner mismatch"},
                                       status=403)
            message, created = chat.post_message(
                data.get("channel"), data.get("from"), data.get("body", ""),
                message_id=data.get("id"), ts=data.get("ts"),
            )
        return Response().json({"message": message, "created": created})

    def peer_sync(request, data):
        viewer = request.headers.get("X-StreapRous-Peer", "")
        rows = chat.sync_messages(
            data.get("channel"), int(data.get("since", 0)),
            int(data.get("limit", 100)), viewer=viewer,
        )
        return {"messages": rows}

    peer_route(peer_register, "/api/peers")
    peer_route(peer_connect, "/api/peers/connect")
    peer_route(peer_message, "/api/peers/broadcast")
    peer_route(peer_message, "/api/peers/send")
    peer_route(peer_message, "/internal/p2p/message")
    peer_route(peer_message, "/internal/p2p/hello")
    peer_route(peer_sync, "/internal/p2p/sync")

    @app.route("/api/peers", methods=["GET"], auth=True)
    @_api
    def get_peers(request):
        if mode == "server":
            online = {item["username"].casefold(): item for item in chat.peers()}
            peers = auth.database.list_users(request.user["username"])
            for item in peers:
                active = online.get(item["username"].casefold())
                item["online"] = active is not None
                if active:
                    item["ip"] = active["ip"]
                    item["port"] = active["port"]
            return {"peers": peers}
        return {"peers": chat.peers(exclude=peer.username if peer else None)}

    @app.route("/get-list", methods=["GET"])
    @_api
    def assignment_get_peers(request):
        return {"peers": chat.peers(exclude=peer.username if peer else None)}

    @app.route("/submit-info", methods=["POST"], auth=True)
    @_api
    def submit_info(request):
        if mode == "server":
            raise BadRequest("central peers must register over the P2P transport")
        data = _json_object(request)
        record = chat.register_peer(
            request.user["username"],
            data.get("ip") or (request.remote_addr[0]
                                if request.remote_addr else "127.0.0.1"),
            data.get("port"), data.get("http_port", 0),
        )
        return Response().json({"peer": record}, status=201)

    @app.route("/add-list", methods=["POST"], auth=True)
    @_api
    def add_list(request):
        return submit_info.__wrapped__(request)

    @app.route("/connect-peer", methods=["POST"])
    @_api
    def assignment_connect(request):
        if peer is not None and not _peer_authorized(auth, request):
            return Response().json({"error": "invalid peer signature"},
                                   status=401)
        data = _json_object(request)
        record = chat.peer_record(data.get("username"))
        if record is None:
            return Response().json({"error": "peer not found"}, status=404)
        return {"peer": record}

    @app.route("/api/peers/connect", methods=["POST"], auth=True)
    @_api
    def api_peer_connect(request):
        data = _json_object(request)
        record = chat.peer_record(data.get("username"))
        if record is None:
            return Response().json({"error": "peer not found"}, status=404)
        return {"peer": record}

    @app.route("/broadcast-peer", methods=["POST"], auth=True)
    @_api
    def assignment_broadcast(request):
        data = _json_object(request)
        if peer is None:
            raise BadRequest("peer transport is disabled")
        message = peer.broadcast(
            data.get("channel"), data.get("body", ""),
            message_id=data.get("id"), sender=request.user["username"],
        )
        return Response().json({"message": message}, status=201)

    @app.route("/send-peer", methods=["POST"])
    @_api
    def assignment_send_peer(request):
        if not _peer_authorized(auth, request):
            return Response().json({"error": "invalid peer signature"},
                                   status=401)
        return peer_message(request, _json_object(request))

    return app


def create_chatapp(ip, port, **kwargs):
    """Build the app, bind the address and start serving."""
    transport = kwargs.pop("transport", None)
    app = build_chatapp(**kwargs)
    app.prepare_address(ip, port)
    app.run(transport=transport)
    return app
