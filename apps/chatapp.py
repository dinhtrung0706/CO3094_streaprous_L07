"""Chat app: JSON control plane plus the UDP peer node."""

import functools
import json
import os
import time

from daemon.auth import install_auth
from daemon.chat import ChatService
from daemon.errors import BadRequest, HTTPError
from daemon.peer import PeerNode
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
    public_ip=None,
    username=None,
    heartbeat=5.0,
    install_peer=True,
    password_iterations=None,
    peer_secret=None,
    http_port=0,
):
    """Create the app, wire services and register every chat route."""
    mode = (mode or "peer").lower()
    if mode not in ("peer", "tracker", "local"):
        raise ValueError("mode must be 'peer', 'tracker' or 'local'")
    configured_peer_secret = peer_secret or os.environ.get(
        "STREAPROUS_PEER_SECRET"
    )
    if mode != "local" and install_peer and (
        not configured_peer_secret or len(configured_peer_secret.encode()) < 32
    ):
        raise ValueError(
            "networked peer/tracker mode requires STREAPROUS_PEER_SECRET "
            "with at least 32 bytes"
        )

    app = StreapRous()
    auth = install_auth(
        app,
        db_path or DEFAULT_DB,
        password_iterations=password_iterations,
        peer_secret=peer_secret,
    )
    chat = ChatService(auth.database)
    app._streaprous_chat = chat
    app._streaprous_auth = auth

    peer = None
    if install_peer and mode != "local":
        node_user = username or ("tracker" if mode == "tracker" else None)
        if not node_user:
            raise ValueError("username is required for peer mode")
        peer = PeerNode(
            chat,
            username=node_user,
            token=auth.issue_peer_token(node_user, ttl=24 * 3600),
            verifier=auth.verify_peer_token,
            role="tracker" if mode == "tracker" else "peer",
            udp_port=udp_port,
            public_ip=public_ip,
            tracker=tracker,
            heartbeat=heartbeat,
            http_port=http_port,
            auth=auth,
        )
        peer.start()
    app._streaprous_peer = peer
    app._streaprous_mode = mode

    @app.route("/api/status", methods=["GET"])
    def status(request):
        return {"mode": mode}

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

        if peer is not None:
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

    @app.route("/api/peers/heartbeat", methods=["POST"], auth=True)
    @_api
    def peer_heartbeat(request):
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
        result = chat.invite_member(
            request.path_params["channel_id"], request.user,
            data.get("username"),
        )
        if peer is not None:
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
        return {"peers": chat.peers(exclude=peer.username if peer else None)}

    @app.route("/get-list", methods=["GET"])
    @_api
    def assignment_get_peers(request):
        return {"peers": chat.peers(exclude=peer.username if peer else None)}

    @app.route("/submit-info", methods=["POST"], auth=True)
    @_api
    def submit_info(request):
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
