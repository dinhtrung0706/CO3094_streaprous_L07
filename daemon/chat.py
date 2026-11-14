"""Chat domain service: channels, membership, messages and peering."""

import re
import time
import uuid

from . import protocol
from .errors import BadRequest, Forbidden, NotFound

CHANNEL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,31}$")
CHAT_USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{2,31}$")
MESSAGE_ID_RE = re.compile(r"^[0-9a-f]{8,64}$")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
DEFAULT_PEER_TTL = 20


class ChatService:
    """Application logic shared by the HTTP API and the peer node."""

    def __init__(self, database, *, clock=time.time, peer_ttl=DEFAULT_PEER_TTL):
        self.database = database
        self.peer_ttl = int(peer_ttl)
        self._clock = clock

    def create_channel(self, name, owner, *, is_private=False):
        if not isinstance(is_private, bool):
            raise BadRequest("is_private must be a boolean")
        if not isinstance(name, str):
            raise BadRequest("channel name must be a string")
        name = name.strip()
        if not CHANNEL_NAME_RE.fullmatch(name):
            raise BadRequest(
                "channel name must be 1-32 characters: letters, digits, "
                "space, '.', '_' or '-'"
            )
        if self.database.get_channel_by_name(name) is not None:
            raise ValueError("channel already exists")

        channel_id = uuid.uuid4().hex
        channel = self.database.create_channel(
            channel_id,
            name,
            owner["id"],
            int(self._clock()),
            is_private=is_private,
            owner_name=owner["username"],
        )
        self.database.add_channel_member(channel_id, owner["id"], int(self._clock()))
        channel["member_count"] = 1
        channel["joined"] = 1
        return channel

    def list_channels(self, user_id):
        return self.database.list_channels(user_id)

    def require_channel(self, channel_id):
        channel = self.database.get_channel(channel_id)
        if channel is None:
            raise NotFound("channel not found")
        return channel

    def join(self, channel_id, user):
        channel = self.require_channel(channel_id)
        if channel["is_private"]:
            raise Forbidden("private channels require an invitation")
        return self.database.add_channel_member(
            channel_id, user["id"], int(self._clock())
        )

    def leave(self, channel_id, user):
        self.require_channel(channel_id)
        return self.database.remove_channel_member(channel_id, user["id"])

    def require_member(self, channel_id, user):
        channel = self.require_channel(channel_id)
        if channel["is_private"] and not self.database.is_channel_member(
            channel_id, user["username"]
        ):
            raise Forbidden("private channel membership required")
        if not self.database.is_channel_member(channel_id, user["username"]):
            raise Forbidden("join the channel before using it")

    def invite_member(self, channel_id, owner, username):
        channel = self.require_channel(channel_id)
        if channel["owner_name"].casefold() != owner["username"].casefold():
            raise Forbidden("only the channel owner can invite members")
        if not isinstance(username, str) or not CHAT_USERNAME_RE.fullmatch(
            username.strip()
        ):
            raise BadRequest("invited username is invalid")
        username = username.strip()
        added = self.database.add_channel_member(
            channel_id, username, int(self._clock())
        )
        return {"channel_id": channel_id, "username": username, "added": added}

    @staticmethod
    def validate_body(body):
        if not isinstance(body, str):
            raise BadRequest("message body must be a string")
        if not body.strip():
            raise BadRequest("message body must not be empty")
        if len(body.encode("utf-8")) > protocol.MAX_BODY_BYTES:
            raise BadRequest(
                f"message body must be at most {protocol.MAX_BODY_BYTES} bytes"
            )
        if _CONTROL_RE.search(body):
            raise BadRequest("message body contains control characters")
        return body

    def post_message(self, channel_id, sender, body, *, message_id=None,
                     ts=None, recipient="*"):
        """Return ``(message, created)``; ``created`` is False on duplicate id."""
        self.validate_body(body)
        if not isinstance(recipient, str) or not recipient:
            raise BadRequest("message recipient must be a non-empty string")
        if message_id is None:
            message_id = protocol.new_message_id()
        elif not MESSAGE_ID_RE.fullmatch(str(message_id)):
            raise BadRequest("message id must be a hex string")

        existing = self.database.get_message(message_id)
        if existing is not None:
            return existing, False

        message = self.database.append_message(
            message_id,
            channel_id,
            sender,
            body,
            int(ts if ts is not None else self._clock()),
            recipient,
            owner=(self.database.get_channel(channel_id) or {}).get("owner_name", ""),
            channel_name=(self.database.get_channel(channel_id) or {}).get("name", ""),
            is_private=(self.database.get_channel(channel_id) or {}).get("is_private", False),
        )
        return message, True

    def messages(self, channel_id, since=0, limit=100, viewer=None):
        if since < 0:
            raise BadRequest("since must not be negative")
        limit = max(1, min(int(limit), 500))
        return self.database.list_messages(
            channel_id, since, limit, viewer=viewer
        )

    def post_direct_message(self, sender, recipient, body, *, message_id=None,
                            ts=None):
        self.validate_body(body)
        if not isinstance(sender, str) or not isinstance(recipient, str):
            raise BadRequest("direct-message participants must be usernames")
        if sender.casefold() == recipient.casefold():
            raise BadRequest("direct-message recipient must be another user")
        for username in (sender, recipient):
            if not isinstance(username, str) or not CHAT_USERNAME_RE.fullmatch(
                username
            ):
                raise BadRequest("direct-message username is invalid")
        if self.database.get_user(sender) is None:
            raise NotFound("direct-message sender not found")
        if message_id is None:
            message_id = protocol.new_message_id()
        elif not MESSAGE_ID_RE.fullmatch(str(message_id)):
            raise BadRequest("message id must be a hex string")
        message, created = self.database.append_direct_message(
            str(message_id), sender, recipient, body,
            int(ts if ts is not None else self._clock()),
        )
        return message, created

    def receive_direct_message(self, sender, recipient, body, *, message_id,
                               ts=None):
        self.validate_body(body)
        if not isinstance(sender, str) or not isinstance(recipient, str):
            raise BadRequest("direct-message participants must be usernames")
        if sender.casefold() == recipient.casefold():
            raise BadRequest("direct-message recipient must be another user")
        for username in (sender, recipient):
            if not isinstance(username, str) or not CHAT_USERNAME_RE.fullmatch(
                username
            ):
                raise BadRequest("direct-message username is invalid")
        if self.database.get_user(recipient) is None:
            raise NotFound("direct-message recipient not found")
        if not MESSAGE_ID_RE.fullmatch(str(message_id)):
            raise BadRequest("message id must be a hex string")
        return self.database.insert_direct_message({
            "id": str(message_id), "sender": sender, "recipient": recipient,
            "body": body,
            "created_at": int(ts if ts is not None else self._clock()),
        })

    def direct_messages(self, viewer, other, since=0, limit=100):
        if not isinstance(viewer, str) or not isinstance(other, str):
            raise Forbidden("direct-message participant required")
        if viewer.casefold() == other.casefold():
            raise Forbidden("direct messages are only visible to participants")
        if since < 0:
            raise BadRequest("since must not be negative")
        return self.database.list_direct_messages(
            viewer, other, since, max(1, min(int(limit), 500))
        )

    def register_peer(self, username, ip, port, http_port=0):
        if not isinstance(username, str) or not username.strip():
            raise BadRequest("peer username must be a non-empty string")
        if not isinstance(ip, str) or not ip:
            raise BadRequest("peer ip must be a non-empty string")
        try:
            port = int(port)
        except (TypeError, ValueError) as exc:
            raise BadRequest("peer port must be an integer") from exc
        if not 1 <= port <= 65535:
            raise BadRequest("peer port must be between 1 and 65535")
        try:
            http_port = int(http_port or 0)
        except (TypeError, ValueError) as exc:
            raise BadRequest("peer HTTP port must be an integer") from exc
        if not 0 <= http_port <= 65535:
            raise BadRequest("peer HTTP port must be between 0 and 65535")
        return self.database.upsert_peer(
            username.strip(), ip, port, int(self._clock()), http_port=http_port
        )

    def peers(self, *, exclude=None):
        not_before = self._clock() - self.peer_ttl
        return self.database.list_peers(not_before, exclude_username=exclude)

    def prune_peers(self):
        return self.database.expire_peers(self._clock() - self.peer_ttl)

    def peer_address(self, username):
        for peer in self.peers():
            if peer["username"].lower() == str(username).lower():
                return peer["ip"], int(peer["port"])
        return None

    def peer_record(self, username):
        for peer in self.peers():
            if peer["username"].casefold() == str(username).casefold():
                return peer
        return None

    def sync_messages(self, channel_id, since=0, limit=100, *, viewer=None):
        channel = self.require_channel(channel_id)
        if channel["is_private"] and (
            not viewer or not self.database.is_channel_member(channel_id, viewer)
        ):
            raise Forbidden("private channel membership required")
        return self.messages(channel_id, since=since, limit=limit, viewer=viewer)
