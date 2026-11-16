"""Persistent X25519 identities, key pinning, and peer encryption."""

import base64
import hashlib
import hmac
import json
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import (
    Encoding, PrivateFormat, PublicFormat, NoEncryption,
)

KDF_INFO = b"streaprous-chat-x25519-v1"


def encode_key(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def decode_key(text, size):
    if not isinstance(text, str):
        raise ValueError("invalid base64 key")
    try:
        data = base64.b64decode(
            text + "=" * (-len(text) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid base64 key") from exc
    if len(data) != size or encode_key(data) != text:
        raise ValueError("invalid key size or encoding")
    return data


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True).encode("ascii")


class PeerCrypto:
    """One X25519 identity per database, plus persisted peer-key pins."""

    def __init__(self, database):
        self.database = database
        if database.path != ":memory:":
            os.chmod(database.path, 0o600)
        raw = database.get_identity_key()
        if raw is None:
            private = X25519PrivateKey.generate()
            raw = private.private_bytes(
                Encoding.Raw, PrivateFormat.Raw, NoEncryption()
            )
            raw = database.save_identity_key(raw)
        self.private = X25519PrivateKey.from_private_bytes(raw)
        self.public = self.private.public_key().public_bytes(
            Encoding.Raw, PublicFormat.Raw
        )
        self.public_text = encode_key(self.public)

    def _shared_key(self, public, purpose):
        peer = X25519PublicKey.from_public_bytes(public)
        secret = self.private.exchange(peer)
        return HKDF(
            algorithm=hashes.SHA256(), length=32, salt=None,
            info=KDF_INFO + b":" + purpose + min(self.public, public) + max(self.public, public),
            backend=default_backend(),
        ).derive(secret)

    def pinned(self, username):
        return self.database.get_peer_key(username)

    def pin(self, username, public):
        if not isinstance(username, str) or not username:
            return False
        return self.database.pin_peer_key(username, public)

    def mac(self, public, purpose, body):
        key = self._shared_key(public, b"mac")
        return encode_key(hmac.new(
            key, b"mac:" + purpose.encode("ascii") + b":" + body,
            hashlib.sha256
        ).digest())

    def verify_mac(self, public, purpose, body, supplied):
        if not isinstance(supplied, str):
            return False
        try:
            return hmac.compare_digest(self.mac(public, purpose, body), supplied)
        except (ValueError, TypeError):
            return False

    def seal(self, recipient, envelope):
        public = self.pinned(recipient)
        if public is None:
            raise ValueError("recipient public key is not trusted")
        nonce = os.urandom(12)
        plaintext = canonical(envelope)
        aad = b"chat:" + envelope["from"].encode() + b":" + recipient.encode()
        ciphertext = AESGCM(self._shared_key(public, b"aead")).encrypt(nonce, plaintext, aad)
        return nonce + ciphertext

    def open(self, sender, recipient, payload):
        public = self.pinned(sender)
        if public is None or len(payload) < 29:
            raise ValueError("untrusted sender or invalid ciphertext")
        aad = b"chat:" + sender.encode() + b":" + recipient.encode()
        try:
            return json.loads(AESGCM(self._shared_key(public, b"aead")).decrypt(
                payload[:12], payload[12:], aad
            ).decode("utf-8"))
        except (InvalidTag, ValueError, UnicodeDecodeError) as exc:
            raise ValueError("message authentication failed") from exc
