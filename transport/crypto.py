"""AES-GCM payload encryption and Session key derivation."""

import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from .constant import SESSION_KDF_INFO

IV_SIZE = 12
GCM_TAG_SIZE = 16
GCM_OVERHEAD = IV_SIZE + GCM_TAG_SIZE  # Authentication tag


def generate_key_pair():
    """
    Generate an ephemeral X25519 key pair.

    Returns:
        tuple: Private key and public key bytes.
    """
    private_key = X25519PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        Encoding.Raw, PublicFormat.Raw
    )
    return private_key, public_key


def derive_cipher(private_key, peer_public_key):
    """
    Derive an AES-GCM cipher from X25519 key agreement.

    Args:
        private_key: Local ephemeral private key.
        peer_public_key: Remote public key bytes.

    Returns:
        AESGCM: Cipher initialized with the derived Session key.
    """
    public_key = X25519PublicKey.from_public_bytes(peer_public_key)
    shared_secret = private_key.exchange(public_key)
    key = HKDF(hashes.SHA256(), 32, None, SESSION_KDF_INFO).derive(shared_secret)
    return AESGCM(key)


def encrypt_payload(cipher, payload):
    """
    Encrypt a payload using AES-GCM.

    Args:
        cipher: Session AES-GCM cipher.
        payload: Plaintext payload.

    Returns:
        bytes: IV, ciphertext, and authentication tag.
    """
    iv = os.urandom(IV_SIZE)
    return iv + cipher.encrypt(iv, payload, None)


def decrypt_payload(cipher, payload):
    """
    Decrypt and authenticate an AES-GCM payload.

    Args:
        cipher: Session AES-GCM cipher.
        payload: IV, ciphertext, and authentication tag.

    Returns:
        bytes: Decrypted payload.
    """
    if len(payload) < GCM_OVERHEAD:
        raise ValueError("Encrypted payload is too short")
    try:
        return cipher.decrypt(payload[:IV_SIZE], payload[IV_SIZE:], None)
    except InvalidTag:
        raise ValueError("Invalid encrypted payload")
