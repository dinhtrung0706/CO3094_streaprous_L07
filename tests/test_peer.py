"""Tests for the UDP peer node."""

import time
import unittest

from daemon.auth import AuthService
from daemon.chat import ChatService
from daemon.database import Database
from daemon.peer import PeerNode
from daemon import protocol


def wait_for(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


class PeerHarness(unittest.TestCase):
    def setUp(self):
        self.channel_id = "a" * 32
        self.services = {}
        self.auth_services = {}
        self.nodes = []

    def make_service(self, *usernames):
        database = Database(":memory:")
        self.addCleanup(database.close)
        auth = AuthService(
            database,
            signing_key=b"test session signing key with enough entropy",
            password_iterations=1000,
        )
        for username in dict.fromkeys(("alice", "bob") + usernames):
            auth.register(username, "test password long enough")
        service = ChatService(database)
        self.services[usernames[0]] = service
        self.auth_services[database] = auth
        return service

    def make_channel(self, service, owner="alice"):
        database = service.database
        owner_row = database.get_user(owner)
        channel = database.get_channel(self.channel_id)
        if channel is None:
            channel = database.create_channel(
                self.channel_id, "general", owner_row["id"], 1000
            )
        for username in ("alice", "bob"):
            user = database.get_user(username)
            if user is not None:
                database.add_channel_member(self.channel_id, user["id"], 1000)
        return channel

    def make_node(self, service, username, **kwargs):
        node = PeerNode(
            service,
            username=username,
            auth=self.auth_services[service.database],
            listen_ip="127.0.0.1",
            udp_port=0,
            peer_transport="udp",
            tick=0.02,
            heartbeat=0.2,
            **kwargs,
        )
        node.start()
        self.addCleanup(node.stop)
        self.nodes.append(node)
        return node

    def test_hello_registers_peer_with_tracker(self):
        tracker_service = self.make_service("tracker")
        peer_service = self.make_service("alice")
        tracker = self.make_node(tracker_service, "tracker", role="tracker")
        self.make_node(peer_service, "alice", tracker=tracker.node.address)

        registered = wait_for(
            lambda: tracker_service.peers(),
        )
        self.assertEqual([peer["username"] for peer in registered], ["alice"])
        self.assertEqual(registered[0]["port"], self.nodes[1].node.port)

    def test_peers_learn_about_each_other(self):
        tracker_service = self.make_service("tracker")
        alice_service = self.make_service("alice")
        bob_service = self.make_service("bob")
        tracker = self.make_node(tracker_service, "tracker", role="tracker")
        alice = self.make_node(alice_service, "alice", tracker=tracker.node.address)
        bob = self.make_node(bob_service, "bob", tracker=tracker.node.address)

        alice_knows_bob = wait_for(
            lambda: [peer for peer in alice_service.peers() if peer["username"] == "bob"]
        )
        bob_knows_alice = wait_for(
            lambda: [peer for peer in bob_service.peers() if peer["username"] == "alice"]
        )
        self.assertTrue(alice_knows_bob)
        self.assertTrue(bob_knows_alice)
        self.assertEqual(alice_knows_bob[0]["port"], bob.node.port)

    def test_broadcast_is_delivered_and_stored(self):
        tracker_service = self.make_service("tracker")
        alice_service = self.make_service("alice")
        bob_service = self.make_service("bob")
        self.make_channel(alice_service)
        self.make_channel(bob_service)
        tracker = self.make_node(tracker_service, "tracker", role="tracker")
        alice = self.make_node(alice_service, "alice", tracker=tracker.node.address)
        bob = self.make_node(bob_service, "bob", tracker=tracker.node.address)

        wait_for(lambda: [p for p in alice_service.peers() if p["username"] == "bob"])
        message = alice.broadcast(self.channel_id, "hello bob")

        received = wait_for(lambda: bob_service.messages(self.channel_id))
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["id"], message["id"])
        self.assertEqual(received[0]["body"], "hello bob")
        self.assertEqual(received[0]["sender"], "alice")
        self.assertEqual(bob.stats["messages_received"], 1)

    def test_duplicate_encrypted_message_is_ignored(self):
        tracker_service = self.make_service("tracker")
        alice_service = self.make_service("alice")
        bob_service = self.make_service("bob")
        self.make_channel(alice_service)
        self.make_channel(bob_service)
        tracker = self.make_node(tracker_service, "tracker", role="tracker")
        alice = self.make_node(alice_service, "alice", tracker=tracker.node.address)
        bob = self.make_node(bob_service, "bob", tracker=tracker.node.address)
        wait_for(lambda: [p for p in alice_service.peers() if p["username"] == "bob"])

        message = alice.broadcast(self.channel_id, "once")
        wait_for(lambda: bob_service.messages(self.channel_id))

        envelope = PeerNode._envelope(message)
        payload = alice._encode_payload(envelope, "bob")
        frame = protocol.Frame(protocol.MSG, payload=payload)
        bob._handle_message(frame, alice.node.address)
        self.assertEqual(len(bob_service.messages(self.channel_id)), 1)
        self.assertEqual(bob.stats["duplicates"], 1)

    def test_direct_message_reaches_one_peer(self):
        tracker_service = self.make_service("tracker")
        alice_service = self.make_service("alice")
        bob_service = self.make_service("bob")
        self.make_channel(alice_service)
        self.make_channel(bob_service)
        tracker = self.make_node(tracker_service, "tracker", role="tracker")
        alice = self.make_node(alice_service, "alice", tracker=tracker.node.address)
        bob = self.make_node(bob_service, "bob", tracker=tracker.node.address)
        wait_for(lambda: [p for p in alice_service.peers() if p["username"] == "bob"])

        alice.send_direct(self.channel_id, "bob", "psst")
        received = wait_for(lambda: bob_service.messages(self.channel_id))
        self.assertEqual(received[0]["body"], "psst")

    def test_untrusted_peer_key_is_rejected(self):
        alice_service = self.make_service("alice")
        bob_service = self.make_service("bob")
        self.make_channel(bob_service)
        alice_auth = self.auth_services[alice_service.database]
        bob = self.make_node(bob_service, "bob", tracker=None)
        alice_auth.peer_crypto.pin("bob", bob.crypto.public)
        bob.chat.register_peer("alice", "127.0.0.1", 1234)

        envelope = protocol.message_envelope(
            channel=self.channel_id, sender="alice", body="untrusted"
        )
        payload = alice_auth.peer_crypto.seal("bob", envelope)
        encoded = protocol.encode_message("alice", envelope["id"], payload)
        frame = protocol.Frame(protocol.MSG, seq=0, payload=encoded)
        bob._handle_message(frame, ("127.0.0.1", 1234))

        self.assertEqual(bob_service.messages(self.channel_id), [])
        self.assertGreaterEqual(bob.stats["rejected"], 1)

    def test_tampered_peer_ciphertext_is_rejected(self):
        alice_service = self.make_service("alice")
        bob_service = self.make_service("bob")
        self.make_channel(bob_service)
        alice_auth = self.auth_services[alice_service.database]
        bob = self.make_node(bob_service, "bob", tracker=None)
        alice_auth.peer_crypto.pin("bob", bob.crypto.public)
        bob.crypto.pin("alice", alice_auth.peer_crypto.public)
        bob.chat.register_peer("alice", "127.0.0.1", 1234)

        envelope = protocol.message_envelope(
            channel=self.channel_id, sender="alice", body="tampered"
        )
        sealed = bytearray(alice_auth.peer_crypto.seal("bob", envelope))
        sealed[-1] ^= 1
        payload = protocol.encode_message("alice", envelope["id"], bytes(sealed))
        frame = protocol.Frame(protocol.MSG, seq=0, payload=payload)
        bob._handle_message(frame, ("127.0.0.1", 1234))

        self.assertEqual(bob_service.messages(self.channel_id), [])
        self.assertGreaterEqual(bob.stats["rejected"], 1)

    def test_status_is_json_safe(self):
        tracker_service = self.make_service("tracker")
        alice_service = self.make_service("alice")
        tracker = self.make_node(tracker_service, "tracker", role="tracker")
        alice = self.make_node(alice_service, "alice", tracker=tracker.node.address)
        self.assertIn("role", alice.status())
        self.assertEqual(alice.status()["role"], "peer")

    def test_stop_is_idempotent(self):
        service = self.make_service("alice")
        node = self.make_node(service, "alice")
        node.stop()
        node.stop()
