import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from transport.constant import RECV_DUPLICATE, RECV_ERROR, RECV_STORED
from transport.packet import ACK, DATA, EOM, TRANSFER, VERSION, Packet
from transport.session import Session
from transport.stream_buffer import SendBuffer
from tests.transport_connection_fixture import FakeConnection


def data_packet(transfer_id, seq, data, last=False):
    flags = DATA | EOM if last else DATA
    return Packet(VERSION, TRANSFER, flags, seq, 0, seq, 65535,
                  struct.pack("!I", transfer_id) + data)


class TestSession(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.conn = FakeConnection()
        starter = patch("transport.session.Transmitter.start_thread_deadline")
        starter.start()
        self.addCleanup(starter.stop)
        timer = patch("transport.session.threading.Timer")
        self.timer = timer.start()
        self.addCleanup(timer.stop)
        self.session = Session(self.conn, 2, self.temp.name)
        self.addCleanup(self.session.abort)

    def test_send_and_reject_duplicate_pending(self):
        buffer = SendBuffer(b"hello")
        self.assertTrue(self.session.send_next_chunk(buffer, 42))
        self.assertIsNone(buffer.acquire_send_data())
        self.assertEqual(self.conn.sent[-1].payload[4:], b"hello")
        self.assertEqual(self.conn.sent[-1].flags, DATA | EOM)
        self.assertFalse(self.session.send_packet(data_packet(42, 0, b"hello")))

    def test_window_full_keeps_unsent_data(self):
        self.session._fcontrol.update_rwnd(2)
        buffer = SendBuffer(b"hello")
        self.assertFalse(self.session.send_next_chunk(buffer, 42))
        self.assertEqual(buffer.acquire_send_data(), (0, b"hello"))

    def test_transfer_delivery_and_duplicate_validation(self):
        self.assertEqual(self.session.handle_data(data_packet(9, 1, b"B", True)), RECV_STORED)
        self.assertEqual(self.session.handle_data(data_packet(9, 0, b"A")), RECV_STORED)
        self.assertEqual(self.session.receive_transfer(9), b"AB")
        self.assertEqual(self.session.handle_data(data_packet(9, 0, b"A")), RECV_DUPLICATE)

    def test_unrecognized_old_duplicate_rejected(self):
        self.assertEqual(self.session.handle_data(data_packet(8, 0, b"A")), RECV_STORED)
        buffer = self.session._recv_buffers[8]
        buffer._s6t_unacked_seq = 2
        self.assertEqual(self.session.handle_data(data_packet(8, 1, b"B")), RECV_ERROR)

    def test_file_completion_after_gap_filled(self):
        partial = self.session.register_receive_file(5)
        self.assertTrue(partial.name.endswith(".part"))
        self.assertEqual(self.session.handle_data(data_packet(5, 1, b"B", True)), RECV_STORED)
        self.assertIsNone(self.session.receive_file(5))
        self.assertEqual(self.session.handle_data(data_packet(5, 0, b"A")), RECV_STORED)
        completed = self.session.receive_file(5)
        self.assertIsInstance(completed, Path)
        self.assertEqual(completed.read_bytes(), b"AB")
        self.assertFalse(partial.exists())

    def test_delayed_ack_milestone(self):
        self.session._process_received_packet(data_packet(7, 0, b"A" * 1200))
        self.assertFalse(any(p.flags == ACK for p in self.conn.sent))
        self.session._process_received_packet(data_packet(7, 1, b"B" * 1200))
        self.assertTrue(any(p.flags == ACK for p in self.conn.sent))

    def test_delayed_ack_deadline(self):
        self.session._process_received_packet(data_packet(11, 0, b"x"))
        self.assertFalse(self.conn.sent)
        self.session._send_delayed_acks()
        self.assertTrue(any(p.flags == ACK for p in self.conn.sent))

    def test_abort_removes_partial_but_not_completed_file(self):
        incomplete = self.session.register_receive_file(3)
        self.session.register_receive_file(4)
        self.session.handle_data(data_packet(4, 0, b"done", True))
        completed = self.session.receive_file(4)
        self.session.abort()
        self.assertFalse(incomplete.exists())
        self.assertTrue(completed.exists())
        self.assertEqual(self.session._pending_packets, {})
        self.assertEqual(self.session._recv_buffers, {})


if __name__ == "__main__":
    unittest.main()
