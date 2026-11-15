import io
import unittest

from transport.constant import MAX_SEGMENT_SIZE, RECV_DUPLICATE, RECV_ERROR, RECV_FULL, RECV_STORED, RECV_WAITING
from transport.stream_buffer import ReceiveBuffer, SendBuffer


class TestSendBuffer(unittest.TestCase):
    def test_acquire_does_not_consume(self):
        buffer = SendBuffer(b"hello")
        self.assertEqual(buffer.acquire_send_data(), (0, b"hello"))
        self.assertEqual(buffer.acquire_send_data(), (0, b"hello"))
        self.assertTrue(buffer.is_last_chunk())
        buffer.commit_send_data(5)
        self.assertIsNone(buffer.acquire_send_data())

    def test_multiple_messages_have_separate_eom(self):
        buffer = SendBuffer(b"first")
        buffer.register_send_data(b"second")
        self.assertEqual(buffer.acquire_send_data(), (0, b"first"))
        self.assertTrue(buffer.is_last_chunk())
        buffer.commit_send_data(5)
        self.assertEqual(buffer.acquire_send_data(), (1, b"second"))
        self.assertTrue(buffer.is_last_chunk())

    def test_partial_registration_joins_message(self):
        buffer = SendBuffer(b"")
        buffer.register_send_partial_data(b"hello ")
        buffer.register_send_data(b"world")
        self.assertEqual(buffer.acquire_send_data(), (0, b"hello world"))
        self.assertTrue(buffer.is_last_chunk())

    def test_large_message_fragmentation(self):
        buffer = SendBuffer(b"a" * (MAX_SEGMENT_SIZE + 3))
        seq, chunk = buffer.acquire_send_data()
        self.assertEqual((seq, len(chunk)), (0, MAX_SEGMENT_SIZE))
        self.assertFalse(buffer.is_last_chunk())
        buffer.commit_send_data(len(chunk))
        self.assertEqual(buffer.acquire_send_data(), (1, b"aaa"))
        self.assertTrue(buffer.is_last_chunk())

    def test_logical_sequence_wraps(self):
        buffer = SendBuffer(b"x")
        buffer._lseq = 65535
        self.assertEqual(buffer.acquire_send_data(), (65535, b"x"))
        buffer.commit_send_data(1)
        buffer.register_send_data(b"y")
        self.assertEqual(buffer.acquire_send_data(), (0, b"y"))


class TestReceiveBuffer(unittest.TestCase):
    def test_out_of_order_message_assembly(self):
        buffer = ReceiveBuffer()
        self.assertEqual(buffer.register_receive_data(1, b"B", True), RECV_STORED)
        self.assertIsNone(buffer.form_receive_data())
        self.assertEqual(buffer.register_receive_data(0, b"A"), RECV_STORED)
        self.assertEqual(buffer.form_receive_data(), b"AB")
        self.assertEqual(buffer.buffered_size(), 0)

    def test_duplicate_and_conflicting_duplicate(self):
        buffer = ReceiveBuffer()
        self.assertEqual(buffer.register_receive_data(0, b"one"), RECV_STORED)
        self.assertEqual(buffer.register_receive_data(0, b"one"), RECV_DUPLICATE)
        self.assertEqual(buffer.register_receive_data(0, b"two"), RECV_ERROR)

    def test_capacity_rejects_without_storing(self):
        buffer = ReceiveBuffer(capacity=3)
        self.assertEqual(buffer.register_receive_data(0, b"abc"), RECV_STORED)
        self.assertEqual(buffer.register_receive_data(1, b"d"), RECV_FULL)
        self.assertEqual(buffer.buffered_size(), 3)

    def test_flush_only_contiguous_chunks(self):
        buffer = ReceiveBuffer()
        output = io.BytesIO()
        buffer.register_receive_data(0, b"A")
        buffer.register_receive_data(2, b"C", True)
        self.assertEqual(buffer.flush_to_file(output), (0, 1))
        self.assertEqual(output.getvalue(), b"A")
        self.assertEqual(buffer.flush_to_file(output), RECV_WAITING)
        buffer.register_receive_data(1, b"B")
        self.assertEqual(buffer.flush_to_file(output), (1, 3))
        self.assertTrue(buffer.flushed_eom())
        self.assertEqual(output.getvalue(), b"ABC")

    def test_wraparound_assembly(self):
        buffer = ReceiveBuffer()
        buffer._s6t_unacked_seq = 65535
        buffer.register_receive_data(0, b"B", True)
        buffer.register_receive_data(65535, b"A")
        self.assertEqual(buffer.form_receive_data(), b"AB")
        self.assertEqual(buffer._s6t_unacked_seq, 1)


if __name__ == "__main__":
    unittest.main()
