import time
import unittest
from unittest.mock import Mock

from transport.constant import MAX_ACK_DELAY
from transport.transmitter import MIN_RTO, Transmitter


class TestTransmitter(unittest.TestCase):
    def test_register_and_ack(self):
        transmitter = Transmitter(0.1)
        key = (1, 7, 0)
        transmitter.register_packet(key, time.monotonic() - 0.05)
        self.assertTrue(transmitter.acking_packet(key))
        self.assertNotIn(key, transmitter._pktmeta)
        self.assertGreaterEqual(transmitter._rto, MIN_RTO)

    def test_retransmission_after_ack_delay_allowance(self):
        transmitter = Transmitter(0.1)
        session = Mock()
        key = (1, 7, 0)
        transmitter.register_packet(key, 0)
        due = transmitter._rto + MAX_ACK_DELAY
        transmitter._on_deadline(session, due - 0.001)
        session.retransmit_packet.assert_not_called()
        transmitter._on_deadline(session, due + 0.001)
        session.retransmit_packet.assert_called_once_with(key)

    def test_retry_exhaustion_notifies_session(self):
        transmitter = Transmitter(0.1, max_nr_retries=1)
        session = Mock()
        key = (1, 8, 0)
        transmitter.register_packet(key, 0)
        transmitter._on_deadline(session, transmitter._rto + MAX_ACK_DELAY + 1)
        transmitter._on_deadline(session, 100)
        session.handle_dropped_packets.assert_called_once_with([key])
        self.assertNotIn(key, transmitter._pktmeta)

    def test_ack_cancels_retransmission(self):
        transmitter = Transmitter(0.1)
        session = Mock()
        key = (1, 9, 0)
        transmitter.register_packet(key, time.monotonic())
        self.assertTrue(transmitter.acking_packet(key))
        transmitter._on_deadline(session, time.monotonic() + 100)
        session.retransmit_packet.assert_not_called()

    def test_abort_clears_metadata(self):
        transmitter = Transmitter(0.1)
        transmitter.register_packet((1, 1, 0), 0)
        transmitter.abort()
        self.assertEqual(transmitter._pktmeta, {})
        self.assertEqual(transmitter._drop_list, [])


if __name__ == "__main__":
    unittest.main()
