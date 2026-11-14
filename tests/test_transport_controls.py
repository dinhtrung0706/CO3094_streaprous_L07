import unittest

from transport.constant import MAX_SEGMENT_SIZE
from transport.congestion_control import CControl
from transport.flow_control import FControl


class TestFlowControl(unittest.TestCase):
    def test_capacity_and_ack(self):
        control = FControl(100)
        self.assertTrue(control.account_send(60))
        self.assertFalse(control.account_send(50))
        control.account_ack(30)
        self.assertTrue(control.account_send(50))

    def test_zero_window_and_reopen(self):
        control = FControl(0)
        self.assertFalse(control.account_send(1))
        control.update_rwnd(120)
        self.assertTrue(control.account_send(120))

    def test_window_update_preserves_outstanding(self):
        control = FControl(100)
        control.account_send(80)
        control.update_rwnd(60)
        self.assertFalse(control.can_send(1))
        control.account_ack(40)
        self.assertTrue(control.can_send(20))

    def test_invalid_ack_is_rejected(self):
        control = FControl(100)
        with self.assertRaises(ValueError):
            control.account_ack(1)


class TestCongestionControl(unittest.TestCase):
    def test_slow_start_grows_window(self):
        control = CControl(3000, 6000)
        self.assertTrue(control.account_send(1000))
        control.account_ack(1000)
        self.assertEqual(control._in_flight, 0)
        self.assertEqual(control._cwnd, 4000)

    def test_congestion_window_blocks_send(self):
        control = CControl(1000, 1000)
        self.assertTrue(control.account_send(700))
        self.assertFalse(control.account_send(400))

    def test_drop_does_not_grow_window(self):
        control = CControl(3000, 5000)
        control.account_send(1500)
        initial_cwnd = control._cwnd
        control.account_drop(1500)
        self.assertEqual(control._in_flight, 0)
        self.assertEqual(control._cwnd, initial_cwnd)

    def test_timeout_reduces_window(self):
        control = CControl(10 * MAX_SEGMENT_SIZE, 15 * MAX_SEGMENT_SIZE)
        control.on_timeout()
        self.assertEqual(control._cwnd, 2 * MAX_SEGMENT_SIZE)
        self.assertEqual(control._ssthresh, 5 * MAX_SEGMENT_SIZE)


if __name__ == "__main__":
    unittest.main()
