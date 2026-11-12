import unittest
from transport import ACKTracker

class TestAckTracker(unittest.TestCase):
    def test_merge(self):
        ackt = ACKTracker()
        self.assertTrue(ackt.merge(1, 2))
        self.assertEqual(ackt._ack_windows, [(1, 2)])
        self.assertFalse(ackt.merge(1, 2))
        self.assertEqual(ackt._ack_windows, [(1, 2)])
        self.assertTrue(ackt.merge(8, 10))
        self.assertEqual(ackt._ack_windows, [(1, 2), (8, 10)])
        self.assertTrue(ackt.merge(2, 8))
        self.assertEqual(ackt._ack_windows, [(1, 10)])
        self.assertTrue(ackt.merge(12, 20))
        self.assertEqual(ackt._ack_windows, [(1, 10), (12, 20)])

    def test_has_acked(self):
        ackt = ACKTracker()
        self.assertTrue(ackt.merge(1, 2))
        self.assertFalse(ackt.merge(1, 2))
        self.assertTrue(ackt.merge(8, 10))
        self.assertTrue(ackt.has_acked(9))
        self.assertFalse(ackt.has_acked(5))
        self.assertFalse(ackt.has_acked(10))

if __name__ == "__main__":
    unittest.main()
