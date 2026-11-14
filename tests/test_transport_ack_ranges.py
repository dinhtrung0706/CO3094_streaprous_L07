import unittest

from transport.ack_track import ACKTracker, lseq_distance, lseq_next, lseq_range, newer
from transport.constant import MAX_ACK_RANGES


class TestLogicalSequence(unittest.TestCase):
    def test_wraparound_helpers(self):
        self.assertEqual(lseq_next(65535), 0)
        self.assertEqual(lseq_distance(1, 65535), 2)
        self.assertEqual(lseq_range(65534, 2), [65534, 65535, 0, 1])
        self.assertTrue(newer(0, 65535))
        self.assertFalse(newer(65535, 0))


class TestAckRanges(unittest.TestCase):
    def test_out_of_order_then_gap_filled(self):
        tracker = ACKTracker()
        tracker.record(0)
        tracker.record(2)
        self.assertEqual(tracker.contiguous_end(), 1)
        self.assertEqual(tracker.get_ranges(), [(0, 1), (2, 3)])
        tracker.record(1)
        self.assertEqual(tracker.contiguous_end(), 3)
        self.assertEqual(tracker.cumulative_ack(), 2)

    def test_merge_adjacent_ranges(self):
        tracker = ACKTracker()
        self.assertTrue(tracker.merge(5, 8))
        self.assertTrue(tracker.merge(8, 10))
        self.assertEqual(tracker.get_ranges(), [(5, 10)])
        self.assertFalse(tracker.merge(6, 9))

    def test_wrapped_cumulative_range(self):
        tracker = ACKTracker()
        tracker._cumulative_next = 65535
        self.assertTrue(tracker.merge(65535, 2))
        self.assertEqual(tracker.contiguous_end(), 2)
        self.assertEqual(tracker.cumulative_ack(), 1)

    def test_range_snapshot_is_limited(self):
        tracker = ACKTracker()
        for seq in range(0, (MAX_ACK_RANGES + 3) * 2, 2):
            tracker.record(seq)
        self.assertEqual(len(tracker.get_ranges()), MAX_ACK_RANGES)
        self.assertTrue(tracker.has_acked((MAX_ACK_RANGES + 2) * 2))

    def test_cumulative_ack_cannot_jump_over_gap(self):
        tracker = ACKTracker()
        tracker.acknowledge(65535, [(2, 3)], [0, 1, 2])
        self.assertEqual(tracker.contiguous_end(), 0)
        self.assertTrue(tracker.has_acked(2))
        self.assertFalse(tracker.has_acked(0))

if __name__ == "__main__":
    unittest.main()
