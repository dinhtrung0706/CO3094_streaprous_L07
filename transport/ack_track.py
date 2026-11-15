"""
Track acknowledged logical sequence ranges.
"""

import threading
from typing import List, Tuple

from .constant import HALF_LSEQ, MASK_LSEQ, MAX_ACK_RANGES


def lseq_distance(a: int, b: int) -> int:
    """
    Calculate forward distance with 16-bit wraparound.

    Args:
        a       destination sequence
        b       starting sequence

    Returns:
        Forward distance
    """
    return (a - b) & MASK_LSEQ


def lseq_next(seq: int) -> int:
    """
    Increment a logical sequence number.

    Args:
        seq     current sequence

    Returns:
        Next sequence
    """
    return (seq + 1) & MASK_LSEQ


def lseq_range(start: int, end: int) -> List[int]:
    """
    Get a half-open range with wraparound.

    Args:
        start   first sequence
        end     exclusive end

    Returns:
        List of sequences
    """
    return [(start + offset) & MASK_LSEQ for offset in range(lseq_distance(end, start))]


def newer(a: int, b: int) -> bool:
    """
    Compare logical sequences across wraparound.

    Args:
        a       candidate sequence
        b       reference sequence

    Returns:
        True if a is newer than b
    """
    return 0 < lseq_distance(a, b) < HALF_LSEQ


def _lseq_segments(start: int, end: int) -> List[Tuple[int, int]]:
    """
    Split a wrapping range into ordinary half-open ranges.

    Args:
        start   first sequence
        end     exclusive end

    Returns:
        List of nonwrapping ranges
    """
    limit = MASK_LSEQ + 1
    stop = start + lseq_distance(end, start)
    segments = [(start, min(stop, limit))]
    if stop > limit:
        segments.append((0, stop - limit))
    return segments


def _binsearch_placement(key, arr, start, end) -> Tuple[int, bool]:
    """
    Binary search for the placement of key in the array of ranges.

    Args:
        key     key value to search for in the array
        arr     array
        start   starting index to search for
        end     ending (not included) index to search for

    Returns:
        Tuple (index, is_in_range) represents whether the key value
        is in the range i or is between ranges i - 1 and i. More specific:
        - i, True: key value is in arr[i]
        - i, False: key value is in arr[i - 1] and arr[i]
    """
    if len(arr) == 0:
        return 0, False
    while start < end:
        mid = start + (end - start) // 2
        if arr[mid][0] <= key <= arr[mid][1]:
            return mid, True
        if key < arr[mid][0]:
            end = mid
        else:
            start = mid + 1

    return start, False


class ACKTracker:
    """
    Track acknowledged ranges in the 16-bit sequence space.
    """

    def __init__(self):
        """
        Initialize the ACK ranges and cumulative cursor.
        """
        # List of ACK ranges [a, b) that is represented as tuple (a, b)
        self._ack_windows = []
        self._cumulative_next = 0
        self._ack_windows_lock = threading.Lock()

    def _contains(self, seq: int) -> bool:
        """
        Check whether a sequence belongs to an ACK range.

        Args:
            seq     logical sequence

        Returns:
            True if acknowledged
        """
        index, found = _binsearch_placement(seq, self._ack_windows, 0, len(self._ack_windows))
        return found and seq != self._ack_windows[index][1]

    def _merge_range(self, start: int, end: int) -> bool:
        """
        Merge one nonwrapping range with existing ACK ranges.

        Args:
            start   first sequence
            end     exclusive end

        Returns:
            True if new sequences were acknowledged
        """
        count = len(self._ack_windows)
        istart, is_instart = _binsearch_placement(start, self._ack_windows, 0, count)
        iend, is_inend = _binsearch_placement(end, self._ack_windows, 0, count)

        # Duplicate ack
        if istart == iend and is_instart and is_inend:
            return False

        repl_start = self._ack_windows[istart][0] if is_instart else start
        repl_end = self._ack_windows[iend][1] if is_inend else end
        self._ack_windows[istart:iend + int(is_inend)] = [(repl_start, repl_end)]
        return True

    def _discard_range(self, start: int, end: int):
        """
        Remove a nonwrapping range from ACK history.

        Args:
            start   first sequence
            end     exclusive end
        """
        retained = []
        for first, last in self._ack_windows:
            if last <= start or first >= end:
                retained.append((first, last))
            else:
                if first < start:
                    retained.append((first, start))
                if last > end:
                    retained.append((end, last))
        self._ack_windows = retained

    def merge(self, ack_start, ack_end) -> bool:
        """
        Merge a half-open ACK range with wraparound support.

        Args:
            ack_start   first ACK sequence
            ack_end     exclusive end

        Returns:
            True if new sequences were acknowledged
        """
        if not (0 <= ack_start <= MASK_LSEQ and 0 <= ack_end <= MASK_LSEQ + 1):
            print(f"[ACKTracker] Warning: Invalid ACK range ({ack_start}, {ack_end})")
            return False

        span = lseq_distance(ack_end, ack_start)
        if not 0 < span < HALF_LSEQ:
            print(f"[ACKTracker] Warning: Invalid ACK range ({ack_start}, {ack_end})")
            return False

        changed = False
        with self._ack_windows_lock:
            window_start = self._cumulative_next
            window_end = (window_start + HALF_LSEQ) & MASK_LSEQ
            for start, end in _lseq_segments(ack_start, ack_end):
                for first, last in _lseq_segments(window_start, window_end):
                    left, right = max(start, first), min(end, last)
                    if left < right:
                        changed |= self._merge_range(left, right)

            while self._contains(self._cumulative_next):
                index, _ = _binsearch_placement(
                    self._cumulative_next, self._ack_windows, 0, len(self._ack_windows)
                )
                old_seq = self._cumulative_next
                self._cumulative_next = self._ack_windows[index][1] & MASK_LSEQ
                old_past_acked_limit = (old_seq - HALF_LSEQ) & MASK_LSEQ
                new_past_acked_limit = (self._cumulative_next - HALF_LSEQ) & MASK_LSEQ
                for first, last in _lseq_segments(old_past_acked_limit, new_past_acked_limit):
                    if first < last:
                        self._discard_range(first, last)

        return changed

    def record(self, logical_seq: int) -> bool:
        """
        Acknowledge one logical sequence.

        Args:
            logical_seq     received sequence

        Returns:
            True if newly acknowledged
        """
        return self.merge(logical_seq, lseq_next(logical_seq))

    def acknowledge(self, ack_seq: int, ranges: List[Tuple[int, int]],
                    pending_sequences) -> None:
        """
        Merge cumulative and selective ACK ranges.

        Args:
            ack_seq           cumulative ACK sequence
            ranges            selective ACK ranges
            pending_sequences outstanding sequences
        """
        start = self.contiguous_end()
        end = lseq_next(ack_seq)
        distance = lseq_distance(end, start)
        farthest = max((lseq_distance(seq, start) for seq in pending_sequences), default=None)

        if (0 < distance < HALF_LSEQ and farthest is not None
                and distance <= farthest + 1):
            self.merge(start, end)

        for first, last in ranges:
            self.merge(first, last)

    def is_cumulatively_acked(self, logical_seq: int) -> bool:
        """
        Check whether a sequence number precedes the cumulative ACKed or not.

        Args:
            logical_seq     sequence to check

        Returns:
            True if cumulatively acknowledged
        """
        with self._ack_windows_lock:
            return newer(self._cumulative_next, logical_seq)

    def cumulative_ack(self) -> int:
        """
        Get the most recent cumulative ACK.

        Returns:
            Last contiguous sequence
        """
        with self._ack_windows_lock:
            return (self._cumulative_next - 1) & MASK_LSEQ

    def has_acked(self, ack_seq) -> bool:
        """
        Check whether a sequence is acknowledged.

        Args:
            ack_seq     sequence to check

        Returns:
            True if acknowledged
        """
        with self._ack_windows_lock:
            return self._contains(ack_seq)

    def contiguous_end(self) -> int:
        """
        Get the first unacknowledged sequence.

        Returns:
            First unacknowledged sequence
        """
        with self._ack_windows_lock:
            return self._cumulative_next

    def get_ranges(self) -> List[Tuple[int, int]]:
        """
        Get the earliest acknowledged ranges.

        Returns:
            Up to MAX_ACK_RANGES half-open ACK ranges
        """
        with self._ack_windows_lock:
            return list(self._ack_windows[:MAX_ACK_RANGES])
