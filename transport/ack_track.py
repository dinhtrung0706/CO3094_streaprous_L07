"""
ACK tracker to ease the tracking ACKed or not ACKed packets process
"""

import threading

def _binsearch_placement(key, arr, start, end) -> tuple[int, bool]:
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
    def __init__(self):
        # List of ACK ranges [a, b) that is represented as tuple (a, b)
        self._ack_windows = []
        self._ack_windows_lock = threading.Lock()

    def merge(self, ack_start, ack_end) -> bool:
        """
        Insert [ack_start, ack_end) and merge relevant windows into one

        Args:
            ack_start     ack starting sequence number
            ack_end       ack ending sequence number (not included)

        Returns:
            True if the inserted window did contribute acking the unacked packet.
            False if all in the window are duplicate.
        """
        if ack_start >= ack_end:
            print(f"[Streap_raw] Warning: Incorrect order of ACK start and ACK end in the ACKTracker.merge({ack_start}, {ack_end})")
            return False
        with self._ack_windows_lock:
            len_ack_windows = len(self._ack_windows)
            istart, is_instart = _binsearch_placement(ack_start, self._ack_windows, 0, len_ack_windows)
            iend, is_inend = _binsearch_placement(ack_end, self._ack_windows, 0, len_ack_windows)

            # Duplicate ack
            if istart == iend and is_instart and is_inend:
                return False
            repl_start = self._ack_windows[istart][0] if is_instart else ack_start
            repl_end = self._ack_windows[iend][1] if is_inend else ack_end

            self._ack_windows[istart : iend + int(is_inend)] = [(repl_start, repl_end)]

        return True

    def has_acked(self, ack_seq) -> bool:
        """
        Check if ack packet is duplicate
        Args:
            ack_seq      The ack sequence number to be checked

        Returns:
            True if ack packet is duplicate, otherwise False.
        """
        with self._ack_windows_lock:
            index, result = _binsearch_placement(ack_seq, self._ack_windows, 0, len(self._ack_windows))
            return result and ack_seq != self._ack_windows[index][1]
