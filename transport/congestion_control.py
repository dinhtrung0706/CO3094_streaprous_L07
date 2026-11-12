"""
Track congestion window usage and AIMD adjustments.
"""

import threading

from .constant import MAX_SEGMENT_SIZE, MIN_CWND, MIN_SSTHRESH


class CControl:
    """
    Manage congestion window growth and recovery.
    """

    def __init__(self, cwnd, ssthresh):
        """
        Initialize congestion control state.

        Args:
            cwnd: Initial congestion window in bytes.
            ssthresh: Initial slow-start threshold in bytes.

        Returns:
            None.
        """
        self._in_flight = 0
        self._cwnd = float(cwnd)
        self._ssthresh = float(ssthresh)
        self._wnd_lock = threading.Lock()
        self._in_fast_recovery = False

    def can_send(self, data_size):
        """
        Check available congestion-window capacity.

        Args:
            data_size: Number of bytes to send.

        Returns:
            True if the data fits the congestion window.
        """
        with self._wnd_lock:
            return self._in_flight + data_size <= self._cwnd

    def account_send(self, data_size):
        """
        Account for newly sent bytes in flight.

        Args:
            data_size: Number of newly sent bytes.

        Returns:
            True if accounted, or False if the window is full.
        """
        if data_size <= 0:
            raise ValueError("Data size must be positive")

        with self._wnd_lock:
            if self._in_flight + data_size > self._cwnd:
                return False

            self._in_flight += data_size
            return True

    def account_ack(self, ack_bytes):
        """
        Remove acknowledged bytes and grow the congestion window.

        Args:
            ack_bytes: Newly acknowledged bytes.

        Returns:
            None.
        """
        if ack_bytes < 0:
            raise ValueError("ACK bytes cannot be negative")
        with self._wnd_lock:
            if ack_bytes > self._in_flight:
                print(f"[CControl] ACK exceeds in-flight bytes")
                raise ValueError("ACK exceeds in-flight bytes")

            self._in_flight -= ack_bytes

            if self._in_fast_recovery:
                return

            if self._cwnd < self._ssthresh:
                self._cwnd += min(ack_bytes, MAX_SEGMENT_SIZE)
            else:
                self._cwnd += MAX_SEGMENT_SIZE * ack_bytes / self._cwnd

    def switch_fast_recovery(self, session, lseq_list):
        """
        Reduce the congestion window when loss is detected.

        Args:
            session: Associated transport session.
            lseq_list: Logical sequences identified as lost.

        Returns:
            False if recovery is already active, otherwise None.
        """
        with self._wnd_lock:
            if self._in_fast_recovery:
                return False

            self._ssthresh = max(self._cwnd / 2, MIN_SSTHRESH)
            self._cwnd = self._ssthresh
            self._in_fast_recovery = True

    def finish_fast_recovery(self):
        """
        End fast recovery.

        Args:
            self: Congestion controller.

        Returns:
            None.
        """
        with self._wnd_lock:
            self._in_fast_recovery = False

    def on_timeout(self):
        """
        Reduce the congestion window following a timeout.

        Args:
            self: Congestion controller.

        Returns:
            None.
        """
        with self._wnd_lock:
            self._ssthresh = max(self._cwnd / 2, MIN_SSTHRESH)
            self._cwnd = float(MIN_CWND)
            self._in_fast_recovery = False
