"""
Track the peer's advertised receive window.
"""

import threading


class FControl:
    """
    Manage available peer receive-window capacity.
    """

    def __init__(self, rwnd):
        """
        Initialize flow control with the peer's receive window.

        Args:
            rwnd: Advertised receive window in bytes.

        Returns:
            None.
        """
        if rwnd < 0:
            raise ValueError("Receive window cannot be negative")

        self._rwnd = rwnd
        self._outstanding = 0
        self._lock = threading.Lock()

    @property
    def rwnd(self):
        """
        Read the advertised receive window.

        Args:
            self: Flow controller.

        Returns:
            Current receive window in bytes.
        """
        with self._lock:
            return self._rwnd

    def update_rwnd(self, rwnd):
        """
        Update the peer's advertised receive window.

        Args:
            rwnd: New receive window in bytes.

        Returns:
            None.
        """
        if rwnd < 0:
            raise ValueError("Receive window cannot be negative")
        with self._lock:
            if rwnd != self._rwnd:
                print(f"[FControl] rwnd {self._rwnd}->{rwnd}")
            self._rwnd = rwnd

    def can_send(self, data_size):
        """
        Check whether new data fits the receive window.

        Args:
            data_size: Number of bytes to send.

        Returns:
            True if sufficient window capacity remains.
        """
        with self._lock:
            return self._outstanding + data_size <= self._rwnd

    def account_send(self, data_size):
        """
        Account for newly sent, unacknowledged bytes.

        Args:
            data_size: Number of newly sent bytes.

        Returns:
            True if accounted, or False if the window is full.
        """
        if data_size <= 0:
            raise ValueError("Data size must be positive")
        with self._lock:
            if self._outstanding + data_size > self._rwnd:
                print(f"[FControl] BLOCK data_size={data_size} rwnd={self._rwnd} "
                      f"outstanding_size={self._outstanding}")
                return False

            self._outstanding += data_size
            return True

    def account_ack(self, ack_bytes):
        """
        Release window capacity for acknowledged bytes.

        Args:
            ack_bytes: Newly acknowledged bytes.

        Returns:
            None.
        """
        if ack_bytes < 0:
            raise ValueError("ACK bytes cannot be negative")

        with self._lock:
            if ack_bytes > self._outstanding:
                raise ValueError("ACK exceeds outstanding bytes")

            self._outstanding -= ack_bytes
