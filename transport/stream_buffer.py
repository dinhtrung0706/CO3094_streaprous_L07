"""
Buffers for sending and assembling transport data.
"""

import threading

from .constant import MAX_SEGMENT_SIZE, MASK_SEQ_NR


class SendBuffer:
    def __init__(self, send_data):
        """
        Split outgoing data into logical chunks.

        Args:
            send_data: Bytes to send.

        """
        self._lseq = 0
        self._sbuffer = bytearray(send_data)
        self._max_chunk_snr = (len(self._sbuffer) + MAX_SEGMENT_SIZE - 1) // MAX_SEGMENT_SIZE

    def _inc_seq_nr(self):
        """
        Advance the logical sequence number with wraparound.

        Args:
            self: Send buffer.

        """
        self._lseq = (self._lseq + 1) & MASK_SEQ_NR

    def acquire_send_data(self):
        """
        Retrieve the next chunk of outgoing data.

        Args:
            self: Send buffer.

        Returns:
            (logical_seq, chunk), or None when all data is acquired.
        """
        if self._lseq >= self._max_chunk_snr:
            return None

        start = self._lseq * MAX_SEGMENT_SIZE
        end = start + MAX_SEGMENT_SIZE
        ret = self._lseq, self._sbuffer[start:end]
        self._inc_seq_nr()
        return ret


class ReceiveBuffer:
    def __init__(self):
        """
        Initialize an empty receive buffer.

        Args:
            self: Receive buffer.

        """
        self._rbuffer = bytearray()
        self._max_chunk_rnr = -1
        self._rbuffer_lock = threading.Lock()

    def register_receive_data(self, logical_seq, data: bytes):
        """
        Place a received chunk at its logical offset.

        Args:
            logical_seq: Logical chunk number.
            data: Received chunk bytes.

        """
        start = logical_seq * MAX_SEGMENT_SIZE
        end = start + len(data)

        with self._rbuffer_lock:
            if end > len(self._rbuffer):
                self._rbuffer.extend(b"\x00" * (end - len(self._rbuffer)))

            self._rbuffer[start : end] = data
            self._max_chunk_rnr = max(self._max_chunk_rnr, logical_seq)

    def form_receive_data(self):
        """
        Return the assembled bytes and clear the buffer.

        Args:
            self: Receive buffer.

        Returns:
            Assembled data as bytes.
        """
        with self._rbuffer_lock:
            self._max_chunk_rnr = -1
            data = bytes(self._rbuffer)
            self._rbuffer = bytearray()

        return data
