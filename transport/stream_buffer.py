"""
Buffers for sending and assembling transport data.
"""

import errno
import threading

from .constant import (
    MAX_SEGMENT_SIZE,
    MAX_RECEIVE_BUFFER,
    MASK_LSEQ,
    RECV_STORED,
    RECV_DUPLICATE,
    RECV_FULL,
    RECV_ERROR,
    RECV_WAITING,
)


class SendBuffer:
    def __init__(self, send_data):
        """
        Store outgoing data until Session accepts each chunk.

        Args:
            send_data: Bytes to send.
        """
        self._lseq = 0
        self._sbuffer = bytearray(send_data)
        self._acquired = False

    def _inc_seq_nr(self):
        """
        Advance the logical sequence number with wraparound.
        """
        self._lseq = (self._lseq + 1) & MASK_LSEQ

    def acquire_send_data(self):
        """
        Read the next chunk without removing it from the buffer.

        Returns:
            (logical_seq, chunk), or None when no data remains.
        """
        if not self._sbuffer:
            return None

        self._acquired = True
        return self._lseq, bytes(self._sbuffer[:MAX_SEGMENT_SIZE])

    def commit_send_data(self):
        """
        Release the acquired chunk after Session takes ownership.
        """
        if not self._acquired:
            raise ValueError("No acquired chunk to commit")

        del self._sbuffer[:MAX_SEGMENT_SIZE]
        self._inc_seq_nr()
        self._acquired = False


class ReceiveBuffer:
    def __init__(self, capacity=MAX_RECEIVE_BUFFER):
        """
        Store received chunks and the ends of queued messages.

        Args:
            capacity: Maximum number of buffered payload bytes.
        """
        self._capacity = capacity
        self._received_bytes = 0
        self._chunks = {} # { lseq : data }
        self._last_seq = []
        self._s6t_unacked_seq = 0 # smallest unacked seq
        self._rbuffer_lock = threading.Lock()

    def register_receive_data(self, logical_seq, data: bytes, is_last=False) -> int:
        """
        Store a chunk or report why it could not be accepted.

        Args:
            logical_seq: Logical sequence number of the chunk.
            data: Received chunk bytes.
            is_last: Whether this chunk ends a message.

        Returns:
            RECV_STORED for new data, RECV_DUPLICATE for repeated data,
            RECV_FULL when capacity is exhausted, or RECV_ERROR for invalid data.
        """
        with self._rbuffer_lock:
            if logical_seq < 0:
                return RECV_ERROR
            if logical_seq < self._s6t_unacked_seq:
                return RECV_DUPLICATE

            if logical_seq in self._chunks:
                if self._chunks[logical_seq] != data:
                    return RECV_ERROR
                status = RECV_DUPLICATE
            else:
                if self._received_bytes + len(data) > self._capacity:
                    return RECV_FULL
                self._chunks[logical_seq] = bytes(data)
                self._received_bytes += len(data)
                status = RECV_STORED

            if is_last and logical_seq not in self._last_seq:
                self._last_seq.append(logical_seq)
                self._last_seq.sort()

            return status

    def flush_to_file(self, output) -> tuple[int, int] | int:
        """
        Flush only consecutive buffered chunks to a binary file.

        Args:
            output: Open binary file supporting seek() and truncate().

        Returns:
            Flushed half-open sequence range
            or RECV_WAITING when there is nothing to be flushed,
            or RECV_ERROR when there is unexpected error thus nothing was flushed (inspect further in the function).
        """
        with self._rbuffer_lock:
            start_seq = self._s6t_unacked_seq
            end_seq = start_seq
            while end_seq in self._chunks:
                if len(self._chunks[end_seq]) > MAX_SEGMENT_SIZE:
                    return RECV_ERROR
                end_seq += 1

            if end_seq == start_seq:
                return RECV_WAITING

            try:
                output.seek(0, 2)
                start = output.tell()
                for seq in range(start_seq, end_seq):
                    data = self._chunks[seq]
                    if output.write(data) != len(data):
                        raise OSError(errno.ENOSPC, "Incomplete file write")
                output.flush()
            except (OSError, ValueError):
                print(f"[ReceiveBuffer] Unexpected error happened when trying to flush fromm the buffer to the file.")
                try:
                    output.seek(start)
                    output.truncate()
                except (OSError, ValueError, UnboundLocalError):
                    print(f"[ReceiveBuffer] Unexpected error happened when trying to revert the file when early error occur.")
                    return RECV_ERROR
                return RECV_ERROR

            for seq in range(start_seq, end_seq):
                self._received_bytes -= len(self._chunks.pop(seq))
            self._last_seq = [seq for seq in self._last_seq if seq >= end_seq]
            self._s6t_unacked_seq = end_seq
            return start_seq, end_seq

    def buffered_size(self) -> int:
        """
        Return the number of payload bytes currently held in memory.

        Returns:
            Buffered payload size in bytes.
        """
        with self._rbuffer_lock:
            return self._received_bytes

    def available_capacity(self) -> int:
        """
        Return the remaining receive-buffer space.

        Returns:
            Number of available payload bytes.
        """
        with self._rbuffer_lock:
            return self._capacity - self._received_bytes

    def form_receive_data(self) -> bytes | None:
        """
        Assemble the next complete message, keeping later chunks buffered.

        Returns:
            Assembled message bytes, or None if it is incomplete.
        """
        with self._rbuffer_lock:
            if not self._last_seq:
                return None

            end = self._last_seq[0]
            if any(seq not in self._chunks for seq in range(self._s6t_unacked_seq, end + 1)):
                return None

            data = b"".join(self._chunks.pop(seq) for seq in range(self._s6t_unacked_seq, end + 1))
            self._received_bytes -= len(data)
            self._s6t_unacked_seq = end + 1
            self._last_seq.pop(0)
            return data
