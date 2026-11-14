"""
Buffers for sending and assembling transport data.
"""

import errno
import threading

from .ack_track import lseq_distance, lseq_next, lseq_range, newer
from .constant import (
    MAX_SEGMENT_SIZE,
    SND_EAGER_CLEAN,
    MASK_PSEQ,
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
            send_data   initial outgoing bytes
        """
        self._lseq = 0
        self._sbuffer = bytearray()
        self._milestone_entries = []
        self._committed = 0
        if send_data:
            self.register_send_data(send_data)

    def _inc_seq_nr(self):
        """
        Advance the logical sequence number with wraparound.
        """
        self._lseq = lseq_next(self._lseq)

    def _sanitize_milestone(self):
        """
        Rebase remaining milestones after committed bytes are released.
        """
        if self._committed >= MASK_PSEQ or len(self._milestone_entries) <= SND_EAGER_CLEAN:
            self._milestone_entries = [length - self._committed for length in self._milestone_entries]
            self._committed = 0

    def register_send_partial_data(self, send_data):
        """
        Append outgoing data without ending the current message.

        Args:
            send_data   bytes to append
        """
        self._sbuffer.extend(send_data)

    def register_send_data(self, send_data):
        """
        Append outgoing data and mark the end of a message.

        Args:
            send_data   bytes to append
        """
        self.register_send_partial_data(send_data)
        self._milestone_entries.append(self._committed + len(self._sbuffer))

    def acquire_send_data(self):
        """
        Read the next chunk without removing it from the buffer.

        Returns:
            (logical_seq, chunk), or None when no data remains
        """
        if not self._sbuffer and not self._milestone_entries:
            return None

        remaining = (self._milestone_entries[0] - self._committed
                     if self._milestone_entries else len(self._sbuffer))
        return self._lseq, bytes(self._sbuffer[:min(MAX_SEGMENT_SIZE, remaining)])

    def is_last_chunk(self) -> bool:
        """
        Check whether the next chunk completes a message.

        Returns:
            True if the chunk has EOM
        """
        return bool(self._milestone_entries) and self._milestone_entries[0] - self._committed <= MAX_SEGMENT_SIZE

    def commit_send_data(self, size: int):
        """
        Release a chunk after Session accepts it.

        Args:
            size   accepted payload bytes
        """
        if not 0 <= size <= min(len(self._sbuffer), MAX_SEGMENT_SIZE):
            raise ValueError("Invalid committed chunk size")
        if self._milestone_entries:
            remaining = self._milestone_entries[0] - self._committed
            if size > remaining or (size == 0 and remaining != 0):
                raise ValueError("Committed chunk crosses a message boundary")
        elif size == 0:
            raise ValueError("No data to commit")

        del self._sbuffer[:size]
        self._committed += size
        # If the EOM data has committed, we remove its milestone entry
        if self._milestone_entries and self._committed == self._milestone_entries[0]:
            self._milestone_entries.pop(0)
        self._inc_seq_nr()

        # Sanitize the milestone entries
        self._sanitize_milestone()


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
            if not 0 <= logical_seq <= MASK_LSEQ:
                return RECV_ERROR
            if (logical_seq != self._s6t_unacked_seq
                    and not newer(logical_seq, self._s6t_unacked_seq)):
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
            # Search for sthe EOM lseq
            while end_seq in self._chunks:
                is_last = end_seq in self._last_seq
                end_seq = lseq_next(end_seq)
                if is_last:
                    break

            if end_seq == start_seq:
                return RECV_WAITING

            # Try to flush the chunks [start_seq, end_seq)
            try:
                output.seek(0, 2)
                start = output.tell()
                for seq in lseq_range(start_seq, end_seq):
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

            for seq in lseq_range(start_seq, end_seq):
                self._received_bytes -= len(self._chunks.pop(seq))
            self._s6t_unacked_seq = end_seq
            return start_seq, end_seq

    def flushed_eom(self) -> bool:
        """
        Check whether the lastest flushed chunk is eom.

        Returns:
            True if EOM was flushed
        """
        with self._rbuffer_lock:
            return (bool(self._last_seq)
                    and lseq_next(self._last_seq[0]) == self._s6t_unacked_seq)

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

            end = min(
                self._last_seq,
                key=lambda seq: lseq_distance(seq, self._s6t_unacked_seq),
            )
            sequences = lseq_range(self._s6t_unacked_seq, lseq_next(end))
            if any(seq not in self._chunks for seq in sequences):
                return None

            data = b"".join(self._chunks.pop(seq) for seq in sequences)
            self._received_bytes -= len(data)
            self._s6t_unacked_seq = lseq_next(end)
            self._last_seq.remove(end)
            return data
