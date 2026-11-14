"""
Coordinate transport packets across CONTROL and TRANSFER lanes.
"""

import struct
import threading
import time
from pathlib import Path
from typing import Optional, Tuple, Union

from .ack_track import ACKTracker, lseq_next, lseq_range
from .congestion_control import CControl
from .constant import (
    PK_ILANE, PK_ITRANSID, PK_ILSEQ, MAX_RECEIVE_BUFFER, MAX_SEGMENT_SIZE,
    MASK_PSEQ, TRANSFER_ID_SIZE, ZERO_WINDOW_PROBE_INTERVAL,
    ACK_MILESTONE_BYTES, MAX_ACK_DELAY,
    RECV_STORED, RECV_DUPLICATE, RECV_FULL, RECV_ERROR, RECV_WAITING,
)
from .flow_control import FControl
from .packet import (
    ACK, CONTROL, DATA, EOM, HEADER_SIZE, PROBE, TRANSFER,
    VERSION, Packet, decode_ack_ranges, encode_ack_ranges,
)
from .stream_buffer import ReceiveBuffer, SendBuffer
from .transmitter import Transmitter


class Session:
    """
    Manage packet sending and receiving over an established connection.
    """

    def __init__(self, conn, max_retries, receive_dir="Downloads"):
        """
        Initialize lane trackers and shared transport controls.

        Args:
            conn: Established connection.
            max_retries: Maximum retransmission attempts per packet.
            receive_dir: Directory used for transfers saved to disk
        """
        self._conn = conn
        self._send_trackers = {CONTROL: ACKTracker(), TRANSFER: {}}
        self._recv_trackers = {CONTROL: ACKTracker(), TRANSFER: {}}
        self._recv_buffers = {}
        self._file_outputs = {}
        self._completed_files = {}
        self._receive_dir = Path(receive_dir)
        self._next_packet_seq = 0
        self._last_window_seq = None

        self._transmitter = Transmitter(conn._handshake_rtt, max_retries)
        self._fcontrol = FControl(conn._rwnd)
        self._ccontrol = CControl(conn._cwnd, conn._cwnd / 2)

        self._pending_packets = {} # { packet_key : Packet }
        self._send_lock = threading.Lock()
        self._recv_lock = threading.Lock()
        self._aborted = False
        self._probe_event = threading.Event()
        self._probe_thread = None
        self._pending_acks = {} # { transfer_id : (Packet, accumulated_bytes) }
        self._ack_timer = None
        self._transmitter.start_thread_deadline(self)
        conn.register_disconnect_handler(self.abort)
        if self._fcontrol.rwnd == 0 and not self._aborted:
            self._start_probing()

    def _guard_active(self) -> bool:
        """
        Check whether the session can still handle packets.

        Returns:
            True if active, False if aborted
        """
        if self._aborted:
            print("[Session] Warning: Session is aborted")
            return False
        return True

    def _available_receive_name(self, path: Path, check_partial=False) -> Path:
        """
        Find an unused filename by appending -1 as needed.

        Args:
            path           desired completed file path
            check_partial  also avoid existing .part files

        Returns:
            Unused file path
        """
        while path.exists() or (check_partial and path.with_name(path.name + ".part").exists()):
            path = path.with_name(path.name + "-1")
        return path

    def _close_receive_output(self, output, remove=False):
        """
        Close an output and optionally discard its file.

        Args:
            output  opened receive file
            remove  delete the partial file
        """
        try:
            output.close()
        except OSError as exc:
            print(f"[Session] Warning: Cannot close receive file: {exc}")
        if remove:
            try:
                Path(output.name).unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                print(f"[Session] Warning: Cannot remove partial file: {exc}")

    def _transfer_id(self, packet: Packet) -> Optional[int]:
        """
        Read the transfer identifier from a TRANSFER packet.

        Args:
            packet: packet containing a transfer identifier

        Returns:
            Transfer identifier, or None if missing
        """
        if len(packet.payload) < TRANSFER_ID_SIZE:
            print("[Session] Warning: TRANSFER packet is missing its identifier")
            return None
        return struct.unpack_from("!I", packet.payload)[0]

    def _packet_key(self, packet: Packet) -> Tuple[int, Optional[int], int]:
        """
        Identify a logical packet by its lane, transfer, and sequence.

        Args:
            packet: Packet whose identity is required.

        Returns:
            A (lane, transfer_id, logical_seq) tuple.
        """
        transfer_id = self._transfer_id(packet) if packet.lane == TRANSFER else None
        return packet.lane, transfer_id, packet.logical_seq

    def _acquire_tracker(self, trackers: dict, lane: int,
                         transfer_id: Optional[int] = None,
                         create: bool = False) -> Optional[ACKTracker]:
        """
        Get the ACK tracker for a lane or transfer.

        Args:
            trackers: Send or receive tracker collection.
            lane: Packet lane.
            transfer_id: Transfer identifier.
            create: Whether to create a missing transfer tracker.

        Returns:
            Matching tracker, or None if the transfer is unknown.
        """
        if lane == CONTROL:
            return trackers[CONTROL]
        if transfer_id is None:
            print("[Session] Warning: TRANSFER tracker requires an identifier")
            return None
        if create:
            return trackers[TRANSFER].setdefault(transfer_id, ACKTracker())
        return trackers[TRANSFER].get(transfer_id)

    def send_packet(self, packet: Packet) -> bool:
        """
        Reserve DATA capacity before transmitting a packet.

        Args:
            packet: Packet to encode and send.

        Returns:
            True if accepted; False if rejected or CONTROL send fails.
        """
        is_data = bool(packet.flags & DATA)

        with self._send_lock:
            if not self._guard_active():
                return False
            packet.packet_seq = self._next_packet_seq
            encoded = packet.encode()
            packet_size = len(encoded)

            if is_data:
                key = self._packet_key(packet)
                if key in self._pending_packets:
                    print("[Session] Warning: Logical packet is already pending")
                    return False

                transfer_id = key[PK_ITRANSID]

                flow_size = len(packet.payload) - TRANSFER_ID_SIZE if packet.lane == TRANSFER else 0
                if flow_size and not self._fcontrol.account_send(flow_size):
                    return False
                if not self._ccontrol.account_send(packet_size):
                    if flow_size:
                        self._fcontrol.account_ack(flow_size)
                    return False

                self._acquire_tracker(
                    self._send_trackers, packet.lane, transfer_id, create=True
                )

                self._pending_packets[key] = packet
                self._transmitter.register_packet(key, time.monotonic())

            self._next_packet_seq = (self._next_packet_seq + 1) & MASK_PSEQ
            try:
                bytes_sent = self._conn.send_data_segment(encoded)
            except OSError as exc:
                print(f"[Session] Warning: Packet send failed: {exc}")
                return is_data
            if bytes_sent != packet_size:
                print("[Session] Warning: Incomplete packet send")
                return is_data

            return True

    def send_next_chunk(self, buffer: SendBuffer,
                        transfer_id: Optional[int] = None) -> Optional[bool]:
        """
        Send one buffered chunk with its transfer identifier.

        Args:
            buffer        outgoing data buffer
            transfer_id   transfer identifier

        Returns:
            True if accepted, False if rejected, None if empty
        """
        if not isinstance(transfer_id, int) or not 0 <= transfer_id <= MASK_PSEQ:
            print("[Session] Warning: TRANSFER requires a valid identifier")
            return False

        acquired = buffer.acquire_send_data()
        if acquired is None:
            return None

        logical_seq, data = acquired
        payload = struct.pack("!I", transfer_id) + data
        flags = DATA | EOM if buffer.is_last_chunk() else DATA
        packet = Packet(VERSION, TRANSFER, flags, logical_seq, 0, 0,
                        self._receive_window(), payload)

        accepted = self.send_packet(packet)
        if accepted:
            buffer.commit_send_data(len(data))
        return accepted

    def retransmit_packet(self, packet_key: Tuple[int, Optional[int], int]) -> bool:
        """
        Retransmit a pending logical packet without reserving capacity again.

        Args:
            packet_key: Lane, transfer ID, and logical sequence of the packet.

        Returns:
            True if sent; False if pending packet is missing or send fails.
        """
        with self._send_lock:
            if not self._guard_active():
                return False
            packet = self._pending_packets.get(packet_key)
            if packet is None:
                return False

            lane, transfer_id, logical_seq = packet_key
            tracker = self._acquire_tracker(
                self._send_trackers, lane, transfer_id
            )
            if tracker is not None and tracker.has_acked(logical_seq):
                return False

            packet.packet_seq = self._next_packet_seq
            encoded = packet.encode()
            self._next_packet_seq = (self._next_packet_seq + 1) & MASK_PSEQ
            try:
                bytes_sent = self._conn.send_data_segment(encoded)
            except OSError as exc:
                print(f"[Session] Warning: Retransmission failed: {exc}")
                return False
            if bytes_sent != len(encoded):
                print("[Session] Warning: Incomplete retransmission")
                return False
            return True

    def _update_peer_window(self, packet: Packet):
        """
        Apply a receive-window update from a newer packet.

        Args:
            packet      incoming packet with advertised window
        """
        if self._last_window_seq is not None:
            distance = (packet.packet_seq - self._last_window_seq) & MASK_PSEQ
            if distance == 0 or distance >= (MASK_PSEQ + 1) // 2:
                return

        self._fcontrol.update_rwnd(packet.rwnd)
        self._last_window_seq = packet.packet_seq
        if packet.rwnd == 0:
            self._start_probing()
        else:
            self._probe_event.set()

    def _start_probing(self):
        """
        Start the zero-window probe worker when needed.
        """
        if self._probe_thread is None:
            self._probe_thread = threading.Thread(target=self._probe_loop, daemon=True)
            self._probe_thread.start()
        else:
            self._probe_event.set()

    def _wait_close_probe(self):
        """
        Stop the zero-window probe worker.
        """
        self._probe_event.set()
        if self._probe_thread is not None and threading.current_thread() != self._probe_thread:
            self._probe_thread.join()

    def _probe_loop(self):
        """
        Probe periodically while the peer receive window is closed.
        """
        while not self._aborted:
            self._probe_event.clear()
            if self._fcontrol.rwnd != 0:
                self._probe_event.wait()
                continue
            if self._probe_event.wait(ZERO_WINDOW_PROBE_INTERVAL):
                continue
            if self._aborted or self._fcontrol.rwnd != 0:
                continue
            with self._recv_lock:
                rwnd = self._receive_window()
            probe = Packet(VERSION, CONTROL, PROBE, 0, 0, 0, rwnd)
            self.send_packet(probe)

    def handle_ack(self, packet: Packet):
        """
        Apply received ACKs and release acknowledged send capacity.

        Args:
            packet: Incoming ACK packet.
        """
        if not packet.flags & ACK:
            print("[Session] Warning: Expected an ACK packet")
            return

        with self._send_lock:
            if not self._guard_active():
                return
            if packet.flags & PROBE:
                self._update_peer_window(packet)
                return

            transfer_id = None
            if packet.lane == CONTROL:
                if packet.payload:
                    print("[Session] CONTROL ACK currently is not supported content payload")
                ranges = []
            else:
                transfer_id = self._transfer_id(packet)
                if transfer_id is None:
                    return
                ranges = decode_ack_ranges(packet.payload[TRANSFER_ID_SIZE:])

            tracker = self._acquire_tracker(
                self._send_trackers, packet.lane, transfer_id
            )

            if tracker is None:
                self._update_peer_window(packet)
                return

            tracker.acknowledge(
                packet.ack_seq, ranges,
                (
                    key[PK_ILSEQ] for key in self._pending_packets
                    if key[PK_ILANE] == packet.lane
                    and key[PK_ITRANSID] == transfer_id
                ),
            )
            congestion_bytes = 0
            flow_bytes = 0

            for key, pending in list(self._pending_packets.items()):
                if key[PK_ILANE] != packet.lane:
                    continue
                if key[PK_ITRANSID] != transfer_id:
                    continue

                if not (tracker.has_acked(key[PK_ILSEQ])
                        or tracker.is_cumulatively_acked(key[PK_ILSEQ])):
                    continue

                congestion_bytes += HEADER_SIZE + len(pending.payload)
                if packet.lane == TRANSFER:
                    flow_bytes += len(pending.payload) - TRANSFER_ID_SIZE
                self._transmitter.acking_packet(key)
                del self._pending_packets[key]

            if congestion_bytes:
                self._ccontrol.account_ack(congestion_bytes)
            if flow_bytes:
                self._fcontrol.account_ack(flow_bytes)
            self._update_peer_window(packet)

    def handle_dropped_packets(self, packet_keys):
        """
        Discard exhausted packets and account for their bytes.

        Args:
            packet_keys   logical packet keys to discard
        """
        with self._send_lock:
            if not self._guard_active():
                return

            congestion_bytes = 0
            for key in packet_keys:
                packet = self._pending_packets.pop(key, None)
                if packet is None:
                    continue

                print(f"[Session] Warning: Packet exhausted retries: {key}")
                congestion_bytes += HEADER_SIZE + len(packet.payload)

            if congestion_bytes:
                self._ccontrol.account_drop(congestion_bytes)

    def _acquire_receive_buffer(self, transfer_id: int) -> ReceiveBuffer:
        """
        Get the receive buffer for a transfer.

        Args:
            transfer_id   transfer identifier

        Returns:
            Transfer receive buffer
        """
        return self._recv_buffers.setdefault(transfer_id, ReceiveBuffer())

    def _flush_receive_file(self, transfer_id: int) -> Union[Tuple[int, int], int]:
        """
        Flush consecutive chunks to the partial file.

        Args:
            transfer_id  transfer identifier

        Returns:
            Flushed range or receive status
        """
        buffer = self._recv_buffers[transfer_id]
        output = self._file_outputs[transfer_id]
        return buffer.flush_to_file(output)

    def _complete_receive_file(self, transfer_id: int) -> int:
        """
        Finalize a file after its EOM has been flushed.
        This should be use after calling flush_receive_file() (if success)

        Args:
            transfer_id  transfer identifier

        Returns:
            RECV_STORED if complete, RECV_WAITING if incomplete, or RECV_ERROR
        """
        buffer = self._recv_buffers[transfer_id]
        if not buffer.flushed_eom():
            return RECV_WAITING

        output = self._file_outputs[transfer_id]
        partial_path = Path(output.name)
        completed_path = self._available_receive_name(partial_path.with_suffix(""))
        try:
            partial_path.rename(completed_path)
        except OSError as exc:
            print(f"[Session] Warning: Cannot finalize receive file: {exc}")
            return RECV_ERROR

        self._close_receive_output(output)
        self._file_outputs.pop(transfer_id)
        self._recv_buffers.pop(transfer_id, None)
        self._completed_files[transfer_id] = completed_path
        return RECV_STORED

    def register_receive_file(self, transfer_id: int) -> Optional[Path]:
        """
        Save an incoming transfer to disk instead of delivering bytes.

        Args:
            transfer_id   transfer identifier

        Returns:
            Partial or completed file path, or None on failure
        """
        if not isinstance(transfer_id, int) or not 0 <= transfer_id <= MASK_PSEQ:
            print("[Session] Warning: Invalid transfer identifier")
            return None

        with self._recv_lock:
            if not self._guard_active():
                return None
            if transfer_id in self._completed_files:
                return self._completed_files[transfer_id]
            if transfer_id in self._file_outputs:
                return Path(self._file_outputs[transfer_id].name)

            try:
                self._receive_dir.mkdir(parents=True, exist_ok=True)
                basename = self._available_receive_name(
                    self._receive_dir / str(transfer_id), check_partial=True
                )
                path = basename.with_name(basename.name + ".part")
                output = path.open("x+b")
            except OSError as exc:
                print(f"[Session] Warning: Cannot create receive file: {exc}")
                return None

            self._acquire_receive_buffer(transfer_id)
            self._file_outputs[transfer_id] = output

            if self._flush_receive_file(transfer_id) == RECV_ERROR:
                self._file_outputs.pop(transfer_id, None)
                self._close_receive_output(output, remove=True)
                print("[Session] Warning: Failed to flush buffered transfer data")
                return None
            if self._complete_receive_file(transfer_id) == RECV_ERROR:
                return None

            return self._completed_files.get(transfer_id, path)

    def _receive_window(self) -> int:
        """
        Calculate free memory across all transfer buffers.

        Returns:
            Available receive-buffer capacity in bytes
        """
        used = sum(buffer.buffered_size() for buffer in self._recv_buffers.values())
        return max(0, MAX_RECEIVE_BUFFER - used)

    def _make_ack(self, packet: Packet) -> Optional[Packet]:
        """
        Build an ACK from the received logical ranges.

        Args:
            packet: Packet that should be acknowledged.

        Returns:
            ACK packet, or None if no CONTROL prefix is complete.
        """
        if packet.flags == PROBE:
            return Packet(VERSION, packet.lane, ACK | PROBE, 0, 0, 0,
                          self._receive_window())
        # Normal ACK packet forming
        transfer_id = self._transfer_id(packet) if packet.lane == TRANSFER else None
        if packet.lane == TRANSFER and transfer_id is None:
            return None
        tracker = self._acquire_tracker(self._recv_trackers, packet.lane, transfer_id)
        if tracker is None:
            return None

        ranges = tracker.get_ranges()
        cumulative = tracker.cumulative_ack()
        if packet.lane == CONTROL:
            if not tracker.has_acked(cumulative):
                return None
            payload = b""
        else:
            payload = encode_ack_ranges(ranges)
            payload = struct.pack("!I", transfer_id) + payload

        return Packet(VERSION, packet.lane, ACK, 0, cumulative, 0,
                      self._receive_window(), payload)

    def _send_delayed_acks(self):
        """
        Send accumulated ACKs after the maximum delay.
        """
        with self._recv_lock:
            self._ack_timer = None
            if self._aborted:
                return
            responses = [self._make_ack(packet) for packet, _ in self._pending_acks.values()]
            self._pending_acks.clear()

        for response in responses:
            if response is not None:
                self.send_packet(response)

    def _schedule_ack(self, packet: Packet, status: int) -> Optional[Packet]:
        """
        Delay a DATA ACK unless an immediate reply is needed.

        Args:
            packet  received DATA packet
            status  receive status

        Returns:
            Immediate ACK packet, or None if delayed
        """
        if packet.lane == CONTROL:
            return self._make_ack(packet)
        transfer_id = self._transfer_id(packet)
        tracker = self._acquire_tracker(self._recv_trackers, packet.lane, transfer_id)

        previous = self._pending_acks.get(transfer_id)
        received_bytes = len(packet.payload) - TRANSFER_ID_SIZE if status == RECV_STORED else 0
        total_bytes = received_bytes + (previous[1] if previous is not None else 0)

        gap_changed = (status == RECV_STORED
                       and tracker.contiguous_end() != lseq_next(packet.logical_seq))
        if ((status == RECV_STORED and packet.flags & EOM) or gap_changed
                or self._receive_window() == 0 or total_bytes >= ACK_MILESTONE_BYTES):
            self._pending_acks.pop(transfer_id, None)
            return self._make_ack(packet)

        self._pending_acks[transfer_id] = (packet, total_bytes)
        if self._ack_timer is None:
            self._ack_timer = threading.Timer(MAX_ACK_DELAY, self._send_delayed_acks)
            self._ack_timer.daemon = True
            self._ack_timer.start()
        return None

    def handle_data(self, packet: Packet) -> int:
        """
        Store incoming DATA before recording its acknowledgment.

        Args:
            packet: Incoming DATA packet.

        Returns:
            Receive status for the incoming chunk.
        """
        if not self._guard_active():
            return RECV_ERROR
        if not packet.flags & DATA:
            print("[Session] Warning: Expected a DATA packet")
            return RECV_ERROR

        transfer_id = self._transfer_id(packet) if packet.lane == TRANSFER else None
        if packet.lane == TRANSFER and transfer_id is None:
            return RECV_ERROR
        tracker = self._acquire_tracker(
            self._recv_trackers, packet.lane, transfer_id, create=True
        )
        if packet.lane == CONTROL:
            if not tracker.record(packet.logical_seq):
                return (RECV_DUPLICATE if tracker.has_acked(packet.logical_seq)
                        else RECV_ERROR)
            return RECV_STORED
        if transfer_id in self._completed_files:
            return (RECV_DUPLICATE if tracker.has_acked(packet.logical_seq)
                    else RECV_ERROR)

        buffer = self._acquire_receive_buffer(transfer_id)
        data = packet.payload[TRANSFER_ID_SIZE:]
        if len(data) > MAX_SEGMENT_SIZE:
            return RECV_ERROR
        is_last = bool(packet.flags & EOM)
        if not tracker.has_acked(packet.logical_seq) and len(data) > self._receive_window():
            if transfer_id in self._file_outputs:
                if self._flush_receive_file(transfer_id) == RECV_ERROR:
                    return RECV_ERROR
            if len(data) > self._receive_window():
                return RECV_FULL

        status = buffer.register_receive_data(packet.logical_seq, data, is_last)
        if status == RECV_STORED:
            tracker.record(packet.logical_seq)
        elif status == RECV_DUPLICATE and not tracker.has_acked(packet.logical_seq):
            return RECV_ERROR

        if transfer_id in self._file_outputs and status in (RECV_STORED, RECV_DUPLICATE):
            if self._flush_receive_file(transfer_id) == RECV_ERROR:
                return RECV_ERROR
            if self._complete_receive_file(transfer_id) == RECV_ERROR:
                return RECV_ERROR

        return status

    def receive_transfer(self, transfer_id: int) -> Optional[bytes]:
        """
        Return completed in-memory transfer data.

        Args:
            transfer_id   transfer identifier

        Returns:
            Completed bytes or None
        """
        with self._recv_lock:
            if transfer_id in self._file_outputs or transfer_id in self._completed_files:
                return None
            buffer = self._recv_buffers.get(transfer_id)
            return buffer.form_receive_data() if buffer is not None else None

    def receive_file(self, transfer_id: int) -> Optional[Path]:
        """
        Return the path of a completed file transfer.

        Args:
            transfer_id   transfer identifier

        Returns:
            Completed file path or None
        """
        with self._recv_lock:
            return self._completed_files.get(transfer_id)

    def close_receive_files(self):
        """
        Close output files owned by this session.
        """
        with self._recv_lock:
            for transfer_id, output in self._file_outputs.items():
                self._close_receive_output(output)
                self._recv_buffers.pop(transfer_id, None)
            self._file_outputs.clear()

    def abort(self):
        """
        Stop the session and discard incomplete transfer data.
        """
        with self._send_lock:
            if self._aborted:
                return
            self._aborted = True

        self._conn.unregister_disconnect_handler(self.abort)
        self._wait_close_probe()
        self._transmitter.abort()

        with self._recv_lock:
            if self._ack_timer is not None:
                self._ack_timer.cancel()
                self._ack_timer = None
            self._pending_acks.clear()
            for output in self._file_outputs.values():
                self._close_receive_output(output, remove=True)
            self._file_outputs.clear()
            self._recv_buffers.clear()
            self._recv_trackers = {CONTROL: ACKTracker(), TRANSFER: {}}

        with self._send_lock:
            self._pending_packets.clear()
            self._send_trackers = {CONTROL: ACKTracker(), TRANSFER: {}}

    def receive_packet(self) -> Optional[Packet]:
        """
        Decode and handle one packet from the connection.

        Returns:
            Received packet, or None if the connection closes.
        """
        if not self._guard_active():
            return None
        try:
            data = self._conn.recv_data_segment()
        except OSError as exc:
            print(f"[Session] Warning: Connection receive failed: {exc}")
            self.abort()
            return None
        if not data:
            print("[Session] Warning: Connection closed; aborting incomplete transfers")
            self.abort()
            return None
        if not self._guard_active():
            return None

        packet = Packet.decode(data)
        response = None
        if packet.flags & DATA:
            with self._recv_lock:
                status = self.handle_data(packet)
                if status in (RECV_STORED, RECV_DUPLICATE):
                    response = self._schedule_ack(packet, status)
        elif packet.flags == PROBE:
            with self._recv_lock:
                response = self._make_ack(packet)
        elif packet.flags & ACK:
            self.handle_ack(packet)

        if response is not None:
            self.send_packet(response)
        return packet
