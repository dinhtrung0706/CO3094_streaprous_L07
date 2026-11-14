"""
Transmitter handles the transmission and retransmission process
"""

import threading
import time

from .constant import MAX_ACK_DELAY

RTT_VAR_RCONST = 0.25
SRTT_RCONST = 0.125
RTO_VAR_MCONST = 4
TIME_GRANULARITY = 0.001
MIN_RTO = 1
MAX_RTO = 60

class Transmitter:
    def __init__(self, init_rtt, max_nr_retries = 5):
        # This is the init of srtt, rttvar, rto; refer formula for these variables' changes at self.acking_packet()
        self._srtt = init_rtt
        self._rtt_var = init_rtt / 2
        self._rto = max(MIN_RTO, min(MAX_RTO, self._srtt + max(TIME_GRANULARITY, RTO_VAR_MCONST * self._rtt_var)))
        self._time_lock = threading.Lock()

        self._max_nr_retries = max_nr_retries
        self._pktmeta = {} # {packet_key : [timestamp]}
        self._drop_list = []
        self._list_lock = threading.Lock()

        self._thread = None
        self._stop = threading.Event()
        self._thread_lock = threading.Lock()

    def register_packet(self, packet_key, timestamp):
        """
        Store the initial send time for a logical packet.

        Args:
            packet_key: Lane, transfer ID, and logical sequence.
            timestamp: Initial send time.
        """
        with self._list_lock:
            if packet_key in self._pktmeta:
                print(f"[Transmitter] Packet is already registered: {packet_key}")
                return
            self._pktmeta[packet_key] = [timestamp]

    def acking_packet(self, packet_key):
        """
        Mark a logical packet acknowledged and update RTT when eligible.

        Args:
            packet_key: Lane, transfer ID, and logical sequence.

        Returns:
            True if registered, False if the packet is unknown.
        """
        current_time = time.monotonic()
        with self._list_lock:
            attempts = self._pktmeta.pop(packet_key, None)

        if attempts is None:
            print(f"[Transmitter] Unknown acknowledged packet: {packet_key}")
            return False

        if len(attempts) > 1:
            return True

        r = current_time - attempts[0]
        with self._time_lock:
            # Apply Karn's formula
            self._rtt_var = (1 - RTT_VAR_RCONST) * self._rtt_var + RTT_VAR_RCONST * abs(self._srtt - r)
            self._srtt = (1 - SRTT_RCONST) * self._srtt + SRTT_RCONST * r
            old_rto = self._rto
            self._rto = max(MIN_RTO, min(MAX_RTO, self._srtt + max(TIME_GRANULARITY, RTO_VAR_MCONST * self._rtt_var)))
            print(f"[Transmitter] RTT={r:.3f}s srtt={self._srtt:.3f}s "
                  f"rto={old_rto:.3f}->{self._rto:.3f}s")
        return True

    def pacing_time_before_send(self, nr_packets):
        if nr_packets <= 0:
            print(f"[Transmitter] Error on number of packets = {nr_packets}, number packets should be positive.")
            raise ValueError("Number packets should be positive.")
        with self._time_lock:
            return self._srtt / nr_packets

    def _retransmit(self, session, packet_key):
        """
        Retry a pending logical packet within its retry limit.

        Args:
            session: Session that owns the packet.
            packet_key: Lane, transfer ID, and logical sequence.

        Returns:
            True if a retry was initiated, otherwise False.
        """
        with self._list_lock:
            attempts = self._pktmeta.get(packet_key)

            if attempts is None:
                return False

            if len(attempts) >= 1 + self._max_nr_retries:
                return False

            attempts.append(time.monotonic())

        print(f"[Transmitter] FAST RETX key={packet_key} retry={len(attempts) - 1}")
        return session.retransmit_packet(packet_key)

    def _on_deadline(self, session, current_time = None):
        """
        Find timed-out logical packets and request retransmission.

        Args:
            session: Session that owns the pending packets.
            current_time: Time used to check retransmission deadlines.
        """
        current_time = current_time if current_time is not None else time.monotonic()
        retransmit_packets = []
        with self._time_lock:
            rto = self._rto + MAX_ACK_DELAY

        with self._list_lock:
            for packet_key, attempts in list(self._pktmeta.items()):
                if current_time - attempts[-1] < rto:
                    continue

                if len(attempts) >= 1 + self._max_nr_retries:
                    # Out of retries and the last retransmission packet has timeout
                    # Move it to the drop list
                    self._drop_list.append(packet_key)
                    del self._pktmeta[packet_key]
                    print(f"[Transmitter] DROP key={packet_key} retries={len(attempts) - 1}")
                else:
                    attempts.append(current_time)
                    retransmit_packets.append(packet_key)
                    print(f"[Transmitter] TIMEOUT key={packet_key} retry={len(attempts) - 1}")

        if retransmit_packets:
            # Because previous retransmission failed, we increase RTO
            with self._time_lock:
                old_rto = self._rto
                self._rto = min(MAX_RTO, 2 * self._rto)
                print(f"[Transmitter] RTO {old_rto:.3f}->{self._rto:.3f}s")

            session._ccontrol.on_timeout()

            for packet_key in retransmit_packets:
                session.retransmit_packet(packet_key)

        dropped = self.flush_drop_list()
        if dropped:
            session.handle_dropped_packets(dropped)

    def _deadline_loop(self, session):
        # We approximate the waiting time instead of execute every time
        wait_time = 1.0
        while not self._stop.wait(timeout = wait_time):
            with self._time_lock:
            # We approximate the waiting time instead of execute every time
                wait_time = self._rto / 2

            self._on_deadline(session)

    def start_thread_deadline(self, session):
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop.clear()
            self._thread = threading.Thread(target=self._deadline_loop, args=(session,), daemon=True)
            self._thread.start()

    def stop_thread_deadline(self):
        with self._thread_lock:
            self._stop.set()
            if self._thread is not None:
                self._thread.join()

    def abort(self):
        """
        Stop retransmission and discard packet metadata.
        """
        self.stop_thread_deadline()
        with self._list_lock:
            self._pktmeta.clear()
            self._drop_list.clear()

    def flush_drop_list(self):
        """
        Retrieve drop packet list and remove all entries in the drop list buffer
        Thread unsafe.
        """
        with self._list_lock:
            result = self._drop_list
            self._drop_list = []
            return result

    def fast_retransmit(self, session, packet_keys):
        """
        Retry the requested logical packets after detecting loss.

        Args:
            session: Session that owns the pending packets.
            packet_keys: Composite keys of packets to retransmit.
        """
        for packet_key in packet_keys:
            self._retransmit(session, packet_key)
