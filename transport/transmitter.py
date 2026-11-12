"""
Transmitter handles the transmission and retransmission process
"""

import threading
import time

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
        self._pktmeta = {} # logical_seq : [timestamp]
        self._drop_list = []
        self._list_lock = threading.Lock()

        self._thread = None
        self._stop = threading.Event()
        self._thread_lock = threading.Lock()

    def register_packet(self, logical_seq, timestamp):
        with self._list_lock:
            if logical_seq in self._pktmeta:
                print(f"[Transmitter] packet's metadata has already been registered, packet's logical sequence: {logical_seq}")
                return
            self._pktmeta[logical_seq] = [timestamp]

    def acking_packet(self, logical_seq):
        current_time = time.monotonic()
        with self._list_lock:
            attempts = self._pktmeta.pop(logical_seq, None)

        # Why acking the ghost logical sequence number ?
        if attempts is None:
            print(f"[Transmitter] Warning you try to ack packet that has sequence number: {logical_seq}, which does not exist.")
            return False

        if len(attempts) > 1:
            return True

        r = current_time - attempts[0]
        with self._time_lock:
            # Apply Karn's formula
            self._rtt_var = (1 - RTT_VAR_RCONST) * self._rtt_var + RTT_VAR_RCONST * abs(self._srtt - r)
            self._srtt = (1 - SRTT_RCONST) * self._srtt + SRTT_RCONST * r
            self._rto = max(MIN_RTO, min(MAX_RTO, self._srtt + max(TIME_GRANULARITY, RTO_VAR_MCONST * self._rtt_var)))
        return True

    def pacing_time_before_send(self, nr_packets):
        if nr_packets <= 0:
            print(f"[Transmitter] Error on number of packets = {nr_packets}, number packets should be positive.")
            raise ValueError("Number packets should be positive.")
        with self._time_lock:
            return self._srtt / nr_packets

    def _retransmit(self, session, logical_seq):
        with self._list_lock:
            attempts = self._pktmeta.get(logical_seq)

            if attempts is None:
                return False

            if len(attempts) >= 1 + self._max_nr_retries:
                return False

            attempts.append(time.monotonic())

        session.send(logical_seq)
        return True

    def _on_deadline(self, session, current_time = None):
        """
        Main flow of the retransmitter thread.
        Only one thread calls this at a time.
        """
        current_time = current_time if current_time is not None else time.monotonic()
        retransmit_packets = []
        with self._time_lock:
            rto = self._rto

        with self._list_lock:
            for lseq, attempts in list(self._pktmeta.items()):
                if current_time - attempts[-1] < rto:
                    continue

                if len(attempts) >= 1 + self._max_nr_retries:
                    # Out of retries and the last retransmission packet has timeout
                    # Move it to the drop list
                    self._drop_list.append(lseq)
                    del self._pktmeta[lseq]
                else:
                    attempts.append(current_time)
                    retransmit_packets.append(lseq)

        if retransmit_packets:
            # Because previous retransmission failed, we increase RTO
            with self._time_lock:
                self._rto = min(MAX_RTO, 2 * self._rto)

            session._ccontrol.on_timeout()

            for lseq in retransmit_packets:
                session.send(lseq)

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
            self._thread = threading.Thread(target=self._deadline_loop, args=(session,))
            self._thread.start()

    def stop_thread_deadline(self):
        with self._thread_lock:
            self._stop.set()
            if self._thread is not None:
                self._thread.join()

    def flush_drop_list(self):
        """
        Retrieve drop packet list and remove all entries in the drop list buffer
        Thread unsafe.
        """
        with self._list_lock:
            result = self._drop_list
            self._drop_list = []
            return result

    def fast_retransmit(self, session, lseq_list):
        for logical_seq in lseq_list:
            self._retransmit(session, logical_seq)
