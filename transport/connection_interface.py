"""Connection contract used by transport sessions."""

from abc import ABC, abstractmethod


class ConnectionInterface(ABC):
    @property
    @abstractmethod
    def handshake_rtt(self):
        pass

    @property
    @abstractmethod
    def cwnd(self):
        pass

    @property
    @abstractmethod
    def rwnd(self):
        pass

    @abstractmethod
    def send_data_segment(self, payload):
        pass

    @abstractmethod
    def recv_data_segment(self):
        pass

    @abstractmethod
    def stop_receive(self):
        """
        Interrupt a blocking receive during Session shutdown.
        """
        pass

    @abstractmethod
    def register_disconnect_handler(self, handler):
        pass

    @abstractmethod
    def unregister_disconnect_handler(self, handler):
        pass
