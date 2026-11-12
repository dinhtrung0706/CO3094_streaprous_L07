"""Connection provide raw TCP send and receive operations."""

from streaprous_socket.streaprous_raw_connection import (
    conn_struct,
    run_conn_recv,
    run_conn_send,
)


class Connection:
    """Store connection details and exchange raw TCP payloads with a peer."""

    def __init__(
            self,
            socket,
            client_ip,
            client_port,
            server_ip,
            server_port,
            state,
            server_seq,
            client_seq,
            expected_final_ack,
            last_activity,
            cwnd,
            rwnd,
            handshake_rtt
    ):
        """Initialize a connection using its established TCP state.

        Args:
            socket: Raw TCP socket.
            client_ip: Client IP address.
            client_port: Client TCP port.
            server_ip: Server IP address.
            server_port: Server TCP port.
            state: Connection state.
            server_seq: Server TCP sequence number.
            client_seq: Client TCP sequence number.
            expected_final_ack: Expected handshake acknowledgment.
            last_activity: Time of the last activity.
            cwnd: Initial congestion window.
            rwnd: Initial receive window.
            handshake_rtt: Round-trip time measured during handshake.

        """
        self._socket = socket
        self._client_ip = client_ip
        self._client_port = client_port
        self._server_ip = server_ip
        self._server_port = server_port
        self._state = state
        self._server_seq = server_seq
        self._client_seq = client_seq
        self._expected_final_ack = expected_final_ack
        self._last_activity = last_activity
        self._cwnd = cwnd
        self._rwnd = rwnd
        self._handshake_rtt = handshake_rtt
        self._conn_info = conn_struct(
            client_ip, client_port, server_ip, server_port, state,
            server_seq, client_seq, expected_final_ack, last_activity
        )

    def send_data_segment(self, payload: bytes) -> int:
        """
        Send encoded packet bytes over the raw TCP connection.

        Args:
            payload: Bytes to send.

        Returns:
            Number of bytes sent, or -1 if sending fails.
        """
        return run_conn_send(self._socket, self._conn_info, payload)

    def recv_data_segment(self) -> bytes:
        """
        Receive the next TCP payload from the connected peer.

        Args:
            self: Connection receiving data.

        Returns:
            Received bytes, or empty bytes when the connection closes.
        """
        return run_conn_recv(self._socket, self._conn_info)
