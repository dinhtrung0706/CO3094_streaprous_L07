import unittest
from unittest.mock import Mock, patch

from tests.transport_connection_fixture import Connection, FakeConnection


class TestConnection(unittest.TestCase):
    def setUp(self):
        self.conn = Connection(
            socket=object(), client_ip="192.0.2.1", client_port=12345,
            server_ip="192.0.2.2", server_port=23456,
            state="ESTABLISHED", server_seq=10, client_seq=20,
            expected_final_ack=21, last_activity=1.0,
            cwnd=4096, rwnd=2048, handshake_rtt=0.05,
        )

    def test_connection_information(self):
        info = self.conn._conn_info
        self.assertEqual(info.client_ip, "192.0.2.1")
        self.assertEqual(info.client_port, 12345)
        self.assertEqual(info.server_port, 23456)
        self.assertEqual(info.server_seq, 10)
        self.assertEqual(self.conn._handshake_rtt, 0.05)

    @patch("transport.connection.run_conn_send")
    def test_send_delegates_to_raw_connection(self, send):
        send.return_value = 4
        self.assertEqual(self.conn.send_data_segment(b"data"), 4)
        send.assert_called_once_with(self.conn._socket, self.conn._conn_info, b"data")

    @patch("transport.connection.run_conn_send", return_value=-1)
    def test_send_failure_is_forwarded(self, send):
        self.assertEqual(self.conn.send_data_segment(b"data"), -1)
        send.assert_called_once()

    @patch("transport.connection.run_conn_recv", return_value=b"message")
    def test_receive_delegates_to_raw_connection(self, recv):
        self.assertEqual(self.conn.recv_data_segment(), b"message")
        recv.assert_called_once_with(self.conn._socket, self.conn._conn_info)

    @patch("transport.connection.run_conn_recv", return_value=b"")
    def test_receive_connection_loss(self, recv):
        self.assertEqual(self.conn.recv_data_segment(), b"")
        recv.assert_called_once()

    @patch("transport.connection.unregister_connection_loss_handler")
    @patch("transport.connection.register_connection_loss_handler")
    def test_disconnect_handler_registration(self, register, unregister):
        handler = Mock()
        self.conn.register_disconnect_handler(handler)
        self.conn.unregister_disconnect_handler(handler)
        register.assert_called_once_with(self.conn._conn_info, handler)
        unregister.assert_called_once_with(self.conn._conn_info, handler)

    def test_fake_connection_is_connection(self):
        fake = FakeConnection()
        self.assertIsInstance(fake, Connection)
        self.assertEqual(fake._conn_info.client_port, 30001)


if __name__ == "__main__":
    unittest.main()
