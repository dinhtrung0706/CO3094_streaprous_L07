import socket
import threading
import unittest

from daemon.proxy import forward_request, resolve_routing_policy


class ResolveRoutingPolicyTests(unittest.TestCase):
    def test_single_string_route(self):
        routes = {"a.local": ("10.0.0.1:9001", "round-robin")}
        self.assertEqual(
            resolve_routing_policy("a.local", routes), ("10.0.0.1", "9001"))

    def test_single_item_list_route(self):
        routes = {"b.local": (["10.0.0.2:9002"], "round-robin")}
        self.assertEqual(
            resolve_routing_policy("b.local", routes), ("10.0.0.2", "9002"))


class ForwardRequestTests(unittest.TestCase):
    def test_forces_connection_close_upstream(self):
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        received = []

        def backend():
            conn, _ = server.accept()
            try:
                received.append(conn.recv(4096).decode())
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
                )
            finally:
                conn.close()

        thread = threading.Thread(target=backend, daemon=True)
        thread.start()
        request = ("GET / HTTP/1.1\r\nHost: x\r\n"
                   "Connection: keep-alive\r\n\r\n")
        response = forward_request(
            "127.0.0.1", server.getsockname()[1], request)
        thread.join(2)
        server.close()

        self.assertTrue(response.endswith(b"ok"))
        self.assertIn("Connection: close\r\n\r\n", received[0])
        self.assertNotIn("keep-alive", received[0])


if __name__ == "__main__":
    unittest.main()
