"""
Coordinate two-VM E2E test cases and run each scenario in isolation.
"""

import argparse
import socket
import subprocess
import sys
import time
import unittest

from start_backend import setup_rst_block


class TwoVMTest(unittest.TestCase):
    """
    Run matching sender and receiver test cases in separate processes.
    """

    settings = None
    control = None
    scenario_module = None

    @classmethod
    def setUpClass(cls):
        """
        Open one ordinary TCP control connection for test synchronization.
        """
        args = cls.settings
        if args.role == "receiver":
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind((args.local_ip, args.control_port))
                listener.listen(1)
                listener.settimeout(60)
                cls.control, _ = listener.accept()
            finally:
                listener.close()
        else:
            deadline = time.monotonic() + 60
            while True:
                try:
                    cls.control = socket.create_connection(
                        (args.peer_ip, args.control_port), timeout=2
                    )
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)
        cls.control.settimeout(120)

    @classmethod
    def tearDownClass(cls):
        """
        Close the control connection after all test cases.
        """
        if cls.control is not None:
            cls.control.close()
            cls.control = None

    def _send_control(self, message):
        """
        Write one control message to the peer.

        Args:
            message: One-line control message.
        """
        self.control.sendall((message + "\n").encode("ascii"))

    def _receive_control(self):
        """
        Read one complete control message.

        Returns:
            The received message.
        """
        message = bytearray()
        while True:
            char = self.control.recv(1)
            if not char:
                raise ConnectionError("E2E control connection closed")
            if char == b"\n":
                return message.decode("ascii")
            message.extend(char)

    def run_scenario(self, expected=None):
        """
        Run the current test case in a separate process on each VM.

        Args:
            expected: Output required from the successful process.
        """
        name = self._testMethodName
        args = self.settings
        if args.role == "receiver":
            self._send_control("START " + name)
        else:
            self.assertEqual(self._receive_control(), "START " + name)
            # Allow the receiver to open its listening socket.
            time.sleep(1)

        command = [
            sys.executable, "-m", self.scenario_module, args.role, name,
            "--local-ip", args.local_ip,
            "--port", str(args.port),
            "--local-port", str(args.local_port),
            "--connection", args.connection,
        ]
        if args.role == "sender":
            command.extend(["--peer-ip", args.peer_ip])
        output = ""
        code = None
        try:
            try:
                result = subprocess.run(
                    command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    universal_newlines=True, timeout=120,
                )
                output = result.stdout
                code = result.returncode
            except subprocess.TimeoutExpired as exc:
                output = str(exc)
        finally:
            if args.role == "sender":
                self._send_control("DONE " + name)
                self.assertEqual(self._receive_control(), "COMPLETE " + name)
            else:
                self.assertEqual(self._receive_control(), "DONE " + name)
                self._send_control("COMPLETE " + name)

        self.assertEqual(code, 0, "E2E scenario failed:\n" + output[-12000:])
        if expected is not None:
            self.assertIn(expected, output, "Expected output missing:\n" + output[-12000:])


def run_two_vm_tests(test_class, description):
    """
    Parse connection settings and execute selected unittest methods.

    Args:
        test_class: TestCase subclass to run.
        description: CLI description.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("role", choices=("receiver", "sender"))
    parser.add_argument("--local-ip", required=True)
    parser.add_argument("--peer-ip")
    parser.add_argument("--port", type=int, default=22239)
    parser.add_argument("--local-port", type=int, default=33333)
    parser.add_argument("--control-port", type=int, default=22240)
    parser.add_argument("--connection", choices=("raw", "tcp"), default="raw")
    parser.add_argument("tests", nargs="*", help="Optional unittest method names")
    args = parser.parse_args()
    if args.role == "sender" and not args.peer_ip:
        parser.error("sender requires --peer-ip")

    if args.connection == "raw":
        port = args.port if args.role == "receiver" else args.local_port
        setup_rst_block(args.local_ip, port)

    test_class.settings = args
    loader = unittest.defaultTestLoader
    if args.tests:
        suite = unittest.TestSuite(test_class(name) for name in args.tests)
    else:
        suite = loader.loadTestsFromTestCase(test_class)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1
