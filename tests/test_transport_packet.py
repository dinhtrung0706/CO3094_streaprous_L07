import struct
import unittest

from transport.constant import MAX_ACK_RANGES, TRANSFER_ID_SIZE
from transport.packet import (
    ACK, CONTROL, DATA, EOM, HEADER_SIZE, PROBE, TRANSFER,
    VERSION, Packet, decode_ack_ranges, encode_ack_ranges, parse_buffer,
)


class TestTransportPacket(unittest.TestCase):
    def test_transfer_round_trip(self):
        payload = struct.pack("!I", 42) + b"hello"
        original = Packet(VERSION, TRANSFER, DATA | EOM, 65535, 0, 10, 4096, payload)
        decoded = Packet.decode(original.encode())
        self.assertEqual(decoded, original)
        self.assertEqual(len(original.encode()), HEADER_SIZE + len(payload))

    def test_control_probe_round_trip(self):
        packet = Packet(VERSION, CONTROL, PROBE, 0, 0, 1, 0)
        self.assertEqual(Packet.decode(packet.encode()), packet)

    def test_partial_and_concatenated_packets(self):
        first = Packet(VERSION, CONTROL, PROBE, 0, 0, 1, 100).encode()
        second = Packet(VERSION, CONTROL, ACK | PROBE, 0, 0, 2, 200).encode()
        self.assertEqual(parse_buffer(first[:-1]), (None, first[:-1]))
        packet, remaining = parse_buffer(first + second)
        self.assertEqual(packet.packet_seq, 1)
        self.assertEqual(Packet.decode(remaining).packet_seq, 2)
        with self.assertRaises(ValueError):
            Packet.decode(first + second)

    def test_invalid_transfer_payload(self):
        with self.assertRaises(ValueError):
            Packet(VERSION, TRANSFER, DATA, 0, 0, 0, 0, b"x").encode()
        with self.assertRaises(ValueError):
            Packet(VERSION, TRANSFER, ACK, 0, 0, 0, 0,
                   b"x" * TRANSFER_ID_SIZE).encode()

    def test_invalid_version_and_flags(self):
        with self.assertRaises(ValueError):
            Packet(VERSION + 1, CONTROL, PROBE, 0, 0, 0, 0).encode()
        with self.assertRaises(ValueError):
            Packet(VERSION, CONTROL, DATA | ACK, 0, 0, 0, 0).encode()

    def test_ack_ranges_round_trip(self):
        ranges = [(0, 3), (8, 12), (65535, 65536)]
        self.assertEqual(decode_ack_ranges(encode_ack_ranges(ranges)), ranges)

    def test_ack_range_limit_and_malformed_data(self):
        with self.assertRaises(ValueError):
            encode_ack_ranges([(i * 2, i * 2 + 1)
                               for i in range(MAX_ACK_RANGES + 1)])
        with self.assertRaises(ValueError):
            decode_ack_ranges(b"\x01")
        with self.assertRaises(ValueError):
            encode_ack_ranges([(5, 7), (2, 3)])


if __name__ == "__main__":
    unittest.main()
