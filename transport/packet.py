"""Encode and decode framed CONTROL, MESSAGE, and FILE packets."""

import struct
from dataclasses import dataclass

VERSION = 1

CONTROL = 0
MESSAGE = 1
FILE = 2

DATA = 0x01
ACK = 0x02
PROBE = 0x04
EOM = 0x08

HEADER_FORMAT = "!BBBBHHIHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

VALID_LANES = (CONTROL, MESSAGE, FILE)
VALID_FLAGS = (DATA, DATA | EOM, ACK, PROBE, ACK | PROBE)
MAX_ACK_RANGES = 16


def _validate_fields(version, lane, flags, logical_seq, ack_seq, packet_seq, rwnd, payload_len):
    """Check that every header field has a valid value.

    Args:
        version: Protocol version.
        lane: CONTROL, MESSAGE, or FILE.
        flags: Packet operation flags.
        logical_seq: Logical sequence number for the lane.
        ack_seq: Cumulative logical ACK number.
        packet_seq: Physical packet identifier.
        rwnd: Advertised receive window.
        payload_len: Payload size in bytes.

    Returns:
        None if all fields are valid.
    """
    if version != VERSION:
        raise ValueError("Unsupported packet version")
    if lane not in VALID_LANES:
        raise ValueError("Invalid packet lane")
    if flags not in VALID_FLAGS:
        raise ValueError("Invalid packet flags")
    for value, maximum in (
        (logical_seq, 0xFFFF),
        (ack_seq, 0xFFFF),
        (packet_seq, 0xFFFFFFFF),
        (rwnd, 0xFFFF),
        (payload_len, 0xFFFF),
    ):
        if not isinstance(value, int) or not 0 <= value <= maximum:
            raise ValueError("Packet field out of range")
    if flags in (PROBE, ACK | PROBE) and payload_len:
        raise ValueError("Window probes must have empty payloads")


@dataclass
class Packet:
    """Represent one framed transport packet."""

    version: int
    lane: int
    flags: int
    logical_seq: int
    ack_seq: int
    packet_seq: int
    rwnd: int
    payload: bytes = b""

    def encode(self) -> bytes:
        """Serialize this packet into a header and payload.

        Args:
            self: Packet to serialize.

        Returns:
            Encoded packet bytes in network byte order.
        """
        if not isinstance(self.payload, (bytes, bytearray)):
            raise TypeError("Payload must be bytes")
        _validate_fields(
            self.version, self.lane, self.flags, self.logical_seq,
            self.ack_seq, self.packet_seq, self.rwnd, len(self.payload)
        )
        header = struct.pack(
            HEADER_FORMAT,
            self.version, self.lane, self.flags, 0,
            self.logical_seq, self.ack_seq, self.packet_seq,
            self.rwnd, len(self.payload)
        )
        return header + self.payload

    @classmethod
    def decode(cls, data: bytes):
        """Decode exactly one complete packet.

        Args:
            cls: Packet class to construct.
            data: Bytes containing one complete packet.

        Returns:
            Packet decoded from data.
        """
        packet, remaining = parse_buffer(data)
        if packet is None:
            raise ValueError("Incomplete packet")
        if remaining:
            raise ValueError("Unexpected bytes after packet")
        return packet


def parse_buffer(data: bytes):
    """Extract one complete packet from a byte buffer.

    Args:
        data: Received bytes, possibly containing multiple frames.

    Returns:
        (packet, remaining bytes), or (None, data) if incomplete.
    """
    if len(data) < HEADER_SIZE:
        return None, data

    version, lane, flags, reserved, logical_seq, ack_seq, packet_seq, rwnd, payload_len = (
        struct.unpack_from(HEADER_FORMAT, data)
    )
    if reserved:
        raise ValueError("Reserved header byte must be zero")
    _validate_fields(version, lane, flags, logical_seq, ack_seq, packet_seq, rwnd, payload_len)

    total_size = HEADER_SIZE + payload_len
    if len(data) < total_size:
        return None, data

    packet = Packet(
        version, lane, flags, logical_seq, ack_seq,
        packet_seq, rwnd, bytes(data[HEADER_SIZE:total_size])
    )
    return packet, data[total_size:]


def encode_ack_ranges(ranges: list[tuple[int, int]]) -> bytes:
    """
    Encode ordered, half-open ACK ranges into bytes.
    Args:
        ranges: A list of (start, end) sequence ranges.

    Returns:
        One-byte range count followed by 32-bit start/end pairs.
    """
    if len(ranges) > MAX_ACK_RANGES:
        raise ValueError("Too many ACK ranges")

    data = bytearray([len(ranges)])
    previous_end = -1
    for start, end in ranges:
        if (not isinstance(start, int) or not isinstance(end, int)
                or not 0 <= start < end <= 0xFFFFFFFF
                or start < previous_end):
            raise ValueError("Invalid ACK range")
        data.extend(struct.pack("!II", start, end))
        previous_end = end

    return bytes(data)


def decode_ack_ranges(data: bytes) -> list[tuple[int, int]]:
    """
    Decode and validate ACK ranges from a packet payload.
    Args:
        data: Encoded range count and 32-bit start/end pairs.

    Returns:
        A list of ordered, half-open ACK ranges.
    """
    if not data:
        raise ValueError("Missing ACK range count")

    count = data[0]
    if count > MAX_ACK_RANGES or len(data) != 1 + count * 8:
        raise ValueError("Invalid ACK range payload length")

    ranges = []
    previous_end = -1
    for index in range(count):
        start, end = struct.unpack_from("!II", data, 1 + index * 8)
        if start >= end or start < previous_end:
            raise ValueError("Invalid ACK range")
        ranges.append((start, end))
        previous_end = end

    return ranges
