"""
Send one short message between two virtual machines.
"""

import argparse

from streaprous_socket import streaprous_raw_connection as raw
from transport.connection import Connection
from transport.constant import MAX_RECEIVE_BUFFER, MAX_SEGMENT_SIZE, TRANSFER_ID_SIZE
from transport.packet import ACK, TRANSFER, decode_ack_ranges
from transport.session import Session
from transport.stream_buffer import SendBuffer


TRANSFER_ID = 1
MESSAGE = b"Hello from the other VM!"


def run_sender(session: Session) -> bool:
    """
    Send one message and wait for its acknowledgment.

    Args:
        session  active session

    Returns:
        True if acknowledged
    """
    if not session.send_next_chunk(SendBuffer(MESSAGE), TRANSFER_ID):
        print("[LiveTransport] Warning: Cannot send message")
        return False

    while True:
        packet = session.receive_packet()
        if packet is None:
            return False
        if packet.lane != TRANSFER or packet.flags != ACK:
            continue
        if packet.payload[:TRANSFER_ID_SIZE] != TRANSFER_ID.to_bytes(TRANSFER_ID_SIZE, "big"):
            continue
        ranges = decode_ack_ranges(packet.payload[TRANSFER_ID_SIZE:])
        if any(start <= 0 < end for start, end in ranges):
            print("[LiveTransport] Message acknowledged")
            return True


def run_receiver(session: Session) -> bool:
    """
    Receive and print one complete message.

    Args:
        session  active session

    Returns:
        True if a complete message arrived
    """
    while session.receive_packet() is not None:
        message = session.receive_transfer(TRANSFER_ID)
        if message is not None:
            print(f"[LiveTransport] Received: {message.decode('utf-8', errors='replace')}")
            return True
    return False


def main() -> int:
    """
    Run one side of the basic two-VM demo.

    Returns:
        Process exit code
    """
    parser = argparse.ArgumentParser(description="Simple two-VM transport demo")
    parser.add_argument("mode", choices=("sender", "receiver"))
    parser.add_argument("--local-ip", required=True)
    parser.add_argument("--peer-ip")
    parser.add_argument("--port", type=int, default=22239)
    parser.add_argument("--local-port", type=int, default=33333)
    args = parser.parse_args()

    if args.mode == "sender" and not args.peer_ip:
        parser.error("sender requires --peer-ip")

    sock = raw.create_raw_socket()
    info = None
    session = None
    try:
        if args.mode == "receiver":
            info = raw.accept(sock, args.local_ip, args.port)
        else:
            info = raw.connect(sock, args.local_ip, args.local_port,
                               args.peer_ip, args.port)
        if info is None:
            return 1

        conn = Connection(socket=sock, **info._asdict(),
                          cwnd=16 * MAX_SEGMENT_SIZE,
                          rwnd=MAX_RECEIVE_BUFFER, handshake_rtt=0.1)
        session = Session(conn, max_retries=5)
        success = run_sender(session) if args.mode == "sender" else run_receiver(session)
        return 0 if success else 1
    finally:
        if session is not None:
            session.abort()
        if info is not None:
            raw.close_connection(sock, info)
        raw.stop_packet_dispatcher()
        sock.close()


if __name__ == "__main__":
    raise SystemExit(main())
