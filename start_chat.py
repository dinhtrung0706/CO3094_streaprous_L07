"""Run the chat daemon (HTTP UI + selectable peer transport)."""

import argparse
import os
import sys

from apps.chatapp import create_chatapp


def parse_endpoint(value):
    """Parse ``host:port`` into a ``(host, port)`` tuple."""
    host, separator, port = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("expected host:port, e.g. 127.0.0.1:9100")
    try:
        port = int(port)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return host, port


def build_parser():
    parser = argparse.ArgumentParser(
        prog="start_chat", description="StreapRous chat daemon"
    )
    parser.add_argument("--mode", choices=("peer", "tracker", "local", "server", "client"),
                        default="peer")
    parser.add_argument("--server-ip", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=None,
                        help="central HTTP server port (default: 8000)")
    parser.add_argument("--public-http-port", type=int, default=None)
    parser.add_argument("--sar-mode", choices=("udp", "raw"), default="raw",
                        help="transport for both tracker discovery and peer messages")
    parser.add_argument("--client-port", type=int, default=None,
                        help="P2P listening port (RAW by default; UDP with --sar-mode udp)")
    parser.add_argument(
        "--tracker",
        type=parse_endpoint,
        default=None,
        help="tracker address as host:port using the selected peer transport",
    )
    parser.add_argument("--public-ip", default=None)
    parser.add_argument("--username", default=None, help="legacy peer node account")
    parser.add_argument("--central-url", default=None,
                        help="central HTTP URL for local P2P client")
    parser.add_argument("--db", default=os.path.join("data", "chat.sqlite3"))
    parser.add_argument(
        "--transport",
        choices=("standard", "async", "auto", "raw"),
        default="standard",
        help="HTTP transport; 'standard' works without root",
    )
    parser.add_argument(
        "--heartbeat", type=float, default=5.0, help="tracker heartbeat seconds"
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.mode == "peer" and not args.username:
        print("error: --username is required in peer mode", file=sys.stderr)
        return 2
    if args.mode == "peer" and args.tracker is None:
        print("error: --tracker is required in peer mode", file=sys.stderr)
        return 2
    if args.mode == "client" and not args.central_url:
        print("error: --central-url is required in client mode", file=sys.stderr)
        return 2
    if args.mode == "client" and args.tracker is None:
        print("error: --tracker is required in client mode", file=sys.stderr)
        return 2
    if args.mode == "client" and args.server_port is not None:
        print("error: --server-port applies to the central server only",
              file=sys.stderr)
        return 2
    client_port = args.client_port or (
        (9101 if args.mode == "client" else 9100) if args.sar_mode == "udp"
        else (9201 if args.mode == "client" else 9200)
    )
    if args.mode != "local" and args.sar_mode == "raw":
        from start_backend import setup_rst_block
        setup_rst_block(args.public_ip or args.server_ip, client_port)
    if (args.mode != "client" and args.transport in ("auto", "raw")
            and args.server_ip not in ("localhost", "::1")
            and not args.server_ip.startswith("127.")):
        from start_backend import setup_rst_block
        setup_rst_block(args.server_ip, args.server_port or 8000)
    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    if args.mode == "client":
        from daemon.client import run_client
        run_client(args.central_url, args.tracker, args.db, args.public_ip,
                   args.sar_mode, client_port, 8765, args.heartbeat)
        return 0
    create_chatapp(
        args.server_ip,
        args.server_port or 8000,
        mode=args.mode,
        tracker=args.tracker,
        db_path=args.db,
        udp_port=client_port if args.sar_mode == "udp" else 0,
        peer_transport=args.sar_mode,
        raw_port=client_port if args.sar_mode == "raw" else 9200,
        public_ip=args.public_ip,
        username=args.username,
        transport=args.transport,
        heartbeat=args.heartbeat,
        http_port=args.public_http_port or args.server_port or 8000,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
