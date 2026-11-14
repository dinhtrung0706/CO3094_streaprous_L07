#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the LICENSE. Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""StreapRous raw socket transport and packet dispatcher package."""

from . import streaprous_raw_connection as _raw_conn
from .streaprous_packet_dispatcher import (
    DEFAULT_QUEUE_MAXSIZE,
    get_packet_queue,
    remove_packet_queue,
    start_packet_dispatcher,
    stop_packet_dispatcher,
)
from .streaprous_raw_connection import (
    accept,
    build_ip_header,
    build_tcp_packet,
    checksum,
    close_all_connections,
    close_connection,
    conn_struct,
    create_raw_socket,
    get_interface_ip,
    run_conn_recv,
    run_conn_send,
)

ConnectionInfo = getattr(_raw_conn, "ConnectionInfo", conn_struct)
parse_tcp_packet = getattr(_raw_conn, "parse_tcp_packet", None)
get_connection_stats = getattr(_raw_conn, "get_connection_stats", None)

__all__ = [
    "DEFAULT_QUEUE_MAXSIZE",
    "ConnectionInfo",
    "accept",
    "build_ip_header",
    "build_tcp_packet",
    "checksum",
    "close_all_connections",
    "close_connection",
    "conn_struct",
    "create_raw_socket",
    "get_connection_stats",
    "get_interface_ip",
    "get_packet_queue",
    "parse_tcp_packet",
    "remove_packet_queue",
    "run_conn_recv",
    "run_conn_send",
    "start_packet_dispatcher",
    "stop_packet_dispatcher",
]

