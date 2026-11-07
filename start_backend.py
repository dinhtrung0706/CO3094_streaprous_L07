#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the "MIT License Agreement". Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#


"""
start_backend
~~~~~~~~~~~~~~~~~

This module provides a simple entry point for deploying backend server process
using the socket framework. It parses command-line arguments to configure the
server's IP address and port, and then launches the backend server.
"""

import argparse
import platform
import socket
import subprocess
import sys

from daemon import create_backend

# Default port number used if none is specified via command-line arguments.
PORT = 9000

# Default IP and Interface
SERVER_IP = "0.0.0.0"
SERVER_PORT = PORT
SERVER_INTERFACE = "lo0"  # macOS loopback, use 'lo' for Linux


def detect_interface(ip):
    """
    Dynamically detect the network interface for a given IP address.
    
    :param ip (str): IP address to find interface for
    :return: str: Interface name (e.g., 'lo0', 'eth0', 'en0')
    """
    # Try using netifaces if available
    try:
        import netifaces
        # First, try to find exact IP match
        for interface in netifaces.interfaces():
            addrs = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addrs:
                for addr_info in addrs[netifaces.AF_INET]:
                    if addr_info['addr'] == ip:
                        return interface
        
        # If IP is 0.0.0.0 or not found, find first suitable interface
        available_interfaces = netifaces.interfaces()
        
        # For localhost, prefer loopback
        if ip == "127.0.0.1" or ip == "::1" or ip.startswith("127."):
            for iface in ["lo0", "lo", "lo1"]:
                if iface in available_interfaces:
                    return iface
            # Fallback
            return "lo0" if platform.system() == "Darwin" else "lo"
        
        # For 0.0.0.0 or specific IPs, find first non-loopback interface with IPv4
        for iface in available_interfaces:
            if iface.startswith(("lo", "Lo", "LO")):
                continue
            try:
                addrs = netifaces.ifaddresses(iface)
                if netifaces.AF_INET in addrs and len(addrs[netifaces.AF_INET]) > 0:
                    return iface
            except Exception:
                continue
        
        # If no non-loopback with IPv4, return first non-loopback
        for iface in available_interfaces:
            if not iface.startswith(("lo", "Lo", "LO")):
                return iface
                
    except ImportError:
        # netifaces not available, fall back to platform defaults
        pass
    except Exception:
        pass
    
    # Fallback to platform defaults without netifaces
    if platform.system() == "Darwin":
        if ip == "127.0.0.1" or ip == "::1" or ip.startswith("127."):
            return "lo0"
        # On macOS, try to get active interface using ifconfig
        try:
            import subprocess
            result = subprocess.run(["route", "-n", "get", "default"], capture_output=True, text=True)
            for line in result.stdout.split('\n'):
                if 'interface:' in line:
                    return line.split('interface:')[1].strip()
        except Exception:
            pass
        return "en0"  # Default macOS interface
    else:
        if ip == "127.0.0.1" or ip == "::1" or ip.startswith("127."):
            return "lo"
        # Try common Linux interface names
        return "eth0"  # Default Linux interface


def setup_rst_block(ip, port):
    """
    Block RST packets from the kernel to allow raw socket server to handle connections properly.
    On macOS use pfctl, on Linux use iptables.
    """
    if platform.system() == "Darwin":
        subprocess.run([
            "pfctl", "-t", "raw_socket_block", "-T", "add", f"port {port}"
        ], check=False)  # Ignore errors if table doesn't exist
        subprocess.run([
            "pfctl", "-a", "com.apple/250.RawSocketBlock", "-f", "-"
        ], input=f"block drop out proto tcp from any to any port {port} flags R/R\n".encode(), check=False)
    else:
        subprocess.run([
            "iptables",
            "-A", "OUTPUT",
            "-p", "tcp",
            "--sport", str(port),
            "--tcp-flags", "RST", "RST",
            "-j", "DROP"
        ], check=False)


def parse_arguments():
    """
    Process arguments for server IP, port and Interface
    Usage: server_tcp.py [PORT] [INTERFACE]
           server_tcp.py [IP] [PORT] [INTERFACE]
    """
    global SERVER_IP, SERVER_PORT, SERVER_INTERFACE
    
    if len(sys.argv) == 2:
        SERVER_PORT = int(sys.argv[1])
    elif len(sys.argv) == 3:
        # Could be IP PORT or PORT INTERFACE - detect by checking if first arg is IP
        try:
            int(sys.argv[1])
            # First arg is integer, so it's PORT INTERFACE
            SERVER_PORT = int(sys.argv[1])
            SERVER_INTERFACE = sys.argv[2]
        except ValueError:
            # First arg is not integer, so it's IP PORT
            SERVER_IP = sys.argv[1]
            SERVER_PORT = int(sys.argv[2])
    elif len(sys.argv) == 4:
        SERVER_IP = sys.argv[1]
        SERVER_PORT = int(sys.argv[2])
        SERVER_INTERFACE = sys.argv[3]


def main(ip, port, interface=None):
    """
    Main entry point for the backend server.
    """
    # Auto-detect interface if not provided
    if interface is None:
        interface = detect_interface(ip)
    
    # Check if IP is localhost/loopback
    is_localhost = (ip == "127.0.0.1" or ip == "::1" or ip.startswith("127.") or ip == "localhost")
    
    # Setup RST blocking (skip for localhost)
    if not is_localhost:
        setup_rst_block(ip, port)
    
    # Start the backend
    create_backend(ip, port, {}, interface)
    
    return 0


if __name__ == "__main__":
    """
    Entry point for launching the backend server.

    This block parses command-line arguments to determine the server's IP address
    and port. It then calls `create_backend(ip, port)` to start the RESTful
    application server.

    :arg --server-ip (str): IP address to bind the server (default: 127.0.0.1).
    :arg --server-port (int): Port number to bind the server (default: 9000).
    """

    # Parse optional arguments (argparse) first
    parser = argparse.ArgumentParser(
        prog='Backend',
        description='Start the backend process',
        epilog='Backend daemon for http_deamon application'
    )
    parser.add_argument('--server-ip',
        type=str,
        default='0.0.0.0',
        help='IP address to bind the server. Default is 0.0.0.0'
    )
    parser.add_argument(
        '--server-port',
        type=int,
        default=PORT,
        help='Port number to bind the server. Default is {}.'.format(PORT)
    )
    parser.add_argument(
        '--interface',
        type=str,
        default='lo0',
        help='Network interface to bind to. Default is lo0 (macOS) or lo (Linux).'
    )
    # Positional arguments for backward compatibility
    parser.add_argument('positional_args', nargs='*', help=argparse.SUPPRESS)
  
    args = parser.parse_args()
    
    # Parse positional arguments for backward compatibility
    if args.positional_args:
        if len(args.positional_args) == 1:
            SERVER_PORT = int(args.positional_args[0])
        elif len(args.positional_args) == 2:
            # Could be IP PORT or PORT INTERFACE
            try:
                int(args.positional_args[0])
                # First arg is integer, so it's PORT INTERFACE
                SERVER_PORT = int(args.positional_args[0])
                SERVER_INTERFACE = args.positional_args[1]
            except ValueError:
                # First arg is not integer, so it's IP PORT
                SERVER_IP = args.positional_args[0]
                SERVER_PORT = int(args.positional_args[1])
        elif len(args.positional_args) == 3:
            SERVER_IP = args.positional_args[0]
            SERVER_PORT = int(args.positional_args[1])
            SERVER_INTERFACE = args.positional_args[2]
    
    # Use argparse values if provided, otherwise use positional args
    ip = args.server_ip if args.server_ip != '0.0.0.0' else SERVER_IP
    port = args.server_port if args.server_port != PORT else SERVER_PORT
    
    # Auto-detect interface if not explicitly provided (defaults to lo0/lo)
    if args.interface == 'lo0' or args.interface == 'lo':
        SERVER_INTERFACE = detect_interface(ip)
    else:
        SERVER_INTERFACE = args.interface
    
    sys.exit(main(ip, port, SERVER_INTERFACE))
