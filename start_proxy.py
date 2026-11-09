# reverse_proxy.py
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
start_proxy
~~~~~~~~~~~~~~~~~

Entry point for the StreapRous reverse proxy. It parses virtual-host blocks
from the proxy configuration and starts the proxy daemon.

Supported directives inside a host block:
- proxy_pass http://host:port;
- proxy_set_header HeaderName value;
- dist_policy round-robin | least-conn | random;
"""

import argparse
import os
import re
import sys

from daemon import create_proxy
from daemon.proxy import parse_backend_address

PROXY_PORT = 8080
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "config", "proxy.conf")

_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_\x60|~0-9A-Za-z]+$")
_CONTROL_RE = re.compile(r"[\x00-\x1F\x7F]")
_POLICIES = {"round-robin", "least-conn", "random"}


def _normalize_policy(value):
    policy = str(value or "round-robin").strip().lower().replace("_", "-")
    if policy in ("roundrobin", "round-robin"):
        return "round-robin"
    if policy in ("leastconn", "least-conn", "least-connections"):
        return "least-conn"
    if policy == "random":
        return "random"
    raise ValueError("unsupported dist_policy: {!r}".format(value))


def _target_list(value):
    return list(value) if isinstance(value, (list, tuple)) else [value]


def parse_virtual_hosts(config_file):
    """Parse host blocks from config_file into proxy route values.

    Returns a dict mapping hostname to (target_or_targets, policy, headers).
    Raises ValueError with a useful message for invalid configuration.
    """
    with open(config_file, "r") as handle:
        config_text = handle.read()

    config_text = re.sub(r"#.*$", "", config_text, flags=re.MULTILINE)
    host_blocks = re.findall(
        r'host\s+"([^"]+)"\s*\{(.*?)\}', config_text, re.DOTALL
    )
    if not host_blocks:
        raise ValueError("no host blocks found in {}".format(config_file))

    routes = {}
    for host, block in host_blocks:
        host = host.strip()
        if not host:
            raise ValueError("empty host declaration")

        proxy_passes = re.findall(r"proxy_pass\s+([^\s;]+);", block)
        if not proxy_passes:
            raise ValueError("host {!r} has no proxy_pass".format(host))
        targets = []
        for target in proxy_passes:
            try:
                targets.append(parse_backend_address(target))
            except ValueError as exc:
                raise ValueError(
                    "invalid proxy_pass for host {!r}: {}".format(host, exc)
                )
        targets = list(dict.fromkeys(targets))

        headers = {}
        for name, value in re.findall(
            r"proxy_set_header\s+([^\s;]+)\s+([^;]+);", block
        ):
            name = name.strip()
            value = value.strip()
            if not _HEADER_NAME_RE.fullmatch(name):
                raise ValueError(
                    "invalid proxy_set_header name for host {!r}: {!r}".format(
                        host, name
                    )
                )
            if _CONTROL_RE.search(value):
                raise ValueError(
                    "invalid proxy_set_header value for host {!r}".format(host)
                )
            headers[name] = value

        policy_match = re.search(
            r"dist_policy\s+([A-Za-z0-9_-]+)", block
        )
        policy = _normalize_policy(
            policy_match.group(1) if policy_match else "round-robin"
        )

        existing = routes.get(host)
        if existing is None:
            routes[host] = (
                targets[0] if len(targets) == 1 else targets,
                policy,
                headers,
            )
        else:
            old_targets, old_policy, old_headers = existing
            combined = _target_list(old_targets) + targets
            combined = list(dict.fromkeys(combined))
            old_headers.update(headers)
            routes[host] = (
                combined[0] if len(combined) == 1 else combined,
                policy or old_policy,
                old_headers,
            )

    return routes


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="Proxy",
        description="StreapRous reverse proxy",
        epilog="Proxy daemon",
    )
    parser.add_argument("--server-ip", default="0.0.0.0")
    parser.add_argument("--server-port", type=int, default=PROXY_PORT)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args(argv)

    try:
        routes = parse_virtual_hosts(args.config)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    create_proxy(args.server_ip, args.server_port, routes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
