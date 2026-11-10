#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

try:
    from urlparse import unquote, urlparse  # type: ignore
except ImportError:
    from urllib.parse import unquote, urlparse


def get_auth_from_url(url):
    """Extract (username, password) from a URL, or ('', '') if absent."""
    parsed = urlparse(str(url or ""))
    if parsed.username is None:
        return ("", "")
    return (
        unquote(parsed.username),
        unquote(parsed.password or ""),
    )