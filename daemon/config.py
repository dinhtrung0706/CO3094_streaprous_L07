"""Fixed resource limits for the HTTP server.

Tests may monkey-patch these module attributes to exercise limit paths without
allocating huge buffers.
"""

MAX_HEADER_BYTES = 64 * 1024
MAX_HEADERS = 100
MAX_HEADER_LINE_BYTES = 8 * 1024
MAX_BODY_BYTES = 8 * 1024 * 1024
MAX_STATIC_BYTES = 32 * 1024 * 1024
MAX_URI_BYTES = 8 * 1024
MAX_CHUNK_LINE_BYTES = 1024
SOCKET_TIMEOUT = 30.0
KEEP_ALIVE_TIMEOUT = 30.0
MAX_KEEPALIVE_REQUESTS = 100
SERVER_NAME = "StreapRous/1.0"
DEFAULT_CHARSET = "utf-8"
