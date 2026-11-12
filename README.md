# StreapRous - Stream-based Webapp Routing for Course CO3093/CO3094

Webapp Routing Framework with streaming-based socket HTTP server for HCMC University of Technology Course CO3093/CO3094.

## Run http server backend - must be sudo Linux due to the socket raw capture
```bash
python start_backend 192.168.56.x 9000
```

## Run http server proxy - must be sudo Linux due to the socket raw 

```bash
python start_proxy 192.168.56.x 9000 
```

## Run http webapp StreapRous
```python
#    Entry point for launching the server. This block parses command-line
#    arguments to determine the server's IP address and port.
#    :arg --server-ip (str): IP address to bind the server (default: 127.0.0.1).
#    :arg --server-port (int): Port number to bind the server (default: 9000).
    parser = argparse.ArgumentParser(prog='Proxy', description='', epilog='Proxy daemon')
    parser.add_argument('--server-ip', default='0.0.0.0')
    parser.add_argument('--server-port', type=int, default=PROXY_PORT)

    args = parser.parse_args()
    ip = args.server_ip
    port = args.server_port

# From start_proxy.py
from daemon import create_proxy

routes = parse_virtual_hosts("config/proxy.conf")
create_proxy(ip, port, routes)

# From start_backend.py
from daemon import create_backend

create_backend(ip, port)

# From start_sampleapp.py
from daemon.streaprous import StreapRous

# Prepare and launch the RESTful application
app.prepare_address(ip, port)
app.run()
```

## Phase 3: account authentication

The sample app provides `POST /register`, `POST /login`, `POST /logout`, and
`GET /api/me`. Registration and login accept either JSON
(`{"username":"alice","password":"..."}`) or
`application/x-www-form-urlencoded` form data. Registration creates an account;
login creates a server-side session and returns a CSRF token in the JSON body.
Send the `SR_SESSION` cookie on later requests and send the returned token as
`X-CSRF-Token` when calling `POST /logout`.

Passwords are stored as PBKDF2-HMAC-SHA256 hashes with per-user random salts.
Session tokens are random, signed, and stored in SQLite only as SHA-256 hashes.
The default database is `data/streaprous.sqlite3`; set `STREAPROUS_DB_PATH` to
choose another path. Set `STREAPROUS_SESSION_SECRET` to a secret of at least
32 UTF-8 bytes to keep sessions valid across server restarts. If it is unset,
the server generates an in-memory key and invalidates sessions on restart.
Set `STREAPROUS_COOKIE_SECURE=1` when serving over HTTPS.

The framework's `auth=True` route option now accepts either a valid session
cookie or HTTP Basic credentials. Basic credentials are not encrypted by this
server; use them only over a trusted network or behind HTTPS. This phase does
not add password reset, roles, account lockout, or a full registration/login
web interface.

## License

See [LICENSE](LICENSE) — educational use only for CO3093/CO3094.