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
## License

See [LICENSE](LICENSE) — educational use only for CO3093/CO3094.