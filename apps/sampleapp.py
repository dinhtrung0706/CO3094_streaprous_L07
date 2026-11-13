import sys

from daemon import StreapRous
from daemon.auth import install_auth

app = StreapRous()

@app.route('/login', methods=['POST'])
def login(headers="guest", body="anonymous"):
    """
    Handle user login via POST request.

    This route simulates a login process and prints the provided headers and body
    to the console.

    :param headers (str): The request headers or user identifier.
    :param body (str): The request body or login payload.
    """
    print(f"[SampleApp] Logging in {headers} to {body}")
    data = {"message": "Welcome to the RESTful TCP WebApp"}

    # Convert to JSON string
    json_str = json.dumps(data)
    return (json_str.encode("utf-8"))

@app.route("/echo", methods=["POST"])
def echo(headers="guest", body="anonymous"):
    print(f"[SampleApp] received body {body}")
    return (json_str.encode("utf-8"))

def create_sampleapp(ip, port, transport=None):
    install_auth(app)
    # Prepare and launch the RESTful application
    app.prepare_address(ip, port)
    app.run(transport=transport)