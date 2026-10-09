"""Run the mock services inside the API process (a background thread) for one-command local dev.

It is still a real HTTP server on a real port: the API reaches it over the network exactly
like it reaches Salesforce. In Docker, a separate `mock-services` container is used instead.
"""
import socket
import threading
import time

import uvicorn

from mock_services.app import create_app


def port_in_use(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except OSError:
        return False


def start_in_thread(port: int, host: str = "127.0.0.1") -> uvicorn.Server:
    server = uvicorn.Server(uvicorn.Config(create_app(), host=host, port=port, log_level="warning"))
    threading.Thread(target=server.run, daemon=True, name="mock-services").start()
    for _ in range(100):
        if server.started:
            return server
        time.sleep(0.05)
    raise RuntimeError(f"mock services did not start on {host}:{port}")
