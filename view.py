#!/usr/bin/env python3
"""Serve the crawler directory and open viewer.html in the default browser."""
from __future__ import annotations

import http.server
import socketserver
import threading
import webbrowser
from pathlib import Path

PORT = 8765
ROOT = Path(__file__).resolve().parent


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass


def main() -> None:
    import os
    os.chdir(ROOT)
    with socketserver.TCPServer(("127.0.0.1", PORT), QuietHandler) as httpd:
        url = f"http://127.0.0.1:{PORT}/viewer.html"
        print(f"Serving {ROOT} at {url}")
        print("Press Ctrl+C to stop.")
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")


if __name__ == "__main__":
    main()
