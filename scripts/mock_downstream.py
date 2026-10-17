#!/usr/bin/env python3
"""A tiny downstream API for demos: records calls, dedupes on Idempotency-Key,
and fails a configurable fraction of requests with 503 to exercise retries.

    FAIL_RATE=0.3 python scripts/mock_downstream.py --port 9000
    curl localhost:9000/_calls      # what the engine sent
"""

from __future__ import annotations

import argparse
import json
import os
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))
_lock = threading.Lock()
_calls: list[dict] = []
_seen_keys: dict[str, dict] = {}


class Handler(BaseHTTPRequestHandler):
    def _reply(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/_calls":
            with _lock:
                self._reply(200, {"calls": _calls, "unique_keys": len(_seen_keys)})
        else:
            self._reply(200, {"ok": True})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"null")
        key = self.headers.get("Idempotency-Key")
        if random.random() < FAIL_RATE:
            self._reply(503, {"error": "simulated outage"})
            return
        with _lock:
            if key and key in _seen_keys:  # idempotent replay: same response, no new effect
                self._reply(200, {**_seen_keys[key], "replayed": True})
                return
            result = {"id": f"remote-{len(_seen_keys) + 1}", "path": self.path}
            if key:
                _seen_keys[key] = result
            _calls.append({"path": self.path, "idempotency_key": key, "body": body})
        self._reply(201, result)

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[downstream] {self.command} {self.path} key={self.headers.get('Idempotency-Key')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "9000")))
    args = parser.parse_args()
    print(f"mock downstream on :{args.port} (FAIL_RATE={FAIL_RATE})")
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()
