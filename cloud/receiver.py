"""Minimal cloud receiver - QStash webhook to Redis queue.

Deploy this to Railway/Render/Fly. It receives QStash webhooks
and pushes tasks to Upstash Redis for local consumption.

This is intentionally minimal - no heavy dependencies.
"""

import base64
import hashlib
import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import Request, urlopen


# Environment variables
QSTASH_CURRENT_SIGNING_KEY = os.environ.get("QSTASH_CURRENT_SIGNING_KEY", "")
QSTASH_NEXT_SIGNING_KEY = os.environ.get("QSTASH_NEXT_SIGNING_KEY", "")
UPSTASH_REDIS_URL = os.environ.get("UPSTASH_REDIS_URL", "")
UPSTASH_REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_TOKEN", "")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "dev-tasks")


def verify_qstash_signature(body: bytes, signature: str) -> bool:
    """Verify QStash webhook signature."""
    if not signature:
        return False

    def check_key(key: str) -> bool:
        if not key:
            return False
        expected = hmac.new(key.encode(), body, hashlib.sha256).digest()
        expected_b64 = base64.b64encode(expected).decode()
        return hmac.compare_digest(signature, expected_b64)

    return check_key(QSTASH_CURRENT_SIGNING_KEY) or check_key(QSTASH_NEXT_SIGNING_KEY)


def push_to_redis(message: dict) -> bool:
    """Push message to Upstash Redis queue."""
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return False

    payload = base64.b64encode(json.dumps(message).encode()).decode()

    req = Request(
        f"{UPSTASH_REDIS_URL}/lpush/{QUEUE_NAME}",
        data=json.dumps([payload]).encode(),
        headers={
            "Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        return False


class QStashHandler(BaseHTTPRequestHandler):
    """Handle QStash webhook requests."""

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass

    def do_POST(self):
        """Handle incoming webhook."""
        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        # Verify signature
        signature = self.headers.get("Upstash-Signature", "")
        if not verify_qstash_signature(body, signature):
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"Invalid signature")
            return

        # Parse message
        try:
            message = json.loads(body.decode())
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Invalid JSON")
            return

        # Push to Redis
        if push_to_redis(message):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Queue error")

    def do_GET(self):
        """Health check endpoint."""
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()


def main():
    """Run the receiver server."""
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), QStashHandler)
    print(f"Receiver listening on port {port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
