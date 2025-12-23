"""QStash message handling and signature verification."""

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QStashMessage:
    """A message from QStash queue."""

    message_id: str
    body: dict[str, Any]
    timestamp: int
    retries: int = 0
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_response(cls, data: dict[str, Any]) -> "QStashMessage":
        """Parse message from QStash API response."""
        body = data.get("body", "{}")
        if isinstance(body, str):
            try:
                # QStash returns base64 encoded body
                decoded = base64.b64decode(body).decode("utf-8")
                body = json.loads(decoded)
            except (ValueError, json.JSONDecodeError):
                body = {"raw": body}

        return cls(
            message_id=data.get("messageId", ""),
            body=body,
            timestamp=data.get("createdAt", int(time.time() * 1000)),
            retries=data.get("retried", 0),
            headers=data.get("headers", {}),
        )

    def get_job_id(self) -> str:
        """Extract job ID from message."""
        return self.body.get("job_id", self.message_id)


def verify_signature(
    body: bytes,
    signature: str,
    current_key: str,
    next_key: str | None = None,
) -> bool:
    """
    Verify QStash message signature.

    QStash uses HMAC-SHA256 for signing. We check both current
    and next signing keys for seamless key rotation.
    """
    if not signature or not current_key:
        return False

    def check_sig(key: str) -> bool:
        expected = hmac.new(
            key.encode("utf-8"),
            body,
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.b64encode(expected).decode("utf-8")
        return hmac.compare_digest(signature, expected_b64)

    if check_sig(current_key):
        return True

    if next_key and check_sig(next_key):
        return True

    return False
