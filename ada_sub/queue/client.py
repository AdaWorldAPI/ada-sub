"""QStash client for publishing and managing tasks.

Note: QStash is push-based (calls webhooks). For truly pull-based operation,
we use Upstash Redis as the local queue. QStash publishes to a minimal
endpoint that enqueues to Redis. Local worker then pulls from Redis.

This gives us:
- QStash: scheduling, retries, rate limiting
- Redis: pull-based consumption, no exposed ports locally
"""

import base64
import hashlib
import hmac
import json
import time
from typing import Any

import httpx

from ..config import Settings


class QStashClient:
    """Client for QStash API - publishing side (runs in cloud)."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_url = settings.qstash_url
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.settings.qstash_token.get_secret_value()}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    async def publish(
        self,
        destination: str,
        body: dict[str, Any],
        delay_seconds: int = 0,
        retries: int = 3,
        dedup_id: str | None = None,
    ) -> str:
        """
        Publish a message to QStash.

        Args:
            destination: URL or topic to deliver to
            body: Message payload
            delay_seconds: Delay before delivery
            retries: Number of retry attempts
            dedup_id: Deduplication ID

        Returns:
            Message ID from QStash
        """
        client = await self._get_client()

        headers = {
            "Upstash-Retries": str(retries),
        }

        if delay_seconds > 0:
            headers["Upstash-Delay"] = f"{delay_seconds}s"

        if dedup_id:
            headers["Upstash-Deduplication-Id"] = dedup_id

        # Add timestamp for ordering
        body["_ts"] = int(time.time() * 1000)

        response = await client.post(
            f"/v2/publish/{destination}",
            json=body,
            headers=headers,
        )
        response.raise_for_status()

        result = response.json()
        return result.get("messageId", "")

    async def publish_batch(
        self,
        destination: str,
        messages: list[dict[str, Any]],
    ) -> list[str]:
        """Publish multiple messages in a batch."""
        client = await self._get_client()

        batch = [
            {
                "destination": destination,
                "body": base64.b64encode(json.dumps(msg).encode()).decode(),
            }
            for msg in messages
        ]

        response = await client.post("/v2/batch", json=batch)
        response.raise_for_status()

        results = response.json()
        return [r.get("messageId", "") for r in results]

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


def compute_task_hmac(task_body: dict, signing_key: str) -> str:
    """Compute HMAC for task verification."""
    # Serialize deterministically (sorted keys, no spaces)
    canonical = json.dumps(task_body, sort_keys=True, separators=(",", ":"))
    sig = hmac.new(signing_key.encode(), canonical.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()


def verify_task_hmac(task_body: dict, signature: str, signing_key: str) -> bool:
    """Verify task HMAC signature."""
    expected = compute_task_hmac(task_body, signing_key)
    return hmac.compare_digest(signature, expected)


class RedisQueue:
    """
    Pull-based queue using Upstash Redis with priority support.

    Uses separate queues by priority: tasks:p0 (highest) through tasks:p9 (lowest).
    Worker polls high priority first.
    """

    # Priority levels (0 = highest)
    PRIORITY_LEVELS = 10

    def __init__(self, redis_url: str, redis_token: str, signing_key: str = ""):
        self.redis_url = redis_url.rstrip("/")
        self.redis_token = redis_token
        self.signing_key = signing_key
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.redis_url,
                headers={
                    "Authorization": f"Bearer {self.redis_token}",
                },
                timeout=30.0,
            )
        return self._client

    def _priority_queue_name(self, base_name: str, priority: int) -> str:
        """Get queue name for a given priority level."""
        priority = max(0, min(priority, self.PRIORITY_LEVELS - 1))
        return f"{base_name}:p{priority}"

    async def push(
        self,
        queue_name: str,
        message: dict[str, Any],
        priority: int = 5,
        sign: bool = True,
    ) -> bool:
        """Push a message to the priority queue (LPUSH)."""
        client = await self._get_client()

        # Add HMAC signature if signing key is set
        if sign and self.signing_key:
            message = message.copy()
            message["_sig"] = compute_task_hmac(message, self.signing_key)

        payload = base64.b64encode(json.dumps(message).encode()).decode()
        pq_name = self._priority_queue_name(queue_name, priority)

        response = await client.post(
            f"/lpush/{pq_name}",
            json=[payload],
        )

        return response.status_code == 200

    async def pop(
        self,
        queue_name: str,
        verify: bool = True,
    ) -> dict[str, Any] | None:
        """
        Pop a message from the highest priority non-empty queue.

        Polls p0 first, then p1, etc. Returns None if all queues empty.
        """
        client = await self._get_client()

        # Poll queues in priority order
        for priority in range(self.PRIORITY_LEVELS):
            pq_name = self._priority_queue_name(queue_name, priority)

            response = await client.post(f"/rpop/{pq_name}")

            if response.status_code != 200:
                continue

            result = response.json().get("result")
            if not result:
                continue

            try:
                decoded = base64.b64decode(result).decode("utf-8")
                message = json.loads(decoded)

                # Verify HMAC if required
                if verify and self.signing_key:
                    sig = message.pop("_sig", "")
                    if not verify_task_hmac(message, sig, self.signing_key):
                        # Invalid signature - skip this message
                        continue

                return message

            except (ValueError, json.JSONDecodeError):
                continue

        return None

    async def length(self, queue_name: str) -> int:
        """Get total queue length across all priorities."""
        client = await self._get_client()
        total = 0

        for priority in range(self.PRIORITY_LEVELS):
            pq_name = self._priority_queue_name(queue_name, priority)
            response = await client.post(f"/llen/{pq_name}")

            if response.status_code == 200:
                total += response.json().get("result", 0)

        return total

    async def lengths_by_priority(self, queue_name: str) -> dict[int, int]:
        """Get queue lengths by priority level."""
        client = await self._get_client()
        lengths = {}

        for priority in range(self.PRIORITY_LEVELS):
            pq_name = self._priority_queue_name(queue_name, priority)
            response = await client.post(f"/llen/{pq_name}")

            if response.status_code == 200:
                count = response.json().get("result", 0)
                if count > 0:
                    lengths[priority] = count

        return lengths

    async def set(self, key: str, value: str, ex: int = 3600) -> bool:
        """Set a key with expiration (for storing results)."""
        client = await self._get_client()
        response = await client.post(
            "/set/" + key,
            json=[value, "EX", ex],
        )
        return response.status_code == 200

    async def get(self, key: str) -> str | None:
        """Get a key value."""
        client = await self._get_client()
        response = await client.post(f"/get/{key}")
        if response.status_code != 200:
            return None
        return response.json().get("result")

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
