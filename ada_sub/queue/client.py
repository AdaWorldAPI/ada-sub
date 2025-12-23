"""QStash client for publishing and managing tasks.

Note: QStash is push-based (calls webhooks). For truly pull-based operation,
we use Upstash Redis as the local queue. QStash publishes to a minimal
endpoint that enqueues to Redis. Local worker then pulls from Redis.

This gives us:
- QStash: scheduling, retries, rate limiting
- Redis: pull-based consumption, no exposed ports locally
"""

import base64
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


class RedisQueue:
    """
    Pull-based queue using Upstash Redis.

    This is what the local worker actually consumes from.
    No exposed ports, just outbound HTTPS to Redis.
    """

    def __init__(self, redis_url: str, redis_token: str):
        self.redis_url = redis_url.rstrip("/")
        self.redis_token = redis_token
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

    async def push(self, queue_name: str, message: dict[str, Any]) -> bool:
        """Push a message to the queue (LPUSH)."""
        client = await self._get_client()

        payload = base64.b64encode(json.dumps(message).encode()).decode()

        response = await client.post(
            "/lpush/" + queue_name,
            json=[payload],
        )

        return response.status_code == 200

    async def pop(self, queue_name: str, timeout: int = 0) -> dict[str, Any] | None:
        """
        Pop a message from the queue (RPOP or BRPOP).

        For pull-based consumption without blocking, we use RPOP.
        """
        client = await self._get_client()

        # Use RPOP for non-blocking
        response = await client.post(f"/rpop/{queue_name}")

        if response.status_code != 200:
            return None

        result = response.json().get("result")
        if not result:
            return None

        try:
            decoded = base64.b64decode(result).decode("utf-8")
            return json.loads(decoded)
        except (ValueError, json.JSONDecodeError):
            return {"raw": result}

    async def length(self, queue_name: str) -> int:
        """Get queue length."""
        client = await self._get_client()
        response = await client.post(f"/llen/{queue_name}")

        if response.status_code != 200:
            return 0

        return response.json().get("result", 0)

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
