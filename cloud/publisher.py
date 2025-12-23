"""Task publisher - sends tasks to QStash for delivery.

This can run anywhere (local, cloud, CI/CD).
Tasks are sent to QStash which delivers them to the receiver,
which queues them for the local worker.
"""

import json
import os
import time
import uuid
from urllib.request import Request, urlopen


QSTASH_TOKEN = os.environ.get("QSTASH_TOKEN", "")
QSTASH_URL = os.environ.get("QSTASH_URL", "https://qstash.upstash.io")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "")  # Your Railway/Render URL


def publish_task(
    content: str,
    task_type: str = "transform",
    preset: str = "default",
    priority: int = 5,
    delay_seconds: int = 0,
    **params,
) -> str:
    """
    Publish a task to QStash.

    Args:
        content: Text content to process
        task_type: Type of task (transform, generate)
        preset: Preset identifier
        priority: Task priority (0-10, lower = higher)
        delay_seconds: Delay before processing
        **params: Additional parameters

    Returns:
        Message ID from QStash
    """
    if not QSTASH_TOKEN or not RECEIVER_URL:
        raise ValueError("QSTASH_TOKEN and RECEIVER_URL required")

    job_id = str(uuid.uuid4())

    task = {
        "job_id": job_id,
        "task_type": task_type,
        "created_at": int(time.time() * 1000),
        "priority": priority,
        "params": {
            "content": content,
            "preset": preset,
            **params,
        },
    }

    headers = {
        "Authorization": f"Bearer {QSTASH_TOKEN}",
        "Content-Type": "application/json",
        "Upstash-Retries": "3",
    }

    if delay_seconds > 0:
        headers["Upstash-Delay"] = f"{delay_seconds}s"

    # Dedup by job_id to prevent double-processing
    headers["Upstash-Deduplication-Id"] = job_id

    req = Request(
        f"{QSTASH_URL}/v2/publish/{RECEIVER_URL}",
        data=json.dumps(task).encode(),
        headers=headers,
        method="POST",
    )

    with urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode())
        return result.get("messageId", job_id)


def publish_batch(tasks: list[dict]) -> list[str]:
    """Publish multiple tasks in a batch."""
    if not QSTASH_TOKEN or not RECEIVER_URL:
        raise ValueError("QSTASH_TOKEN and RECEIVER_URL required")

    batch = []
    job_ids = []

    for task_params in tasks:
        job_id = str(uuid.uuid4())
        job_ids.append(job_id)

        task = {
            "job_id": job_id,
            "task_type": task_params.get("task_type", "transform"),
            "created_at": int(time.time() * 1000),
            "priority": task_params.get("priority", 5),
            "params": {
                "content": task_params.get("content", ""),
                "preset": task_params.get("preset", "default"),
            },
        }

        batch.append({
            "destination": RECEIVER_URL,
            "body": json.dumps(task),
            "headers": {
                "Upstash-Deduplication-Id": job_id,
            },
        })

    req = Request(
        f"{QSTASH_URL}/v2/batch",
        data=json.dumps(batch).encode(),
        headers={
            "Authorization": f"Bearer {QSTASH_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urlopen(req, timeout=30) as resp:
        results = json.loads(resp.read().decode())
        return [r.get("messageId", job_ids[i]) for i, r in enumerate(results)]


# Example usage
if __name__ == "__main__":
    # Publish a single task
    msg_id = publish_task(
        content="Hello, this is a test.",
        task_type="transform",
        preset="default",
    )
    print(f"Published task: {msg_id}")
