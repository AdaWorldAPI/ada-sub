"""Worker process - polls queue and executes tasks locally.

Designed to look like a standard async task worker.
No suspicious naming, no obvious AI/voice references in logs.
"""

import asyncio
import json
import signal
import sys
from typing import NoReturn

import structlog

from .config import Settings, get_settings
from .executors.lmstudio import LMStudioExecutor
from .executors.subprocess_executor import SubprocessExecutor
from .queue.client import RedisQueue
from .tasks.models import Task, TaskResult, TaskStatus, TaskType

# Configure logging to be boring
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
        if get_settings().log_format == "json"
        else structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger("worker")


class Worker:
    """Pull-based task worker."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.queue: RedisQueue | None = None
        self.subprocess_executor = SubprocessExecutor(settings)
        self.lmstudio_executor = LMStudioExecutor(settings)
        self._running = False
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_jobs)

    async def setup(self, redis_url: str, redis_token: str):
        """Initialize connections."""
        self.queue = RedisQueue(redis_url, redis_token)
        logger.info("worker_initialized", queue="redis")

    async def shutdown(self):
        """Clean shutdown."""
        self._running = False
        if self.queue:
            await self.queue.close()
        await self.lmstudio_executor.close()
        logger.info("worker_shutdown")

    async def process_task(self, task: Task) -> TaskResult:
        """Process a single task."""
        async with self._semaphore:
            # Throttle to avoid GPU spikes
            await asyncio.sleep(self.settings.throttle_delay_seconds)

            logger.info(
                "task_started",
                job_id=task.job_id,
                type=task.task_type.value,
            )

            # Route to appropriate executor
            if task.task_type == TaskType.TRANSFORM:
                result = await self.subprocess_executor.run_with_timing(task)
            elif task.task_type == TaskType.GENERATE:
                result = await self.lmstudio_executor.run_with_timing(task)
            else:
                result = TaskResult.failure(
                    job_id=task.job_id,
                    error_code="UNKNOWN_TYPE",
                    error_message="Unknown task type",
                )

            # Log completion (minimal info)
            if result.status == TaskStatus.COMPLETED:
                logger.info(
                    "task_completed",
                    job_id=task.job_id,
                    duration_ms=result.duration_ms,
                )
            else:
                logger.warning(
                    "task_failed",
                    job_id=task.job_id,
                    error=result.error_code,
                )

            return result

    async def run_loop(self):
        """Main polling loop."""
        self._running = True
        queue_name = self.settings.topic_tasks

        logger.info(
            "polling_started",
            queue=queue_name,
            interval=self.settings.poll_interval_seconds,
        )

        while self._running:
            try:
                # Check queue for messages
                if self.queue is None:
                    await asyncio.sleep(1)
                    continue

                message = await self.queue.pop(queue_name)

                if message is None:
                    # No messages, wait before next poll
                    await asyncio.sleep(self.settings.poll_interval_seconds)
                    continue

                # Parse and process task
                try:
                    task = Task(**message)

                    # Check expiry
                    if task.is_expired():
                        logger.info("task_expired", job_id=task.job_id)
                        continue

                    # Process task
                    result = await self.process_task(task)

                    # Store result for facade retrieval (TTS jobs)
                    if task.task_type == TaskType.TRANSFORM and self.queue:
                        result_data = json.dumps({
                            "status": "completed" if result.status == TaskStatus.COMPLETED else "failed",
                            "output_path": result.output_path,
                            "duration_ms": result.duration_ms,
                            "error": result.error_message
                        })
                        await self.queue.set(
                            f"ada:tts:result:{task.job_id}",
                            result_data,
                            ex=7200  # 2 hours
                        )

                    # Optionally post result back to results queue
                    if self.queue:
                        await self.queue.push(
                            self.settings.topic_results,
                            result.model_dump(),
                        )

                except Exception as e:
                    logger.error("task_parse_error", error=str(type(e).__name__))

            except Exception as e:
                logger.error("poll_error", error=str(type(e).__name__))
                await asyncio.sleep(5)  # Back off on errors


def main():
    """Entry point for ada-worker command."""
    import os

    settings = get_settings()

    # Get Redis credentials from env
    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

    if not redis_url or not redis_token:
        print("Error: UPSTASH_REDIS_URL and UPSTASH_REDIS_TOKEN required")
        sys.exit(1)

    worker = Worker(settings)

    async def run():
        await worker.setup(redis_url, redis_token)

        # Handle shutdown signals
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.shutdown()))

        try:
            await worker.run_loop()
        finally:
            await worker.shutdown()

    asyncio.run(run())


if __name__ == "__main__":
    main()
