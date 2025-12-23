"""Base executor interface."""

import time
from abc import ABC, abstractmethod
from pathlib import Path

from ..config import Settings
from ..tasks.models import Task, TaskResult


class BaseExecutor(ABC):
    """Base class for task executors."""

    def __init__(self, settings: Settings):
        self.settings = settings

    @abstractmethod
    async def execute(self, task: Task) -> TaskResult:
        """Execute a task and return the result."""
        pass

    def get_output_path(self, task: Task, ext: str = "wav") -> Path:
        """Get output path for task result."""
        return self.settings.get_output_path(task.job_id, ext)

    async def run_with_timing(self, task: Task) -> TaskResult:
        """Execute with timing measurement."""
        start = time.perf_counter()
        result = await self.execute(task)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        result.duration_ms = elapsed_ms
        return result
