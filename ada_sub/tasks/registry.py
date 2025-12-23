"""Task registry for mapping task types to executors."""

from collections.abc import Callable, Coroutine
from typing import Any

from .models import Task, TaskResult, TaskType


# Executor type: async function that takes Task and returns TaskResult
ExecutorFunc = Callable[[Task], Coroutine[Any, Any, TaskResult]]


class TaskRegistry:
    """Registry mapping task types to executor functions."""

    def __init__(self):
        self._executors: dict[TaskType, ExecutorFunc] = {}

    def register(self, task_type: TaskType) -> Callable[[ExecutorFunc], ExecutorFunc]:
        """Decorator to register an executor for a task type."""

        def decorator(func: ExecutorFunc) -> ExecutorFunc:
            self._executors[task_type] = func
            return func

        return decorator

    def get_executor(self, task_type: TaskType) -> ExecutorFunc | None:
        """Get the executor for a task type."""
        return self._executors.get(task_type)

    async def execute(self, task: Task) -> TaskResult:
        """Execute a task using the registered executor."""
        executor = self.get_executor(task.task_type)

        if executor is None:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="NO_EXECUTOR",
                error_message=f"No executor for task type: {task.task_type}",
            )

        try:
            return await executor(task)
        except Exception as e:
            # Generic error - don't leak details
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="EXECUTION_ERROR",
                error_message="Task execution failed",
            )


# Global registry instance
registry = TaskRegistry()
