"""LM Studio executor for local LLM inference."""

import httpx

from ..config import Settings
from ..tasks.models import Task, TaskResult, TaskType
from .base import BaseExecutor


class LMStudioExecutor(BaseExecutor):
    """Execute generation tasks via LM Studio's OpenAI-compatible API."""

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.settings.lmstudio_endpoint,
                timeout=120.0,
            )
        return self._client

    async def execute(self, task: Task) -> TaskResult:
        """Execute a generation task."""
        if task.task_type != TaskType.GENERATE:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="WRONG_TYPE",
                error_message="LMStudioExecutor only handles generate tasks",
            )

        params = task.get_generate_params()

        try:
            client = await self._get_client()

            # Use LM Studio's /v1/chat/completions endpoint
            response = await client.post(
                "/chat/completions",
                json={
                    "model": params.model or "local-model",
                    "messages": [
                        {"role": "user", "content": params.prompt},
                    ],
                    "max_tokens": params.max_tokens,
                    "temperature": params.temperature,
                    "stop": params.stop_sequences if params.stop_sequences else None,
                },
            )

            if response.status_code != 200:
                return TaskResult.failure(
                    job_id=task.job_id,
                    error_code="API_ERROR",
                    error_message=f"LM Studio returned {response.status_code}",
                )

            result = response.json()
            content = result["choices"][0]["message"]["content"]

            # For generate tasks, store result in output_data
            task_result = TaskResult.success(job_id=task.job_id)
            task_result.output_data = content
            return task_result

        except httpx.ConnectError:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="CONNECTION_ERROR",
                error_message="Cannot connect to LM Studio",
            )
        except Exception:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="EXECUTION_ERROR",
                error_message="Generation failed",
            )

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> bool:
        """Check if LM Studio is running and responsive."""
        try:
            client = await self._get_client()
            response = await client.get("/models")
            return response.status_code == 200
        except Exception:
            return False
