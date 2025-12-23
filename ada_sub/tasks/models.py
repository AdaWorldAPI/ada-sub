"""Task models - intentionally generic naming."""

import time
import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TaskType(str, Enum):
    """Task types - using neutral terminology."""

    # 'transform' = audio synthesis (Bark)
    TRANSFORM = "transform"

    # 'generate' = text generation (LM Studio)
    GENERATE = "generate"

    # 'process' = generic processing
    PROCESS = "process"

    # 'convert' = format conversion
    CONVERT = "convert"


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TransformParams(BaseModel):
    """Parameters for 'transform' tasks (audio synthesis)."""

    # Input text - called 'content' to stay neutral
    content: str = Field(..., min_length=1, max_length=10000)

    # Preset identifier (maps to voice/speaker internally)
    preset: str = Field(default="default")

    # Sampling parameters
    temperature: float = Field(default=0.7, ge=0.0, le=1.5)
    top_k: int = Field(default=50, ge=1, le=500)
    top_p: float = Field(default=0.95, ge=0.0, le=1.0)

    # Determinism - fixed seed for reproducible output
    seed: int | None = Field(default=None)

    # Output format
    format: str = Field(default="wav")
    sample_rate: int = Field(default=24000)


class GenerateParams(BaseModel):
    """Parameters for 'generate' tasks (text generation)."""

    prompt: str = Field(..., min_length=1)
    max_tokens: int = Field(default=512, ge=1, le=8192)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    stop_sequences: list[str] = Field(default_factory=list)

    # Model selection for LM Studio
    model: str | None = Field(default=None)


class Task(BaseModel):
    """A task to be executed locally."""

    # Identity
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: TaskType = Field(default=TaskType.TRANSFORM)

    # Timestamps
    created_at: int = Field(default_factory=lambda: int(time.time() * 1000))
    expires_at: int | None = Field(default=None)

    # Priority (lower = higher priority)
    priority: int = Field(default=5, ge=0, le=10)

    # Parameters - type depends on task_type
    params: dict[str, Any] = Field(default_factory=dict)

    # Metadata for tracking
    metadata: dict[str, str] = Field(default_factory=dict)

    def get_transform_params(self) -> TransformParams:
        """Parse params as TransformParams."""
        return TransformParams(**self.params)

    def get_generate_params(self) -> GenerateParams:
        """Parse params as GenerateParams."""
        return GenerateParams(**self.params)

    def is_expired(self) -> bool:
        """Check if task has expired."""
        if self.expires_at is None:
            return False
        return int(time.time() * 1000) > self.expires_at


class TaskResult(BaseModel):
    """Result of task execution."""

    job_id: str
    status: TaskStatus
    created_at: int = Field(default_factory=lambda: int(time.time() * 1000))

    # Output location or data
    output_path: str | None = Field(default=None)
    output_data: str | None = Field(default=None)

    # Execution metrics (kept minimal to avoid logging sensitive info)
    duration_ms: int = Field(default=0)

    # Error info (generic messages only)
    error_code: str | None = Field(default=None)
    error_message: str | None = Field(default=None)

    @classmethod
    def success(
        cls,
        job_id: str,
        output_path: str | None = None,
        duration_ms: int = 0,
    ) -> "TaskResult":
        """Create a success result."""
        return cls(
            job_id=job_id,
            status=TaskStatus.COMPLETED,
            output_path=output_path,
            duration_ms=duration_ms,
        )

    @classmethod
    def failure(
        cls,
        job_id: str,
        error_code: str,
        error_message: str,
        duration_ms: int = 0,
    ) -> "TaskResult":
        """Create a failure result."""
        return cls(
            job_id=job_id,
            status=TaskStatus.FAILED,
            error_code=error_code,
            error_message=error_message,
            duration_ms=duration_ms,
        )
