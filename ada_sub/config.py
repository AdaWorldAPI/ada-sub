"""Configuration for task subscriber."""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Subscriber configuration - looks like standard async worker settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ADA_",
        extra="ignore",
    )

    # QStash config (Upstash)
    qstash_token: SecretStr = Field(default=SecretStr(""))
    qstash_current_signing_key: SecretStr = Field(default=SecretStr(""))
    qstash_next_signing_key: SecretStr = Field(default=SecretStr(""))
    qstash_url: str = Field(default="https://qstash.upstash.io")

    # Queue topics - named to look boring
    topic_tasks: str = Field(default="dev-tasks")
    topic_results: str = Field(default="dev-results")

    # Polling config
    poll_interval_seconds: int = Field(default=30, ge=5, le=300)
    batch_size: int = Field(default=5, ge=1, le=20)

    # Local execution
    output_dir: Path = Field(default=Path("./output"))
    executor_type: Literal["local", "lmstudio", "subprocess"] = Field(default="subprocess")
    lmstudio_endpoint: str = Field(default="http://127.0.0.1:1234/v1")

    # Resource limits (to avoid GPU spikes that draw attention)
    max_concurrent_jobs: int = Field(default=2)
    throttle_delay_seconds: float = Field(default=1.0)

    # Logging - intentionally boring
    log_level: str = Field(default="INFO")
    log_format: Literal["json", "console"] = Field(default="console")

    def get_output_path(self, job_id: str, ext: str = "wav") -> Path:
        """Get output path for a job result."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self.output_dir / f"{job_id}.{ext}"


def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
