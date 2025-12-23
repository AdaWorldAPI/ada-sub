"""Subprocess executor for running local tools like Bark."""

import asyncio
import json
import sys
from pathlib import Path

from ..config import Settings
from ..tasks.models import Task, TaskResult, TaskType
from .base import BaseExecutor


# Bark runner script - written to temp location on first use
BARK_RUNNER_SCRIPT = '''
"""Minimal Bark runner - no unnecessary logging."""
import sys
import json
import numpy as np

def run_bark(params):
    """Run Bark synthesis."""
    try:
        from bark import SAMPLE_RATE, generate_audio, preload_models
        import scipy.io.wavfile as wavfile
    except ImportError:
        return {"error": "bark not installed"}

    # Preload models (cached after first run)
    preload_models()

    content = params.get("content", "")
    output_path = params.get("output_path", "output.wav")
    preset = params.get("preset", "v2/en_speaker_6")

    # Map simple presets to Bark speaker IDs
    preset_map = {
        "default": "v2/en_speaker_6",
        "male_1": "v2/en_speaker_0",
        "male_2": "v2/en_speaker_1",
        "female_1": "v2/en_speaker_6",
        "female_2": "v2/en_speaker_9",
    }
    speaker = preset_map.get(preset, preset)

    # Set seed if provided
    seed = params.get("seed")
    if seed is not None:
        np.random.seed(seed)

    # Generate audio
    audio_array = generate_audio(
        content,
        history_prompt=speaker,
    )

    # Save to file
    wavfile.write(
        output_path,
        SAMPLE_RATE,
        (audio_array * 32767).astype(np.int16),
    )

    return {
        "status": "completed",
        "output_path": output_path,
        "sample_rate": SAMPLE_RATE,
    }

if __name__ == "__main__":
    params = json.loads(sys.argv[1])
    result = run_bark(params)
    print(json.dumps(result))
'''


class SubprocessExecutor(BaseExecutor):
    """Execute tasks via subprocess - keeps main process clean."""

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._runner_path: Path | None = None

    def _ensure_runner_script(self) -> Path:
        """Write runner script to temp location if needed."""
        if self._runner_path is None:
            runner_dir = self.settings.output_dir / ".runners"
            runner_dir.mkdir(parents=True, exist_ok=True)
            self._runner_path = runner_dir / "bark_runner.py"
            self._runner_path.write_text(BARK_RUNNER_SCRIPT)
        return self._runner_path

    async def execute(self, task: Task) -> TaskResult:
        """Execute task via subprocess."""
        if task.task_type == TaskType.TRANSFORM:
            return await self._execute_transform(task)
        elif task.task_type == TaskType.GENERATE:
            # Generation tasks go to LMStudioExecutor
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="WRONG_EXECUTOR",
                error_message="Use LMStudioExecutor for generate tasks",
            )
        else:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="UNSUPPORTED_TYPE",
                error_message=f"Unsupported task type: {task.task_type}",
            )

    async def _execute_transform(self, task: Task) -> TaskResult:
        """Execute audio transform (Bark synthesis)."""
        params = task.get_transform_params()
        output_path = self.get_output_path(task, params.format)

        runner_path = self._ensure_runner_script()

        # Build params for subprocess
        subprocess_params = {
            "content": params.content,
            "output_path": str(output_path),
            "preset": params.preset,
            "seed": params.seed,
        }

        try:
            # Run in subprocess to isolate GPU operations
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                str(runner_path),
                json.dumps(subprocess_params),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=300,  # 5 minute timeout
            )

            if proc.returncode != 0:
                return TaskResult.failure(
                    job_id=task.job_id,
                    error_code="SUBPROCESS_ERROR",
                    error_message="Processing failed",
                )

            # Parse result
            try:
                result = json.loads(stdout.decode())
                if "error" in result:
                    return TaskResult.failure(
                        job_id=task.job_id,
                        error_code="RUNNER_ERROR",
                        error_message=result["error"],
                    )

                return TaskResult.success(
                    job_id=task.job_id,
                    output_path=result.get("output_path", str(output_path)),
                )
            except json.JSONDecodeError:
                return TaskResult.failure(
                    job_id=task.job_id,
                    error_code="PARSE_ERROR",
                    error_message="Failed to parse result",
                )

        except asyncio.TimeoutError:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="TIMEOUT",
                error_message="Task timed out",
            )
        except Exception:
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="EXECUTION_ERROR",
                error_message="Task execution failed",
            )
