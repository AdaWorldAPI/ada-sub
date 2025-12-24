"""Subprocess executor for running local tools like Bark.

Uses a persistent subprocess that keeps models loaded across jobs.
Restarts after max_jobs_per_worker for memory hygiene.
"""

import asyncio
import json
import sys
from pathlib import Path

from ..config import Settings
from ..tasks.models import Task, TaskResult, TaskType
from .base import BaseExecutor


# Persistent Bark runner - keeps models warm
BARK_RUNNER_SCRIPT = '''
"""Persistent transform runner - loads models once, processes many."""
import sys
import json
import numpy as np

# Preload on startup (once)
try:
    from bark import SAMPLE_RATE, generate_audio, preload_models
    import scipy.io.wavfile as wavfile
    preload_models()
    READY = True
except ImportError:
    READY = False
    SAMPLE_RATE = 24000

PRESET_MAP = {
    "default": "v2/en_speaker_6",
    "male_1": "v2/en_speaker_0",
    "male_2": "v2/en_speaker_1",
    "female_1": "v2/en_speaker_6",
    "female_2": "v2/en_speaker_9",
}

def process(params):
    """Process a single transform request."""
    if not READY:
        return {"error": "dependencies not available"}

    content = params.get("content", "")
    artifact_path = params.get("artifact_path", "output.wav")
    preset = params.get("preset", "default")
    seed = params.get("seed")

    speaker = PRESET_MAP.get(preset, preset)

    if seed is not None:
        np.random.seed(seed)

    audio_array = generate_audio(content, history_prompt=speaker)

    wavfile.write(
        artifact_path,
        SAMPLE_RATE,
        (audio_array * 32767).astype(np.int16),
    )

    return {
        "status": "completed",
        "artifact_path": artifact_path,
        "encoding": "pcm16",
    }

# Main loop - read jobs from stdin, write results to stdout
sys.stdout.write("READY\\n")
sys.stdout.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line == "SHUTDOWN":
        break

    try:
        params = json.loads(line)
        result = process(params)
    except Exception as e:
        result = {"error": "processing_failed"}

    sys.stdout.write(json.dumps(result) + "\\n")
    sys.stdout.flush()
'''


class SubprocessExecutor(BaseExecutor):
    """Execute tasks via persistent subprocess - models stay warm."""

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._runner_path: Path | None = None
        self._proc: asyncio.subprocess.Process | None = None
        self._jobs_processed: int = 0
        self._max_jobs_per_worker: int = 50  # Restart for memory hygiene
        self._lock = asyncio.Lock()

    def _ensure_runner_script(self) -> Path:
        """Write runner script to temp location if needed."""
        if self._runner_path is None:
            runner_dir = self.settings.output_dir / ".runners"
            runner_dir.mkdir(parents=True, exist_ok=True)
            self._runner_path = runner_dir / "transform_runner.py"
            self._runner_path.write_text(BARK_RUNNER_SCRIPT)
        return self._runner_path

    async def _ensure_worker(self) -> asyncio.subprocess.Process:
        """Ensure a warm worker subprocess is running."""
        async with self._lock:
            # Restart if needed
            if self._proc is not None and self._jobs_processed >= self._max_jobs_per_worker:
                await self._shutdown_worker()

            if self._proc is None or self._proc.returncode is not None:
                runner_path = self._ensure_runner_script()
                self._proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(runner_path),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                self._jobs_processed = 0

                # Wait for READY signal
                ready_line = await asyncio.wait_for(
                    self._proc.stdout.readline(),
                    timeout=120,  # Model loading can take time
                )
                if ready_line.decode().strip() != "READY":
                    raise RuntimeError("Worker failed to start")

            return self._proc

    async def _shutdown_worker(self):
        """Gracefully shutdown the worker subprocess."""
        if self._proc is not None and self._proc.returncode is None:
            try:
                self._proc.stdin.write(b"SHUTDOWN\n")
                await self._proc.stdin.drain()
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except Exception:
                self._proc.kill()
            self._proc = None
            self._jobs_processed = 0

    async def execute(self, task: Task) -> TaskResult:
        """Execute task via persistent subprocess."""
        if task.task_type == TaskType.TRANSFORM:
            return await self._execute_transform(task)
        elif task.task_type == TaskType.GENERATE:
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
        """Execute transform via warm subprocess."""
        params = task.get_transform_params()
        output_path = self.get_output_path(task, params.format)

        subprocess_params = {
            "content": params.content,
            "artifact_path": str(output_path),
            "preset": params.preset,
            "seed": params.seed,
        }

        try:
            proc = await self._ensure_worker()

            # Send job
            job_line = json.dumps(subprocess_params) + "\n"
            proc.stdin.write(job_line.encode())
            await proc.stdin.drain()

            # Read result
            result_line = await asyncio.wait_for(
                proc.stdout.readline(),
                timeout=300,
            )

            self._jobs_processed += 1

            result = json.loads(result_line.decode())

            if "error" in result:
                return TaskResult.failure(
                    job_id=task.job_id,
                    error_code="RUNNER_ERROR",
                    error_message=result["error"],
                )

            return TaskResult.success(
                job_id=task.job_id,
                artifact_path=result.get("artifact_path", str(output_path)),
            )

        except asyncio.TimeoutError:
            await self._shutdown_worker()
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="TIMEOUT",
                error_message="Task timed out",
            )
        except Exception:
            await self._shutdown_worker()
            return TaskResult.failure(
                job_id=task.job_id,
                error_code="EXECUTION_ERROR",
                error_message="Task execution failed",
            )

    async def close(self):
        """Shutdown the worker on executor close."""
        await self._shutdown_worker()
