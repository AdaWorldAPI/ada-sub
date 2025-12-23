"""MCP Server for VS Code integration.

This runs as an MCP server that VS Code can connect to.
It provides tools for:
- Fetching pending tasks from the queue
- Executing tasks locally
- Reporting results

To SOC, this looks like a standard dev automation tool.
"""

import asyncio
import json
import os
import sys
from typing import Any

# MCP protocol types
JSONRPC_VERSION = "2.0"


class MCPServer:
    """Minimal MCP server implementation for task subscription."""

    def __init__(self):
        self.queue = None
        self.worker = None
        self._capabilities = {
            "tools": {
                "listChanged": False,
            },
        }

    def _make_response(self, id: Any, result: Any = None, error: Any = None) -> dict:
        """Create JSON-RPC response."""
        response = {"jsonrpc": JSONRPC_VERSION, "id": id}
        if error:
            response["error"] = error
        else:
            response["result"] = result
        return response

    def _handle_initialize(self, params: dict) -> dict:
        """Handle initialize request."""
        return {
            "protocolVersion": "2024-11-05",
            "capabilities": self._capabilities,
            "serverInfo": {
                "name": "ada-sub",
                "version": "0.1.0",
            },
        }

    def _handle_tools_list(self) -> dict:
        """List available tools."""
        return {
            "tools": [
                {
                    "name": "fetch_tasks",
                    "description": "Fetch pending tasks from the queue",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of tasks to fetch",
                                "default": 5,
                            },
                        },
                    },
                },
                {
                    "name": "execute_task",
                    "description": "Execute a task locally",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "task": {
                                "type": "object",
                                "description": "Task object to execute",
                            },
                        },
                        "required": ["task"],
                    },
                },
                {
                    "name": "worker_status",
                    "description": "Check worker status",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                    },
                },
            ],
        }

    async def _handle_tool_call(self, name: str, arguments: dict) -> dict:
        """Handle tool invocation."""
        if name == "fetch_tasks":
            return await self._fetch_tasks(arguments.get("limit", 5))
        elif name == "execute_task":
            return await self._execute_task(arguments.get("task", {}))
        elif name == "worker_status":
            return await self._worker_status()
        else:
            return {"error": f"Unknown tool: {name}"}

    async def _fetch_tasks(self, limit: int) -> dict:
        """Fetch pending tasks from queue."""
        if self.queue is None:
            return {"error": "Queue not initialized", "tasks": []}

        tasks = []
        for _ in range(limit):
            msg = await self.queue.pop("dev-tasks")
            if msg is None:
                break
            tasks.append(msg)

        return {"tasks": tasks, "count": len(tasks)}

    async def _execute_task(self, task_data: dict) -> dict:
        """Execute a task locally."""
        if self.worker is None:
            return {"error": "Worker not initialized"}

        from .tasks.models import Task

        try:
            task = Task(**task_data)
            result = await self.worker.process_task(task)
            return result.model_dump()
        except Exception as e:
            return {"error": str(type(e).__name__)}

    async def _worker_status(self) -> dict:
        """Get worker status."""
        from .executors.lmstudio import LMStudioExecutor
        from .config import get_settings

        settings = get_settings()
        lm_executor = LMStudioExecutor(settings)

        lm_available = await lm_executor.health_check()
        await lm_executor.close()

        return {
            "status": "running",
            "lm_studio_available": lm_available,
            "output_dir": str(settings.output_dir),
        }

    async def handle_message(self, message: dict) -> dict | None:
        """Handle incoming JSON-RPC message."""
        method = message.get("method", "")
        params = message.get("params", {})
        msg_id = message.get("id")

        # Notifications (no id) don't need response
        if msg_id is None:
            if method == "notifications/initialized":
                return None
            return None

        try:
            if method == "initialize":
                result = self._handle_initialize(params)
            elif method == "tools/list":
                result = self._handle_tools_list()
            elif method == "tools/call":
                result = await self._handle_tool_call(
                    params.get("name", ""),
                    params.get("arguments", {}),
                )
                result = {"content": [{"type": "text", "text": json.dumps(result)}]}
            else:
                return self._make_response(
                    msg_id,
                    error={"code": -32601, "message": f"Unknown method: {method}"},
                )

            return self._make_response(msg_id, result=result)

        except Exception as e:
            return self._make_response(
                msg_id,
                error={"code": -32603, "message": str(e)},
            )

    async def run_stdio(self):
        """Run MCP server over stdio."""
        from .config import get_settings
        from .queue.client import RedisQueue
        from .worker import Worker

        settings = get_settings()

        # Initialize queue if credentials available
        redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
        redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

        if redis_url and redis_token:
            self.queue = RedisQueue(redis_url, redis_token)
            self.worker = Worker(settings)
            await self.worker.setup(redis_url, redis_token)

        # Read from stdin, write to stdout
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await asyncio.get_event_loop().connect_read_pipe(lambda: protocol, sys.stdin)

        while True:
            try:
                # Read Content-Length header
                header = await reader.readline()
                if not header:
                    break

                header_str = header.decode().strip()
                if header_str.startswith("Content-Length:"):
                    length = int(header_str.split(":")[1].strip())
                    await reader.readline()  # Empty line
                    content = await reader.read(length)
                    message = json.loads(content.decode())

                    response = await self.handle_message(message)
                    if response:
                        response_str = json.dumps(response)
                        output = f"Content-Length: {len(response_str)}\r\n\r\n{response_str}"
                        sys.stdout.write(output)
                        sys.stdout.flush()

            except Exception:
                break

        if self.queue:
            await self.queue.close()
        if self.worker:
            await self.worker.shutdown()


def main():
    """Entry point for MCP server."""
    server = MCPServer()
    asyncio.run(server.run_stdio())


if __name__ == "__main__":
    main()
