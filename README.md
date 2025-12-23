# ada-sub

Async task subscriber for development workflows.

## Architecture

```
                    ┌─────────────┐
                    │   QStash    │
                    │  (Upstash)  │
                    └──────┬──────┘
                           │ webhook
                           ▼
                    ┌─────────────┐
                    │  Receiver   │
                    │  (Railway)  │
                    └──────┬──────┘
                           │ LPUSH
                           ▼
                    ┌─────────────┐
                    │   Redis     │
                    │  (Upstash)  │
                    └──────┬──────┘
                           │ RPOP (poll)
                           ▼
                    ┌─────────────┐
                    │   Worker    │
                    │  (Local)    │
                    └─────────────┘
```

## Setup

### 1. Cloud (Upstash + Railway)

1. Create Upstash Redis database
2. Create Upstash QStash instance
3. Deploy `cloud/receiver.py` to Railway
4. Set environment variables in Railway

### 2. Local Worker

```bash
# Install
pip install -e .

# Configure
cp .env.example .env
# Edit .env with your credentials

# Run worker
ada-worker
```

### 3. VS Code MCP

The `.vscode/mcp.json` configures the MCP server for VS Code integration.

## Task Types

- `transform`: Audio synthesis (Bark)
- `generate`: Text generation (LM Studio)

## Publishing Tasks

```python
from cloud.publisher import publish_task

# Publish a task
msg_id = publish_task(
    content="Hello world",
    task_type="transform",
    preset="default",
)
```

## Configuration

See `.env.example` for all options.
