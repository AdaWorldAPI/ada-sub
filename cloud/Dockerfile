# Minimal receiver image for Railway/Render
FROM python:3.11-slim

WORKDIR /app

# No dependencies needed - using stdlib only
COPY receiver.py .

ENV PORT=8080

CMD ["python", "receiver.py"]
