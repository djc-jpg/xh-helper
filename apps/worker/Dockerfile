FROM python:3.11-slim

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY apps/worker/requirements.lock /tmp/requirements.lock
RUN pip install --require-hashes -r /tmp/requirements.lock

COPY apps/worker /workspace/apps/worker
COPY data /workspace/data
COPY runtime_backbone /workspace/runtime_backbone

RUN mkdir -p /workspace/artifacts

ENV PYTHONPATH=/workspace:/workspace/apps/worker

EXPOSE 9001

CMD ["python", "/workspace/apps/worker/worker.py"]
