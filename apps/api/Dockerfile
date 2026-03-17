FROM python:3.11-slim

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY apps/api/requirements.lock /tmp/requirements.lock
RUN pip install --require-hashes -r /tmp/requirements.lock

COPY apps/api /workspace/apps/api
COPY eval /workspace/eval
COPY data /workspace/data
COPY runtime_backbone /workspace/runtime_backbone

RUN mkdir -p /workspace/artifacts

ENV PYTHONPATH=/workspace:/workspace/apps/api

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
