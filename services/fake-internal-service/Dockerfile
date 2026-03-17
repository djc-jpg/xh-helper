FROM python:3.11-slim

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY services/fake-internal-service/requirements.lock /tmp/requirements.lock
RUN pip install --require-hashes -r /tmp/requirements.lock

COPY services/fake-internal-service /workspace/services/fake-internal-service

WORKDIR /workspace/services/fake-internal-service

EXPOSE 8100

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8100"]
