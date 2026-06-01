# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/s7-simulator /workspace/services/s7-simulator
RUN uv sync --frozen --no-dev --package s7-simulator

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/s7-simulator/src /workspace/services/s7-simulator/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
EXPOSE 102 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "s7_simulator"]
