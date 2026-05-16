# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/opcua-simulator /workspace/services/opcua-simulator
RUN uv sync --frozen --no-dev --package opcua-simulator

FROM gcr.io/distroless/python3-debian12 AS runtime
WORKDIR /app
COPY --from=builder /workspace/.venv /app/.venv
COPY --from=builder /workspace/libs /app/libs
COPY --from=builder /workspace/services/opcua-simulator/src /app/src
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONDONTWRITEBYTECODE=1
USER nonroot:nonroot
EXPOSE 4840 8080
ENTRYPOINT ["/app/.venv/bin/python", "-m", "opcua_simulator"]
