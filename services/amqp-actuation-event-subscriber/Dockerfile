# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/amqp-actuation-event-subscriber /workspace/services/amqp-actuation-event-subscriber
RUN uv sync --frozen --no-dev --package amqp-actuation-event-subscriber

FROM python:3.12-slim AS runtime
WORKDIR /workspace
COPY --from=builder /workspace/.venv /workspace/.venv
COPY --from=builder /workspace/libs /workspace/libs
COPY --from=builder /workspace/services/amqp-actuation-event-subscriber/src \
     /workspace/services/amqp-actuation-event-subscriber/src
ENV PATH="/workspace/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER nobody:nogroup
EXPOSE 8080
ENTRYPOINT ["/workspace/.venv/bin/python", "-m", "amqp_actuation_event_subscriber"]
