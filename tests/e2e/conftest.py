"""E2E test fixtures — requires a running k3d cluster (skip if absent)."""

from __future__ import annotations

import subprocess
import time
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import aiomqtt
import pytest

NAMESPACE = "eirvah-edge"
MQTT_LOCAL_PORT = 11883
NATS_LOCAL_PORT = 14222
OPCUA_LOCAL_PORT = 14840
PROM_LOCAL_PORT = 19090


def _cluster_is_up() -> bool:
    try:
        result = subprocess.run(
            ["kubectl", "-n", NAMESPACE, "get", "pods", "--no-headers"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0 and b"Running" in result.stdout
    except Exception:
        return False


def _port_forward(
    service: str, local_port: int, remote_port: int
) -> subprocess.Popen:  # type: ignore[type-arg]
    return subprocess.Popen(
        [
            "kubectl", "-n", NAMESPACE,
            "port-forward", f"svc/{service}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@dataclass
class EirVahCluster:
    mqtt_host: str = "localhost"
    mqtt_port: int = MQTT_LOCAL_PORT
    nats_servers: list[str] = field(
        default_factory=lambda: [f"nats://localhost:{NATS_LOCAL_PORT}"]
    )
    opcua_endpoint: str = (
        f"opc.tcp://localhost:{OPCUA_LOCAL_PORT}/eirvah/simulator"
    )
    prometheus_url: str = f"http://localhost:{PROM_LOCAL_PORT}"

    @asynccontextmanager
    async def mqtt_client(
        self,
        *,
        username: str = "eirvah",
        password: str = "eirvah-dev-password",
    ) -> AsyncGenerator[aiomqtt.Client, None]:
        async with aiomqtt.Client(
            hostname=self.mqtt_host,
            port=self.mqtt_port,
            username=username,
            password=password,
        ) as client:
            yield client


@pytest.fixture(scope="session")
def eirvah_cluster() -> Generator[EirVahCluster, None, None]:
    if not _cluster_is_up():
        pytest.skip(
            "k3d cluster not running — start with ./scripts/dev_up.sh"
        )

    procs = [
        _port_forward("mosquitto", MQTT_LOCAL_PORT, 1883),
        _port_forward("nats", NATS_LOCAL_PORT, 4222),
        _port_forward("opcua-simulator", OPCUA_LOCAL_PORT, 4840),
        _port_forward("prometheus", PROM_LOCAL_PORT, 9090),
    ]
    time.sleep(2)  # give port-forwards a moment to bind

    try:
        yield EirVahCluster()
    finally:
        for p in procs:
            p.terminate()
