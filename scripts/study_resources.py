"""Read-only resource sampling for explicitly selected study targets and owned processes."""

from __future__ import annotations

import ctypes
import json
import os
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from e0_capture import Cluster


def quantity(value: str, kind: str) -> float:
    match = re.fullmatch(r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)([a-zA-Z]*)", value)
    if not match:
        raise ValueError("invalid quantity")
    amount = Decimal(match[1])
    suffix = match[2]
    scales = (
        {"": 1, "n": 1e-9, "u": 1e-6, "m": 1e-3}
        if kind == "cpu"
        else {
            "": 1,
            **{u + "i": 1024**i for i, u in enumerate("KMGTPE", 1)},
            **{u: 1000**i for i, u in enumerate("kMGTPE", 1)},
        }
    )
    if suffix not in scales or not amount.is_finite() or amount < 0:
        raise ValueError("unsupported or negative quantity")
    return float(amount) * scales[suffix]


def process_usage(pid: int) -> dict[str, Any]:
    if os.name == "nt":
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("faults", wintypes.DWORD),
                *[
                    (n, ctypes.c_size_t)
                    for n in (
                        "peak_rss",
                        "rss",
                        "peak_pool",
                        "pool",
                        "peak_nonpool",
                        "nonpool",
                        "pagefile",
                        "peak_pagefile",
                    )
                ],
            ]

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.GetProcessTimes.argtypes = [
            wintypes.HANDLE,
            *[ctypes.POINTER(wintypes.FILETIME)] * 4,
        ]
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(Counters),
            wintypes.DWORD,
        ]
        handle = kernel.OpenProcess(0x0400 | 0x0010, False, pid)
        if not handle:
            raise ProcessLookupError(pid)
        try:
            created, exited, system, user = [wintypes.FILETIME() for _ in range(4)]
            memory = Counters()
            memory.cb = ctypes.sizeof(memory)
            if not kernel.GetProcessTimes(
                handle,
                ctypes.byref(created),
                ctypes.byref(exited),
                ctypes.byref(system),
                ctypes.byref(user),
            ) or not psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb):
                raise OSError("process counters unavailable")

            def ticks(t: Any) -> int:
                return int(t.dwHighDateTime) << 32 | int(t.dwLowDateTime)

            return dict(
                identity=str(ticks(created)),
                created_epoch=ticks(created) / 1e7 - 11644473600,
                cpu_seconds=(ticks(system) + ticks(user)) / 1e7,
                rss_bytes=memory.rss,
            )
        finally:
            kernel.CloseHandle(handle)
    raw = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    frequency = getattr(os, "sysconf")("SC_CLK_TCK")  # noqa: B009 - sysconf is absent from Windows types.
    boot = int(
        next(
            line.split()[1]
            for line in Path("/proc/stat").read_text().splitlines()
            if line.startswith("btime ")
        )
    )
    rss = (
        int(
            next(
                line.split()[1]
                for line in Path(f"/proc/{pid}/status").read_text().splitlines()
                if line.startswith("VmRSS:")
            )
        )
        * 1024
    )
    return dict(
        identity=raw[19],
        created_epoch=boot + int(raw[19]) / frequency,
        cpu_seconds=(int(raw[11]) + int(raw[12])) / frequency,
        rss_bytes=rss,
    )


def phase_for_window(
    begin: float, end: float, phases: list[dict[str, Any]], uncertainty: float | None
) -> str | None:
    if uncertainty is None:
        return None
    matches = [
        p["name"]
        for p in phases
        if begin - uncertainty >= p["begin"] and end + uncertainty <= p["end"]
    ]
    return matches[0] if len(matches) == 1 else None


def kube_sample(binding: dict[str, str]) -> dict[str, Any]:
    cluster = Cluster(binding["context"], binding["namespace"])
    metadata = json.loads(cluster.run("get", "pods", "-o", "json"))["items"]
    identities = {
        p["metadata"]["name"]: dict(
            uid=p["metadata"]["uid"],
            node=p["spec"]["nodeName"],
            containers=[
                dict(name=c["name"], image_id=c.get("imageID"), restarts=c.get("restartCount"))
                for c in p.get("status", {}).get("containerStatuses", [])
            ],
        )
        for p in metadata
        if "nodeName" in p["spec"]
    }
    raw = json.loads(
        cluster.run(
            "get",
            "--raw",
            "/apis/metrics.k8s.io/v1beta1/namespaces/" + binding["namespace"] + "/pods",
        )
    )
    values = []
    for pod in raw["items"]:
        name = pod["metadata"]["name"]
        if name not in identities:
            continue
        for container in pod["containers"]:
            values.append(
                dict(
                    pod=name,
                    **identities[name],
                    container=container["name"],
                    cpu_cores=quantity(container["usage"]["cpu"], "cpu"),
                    memory_bytes=quantity(container["usage"]["memory"], "memory"),
                    server_timestamp=pod["timestamp"],
                    window=pod["window"],
                )
            )
    return dict(
        backend="kubernetes",
        binding=binding,
        status="ok" if values else "missing",
        values=values,
        missing_pods=sorted(set(identities) - {v["pod"] for v in values}),
    )


def queue_sample(context: str) -> dict[str, Any]:
    expression = '{__name__=~"eirvah_(worker_(pending|inflight)|e1_pending|e1_queue_seconds_(bucket|count|sum)|worker_service_seconds_(bucket|count|sum))"}'
    path = (
        "/api/v1/namespaces/eirvah-e1/services/e1-prometheus:9090/proxy/api/v1/query?"
        + urlencode({"query": expression})
    )
    data = json.loads(Cluster(context, "eirvah-e1").run("get", "--raw", path))
    if data["status"] != "success":
        raise ValueError("queue query failed")
    return dict(
        backend="prometheus",
        status="ok" if data["data"]["result"] else "missing",
        values=data["data"]["result"],
    )


class Sampler:
    def __init__(
        self,
        output: Path,
        bindings: list[dict[str, str]],
        events: Path,
        interval: float = 5,
        docker_owner: str | None = None,
        queues: bool = False,
    ) -> None:
        self.output, self.bindings, self.events, self.interval = output, bindings, events, interval
        self.docker_owner, self.queues = docker_owner, queues
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.processes: dict[int, float] = {}
        self.lock = threading.Lock()
        self.errors: list[str] = []

    def add_process(self, pid: int, created: float | None = None) -> None:
        with self.lock:
            self.processes[pid] = created if created is not None else time.time()

    def _capture(self, function: Any, target: Any) -> dict[str, Any]:
        start = datetime.now(UTC).isoformat()
        try:
            value: dict[str, Any] = function(target)
        except Exception as exc:
            value = dict(
                backend="kubernetes" if isinstance(target, dict) else "prometheus",
                target=target,
                status="unknown",
                error=type(exc).__name__,
                values=[],
            )
        return dict(
            **value,
            collected_begin=start,
            collected_end=datetime.now(UTC).isoformat(),
            host_monotonic_ns=time.perf_counter_ns(),
        )

    def sample(self) -> list[dict[str, Any]]:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self._capture, kube_sample, b) for b in self.bindings]
            if self.queues and self.bindings:
                futures.append(
                    pool.submit(self._capture, queue_sample, self.bindings[0]["context"])
                )
            rows = [f.result() for f in futures]
        if self.events.exists():
            for line in self.events.read_text(encoding="utf-8").splitlines():
                try:
                    event = json.loads(line)
                    if event["event"] == "process_started":
                        self.add_process(
                            event["process_pid"],
                            datetime.fromisoformat(event["observed_at"]).timestamp(),
                        )
                except (ValueError, KeyError):
                    continue
        with self.lock:
            processes = list(self.processes.items())
        for pid, created in processes:
            try:
                sample = process_usage(pid)
                if sample["created_epoch"] > created + 0.005:
                    raise ProcessLookupError("PID reused")
                rows.append(
                    dict(
                        backend="host-process",
                        status="ok",
                        pid=pid,
                        observed_at=datetime.now(UTC).isoformat(),
                        **sample,
                    )
                )
            except (OSError, ValueError, StopIteration):
                rows.append(
                    dict(
                        backend="host-process",
                        status="gone",
                        pid=pid,
                        observed_at=datetime.now(UTC).isoformat(),
                    )
                )
                with self.lock:
                    self.processes.pop(pid, None)
        if self.docker_owner:
            try:
                ids = subprocess.check_output(
                    [
                        "docker",
                        "ps",
                        "--filter",
                        "label=eirvah.e2.run=" + self.docker_owner,
                        "--format",
                        "{{.ID}}",
                    ],
                    text=True,
                    timeout=15,
                ).split()
                if ids:
                    text = subprocess.check_output(
                        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                        text=True,
                        timeout=15,
                    )
                    rows.append(
                        dict(
                            backend="docker",
                            status="ok",
                            observed_at=datetime.now(UTC).isoformat(),
                            values=[json.loads(line) for line in text.splitlines()],
                        )
                    )
            except (OSError, subprocess.SubprocessError, ValueError) as exc:
                rows.append(
                    dict(
                        backend="docker",
                        status="unknown",
                        error=type(exc).__name__,
                        observed_at=datetime.now(UTC).isoformat(),
                        values=[],
                    )
                )
        return rows

    def _run(self) -> None:
        try:
            with self.output.open("a", encoding="utf-8") as target:
                while True:
                    start = time.perf_counter()
                    for row in self.sample():
                        target.write(json.dumps(row, allow_nan=False) + "\n")
                    target.flush()
                    if self.stop_event.wait(
                        max(0.1, self.interval - (time.perf_counter() - start))
                    ):
                        break
        except Exception as exc:
            self.errors.append(type(exc).__name__)

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="study-resource-capture", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=75)
            if self.thread.is_alive():
                self.errors.append("resource capture did not finish")
