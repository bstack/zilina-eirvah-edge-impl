"""Create per-cluster command credentials and HMAC keys without printing them."""

from __future__ import annotations

import argparse
import base64
import json
import secrets
import subprocess
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SECRET_NAME = "actuation-security"


def producer_permissions() -> tuple[str, str, str]:
    temporary = r"(amq\.gen-.*|amq_[0-9a-f]{32})"
    return (
        f"^{temporary}$",
        rf"^(eirvah\.actuation\.commands|{temporary})$",
        rf"^(eirvah\.actuation\.results|{temporary})$",
    )


def refresh_permissions(secret: dict[str, Any]) -> bool:
    definitions = json.loads(base64.b64decode(secret["data"]["definitions.json"]))
    changed = False
    for permission in definitions["permissions"]:
        if permission["user"] in {"decision-agent-stub", "experiment-driver"}:
            for key, value in zip(
                ("configure", "write", "read"), producer_permissions(), strict=True
            ):
                if permission[key] != value:
                    permission[key] = value
                    changed = True
    if changed:
        secret["data"]["definitions.json"] = base64.b64encode(
            json.dumps(definitions).encode()
        ).decode()
    return changed


def build_secret(namespace: str) -> dict[str, Any]:
    config = yaml.safe_load((ROOT / "deploy/k3s/base/rabbitmq/configmap.yaml").read_text())
    definitions = json.loads(config["data"]["definitions.json"])
    values = {
        "ingress-key": secrets.token_urlsafe(48),
        "approval-key": secrets.token_urlsafe(48),
        "inspection-receipt-key": secrets.token_urlsafe(48),
    }
    queue = r"eirvah\.actuation\.requests"
    results = r"eirvah\.actuation\.results"
    for user, key in (
        ("decision-agent-stub", "decision-amqp-url"),
        ("experiment-driver", "experiment-amqp-url"),
        ("command-ingress", "ingress-amqp-url"),
        ("command-coordinator", "coordinator-amqp-url"),
    ):
        password = secrets.token_urlsafe(32)
        definitions["users"].append({"name": user, "password": password, "tags": ""})
        if user in {"decision-agent-stub", "experiment-driver"}:
            configure, write, read = producer_permissions()
        elif user == "command-ingress":
            configure, write, read = f"^{queue}$", "^$", f"^{queue}$"
        else:
            configure, write, read = f"^{results}$", f"^{results}$", "^$"
        definitions["permissions"].append(
            {"user": user, "vhost": "/", "configure": configure, "write": write, "read": read}
        )
        values[key] = f"amqp://{user}:{password}@rabbitmq:5672/"
    values["definitions.json"] = json.dumps(definitions)
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": SECRET_NAME, "namespace": namespace},
        "type": "Opaque",
        "data": {k: base64.b64encode(v.encode()).decode() for k, v in values.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", default="eirvah-edge")
    args = parser.parse_args()
    prefix = ["kubectl", "--context", args.context, "-n", args.namespace]
    found = subprocess.run(
        [*prefix, "get", "secret", SECRET_NAME, "--ignore-not-found", "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    if found.stdout.strip():
        existing_secret = json.loads(found.stdout)
        existing = existing_secret["data"]
        if (
            not {
                "ingress-key",
                "approval-key",
                "definitions.json",
                "experiment-amqp-url",
                "decision-amqp-url",
                "ingress-amqp-url",
                "coordinator-amqp-url",
            }
            <= existing.keys()
        ):
            raise SystemExit(
                "Existing actuation-security secret is incomplete; inspect it before replacing it."
            )
        receipt_added = "inspection-receipt-key" not in existing
        if receipt_added:
            existing["inspection-receipt-key"] = base64.b64encode(
                secrets.token_urlsafe(48).encode()
            ).decode()
        if refresh_permissions(existing_secret) or receipt_added:
            subprocess.run(
                [*prefix, "replace", "-f", "-"],
                input=json.dumps(existing_secret),
                text=True,
                capture_output=True,
                check=True,
            )
            print(
                "Ensured recovery receipt key and broker permissions; existing keys and passwords retained. Restart RabbitMQ to load definitions."
            )
        else:
            print("Existing actuation-security secret retained; no credentials rotated.")
        return
    subprocess.run(
        [*prefix, "create", "-f", "-"],
        input=json.dumps(build_secret(args.namespace)),
        text=True,
        capture_output=True,
        check=True,
    )
    print(
        "Created actuation-security secret. Restart RabbitMQ and command services when deploying these changes."
    )


if __name__ == "__main__":
    main()
