# EirVah Edge Code

Edge Integration Layer for the **EirVah** reference architecture — a scalable, cost-efficient, open reference architecture for Unified Namespace (UNS) in Industrial IoT.

This repo is the implementation half of William Francis Stack's PhD work at the University of Žilina (supervisor: Aleš Janota). Scope is **the edge only** — protocol adapters, contextualizers, and publishers that translate industrial signals into the UNS over MQTT/AMQP, plus the actuation path back to devices. The cloud-side layers (persistence, decision/analytics) live in sibling repos.

## What's here

- A vertical-slice implementation of the Edge Integration Layer running on k3s, validated against a simulated bottling-line OPC UA device.
- All open source. No proprietary dependencies.

## Status

- **Plan 1 — Foundations:** in progress.
- **Plan 2 — Telemetry path:** queued.
- **Plan 3 — Actuation path:** queued.
- **Plan 4 — Polish and reproducibility:** queued.

## Key documents

- Spec: [`docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md`](docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md)
- Plan 1: [`docs/superpowers/plans/2026-05-16-plan-1-foundations.md`](docs/superpowers/plans/2026-05-16-plan-1-foundations.md)
- PhD proposal (companion): `UNIZA_Project_Proposal__EirVah__...pdf`

## Prerequisites

- macOS or Linux
- Python 3.12 (managed by uv — no system Python needed)
- [uv](https://github.com/astral-sh/uv)
- Docker
- [k3d](https://k3d.io/)
- kubectl, kustomize

## Getting started (after Plan 1 is complete)

```bash
uv sync                       # install workspace + dev deps
./scripts/dev_up.sh           # create k3d cluster, build, deploy
# Grafana URL printed at the end; open it and switch to "Bottling Line State" dashboard
./scripts/dev_down.sh         # tear it all down
```

## Licensing

This repo is Apache-2.0 licensed. Every runtime and build dependency is OSI-approved open source.
