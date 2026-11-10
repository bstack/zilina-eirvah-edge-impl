# Local logical edge/cloud split

The existing service code already separates approval from native execution. This profile moves RabbitMQ, command ingress, validation and coordination into a cloud namespace, while the single writer, its ledger and the simulated equipment remain at the edge. Domain contracts and policy checks are unchanged.

## Selected placement

| Edge group | Cloud group |
| --- | --- |
| OPC UA, Modbus and S7 simulators and subscribers | RabbitMQ request/outcome broker |
| Converter, contextualizer, integration orchestrator and MQTT publisher | AMQP command ingress bridge |
| Mosquitto and the shared internal NATS backbone | Command validator |
| Fixed decision service, including automatic inspection/rejection | Command coordinator |
| One native writer and its persistent command ledger | No native writer or device endpoint mappings |

The decision service's edge placement is the first rehearsal baseline, not a requirement that every future decision service run at the edge. NATS is one logical backbone in this profile; cloud services use its explicit configured edge address. This change does not introduce a second NATS cluster, leaf-node routing or a second message-contract implementation.

The two Kustomize selections reuse existing component bases:

- `deploy/k3s/overlays/split-edge`
- `deploy/k3s/overlays/split-cloud`

`scripts/split_deployment.py` materializes namespaces, connection settings, image selections and configuration fingerprints from `config/deployments/local-split.json`. Existing local and E1 namespaces are not selected or modified. Each split namespace has its own simulator, recovery and command-ledger PVCs. Applying the profile does not reset existing volumes.

The initial local namespaces are `eirvah-split-edge` and `eirvah-split-cloud` on `kind-eirvah-edge`. Their placement groups are **not distinct physical hosts**: the generated architecture manifest retains the actual cluster-level host grouping and separately records namespace placement. It omits deployment claims for the baseline's Prometheus/Grafana/kube-state-metrics components, which this profile does not deploy.

## Coordinated credentials and endpoints

Credential material is generated once in the cloud namespace and retained on subsequent applies. The edge projection contains only the approval key, inspection receipt key and decision-service broker URL. The ingress key, broker definitions and coordinator/ingress/operator credentials remain in the cloud Secret; each application still receives only its required fields through individual Secret references.

The same approval key reaches the cloud validator and edge writer. Generating an independent full Secret on each side would break that relationship. Incomplete cloud credentials, different deployment ownership or incompatible existing edge keys/identity cause refusal rather than silent rotation. Updating the configured broker hostname retains the existing account passwords and signing keys. No private keys or generated account URLs are printed or written to deployment artifacts.

Cloud coordination forwards an unchanged signed `ApprovedWriteRequest`. The edge writer verifies its binding, intended writer and deadline, including immediately before native I/O, and retains the existing ledger/recovery policy. Retried execution does not acquire a new identity automatically. The native OPC UA/Modbus/S7 endpoints resolve within the edge namespace.

The local profile uses the existing protected-network transport assumptions. It does not configure TLS, NATS accounts/subject permissions, VPNs or enforced network policies. HMAC approval does not provide transport confidentiality or authenticate MQTT measurements. Separate-cluster/public/private/hybrid deployments need reachable protected endpoints and the selected transport controls before these local assumptions can be used there. The first apply/rehearsal CLI intentionally restricts mutations to the named local kind context; rendering and capture bindings are explicit configuration.

## Deploy and rehearse

Use PowerShell from `C:\research\zilina-eirvah-edge-impl`. Docker/kind and the verified local images must already exist. This profile selects the previously verified `data-converter:5fbf81f` image and `eirvah-e1:local` contextualizer image; the remaining service images are the current local baseline. Build/import those images using their existing workflows if absent. Images used for retained experiments should be pinned and recorded, rather than treating mutable local tags as immutable revisions.

```powershell
uv run python scripts/split_deployment.py apply --config config/deployments/local-split.json
# Substitute the deployment output directory printed above:
uv run python scripts/split_rehearsal.py --deployment results/split-deployment/<deployment-run>
```

The deployment output contains rendered resources, public connection configuration, component capture targets and the selected logical architecture manifest. Generated private command credentials go directly to Kubernetes through stdin, never to these files. The rendered edge selection includes the repository's existing development Mosquitto password file; this is the local development configuration, not newly generated private command material.

The rehearsal normally runs a fresh bounded E0 capture automatically. To reuse a retained passing E0 capture for the **same split manifest**, pass `--e0-run PATH`. An independent capture can be run with:

```powershell
uv run python scripts/e0_capture.py --targets results/split-deployment/<deployment-run>/targets.json --manifest results/split-deployment/<deployment-run>/architecture.json --assurance
```

`--manifest` is additive: existing callers keep the single-namespace default. The capture snapshots the selected manifest and its source hash before operating on the cluster. Component bindings direct journal collection, broker/device forwarding and restoration to the correct namespace/context.

## Selected local checks

1. **Runtime placement and trust:** verify the exact cloud/edge Deployment selections, minimal matching edge key projection and configured broker address, then retain actor pod/image/PVC/config identities.
2. **Bounded E0:** exercise the five controls, native readback, exact replay, invalid-value refusal, expired fresh work and unsigned/tampered/wrong-writer direct requests. Retain private execution records and ledger evidence. This is scoped conformance, not universal C01-C13 proof.
3. **Automatic warmer:** submit conveyor speeds 0.7 then 0.4 m/s; observe the real decision service request 26 then 22 degrees C through cloud admission and edge writing. Independently sample native setpoint and simulated temperature. Each selected response must settle within 0.75 degrees on three consecutive 0.5-second polls in a 60-second window. These are polls, not three guaranteed independent simulator updates, and not a validated process-physics result.
4. **Automatic rejection:** retain complete, stable private-journal segments and require at least one correlated decision request, cloud ingress/validation/coordination, edge initiation/acknowledgement, completed ledger row and decision-service process-feedback record. Both warmer transitions need the same complete cross-namespace route. Retain other observed outcomes too; selected confirmation does not imply every industrial event always completes before expiry.
5. **Writer unavailability:** pause the automatic consumer, stop only the split edge writer, submit an operator command, and require an unknown outcome with unchanged native setpoint. Restore the same ledger-backed writer; resubmit the identical command after expiry and require refusal with no native initiation. A distinct explicit operator command then demonstrates fresh recovery. This is not an automatic policy of replacing ambiguous commands with new IDs.
6. **Restoration:** restore the writer, original selected speed/setpoint and one automatic consumer. Temporary port-forwards close; the split remains available for further work. The ordinary baseline and E1 deployments remain separate.

Writer unavailability is a service/responder-loss scenario. It is **not** a WAN partition, packet-loss experiment, private/public cloud deployment or security-boundary proof. Namespaces alone do not deny native-network access. The shared NATS backbone is a declared dependency and single availability boundary in this first realization.

## Evidence and next scope

The retained records include split configuration, manifest attribution, actual pod placement, configuration/image identities, private journal fences, per-command ledger rows, native samples, refusal/unknown outcomes and restoration state. A timed-out collection remains unknown and is not overwritten as passing; run a new capture after checking/restoring the affected roles.

The unchanged warmer rule does not embed an explicit source-observation ID in each generated command. This rehearsal uses controlled sequential speed stimuli and independently observed automatic commands/process response; it does not claim a new payload-level observation-to-command lineage contract. Inspection confirmation retains its existing inspection/command identity linkage.

Metrics endpoints remain available on both sides and private journal collection follows the target map. The original namespace-limited Prometheus configuration does not automatically discover these namespaces. Controlled experiment automation still needs its intended cross-placement resource/coverage collection and repetitions. Separate-cluster networking, protected remote transports, calibrated latency/fault conditions and public/private/hybrid evidence remain later deployment-flexibility experiments.


## Local verification - 10 November 2026

The selected rehearsal **passes**: `results/split-rehearsal/2026-11-10T18-48-11-042021Z/report.md`.

- Deployment: 14 edge and 4 cloud pods, with matching minimal edge trust and the declared role/connection configuration.
- Bounded E0: `results/e0-capture/2026-11-10T17-12-32-349661Z/` passes against the split manifest, retaining 102 execution records and 14 submissions. The controller and temporary simulator binding were restored. Whole-study conformance remains unknown.
- Automatic paths: both warmer transitions reached the selected setpoint and simulated response; four bottle rejections had correlated cloud admission, edge write/acknowledgement, completed ledger and decision feedback records. No manual warmer command substituted for either automatic transition.
- Service interruption: the absent edge writer produced an unknown outcome without changing the native setpoint. The same request was refused after expiry without a new initiation or ledger claim; a fresh explicit command completed after recovery. The restarted writer journal was complete and contained the fresh initiation/acknowledgement evidence.
- Restoration: the writer and automatic consumer returned to one replica; independent post-run native readback confirmed the original 22 degree C setpoint and 0.4 m/s conveyor speed. The baseline/E1 namespaces remained healthy.
- Idempotency: reapplying the split retained both namespaces' credential data and pod UIDs. Private data was compared only in memory; the retained verification contains booleans, not credentials.
- Provenance: selected source hashes match the retained source snapshots. The earlier `17-19-55-502935Z` run remains unknown because journal collection timed out after a long pause; it was restored and not relabelled as passing.

Validation: **685 passed, 16 skipped**. Ruff, strict mypy for the three split/capture modules, Kustomize rendering and `git diff --check` pass. Existing local single-namespace capture callers retain their default manifest.
