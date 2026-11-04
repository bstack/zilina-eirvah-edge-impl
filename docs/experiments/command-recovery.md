# Command retry and recovery policy

The writer keeps a durable record for each admitted command ID. A duplicate retrieves the recorded state instead of initiating another execution. The immutable binding is a digest of the entire normalized command, including requester, target, value, type, reason, requested time, deadline and extra fields. Reusing a retained ID with different content is refused.

This policy applies to commands reaching the writer. Admission refusals are not recorded in the writer ledger. Duplicate business events that generate new command IDs are a separate concern; for example, restarting the decision agent does not make its existing in-memory inspection cache durable.

## Durable states

| Stored state | Reply to a duplicate | Startup recovery |
| --- | --- | --- |
| Preparing | Pending; no competing execution | Retryable because no native write initiation was committed |
| Started | Pending while the current handler runs | Unknown; never repeat this ID while retained |
| Completed | Original completion and timestamp, with `replayed=true` | Keep the completion |
| Retryable | A fresh attempt is allowed only while the original deadline is valid | Keep the known pre-write failure |
| Unknown | Unknown; no new execution | Keep the uncertainty |

A claim is stored before connecting to the device. Immediately before the first native write, the writer verifies approval/expiry and commits the initiation marker. The marker is checked again at subsequent native write boundaries. One application operation can require several protocol frames; those are part of the same attempt.

Successful device acknowledgement is recorded before replying. If only the reply is lost, a replay returns the retained completion. If the writer crashes or its completion commit fails after initiation, recovery reports unknown. It does not assume that the physical write failed. A cancellation also leaves a durable unknown if initiation occurred.

Connection/browse failures before initiation are known not to have issued a native write and may be retried with the same command. An exception after initiation is treated conservatively as unknown, including a lost native acknowledgement. No automatic retry of an uncertain operation is performed.

## Storage and retention

The implementation uses Python's standard SQLite library, WAL journaling and `synchronous=FULL` transactions. No database service or dependency was added. Transactions serialize competing claims, including concurrent duplicate deliveries.

The writer stores `/var/lib/eirvah/commands.sqlite3` on the `actuation-command-ledger` persistent volume claim. The claim requests 1 GiB using the cluster's default storage class. The deployment uses one replica, `Recreate`, and filesystem group 65534 for its existing non-root container. Recovery runs before the writer subscribes to NATS.

The default retention is 86,400 seconds. A terminal/retryable record is eligible for pruning only after 24 hours beyond the later of the original command deadline and its latest lifecycle update. Active preparing/started records are not pruned. Set `ACTUATION_SIGNAL_PUBLISHER_LEDGER_RETENTION_S` to change the window; set `ACTUATION_SIGNAL_PUBLISHER_LEDGER_PATH` only together with an appropriate persistent mount.

The guarantee requires one authoritative, surviving ledger and suitable clock behaviour. Do not scale independent writer replicas or copy the ledger into independent deployments. Pod replacement preserves the volume; deleting the PVC, losing the backing storage or recreating the cluster does not preserve this guarantee. Storage access or commit failures stop new writes or produce an explicit unknown, as appropriate.

The database is a command lifecycle ledger, not the proposed UNS industrial history store. Its state does not prove exactly-once physical effects or successful process response. Full execution trace instrumentation and reconciliation with device/process evidence remain later work.

## Retry without changing the command

Load `EIRVAH_COMMAND_AMQP_URL` as described in [command approval](command-approval.md). Save a new command before submission:

```powershell
uv run python scripts/control.py --target capper --action speed --value 1200 --save-command .\capper-command.json
```

If the reply is lost, submit the exact same file:

```powershell
uv run python scripts/control.py --retry-command .\capper-command.json
```

The tool never overwrites an existing saved file and never refreshes the ID, payload or deadline on replay. Do not recreate a command using a new ID merely because a reply was lost.

Authenticated expired requests may pass the bridge and current admission policy so the writer can retrieve a retained result. This does not renew execution permission. The writer authenticates the request before reading the ledger, and requires unexpired approval again before any new attempt. If there is no retained completion and the command has expired, it cannot start. Current requester/target policy still applies to lookup requests.

A `pending` or `unknown` result exits the CLI with code 2; a definite refusal exits 1 and an approved completion exits 0. A definite pre-write refusal may include `retryable=true`. Unknown requires investigation of the recorded lifecycle and device evidence; this change does not provide an automatic physical reconciliation or an operator override.

## Result and metric meanings

AMQP outcomes now include `approve`, `reject`, `pending` and `unknown`. Update consumers together with the command services. A completed replay preserves the original writer completion time and sets `replayed=true`. The rejection tracker keeps pending work pending and reports explicit unknown outcomes without treating them as confirmed physical rejections.

The orchestrator treats a missing, malformed or mismatched writer reply as unknown. It does not send unknown outcomes to the rejection dead-letter subject. Existing approval counts exclude retained-result replays; new replay, pending and unknown counters distinguish these reports. These counters describe observed reports, not a durable count of physical effects; a lost completion reply can still leave a gap until ledger reconciliation.

## Deploy and verify

Rebuild/import the five command services and apply the local overlay as in the approval instructions. The updated overlay creates the persistent volume claim and changes the writer strategy. Confirm the claim is Bound and the writer becomes Ready before testing. No cluster teardown is needed. Keep producer/bridge, validator, writer, orchestrator and decision-agent revisions consistent because the result semantics have changed.

```powershell
kubectl --context kind-eirvah-edge -n eirvah-edge get pvc actuation-command-ledger
uv run python -m pytest tests/scripts/test_command_recovery.py tests/scripts/test_actuation_security.py -q
```

Tests include concurrent claims, reopening the database, abrupt child-process exit, crash before/after initiation, failed completion commits, exact saved-file replay and typed pipeline outcomes. Loopback Modbus tests verify that a real rejection effect and native acknowledgement sequence are not repeated when the reply or completion commit is lost. These are engineering checks; live deployment/PVC validation is recorded below. Formal E0 coverage remains separate.

## Local cluster validation — 4 November 2026

Deployed to `kind-eirvah-edge` / `eirvah-edge`. Thirteen live checks passed across OPC UA, Modbus and S7, including retained completion replay, conflicting ID reuse, expiry, saved-command CLI replay and unavailable-ledger handling. Replacing the writer pod retained the same bound PVC and cached completion; duplicate requests did not advance the checked native acknowledgements or effect counters.

Recovery boundary cases used explicitly seeded `preparing` and `started` ledger records. They verify restart policy; they do not constitute an observed crash during a physical write. The started fixture remained unknown without execution, while the preparing fixture executed once. Actuator settings and the automatic producer were restored, with all 21 pods Ready. The read-only storage fixture cleanup was corrected after an initial failed run left a SQLite sidecar read-only; the repeated run passed.

Local evidence: `results/cluster-recovery/2026-11-04T09-11-39Z/summary.json` (gitignored). This rehearsal does not establish formal conformance or exactly-once physical effects.
