# Correlated command execution records

Each command-service lifecycle record uses `event=command_execution` and `execution_schema=1.0` on the existing JSON stdout observability path. These records are not published as industrial UNS messages. Command IDs remain log fields, never metric labels. No new service or dependency is required. Keep command-service logging at INFO to retain these records.

## Recorded stages

| Action | Component | Meaning |
| --- | --- | --- |
| `request_submitting`, `request_published` | Decision agent | Automatic producer attempts publication, then observes broker publication completion. |
| `request_received` | AMQP ingress | Command received with requester taken from broker-validated user identity. Covers CLI producers too. |
| `pipeline_received` | Orchestrator | Correlated command enters the control pipeline. |
| `validation_decided` | Validator | Authenticated policy decision and approval issue time/writer, without logging the signature or keys. Expired approvals permit retained-result lookup only. |
| `executor_admitted` | Writer | Fresh, authenticated, unexpired command obtains an exclusive ledger claim. |
| `write_initiated` | Writer | Approval rechecked and durable started state committed, immediately before the first outbound native frame. This boundary does not prove the frame reached the device. |
| `device_acknowledged` | Writer | OPC UA write response or matching Modbus/S7 mailbox acknowledgement observed. It does not prove subsequent physical process response. |
| `writer_result` | Writer | Completed, not-executed, conflict, pending or unknown; cached replies carry `replayed=true` and preserve original completion time. |
| `outcome_published` | Orchestrator | Publication of the correlated AMQP result completed. |
| `process_observed` | Decision agent | Existing bottle-rejection tracker observed the requested bottle ID and increased rejection count, together with writer acknowledgement. Includes inspection ID, feedback values and observation times. |
| `process_response_unresolved` | Decision agent | Rejection feedback timed out or the writer outcome was unknown. |

Every record includes command ID, requester, target, requested value/type, original request/deadline times, component, unique record ID, process instance ID, UTC record time and process-local monotonic time. Writer admission/initiation/acknowledgement records also identify the attempt. Multiple native frames produce one application initiation record. A replay produces a result record, not another initiation or device acknowledgement. A retry after a known pre-write failure has a new attempt identity under the same command ID.

## Evidence boundary

For all five controls, native acknowledgement is distinct from physical/process confirmation. Only the existing bottle-rejection feedback tracker emits `process_observed`; it reports simulator feedback, not a measured real plant. Temperature settling and the other process responses remain unestablished by this instrumentation. Matching state alone would not prove that a particular command caused it, especially with concurrent commands.

Logs are observations, not signed or transactionally durable execution proofs. A crash or log loss can leave gaps even when the ledger survives. UTC clock uncertainty is explicitly unknown (`clock_uncertainty_s=null`); monotonic times are comparable only within the same process instance. Do not infer distributed ordering or exactly-once physical effects from a sorted list. Full E0 conformance checking remains a separate roadmap step.

## Collect one command on Windows

After deploying the five updated command services, submit a command using `scripts/control.py` and copy its `command_id`. Collect the retained records into a new directory:

```powershell
uv run python scripts/command_trace.py COMMAND_ID --context kind-eirvah-edge --namespace eirvah-edge --since 1h --output results/command-traces/COMMAND_ID
```

The collector checks every current replica of the five command services and previous container logs when a current pod has restarted. It writes `records.jsonl` and `summary.json`, preserving source pod and previous/current provenance. It never overwrites an existing evidence directory. Collection errors or no records produce exit code 2; normal collection produces exit code 0, which does not mean conformance passed.

`missing_actions` lists absent stages of a fresh successful command for inspection. Rejection, replay and pre-write failure legitimately omit stages; absence is not automatically a violation. `process_response=unknown` means no retained process observation was collected. The summary always reports `formal_conformance=not_assessed`.

Coverage is restricted to the requested time window, available current/previous logs and a 10 MiB limit per log request. Size-limit hits and unavailable/missing service pods are reported. Deleted pods and older rotated logs cannot be recovered by this tool. Preserve the package before restarting or deleting infrastructure; an empty error list still does not establish complete coverage. Credentials and approval signatures are not collected by these structured records.

## Local validation — 4 November 2026

The full local suite passed with 381 tests and 16 skips; nine new cases cover record content, write/replay boundaries, unknown outcomes, feedback correlation, pipeline publication and collection gaps. Strict mypy passed on all eight changed production files, with Ruff and diff checks also passing.

After rebuilding and deploying the five command services to `kind-eirvah-edge`, a capper command and its replay produced 13 records with every fresh-command stage present, exactly one initiation and one native acknowledgement. The original completion time was retained. An out-of-range capper command produced four refusal-path records without initiating a write. The actual collection CLI wrote both packages successfully. The capper setting was preserved and all 21 pods were Ready. This live run did not exercise process-feedback confirmation; that path has regression coverage through the existing bottle-rejection tracker.

Local evidence: `results/execution-records/2026-11-04T15-41-49Z/summary.json` and its two trace directories (gitignored). These are engineering observations, not formal E0 results.

For a repeatable five-control workload using these records, see [scenario runner](scenario-runner.md).
