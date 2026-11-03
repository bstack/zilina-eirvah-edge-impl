# Command identity and approval

The command path authenticates the producer, signs the bridge handoff, and verifies signed admission immediately before writing. It uses HMAC-SHA256 from Python's standard library; no new dependencies were added. This is the selected implementation for the study, with no second signature implementation planned.

## Command path

1. Decision agent and experiment driver have separate RabbitMQ accounts. They publish to `eirvah.actuation.commands`, with their AMQP `user_id` set. RabbitMQ validates that property against the authenticated connection user. Producer accounts have no impersonator tag and cannot publish command outcomes or consume the command queue.
2. The AMQP bridge accepts only those two validated identities. It overwrites the payload's claimed requester with the broker identity, then signs the entire command using the bridge/validator ingress key. Direct NATS injection without that evidence cannot pass the validator.
3. The validator verifies ingress evidence, checks the target policy, actual value type, range and requester allowlist, then issues approval signed with a separate validator/writer key. Approval binds the entire command, including ID, requester, target, value, type and deadline, plus the intended writer and issue time.
4. The orchestrator forwards the unchanged command and approval as `ApprovedWriteRequest`. It never forwards an unapproved request to the writer.
5. The writer verifies the signature, command binding, writer identity and deadline on receipt and again immediately before native writes. The final checks happen after connection setup for OPC UA, Modbus and S7. Missing deadlines, naive timestamps and future-dated evidence fail closed.

The public `ActuationRequest` remains version 1.0. New internal `AuthenticatedRequest`, `ApprovedWriteRequest` and `SignedEvidence` contracts are version 1.0. The AMQP result remains separate from admission evidence; `written_at` still describes result observation after the writer reply, not an instrumented initiation timestamp.

## Trust and scope

The broker, bridge, validator, writer and Kubernetes administrators are trusted. The ingress key is shared only by bridge and validator; the approval key is shared only by validator and writer. Either holder of a shared key can generate its evidence. The Kubernetes Secret also contains broker definitions and separate account credentials; each pod receives only the fields it needs.

The current local setup assumes a protected network and synchronized clocks. This change does not add TLS or authenticate MQTT measurements. Protect connections when crossing untrusted network boundaries. Administrative broker credentials remain for local operators and must be treated as privileged.

Valid approvals can still be replayed before expiry: durable command identity, duplicate execution prevention and recovery policy are the following implementation step. A matching signature does not establish physical success. Complete execution traces and authenticated outcome reports remain outside this change.

The broker identity mechanism follows RabbitMQ's [validated user ID](https://www.rabbitmq.com/docs/validated-user-id) and [access control](https://www.rabbitmq.com/docs/access-control) behaviour. This implementation uses AMQP 0-9-1 via aio-pika.

## Deploy into the existing local cluster

First rebuild and import the five changed service images using the repository's normal build and kind image-loading commands: `amqp-actuation-event-subscriber`, `actuation-event-validator`, `actuation-control-orchestrator`, `actuation-signal-publisher` and `decision-agent-stub`. Changes to the contracts library are included in those rebuilt service images. Keep all five on the same revision; the secured internal wire formats do not support mixed old/new services.

From the repository root in PowerShell:

```powershell
kubectl --context kind-eirvah-edge apply -f deploy/k3s/base/namespace.yaml
uv run python scripts/configure_actuation_security.py --context kind-eirvah-edge --namespace eirvah-edge
kubectl --context kind-eirvah-edge apply -k deploy/k3s/overlays/local

$services = @(
    'rabbitmq',
    'amqp-actuation-event-subscriber',
    'actuation-event-validator',
    'actuation-control-orchestrator',
    'actuation-signal-publisher',
    'decision-agent-stub'
)
foreach ($service in $services) {
    kubectl --context kind-eirvah-edge -n eirvah-edge rollout restart "deployment/$service"
    if ($LASTEXITCODE -ne 0) { throw "Restart failed: $service" }
    kubectl --context kind-eirvah-edge -n eirvah-edge rollout status "deployment/$service" --timeout=180s
    if ($LASTEXITCODE -ne 0) { throw "Rollout failed: $service" }
}
```

The setup helper generates random credentials and two independent HMAC keys. It sends the Secret to the explicitly selected cluster without printing credentials or writing them to repository files. Subsequent runs retain the existing keys and passwords; known producer permission rules are refreshed if their template changes. `dev_up.sh` also runs this setup for the local kind context. An existing incomplete Secret causes an error instead of silent replacement. The namespace must exist first.

RabbitMQ loads generated user definitions from the Secret. Its restart is required when first introducing these accounts or refreshing broker permissions. The local broker limits Erlang server and diagnostic scheduler threads to avoid probe timeouts under its CPU limit. Restart application services after changing secrets because environment variables and the RabbitMQ definitions mount do not rotate in place. Rotating credentials invalidates in-flight evidence; drain commands and plan recovery first. Automatic key rotation is not implemented.

## Local command tools

Keep the normal RabbitMQ port forward running. Load the experiment driver's URL into this PowerShell session without printing it:

```powershell
$security = kubectl --context kind-eirvah-edge -n eirvah-edge get secret actuation-security -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Cannot read research command credentials' }
$commandUrl = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($security.data.'experiment-amqp-url'))
$env:EIRVAH_COMMAND_AMQP_URL = $commandUrl.Replace('@rabbitmq:5672/', '@localhost:5672/')
Remove-Variable security, commandUrl

uv run python scripts/control.py --target capper --action speed --value 1200
```

`control.py`, `disturbance.py` and experiment A inherit this environment variable. Do not use the former shared `eirvah` login for command submission. For a different local AMQP port, change the localhost port in the URL to match the port forward and harness configuration. Avoid passing credentials as command-line arguments or posting them in logs.

## Verification

Offline suite:

```powershell
uv run python -m pytest -q
```

Optional isolated broker checks require Docker and the `rabbitmq:3.13-management` image:

```powershell
docker pull rabbitmq:3.13-management
$env:EIRVAH_RUN_BROKER_TESTS = '1'
uv run python -m pytest tests/scripts/test_command_broker.py -q
Remove-Item Env:EIRVAH_RUN_BROKER_TESTS
```

These broker tests create a disposable container bound only to localhost, exercise real publisher identity and permission checks, and remove the container and temporary credential definitions afterward. They do not use the research cluster. Unit tests cover tampered requests, invalid signatures, wrong writers, unauthorized ingress, expiry and checks after native connection setup. Local tests support these mechanisms; deployed E0 conformance and distributed clock evidence remain pending.
