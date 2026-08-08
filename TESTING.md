# Manual demo tests

Three copy-paste tests against a running `./scripts/dev_up.sh` cluster, for
demonstrating the actuation path and the telemetry→actuation feedback loop
live. Each command generates its own correlation ID, so they're safe to
re-run — **except the disturbance test, which is a once-off** (see below).

**Run order matters: do 1 and 2 first, 3 last.** The disturbance test (3)
pushes the simulated temperature out of range and leaves it there for a
while — it takes time to decay back down, and `decision-agent-stub` will
keep autonomously firing its own actuation requests every ~60-90s for as
long as it's still breaching. If that happens *after* your manual tests
in 1/2, it silently overwrites the OPC UA setpoint moments later, which
breaks the "read back the setpoint" verification step (the dashboard
counters are unaffected — they still attribute correctly).

If you're re-running the whole sequence, reset first:

```bash
kubectl exec -n eirvah-edge deploy/opcua-simulator -- python3 -c "
import asyncio
from asyncua import Client

async def main():
    async with Client(url='opc.tcp://localhost:4840/eirvah/simulator') as client:
        ns_idx = await client.get_namespace_index('https://eirvah.uniza/zilina/factory1')
        node = await client.nodes.objects.get_child([f'{ns_idx}:bottler', f'{ns_idx}:SetpointTemperature'])
        await node.write_value(20.0)
        print('setpoint reset to 20.0')

asyncio.run(main())
"
kubectl -n eirvah-edge rollout restart deployment/decision-agent-stub
```

Wait ~15-20s after that for the temperature to settle back under 26°C
before starting (check with the temperature query in test 3's "confirm
it's safe to start" step, or just watch "Bottling Line State" in Grafana).

Watch results on the "EirVah Edge Pipeline" Grafana dashboard:

```bash
kubectl -n eirvah-edge port-forward svc/grafana 3000:3000
# http://localhost:3000 — admin / eirvah-dev-grafana
```

The "Actuation Approved"/"Actuation Rejected" panels are a trailing
5-minute window, so results show up within a few seconds and stay visible
for about 5 minutes.

## 1. Actuation success

Publishes a request within the policy's allowed range (`[20.0, 30.0]`)
from an allow-listed requester — validator approves it, and
`actuation-signal-publisher` writes the value back to the OPC UA
simulator's setpoint.

```bash
kubectl exec -n eirvah-edge deploy/amqp-actuation-event-subscriber -- python3 -c "
import asyncio
from datetime import datetime, timezone
import aio_pika
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id

async def main():
    req = ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester='decision-agent-stub',
        target_uns_topic='uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature',
        requested_value=25.0,
        value_type='double',
        reason='demo: actuation success',
        requested_at=datetime.now(timezone.utc),
    )
    conn = await aio_pika.connect_robust('amqp://eirvah:eirvah-dev-password@rabbitmq:5672/')
    async with conn:
        channel = await conn.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=req.model_dump_json().encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key='eirvah.actuation.requests',
        )
        print('published:', req.model_dump_json())

asyncio.run(main())
"
```

**Expect:** "Actuation Approved" increments on the dashboard — this is the
unambiguous signal, always correctly attributed to this one request
regardless of anything else happening on the cluster. To also confirm the
write-back landed on the device itself:

```bash
kubectl exec -n eirvah-edge deploy/opcua-simulator -- python3 -c "
import asyncio
from asyncua import Client

async def main():
    async with Client(url='opc.tcp://localhost:4840/eirvah/simulator') as client:
        ns_idx = await client.get_namespace_index('https://eirvah.uniza/zilina/factory1')
        node = await client.nodes.objects.get_child([f'{ns_idx}:bottler', f'{ns_idx}:SetpointTemperature'])
        print('SetpointTemperature is now:', await node.read_value())

asyncio.run(main())
"
```

Should print `25.0` — but only if nothing else has written to the same
setpoint in the meantime. It's a shared, mutable value: if
`decision-agent-stub` is still autonomously firing (see the run-order note
above), it can overwrite this moments later with its own target (`22.0`).
Run this check right after publishing, before anything else touches the
setpoint.

## 2. Actuation failure

Same path, but rejected by `actuation-event-validator`. Three independent
failure reasons — each is its own bounded category on the "Actuation
Rejected" panel (`out_of_range`, `not_allowlisted`, `not_numeric`); run
whichever you want to show, or all three.

**Out of range** (`requested_value` outside `[20.0, 30.0]`):

```bash
kubectl exec -n eirvah-edge deploy/amqp-actuation-event-subscriber -- python3 -c "
import asyncio
from datetime import datetime, timezone
import aio_pika
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id

async def main():
    req = ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester='decision-agent-stub',
        target_uns_topic='uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature',
        requested_value=60.0,
        value_type='double',
        reason='demo: actuation failure (out_of_range)',
        requested_at=datetime.now(timezone.utc),
    )
    conn = await aio_pika.connect_robust('amqp://eirvah:eirvah-dev-password@rabbitmq:5672/')
    async with conn:
        channel = await conn.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=req.model_dump_json().encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key='eirvah.actuation.requests',
        )
        print('published:', req.model_dump_json())

asyncio.run(main())
"
```

**Not allow-listed** (valid value, unrecognized requester):

```bash
kubectl exec -n eirvah-edge deploy/amqp-actuation-event-subscriber -- python3 -c "
import asyncio
from datetime import datetime, timezone
import aio_pika
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id

async def main():
    req = ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester='untrusted-agent',
        target_uns_topic='uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature',
        requested_value=25.0,
        value_type='double',
        reason='demo: actuation failure (not_allowlisted)',
        requested_at=datetime.now(timezone.utc),
    )
    conn = await aio_pika.connect_robust('amqp://eirvah:eirvah-dev-password@rabbitmq:5672/')
    async with conn:
        channel = await conn.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=req.model_dump_json().encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key='eirvah.actuation.requests',
        )
        print('published:', req.model_dump_json())

asyncio.run(main())
"
```

**Not numeric** (malformed value):

```bash
kubectl exec -n eirvah-edge deploy/amqp-actuation-event-subscriber -- python3 -c "
import asyncio
from datetime import datetime, timezone
import aio_pika
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id

async def main():
    req = ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester='decision-agent-stub',
        target_uns_topic='uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature',
        requested_value='not-a-number',
        value_type='double',
        reason='demo: actuation failure (not_numeric)',
        requested_at=datetime.now(timezone.utc),
    )
    conn = await aio_pika.connect_robust('amqp://eirvah:eirvah-dev-password@rabbitmq:5672/')
    async with conn:
        channel = await conn.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=req.model_dump_json().encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key='eirvah.actuation.requests',
        )
        print('published:', req.model_dump_json())

asyncio.run(main())
"
```

**Expect:** "Actuation Rejected" increments the matching reason
(`out_of_range` / `not_allowlisted` / `not_numeric`) on the dashboard. The
OPC UA setpoint is untouched by all three — nothing gets written on a
rejection.

## 3. Disturbance test — RUN ONCE ONLY

This is the full autonomous loop, not a manual injection: it pushes the
simulated process out of range, and lets the edge close the loop on its
own — temperature crosses threshold → `decision-agent-stub` detects a
sustained breach and fires a real actuation request → validated → written
back — with no manual publish involved.

**Do not repeat this one.** It's a single step-change to the simulator's
setpoint; running it again mid-demo just re-triggers the same disturbance
on top of whatever state it's already in and muddies the timeline. If you
need to redo the demo, use the reset steps at the top of this file first,
then run this once.

Confirm it's safe to start (should read comfortably under 26°C — if it
doesn't, the environment hasn't settled from a previous run; use the
reset steps at the top of this file and wait):

```bash
kubectl exec -n eirvah-edge deploy/opcua-simulator -- python3 -c "
import urllib.request
for line in urllib.request.urlopen('http://localhost:8080/metrics').read().decode().splitlines():
    if line.startswith('eirvah_simulator_temperature_celsius{'):
        print(line)
"
```

```bash
kubectl exec -n eirvah-edge deploy/opcua-simulator -- python3 -c "
import asyncio
from asyncua import Client

async def main():
    endpoint = 'opc.tcp://localhost:4840/eirvah/simulator'
    async with Client(url=endpoint) as client:
        ns_idx = await client.get_namespace_index('https://eirvah.uniza/zilina/factory1')
        node = await client.nodes.objects.get_child([f'{ns_idx}:bottler', f'{ns_idx}:SetpointTemperature'])
        current = float(await node.read_value())
        await node.write_value(30.0)
        print(f'disturbance written: {current:.2f}C -> 30.00C')

asyncio.run(main())
"
```

**Timeline to narrate while it runs:**

- **~0s** — command runs, setpoint jumps to 30°C.
- **~7s** — simulated temperature crosses the 26°C threshold.
- **~30-37s** — sustained breach; `decision-agent-stub` fires its own
  actuation request (watch it in real time:
  `kubectl -n eirvah-edge logs -f deploy/decision-agent-stub | grep actuation_request_emitted`).
- **Seconds later** — validator approves it, `actuation-signal-publisher`
  writes the setpoint back to `22.0`°C, closing the loop. "Actuation
  Approved" ticks up on the dashboard; the OPC UA setpoint settles back to
  `22.0`.
- **60s cooldown** — `decision-agent-stub` won't fire again for 60s even
  if still breaching, so it won't double-trigger while you're talking.
