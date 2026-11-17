# Wolverine sends expired messages to RabbitMQ with a negative expiration

Wolverine's documentation says it discards an outgoing message whose `DeliverBy` time has passed. On RabbitMQ, it sends that message instead, with a negative `expiration` header. RabbitMQ rejects the publish and closes the sender channel, and other messages sent around the same time can be lost with it.

This repository is a single console program that shows the bug against a real broker.

## Run it

You need Docker and the .NET 10 SDK.

```sh
dotnet run
```

The program starts RabbitMQ 4.3.1 in a container (Testcontainers), starts a Wolverine host that sends to and listens on one queue, and runs three steps:

1. **Control.** Send a normal message. It arrives, so the listener works.
2. **Expired send.** Send a message whose `DeliverBy` is one second in the past, then a normal message (`bystander-1`).
3. **Expired scheduled send.** Schedule a message for two seconds from now with a `DeliverBy` one second from now, plus a normal message scheduled for the same time (`bystander-2`).

## Expected

From [`docs/guide/messaging/expiration.md`](https://github.com/JasperFx/wolverine/blob/main/docs/guide/messaging/expiration.md):

> 2. Wolverine will also discard outgoing messages that are past their `DeliverBy` time

Both expired messages are discarded, the control and both bystanders arrive, and the broker logs no errors.

## Actual

Output from one run:

```text
Control: send a normal message before anything expires.
  control was received.
Scenario 1: send a message whose DeliverBy has passed, then a normal message.
warn: Wolverine.RabbitMQ.Internal.RabbitMqSender[0] Unexpected channel shutdown for Rabbit MQ (RabbitMqSender: rabbitmq://queue/repro). Wolverine will attempt to restart...
  bystander-1 was NOT received within 15s.
Scenario 2: schedule a message for 2s from now that expires 1s from now, plus a normal one at 2s.
warn: Wolverine.RabbitMQ.Internal.RabbitMqSender[0] Unexpected channel shutdown for Rabbit MQ (RabbitMqSender: rabbitmq://queue/repro). Wolverine will attempt to restart...
  bystander-2 was NOT received within 15s.
Results
  Received by the listener: [control]
  RabbitMQ 'invalid expiration' errors: 2
    ... [error] <0.650.0> operation basic.publish caused a channel exception precondition_failed: invalid expiration '-1001': {value_negative,-1001}
    ... [error] <0.657.0> operation basic.publish caused a channel exception precondition_failed: invalid expiration '-1014': {value_negative,-1014}
```

In four runs, both expired messages were published with a negative expiration and closed the channel every time. `bystander-1` was lost in all four runs. `bystander-2` was lost in two of the four. Whether a bystander is lost depends on whether it is in the same publish as the expired message.

## Where it comes from

These references are to WolverineFx 6.64.2, at commit [`db2a42c`](https://github.com/JasperFx/wolverine/tree/db2a42c5ae9900c507df373561d9575fb8891d54), which is the commit the package's PDB points to.

- `RabbitMqEnvelopeMapper` sets `props.Expiration` to `DeliverBy - DateTimeOffset.UtcNow` in milliseconds and does not clamp it.
- The send path does not check `Envelope.IsExpired()`. This covers `SendingAgent.EnqueueOutgoingAsync` and `StoreAndForwardAsync`, and `ScheduledSendEnvelopeHandler`, which releases durable scheduled messages through `MessageContext.ForwardScheduledEnvelopeAsync`.
- `IsExpired()` is checked on receive and on the outbox failure and recovery paths (`DurableSendingAgent`, `RecoverOutgoingMessagesCommand`, `RecoverStaleOutgoingStep`), which discard the envelope. The first send is the path where an expired envelope reaches the mapper.

The custom mapper sample in `docs/guide/messaging/transports/rabbitmq/interoperability.md` has the same unclamped computation.

## How we found it

We schedule retries over RabbitMQ with a durable outbox, and each retry had a `DeliverBy` at the end of its retry window. Wolverine releases durable scheduled messages on its `ScheduledJobPollingTime` poll, every 5 seconds by default. A retry due less than one poll interval before its `DeliverBy` was often released after it, and that lost the retry and other messages in the same publish. In our tests, about 11% of retry sequences failed this way. Removing `DeliverWithin` removed every failure: 9 of 40 failed before the change, 0 of 40 after.

This program uses in-memory scheduling and a `DeliverBy` that is earlier than the `ScheduledTime`, so the bug shows up on every run. Both cases reach the same mapper.

## Versions

- WolverineFx, WolverineFx.RabbitMQ and WolverineFx.RuntimeCompilation 6.64.2
- RabbitMQ 4.3.1 (`rabbitmq:4.3.1` image)
- .NET 10
