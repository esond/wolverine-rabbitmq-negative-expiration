# Wolverine sends expired messages to RabbitMQ with a negative expiration

Wolverine's documentation says it discards an outgoing message whose `DeliverBy` time has passed. On RabbitMQ, it sends that message instead, with a negative `expiration` header. RabbitMQ rejects the publish and closes the sender channel, and other messages in the same publish are lost with it.

A message can pass its `DeliverBy` without anyone setting a strange value. Durable scheduled messages are released on the `ScheduledJobPollingTime` poll, every 5 seconds by default. A message scheduled 1 second out with `DeliverWithin = 4 seconds` has 3 seconds of slack after its scheduled time. If the next poll comes later than that, the message goes out past its `DeliverBy`. This repository shows that happening against a real broker and a real message store.

## Run it

You need Docker and the .NET 10 SDK.

```sh
dotnet run
```

The program starts RabbitMQ 4.3.1 and PostgreSQL 17 in containers (Testcontainers). It then starts a Wolverine host with a durable outbox and inbox on PostgreSQL, which sends to and listens on one RabbitMQ queue. `ScheduledJobPollingTime` is left at its default.

1. **Control.** Send a normal message. It arrives, so the listener works.
2. **Ten trials.** Each trial schedules two messages for 1 second from now:
   - `timed-N` has `DeliverWithin = 4 seconds`.
   - `bystander-N` has no expiry.

## Expected

From [`docs/guide/messaging/expiration.md`](https://github.com/JasperFx/wolverine/blob/main/docs/guide/messaging/expiration.md):

> 2. Wolverine will also discard outgoing messages that are past their `DeliverBy` time

A timed message released after its `DeliverBy` is discarded. Every bystander arrives, and the broker logs no errors.

## Actual

Output from one run:

```text
control: received

trial  timed         bystander     broker 'invalid expiration' errors
    1  received      received      0
    2  NOT received  NOT received  1
    3  received      received      0
    4  NOT received  received      1
    5  received      received      0
    6  NOT received  received      1
    7  received      received      0
    8  NOT received  NOT received  1
    9  received      received      0
   10  NOT received  NOT received  1

Results
  timed messages not received:     5 of 10
  bystanders not received:         3 of 10
  RabbitMQ 'invalid expiration':   5
    ... [error] <0.650.0> operation basic.publish caused a channel exception precondition_failed: invalid expiration '-702': {value_negative,-702}
    ... [error] <0.657.0> operation basic.publish caused a channel exception precondition_failed: invalid expiration '-779': {value_negative,-779}
    ...
```

Each broker error comes with this warning from Wolverine:

```text
warn: Wolverine.RabbitMQ.Internal.RabbitMqSender[0] Unexpected channel shutdown for Rabbit MQ (RabbitMqSender: rabbitmq://queue/repro). Wolverine will attempt to restart...
```

In two runs, 5 of 10 timed messages were released late each time. Every late one was published with a negative expiration (-364 ms to -903 ms) instead of being discarded, and closed the channel. Bystanders with no expiry were lost in 1 of 10 trials in the first run and 3 of 10 in the second. Whether a bystander is lost depends on whether it goes out in the same publish as the late message. Whether a trial is late depends on where its scheduled time falls relative to the poll.

## Where it comes from

These references are to WolverineFx 6.64.2, at commit [`db2a42c`](https://github.com/JasperFx/wolverine/tree/db2a42c5ae9900c507df373561d9575fb8891d54), which is the commit the package's PDB points to.

- `ScheduledSendEnvelopeHandler.HandleAsync` releases a durable scheduled message through `MessageContext.ForwardScheduledEnvelopeAsync`. Neither checks `Envelope.IsExpired()`.
- The send path does not check it either: `SendingAgent.EnqueueOutgoingAsync` and `StoreAndForwardAsync`.
- `RabbitMqEnvelopeMapper` sets `props.Expiration` to `DeliverBy - DateTimeOffset.UtcNow` in milliseconds and does not clamp it.
- `IsExpired()` is checked on receive and on the outbox failure and recovery paths (`DurableSendingAgent`, `RecoverOutgoingMessagesCommand`, `RecoverStaleOutgoingStep`), which discard the envelope. A first send, including the release of a scheduled message, is the path where an expired envelope reaches the mapper.

The custom mapper sample in `docs/guide/messaging/transports/rabbitmq/interoperability.md` has the same unclamped computation.

## How we found it

We schedule retries over RabbitMQ with a durable outbox. Each retry had a `DeliverBy` at the end of its retry window, so the last retry in a window often had only a few seconds of slack. About 11% of retry sequences lost a retry this way, and the closed channel took other retries with it. Removing `DeliverWithin` removed every failure: 9 of 40 retry sequences failed before the change, 0 of 40 after.

## Versions

- WolverineFx, WolverineFx.RabbitMQ, WolverineFx.Postgresql and WolverineFx.RuntimeCompilation 6.64.2
- RabbitMQ 4.3.1 (`rabbitmq:4.3.1` image)
- PostgreSQL 17 (`postgres:17` image)
- .NET 10
