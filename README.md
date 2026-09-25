# Provision asynchronous example

This repository contains a deliberately small, framework-neutral asynchronous application used to exercise [Provision](https://github.com/datashaman/provision) deployments. It is an example workload with its own release lifecycle, not part of the Provision product distribution.

The application has two native executables:

- `provision-example-async-task` publishes deterministic messages to an existing RabbitMQ quorum queue and reports a message as accepted only after a publisher confirmation;
- `provision-example-async-worker` is a persistent, gated consumer which reports its revision and gate state, acknowledges successful work, deliberately holds or fails selected messages, and records structured processing evidence.

It has no Laravel or PHP dependency. RabbitMQ is external to the application: the example never creates, replaces, or configures a queue.

## Message and delivery behavior

Each message identity is derived from the Task revision, stable Task Invocation identity, and one-based sequence number. Retrying the same invocation publishes the same identities. A behavior change does not create a different identity.

The Worker maintains a durable file ledger. The first successful processing of a message writes one immutable effect record; repeated delivery with the same identity is acknowledged and recorded as `duplicate_ignored` without applying the effect again. This demonstrates application-level idempotency under at-least-once delivery. It does not claim exactly-once execution.

The Task's `--behavior` option selects deterministic acceptance behavior:

- `process` records the effect and acknowledges the delivery;
- `hold` keeps one delivery in flight until its release file appears, or safely requeues it when the Worker stops;
- `fail-once` requeues the first attempt and processes the next attempt with the same identity;
- `reject` rejects the delivery without requeueing, allowing the queue's declared dead-letter policy to apply.

## Runtime interface

The Task reads the AMQP URL from a credential file so its value is not present in process arguments:

```sh
provision-example-async-task \
  --broker-url-file /run/credentials/rabbitmq-url \
  --queue provision-example \
  --revision task-v1 \
  --invocation schedule-example-2026-09-25T10:00:00Z \
  --count 3 \
  --behavior process
```

It writes one JSON confirmation event for every broker-confirmed message followed by a `publish_complete` summary. If a publish fails or is not confirmed, it exits unsuccessfully without an acceptance event for that message.

The Worker starts gated unless its gate file contains exactly `open`:

```sh
provision-example-async-worker \
  --broker-url-file /run/credentials/rabbitmq-url \
  --queue provision-example \
  --revision worker-v1 \
  --gate-file /run/provision-example-async/worker-v1/gate \
  --state-file /run/provision-example-async/worker-v1/state.json \
  --evidence-file /var/lib/provision-example-async/evidence.jsonl \
  --ledger-dir /var/lib/provision-example-async/ledger \
  --hold-dir /run/provision-example-async/holds
```

The atomic state file reports the Worker revision, queue, connectivity, gate, consumer, and in-flight-message state. Processing evidence is append-only JSON Lines. To release a held message, create a file in the hold directory named with the SHA-256 of its message identity plus `.release`; its exact content is the message identity followed by a newline. Provision acceptance tooling can derive and create this control without changing the application process.

Closing the gate prevents new deliveries. A delivery already in flight may finish. Stopping the Worker while a delivery is held negatively acknowledges it with requeue enabled so another eligible Worker can process it.

## Build and release assets

Run the tests and build reproducible Linux archives:

```sh
go test ./...
./build.sh amd64
```

The proposed `v0.1.0` release contains exactly:

- `provision-example-async-task-linux-amd64.tar.gz`;
- `provision-example-async-worker-linux-amd64.tar.gz`;
- `SHA256SUMS`.

Each archive contains one statically linked executable with normalized archive metadata. `SHA256SUMS` supplies the immutable digests consumed by Provision Revision manifests.

Releases follow semantic versioning. The repository name, first tag, release title, description, and assets are reviewed before public publication.

Licensed under the [MIT License](LICENSE).
