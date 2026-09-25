# Provision asynchronous example v0.1.0

This is the first release of the framework-neutral asynchronous example application used to exercise Provision's Queue, Worker, Task, and Schedule deployment path.

It includes a deterministic RabbitMQ-publishing Task and a gated persistent Worker. The Worker exposes structured state and processing evidence, supports controlled hold, retry, and rejection behavior, and demonstrates application-level idempotency for repeated at-least-once delivery. It does not claim exactly-once execution.

Release assets:

- `provision-example-async-task-linux-amd64.tar.gz`
- `provision-example-async-worker-linux-amd64.tar.gz`
- `SHA256SUMS`

Verify the archives against `SHA256SUMS` before referencing their digests in a Provision Revision.
