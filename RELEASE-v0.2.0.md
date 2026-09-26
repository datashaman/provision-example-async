# Provision asynchronous example v0.2.0

This release adds an informational `--version` command to the Worker binary.
Provision issue #41 uses the resulting distinct immutable Worker Artifact to
exercise a second, gated Worker Generation while retaining the v0.1.1 Task
Artifact.

The release contains:

- `provision-example-async-task-linux-amd64.tar.gz`
- `provision-example-async-worker-linux-amd64.tar.gz`
- `SHA256SUMS`

The Task archive is byte-identical to v0.1.1. The Worker archive is new:

- Task: `sha256:ce1dc7e13900742b3139beb521e9bcd30005470370462b9aa01383f078c999e5`
- Worker: `sha256:14caeac0dbdaff68a2644798b0a1b2549f82342bedbf79d9b8da4625e2080d95`

This is an example-workload release, not a Provision product release.
