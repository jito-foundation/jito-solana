# Jito scheduler bindings

This experimental scheduler runs block-production policy in a separate process.
The validator provides shared-memory ingress, bank checks, transaction execution,
and recording. It retains BAM authentication, networking, and tip signing.

Validator builds include the jito-scheduler binary. To build it separately, run
cargo build --release -p jito-scheduler from the repository root.

Start the validator with --jito-scheduler-bindings, then start the scheduler
against the same ledger:

    agave-validator ... --jito-scheduler-bindings
    jito-scheduler --ledger /path/to/ledger

The scheduler defaults to eight execution workers and four check workers.
Use --workers and --check-workers to change these counts. Both processes must
use compatible Jito protocol versions. The standard Agave bindings remain
available with --enable-scheduler-bindings and retain their BAM exclusion.

The first cut handles ordinary transactions, votes, BAM batches, and legacy
Block Engine bundles. It uses the validator's real check and execution workers.
Votes retain service while BAM is connected; ordinary non-vote TPU traffic does
not compete with BAM. Ordered batches use their complete account sets to prevent
conflicting batches from overtaking one another.

Atomic batches preserve all-or-nothing execution and rollback behavior. Execution
requests identify both slot and BankId. Atomic work waits until ParentReady;
existing non-atomic BAM behavior on a provisional bank is preserved. The validator
keeps tip-program upkeep under its existing signing and account-lock controls.

BAM ingress and replies carry a connection generation. An old generation cannot
be reused after a reconnect. Switching between internal and external scheduling
opens a fresh BAM stream. BundleStage finishes its current work before the
external scheduler takes over its receiver. BAM cutover waits for outstanding
legacy bundles to finish.

Both sides exchange liveness updates. A failed external session causes the
validator to stop and join its execution workers, then resume its internal
scheduler. The external executable reconnects with a fresh session after loss of
the validator. Outstanding outcomes are not invented or replayed as individual
transactions. A fresh BAM connection handles uncertain outcomes.

The protocol uses an explicit opt-in handshake and additional queues, leaving
the upstream Agave ABI unchanged. Input and result allocations have one owner at
a time. A successful completion returns validator-owned ingress and all detailed
result allocations; a failed queue write transfers no ownership.

## Validation

The package tests include queue negotiation, bounds and allocation ownership,
account conflicts, ordered dependencies, atomic readiness, bank replacement,
connection replacement, result backpressure, and an actual scheduler child
process communicating through a Unix socket and shared memory. Core tests
exercise real Bank execution and the BAM response adapter.

Run the relevant suites from the repository root:

    cargo test -p jito-scheduler-bindings --all-targets
    cargo test -p agave-scheduling-utils --features agave-unstable-api --lib
    cargo test -p jito-scheduler --all-targets
    cargo test -p solana-core --lib banking_stage
    cargo test -p solana-core --lib jito_scheduler
    cargo test -p solana-core --test bam_connection

The repository's normal Buildkite suite is also required before treating this
first cut as verified. Passing IPC tests alone does not establish validator or
cluster behavior.
