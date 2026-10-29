# Share status transactions and bound BAM parsing backlog

Date: **2026-10-28**

- Branch: `bam/share-status-and-bound-parser`
- Base: `256fbf9b601a98551636a2a394158067014f47b3` (`master` when the branch was created)
- Status: implementation and regression tests complete. No remote PR created.

## PR summary

Transaction views are independently reconstructed into owned transactions for
account notifications and transaction status reporting. This change prepares the
status transactions once, lets account notifications borrow them during bank
commit, and then moves them into the status message. When both consumers are
enabled, a processed BAM transaction needs two owned reconstructions instead of
three, including the separate representation required for recording.

The BAM parser also currently feeds an unbounded parsed-batch queue. This change
caps that queue at 1,024 batches and reports `ContainerFull` for rejected batches
through the existing outbound response mechanism. Enqueueing remains nonblocking,
so shutdown can stop and join the parsing thread even when the queue is full.

## Implementation

### Share account-notification and status transactions

- [Committer](core/src/banking_stage/committer.rs) prepares a
  `Vec<SanitizedTransaction>` when transaction status reporting is enabled. It
  preserves the complete input order, including transactions that failed processing.
- [Bank](runtime/src/bank.rs) adds
  `commit_transactions_with_transaction_refs(..., Option<&[SanitizedTransaction]>)`.
  The helper checks the supplied slice length; callers must supply the corresponding
  transaction at each input position.
- When an account update notifier is enabled, the bank borrows supplied transactions
  through `Cow::Borrowed`. Without supplied transactions it uses the existing
  conversion path. The original `Bank::commit_transactions` entry point delegates
  with `None`, preserving existing callers.
- After synchronous account notifications finish, the committer moves the prepared
  vector into the transaction status message. That handoff performs no deep clone.

This uses shared banking APIs, so the implementation also applies to other
transaction-view consumers using the committer. The duplicate-reconstruction
saving requires both account notifications and status reporting to be enabled.
Geyser and status message formats remain compatible.

### Bound parsed BAM batches

[BamReceiveAndBuffer](core/src/banking_stage/transaction_scheduler/bam_receive_and_buffer.rs)
uses a bounded channel with `PARSED_BATCH_CHANNEL_CAPACITY = 1024`.

- Successful enqueue transfers the existing transaction views and counts their
  transactions as buffered.
- A full queue returns a `ContainerFull` result with the rejected batch's sequence
  ID, increments the batch-level capacity rejection counter, and releases the batch.
- A disconnected parsed receiver stops the parsing thread.
- Accepted batches retain FIFO order. Queue sends never wait for capacity.

The cap covers queued parsed batches: at five transactions per batch, at most
5,120 transaction views. Shared `Bytes` can retain larger backing allocations, so
this is not a global memory-byte budget. Raw ingress, the parser's active burst,
and the scheduler container remain separate sources of outstanding work. Outbound
results retain the existing best-effort `try_send` behavior if their channel is full.

## Diff size

Source changes relative to the base, excluding this document and the pre-existing
protobuf submodule checkout difference:

| Scope | Added | Removed | Net |
|---|---:|---:|---:|
| Share notification/status transactions: production | 48 | 15 | +33 |
| Bound parsed queue: production | 32 | 22 | +10 |
| Regression tests | 182 | 0 | +182 |
| **Total source** | **262** | **37** | **+225** |

Changed source files:

- `core/src/banking_stage/committer.rs`
- `core/src/banking_stage/transaction_scheduler/bam_receive_and_buffer.rs`
- `runtime/src/bank.rs`
- `runtime/src/bank/tests.rs`

## Validation

**Library compilation and 29 focused tests passed.** Cargo package names were
verified in their manifests before running package-scoped commands.

| Check | Result | Environment |
|---|---|---|
| `solana-core` library check with `agave-unstable-api` | Passed | Current working tree |
| `test_commit_transactions_with_transaction_refs` | 4 passed | Current working tree |
| `bam_receive_and_buffer` tests | 21 passed | Isolated checkout with recorded protobuf revisions |
| Core test binary, `test_write_persist` filter | 4 passed | Same isolated checkout |
| `git diff --check` | Passed | Current working tree |
| Stable `rustfmt` check on changed Rust files | Passed | Nightly-only repository formatting options were ignored |

The four new runtime cases cover every combination of notifier enabled/disabled
and prepared transactions supplied/absent. A failed transaction precedes a
successful one, verifying positional correspondence. Assertions check committed
balances and, when prepared transactions are supplied, the exact transaction
object address received by account notifications.

The new parser regression fills the queue, verifies the overflow batch's sequence
ID and `ContainerFull` reason, drops the receiver while the queue is full and raw
ingress remains connected, and checks that all accepted batches retain FIFO order.

The four existing persistence cases cover transaction status in the consumer and
replay paths, persisted loaded addresses, and invalidated preadmitted lookups.

### Commands

```sh
CARGO_BUILD_JOBS=8 cargo check --locked --offline \
  -p solana-core --lib --features agave-unstable-api

CARGO_BUILD_JOBS=8 cargo test --locked --offline \
  -p solana-runtime --lib test_commit_transactions_with_transaction_refs

# Requires the recorded protobuf revisions; run in a matching isolated checkout.
CARGO_BUILD_JOBS=8 cargo test --locked --offline \
  -p solana-core --lib bam_receive_and_buffer -- --test-threads=4
```

The persistence checks invoked the already-built core library test executable
with `test_write_persist --test-threads=4`. The equivalent Cargo command is:

```sh
cargo test --locked --offline -p solana-core --lib \
  test_write_persist -- --test-threads=4
```

Local validation logs are `/tmp/bam-flow-check.log`,
`/tmp/bam-flow-runtime-tests.log`, `/tmp/bam-flow-parser-tests.log`, and
`/tmp/bam-flow-status-tests.log`. These temporary logs are not part of the PR.

### Existing protobuf checkout mismatch

The current working tree has BAM protobufs checked out at
`9d8f13df8db085099bfd7ebe3a53a4ea0df76762`, while the base records
`642c49ae6da23dcbc6c0c621288d4479799a8910`.

The current checkout lacks `MultipleAtomicTxnBatch.mss_padding`, which an existing
test in `scheduler_controller.rs` references. Consequently, core library tests
cannot compile directly against that checkout. This mismatch predates these changes.

Core tests were instead built and run in a temporary detached worktree containing
the same four changed source files and the repository's recorded protobuf
revisions. The changed files were checked byte-for-byte against the branch working
tree before removing that temporary worktree. The user's original protobuf
checkout was preserved and is excluded from the proposed PR changes.

## Review notes

- Independent subagent reviews found no correctness blockers in either change.
- Status transaction preparation now contributes to `commit_us`; previously that
  work contributed to `find_and_send_votes_us`. Account for this shift when
  comparing individual stage timings.
- The 1,024-batch queue capacity is an initial choice, without workload tuning.
  Representative burst and sustained-overload measurements should assess it.
- Allocation savings follow from the ownership changes; allocation counts,
  throughput, latency, and retained-memory improvements have not been benchmarked.
- Recording still constructs `VersionedTransaction`s. Transport buffering and XDP
  payload placement also have separate copy costs; this PR does not establish
  end-to-end zero-copy execution.
