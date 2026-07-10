---
title: Transaction simulation IPC
sidebar_label: Transaction simulation IPC
---

Agave can expose a local, binary transaction simulation service for a colocated
shred decoder such as Aperture. This avoids the HTTP, JSON, base64, and RPC
scheduling overhead of `simulateTransaction`.

Start the validator with:

```bash
agave-validator \
  --transaction-simulation-ipc /run/agave/transaction-simulation.sock \
  --transaction-simulation-ipc-workers 4 \
  <other validator arguments>
```

The socket is created with mode `0660`. Put Agave and the client in a shared
group when they run as different users. The service removes a stale socket at
startup, refuses to replace an active socket, and removes its socket on clean
shutdown.

## Data flow

1. Aperture reconstructs a `VersionedTransaction` from incoming shreds.
2. It writes a length-delimited protobuf `SimulationRequest` to the persistent
   Unix stream.
3. A bounded Agave worker pool sanitizes the transaction and calls the Bank
   simulation path directly.
4. Agave writes a `SimulationResult` containing status, error, logs, compute
   units, fee, return data, selected bank slot, and timing.

Requests carry a `request_id`, so a client can pipeline many transactions on one
connection. Results can arrive out of order. The wire schema is
`rpc/proto/transaction_simulation_ipc.proto`: every frame is a four-byte
big-endian payload length followed by an `IpcMessage` protobuf payload.

The server bounds both its global work queue and each connection's response
queue. A full work queue returns `SIMULATION_STATUS_SERVER_OVERLOADED`; a client
that stops reading is disconnected. Neither condition blocks replay or banking.

## Bank semantics

`SimulationRequest.bank_slot` selects an exact bank when present. That bank must
still exist in `BankForks` and must be frozen. When the field is omitted, Agave
uses its highest frozen bank. The response always reports the bank slot that was
actually used.

For the earliest shred-level prediction, omit `bank_slot` or name the decoded
slot's frozen parent. This accurately uses the node's account, program, sysvar,
feature, and blockhash state, but it cannot include writes from earlier
transactions in the current slot until that slot has executed and frozen. A
simulation result is therefore a prediction tied to `bank_slot`, not a final
confirmation that the fork will land.

`sig_verify=false` skips signature verification while retaining transaction
sanitization, ALT loading, and runtime execution. This is appropriate when the
upstream shred path already verifies signatures. Set it to `true` when the IPC
service is the trust boundary.

## Aperture client

The Aperture checkout provides `TransactionSimulationClient` in
`src/transaction_simulation_ipc.rs`. It uses the same schema, serializes the
decoded `VersionedTransaction`, multiplexes concurrent calls over one Unix
connection, and correlates out-of-order results by `request_id`.
