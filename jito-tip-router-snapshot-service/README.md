# Tip-Router Snapshot Service

This service consumes validator bank notifications to retain the first observed epoch-boundary
bank, then generates and writes the corresponding snapshot artifact asynchronously. It does not
perform fork reconciliation or wait for the bank to become rooted: the first boundary candidate
observed for an epoch wins.

## Required account index

Validators that enable this service must also configure a ProgramId account index that covers the
stake program. The recommended configuration limits the index to stake accounts:

```text
--account-index program-id \
--account-index-include-key Stake11111111111111111111111111111111111111
```

Startup fails with an actionable configuration error if the ProgramId index is absent, an include
filter omits the stake program, or an exclusion filter explicitly excludes it. An unfiltered
ProgramId index and an exclusion filter that does not exclude the stake program are also accepted,
but an unfiltered index tracks accounts for every program and therefore has a greater memory cost
than the recommended stake-only index.

Changing these flags requires a validator restart. AccountsDB builds the configured secondary index
while loading accounts during startup, so operators should allow for the additional startup work
before the snapshot service becomes available.

## Bank notification forwarding tradeoff

The notification pipeline uses direct producer-side fanout:

```text
Replay/Votor notification producers
              |
              v
   BankNotificationBroadcaster
        |               |
        v               v
  RPC tracker     Tip-router service
```

An alternative would require RPC to be enabled and forward notifications from the RPC optimistic
bank tracker to this service. That alternative has a smaller immediate code and Rust-interface
surface: it does not need a generic broadcaster, and `BankNotificationSenderConfig` can retain a
single Crossbeam sender.

We deliberately use direct fanout instead because RPC forwarding would:

- Require voting validators to configure and run RPC for an internal extension.
- Couple this service's lifecycle and progress to the RPC worker.
- Delay candidate delivery behind older notifications and work queued in the RPC tracker.
- Turn the RPC tracker into an event bus for unrelated validator extensions.
- Require more RPC-specific forwarding parameters when another raw-bank consumer is added.

Direct fanout costs a small amount of additional internal surface area. It introduces the
`BankNotificationBroadcaster`, changes `BankNotificationSenderConfig.sender` from a single channel
sender to the broadcaster, and creates RPC and tip-router channels independently in `Validator`.
The service only needs `Frozen` notifications; rooted-chain notifications remain enabled for Geyser
consumers only.

## Performance considerations

The producer path performs one mutex acquisition and one unbounded-channel send per subscriber for
each bank notification. Cloning a frozen-bank notification only clones an `Arc<Bank>`. These events
occur per bank transition rather than per transaction, so the expected CPU cost is small, while
direct delivery avoids RPC queueing latency.

The more important resource consideration is retention: a queued `Frozen(Arc<Bank>)` keeps that bank
alive. The tip-router consumer must continue draining notifications promptly so an unbounded queue
does not retain an excessive number of banks. On the first epoch boundary, it spawns one worker that
collects metadata and atomically writes the artifact. The notification thread tracks only the last
claimed epoch and that one active worker; all expensive work remains off the Replay, Votor, and RPC
threads.

The selected tradeoff is therefore a slightly larger internal notification abstraction in exchange
for lower candidate latency, RPC-independent operation, and a reusable boundary for validator
extensions.
