# Tip-Router Snapshot Service

This service consumes validator bank notifications to retain the first observed epoch-boundary
bank, then generates and writes a temporary snapshot artifact asynchronously. It publishes the
artifact after observing a root at or above the candidate slot. It does not perform fork
reconciliation: the first boundary candidate observed for an epoch wins.

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

The notification pipeline uses direct producer-side fanout, with each subscriber declaring what it
wants to receive:

```text
Replay/Votor notification producers
              |
              v
   BankNotificationBroadcaster
        |               |
        | unfiltered    | boundary candidates only
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
The service needs epoch-boundary `Frozen` notifications and `NewRootedChain` notifications.
Rooted-chain notifications are enabled when either Geyser or the Tip Router snapshot service is
configured.

## Subscriber filtering

Each subscriber is a `BankNotificationSender`: a channel plus an optional `NotificationFilter`.
`Validator` registers the RPC tracker unfiltered, so RPC continues to receive every notification
variant, and registers this service with `TipRouterEpochBoundaryFilter`.

The broadcaster evaluates a subscriber's filter before cloning the notification, so a rejected
notification is never cloned into or queued on that subscriber's channel. Rejection is not a
delivery failure: a subscriber that filters a notification out is still considered connected, and a
notification that every subscriber rejects is a no-op rather than a broadcast error. The broadcaster
only reports an error when no connected subscriber remains.

`TipRouterEpochBoundaryFilter` performs coarse, stateless classification. It accepts every
`NewRootedChain` notification and a `Frozen` bank whose epoch is greater than its parent's,
deriving the parent's epoch from `parent_slot` so that a boundary is still recognized when the
first slots of an epoch are skipped. It rejects `NewRootBank` notifications.

Stateful policy stays in the service, which still resolves the parent bank, rejects an epoch that
has already been claimed, and rejects a candidate while an artifact worker is running. Competing
forks can therefore still deliver more than one boundary candidate for an epoch, preserving the
first-observed-candidate-wins behavior described above.

## Performance considerations

The subscriber set is fixed when `Validator` builds the broadcaster and is held behind an
`Arc<[BankNotificationSender]>`, so the producer path takes no lock. For each bank notification it
iterates that slice, evaluates each subscriber's filter, and performs one unbounded-channel send per
accepting subscriber. Cloning a frozen-bank notification only clones an `Arc<Bank>`. Filters run
synchronously on the Replay and Votor threads, so they must stay pure, non-blocking and cheap;
`TipRouterEpochBoundaryFilter` reads only fields already resident on the bank. These events occur
per bank transition rather than per transaction, so the expected CPU cost is small, while direct
delivery avoids RPC queueing latency.

The more important resource consideration is retention: a queued `Frozen(Arc<Bank>)` keeps that bank
alive. Filtering on the producer side means ordinary frozen banks and unrelated notification
variants are never queued for this service at all, so retention is bounded by the rate of boundary
candidates rather than the rate of frozen banks. The consumer must still drain promptly, since
competing forks can produce several candidates for one epoch. On the first epoch boundary, it spawns
one worker that collects metadata and writes a uniquely named temporary artifact. Rooted chains now
reach the snapshot service through a dedicated reconciliation handler. That handler is currently
stubbed, so rooted-chain publication policy will be added in the next implementation stage. All
expensive work remains off the Replay, Votor, and RPC threads.

The selected tradeoff is therefore a slightly larger internal notification abstraction in exchange
for lower candidate latency, RPC-independent operation, and a reusable boundary for validator
extensions.
