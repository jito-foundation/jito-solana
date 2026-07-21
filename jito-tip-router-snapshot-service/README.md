# Tip-Router Snapshot Service

This service consumes validator bank notifications so it can retain an epoch-boundary candidate
bank early, compute stake metadata from its voting state, and later verify that the exact bank
became rooted.

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
The exact-root requirement independently changes `NewRootedChain` from slots to
`RootedBankIdentity { slot, bank_hash }`; requiring RPC would not remove that change because a slot
alone cannot distinguish competing banks at the same slot.

## Performance considerations

The producer path performs one mutex acquisition and one unbounded-channel send per subscriber for
each bank notification. Cloning a frozen-bank notification only clones an `Arc<Bank>`. Root-chain
notifications also copy their identity vector and read already-computed bank hashes before the root
bank is squashed. These events occur per bank transition rather than per transaction, so the
expected CPU cost is small, while direct delivery avoids RPC queueing latency.

The more important resource consideration is retention: a queued `Frozen(Arc<Bank>)` keeps that bank
alive. The tip-router consumer must continue draining notifications promptly so an unbounded queue
does not retain an excessive number of banks. Expensive metadata computation remains off the
Replay, Votor, and RPC threads.

The selected tradeoff is therefore a slightly larger internal notification abstraction in exchange
for lower candidate latency, RPC-independent operation, and a reusable boundary for validator
extensions.
