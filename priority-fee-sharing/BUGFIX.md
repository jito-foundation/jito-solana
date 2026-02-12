# Priority Fee Sharing `getBlock` Parse Failure (v3.1.8)

## Summary

`priority-fee-sharing` intermittently fails on `getBlock` with:

`invalid type: unit variant, expected newtype variant`

This is a response-deserialization compatibility issue, not an RPC availability issue.

## Root Cause

In `priority-fee-sharing/src/lib.rs`, `get_rewards_safe()` calls:

```rust
rpc_client.get_block_with_config(slot, RpcBlockConfig {
    max_supported_transaction_version: Some(0),
    rewards: Some(true),
    commitment: Some(commitment),
    ..RpcBlockConfig::default()
})
```

`RpcBlockConfig::default()` leaves `transaction_details` as `None`.

On RPC, `None` means "use server default", and server default is `TransactionDetails::Full`:

- `rpc/src/rpc.rs`: `transaction_details: config.transaction_details.unwrap_or_default()`
- `transaction-status-client-types/src/lib.rs`: `impl Default for TransactionDetails { Full }`

So the client receives full transaction metadata and tries to deserialize transaction errors for all txs in the block.

The parse failure happens when a transaction error variant is encoded differently across versions (notably `BorshIoError` unit vs newtype format).

## Why This Breaks Priority Fee Sharing

Priority fee sharing only needs `block.rewards` and filters for `RewardType::Fee`.  
It does not need transaction metadata (`meta.err`, logs, inner instructions, etc.).

Requesting full tx details introduces unnecessary deserialization surface and causes the crash.

## Minimal, Correct Fix

Set `transaction_details` explicitly to `None` (the enum variant), not `None` (the option), so RPC does not include tx metadata:

```rust
use solana_transaction_status_client_types::TransactionDetails;

rpc_client.get_block_with_config(slot, RpcBlockConfig {
    max_supported_transaction_version: Some(0),
    transaction_details: Some(TransactionDetails::None),
    rewards: Some(true),
    commitment: Some(commitment),
    ..RpcBlockConfig::default()
})
```

Equivalent helper form (also valid):

```rust
RpcBlockConfig {
    max_supported_transaction_version: Some(0),
    rewards: Some(true),
    commitment: Some(commitment),
    ..RpcBlockConfig::rewards_only()
}
```

## Expected Behavior After Fix

- `getBlock` no longer attempts to deserialize full transaction error payloads.
- Blocks containing problematic tx error variants no longer fail parsing.
- Priority fee extraction from rewards continues to work.
- Lower response size and lower parse overhead.

## Verification Steps

1. Build and run the tool with the same RPC endpoint and previously failing slots.
2. Confirm logs no longer show:
   - `invalid type: unit variant, expected newtype variant`
3. Confirm rewards are still processed and fee records are updated.
4. Optional sanity check:
   - Query `getBlock` with `transactionDetails: "none", rewards: true` and verify reward entries remain present.

## Alternative Fixes (Higher Risk / More Work)

1. Upgrade transaction error dependencies to align formats across all involved crates.
2. Add compatibility deserializer shim for both legacy/new `InstructionError` encodings.

These are broader changes. For this service, explicitly requesting reward-only block data is the safest hotfix.
