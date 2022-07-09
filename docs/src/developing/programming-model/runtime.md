---
title: "Runtime"
---

## Capability of Programs

The runtime only permits the owner program to debit the account or modify its
data. The program then defines additional rules for whether the client can
modify accounts it owns. In the case of the System program, it allows users to
transfer lamports by recognizing transaction signatures. If it sees the client
signed the transaction using the keypair's _private key_, it knows the client
authorized the token transfer.

In other words, the entire set of accounts owned by a given program can be
regarded as a key-value store, where a key is the account address and value is
program-specific arbitrary binary data. A program author can decide how to
manage the program's whole state, possibly as many accounts.

After the runtime executes each of the transaction's instructions, it uses the
account metadata to verify that the access policy was not violated. If a program
violates the policy, the runtime discards all account changes made by all
instructions in the transaction, and marks the transaction as failed.

### Policy

After a program has processed an instruction, the runtime verifies that the
program only performed operations it was permitted to, and that the results
adhere to the runtime policy.

The policy is as follows:

- Only the owner of the account may change owner.
  - And only if the account is writable.
  - And only if the account is not executable.
  - And only if the data is zero-initialized or empty.
- An account not assigned to the program cannot have its balance decrease.
- The balance of read-only and executable accounts may not change.
- Only the system program can change the size of the data and only if the system
  program owns the account.
- Only the owner may change account data.
  - And if the account is writable.
  - And if the account is not executable.
- Executable is one-way (false->true) and only the account owner may set it.
- No one can make modifications to the rent_epoch associated with this account.

## Compute Budget

To prevent abuse of computational resources, each transaction is allocated a
compute budget. The budget specifies a maximum number of compute units that a
transaction can consume, the costs associated with different types of operations
the transaction may perform, and operational bounds the transaction must adhere
to.  As the transaction is processed compute units are consumed by its
instruction's programs performing operations such as executing BPF instructions,
calling syscalls, etc... When the transaction consumes its entire budget, or
exceeds a bound such as attempting a call stack that is too deep, the runtime
halts the transaction processing and returns an error.

The following operations incur a compute cost:

- Executing BPF instructions
- Passing data between programs
- Calling system calls
  - logging
  - creating program addresses
  - cross-program invocations
  - ...

For cross-program invocations, the instructions invoked inherit the budget of
their parent. If an invoked instruction consumes the transactions remaining
budget, or exceeds a bound, the entire invocation chain and the top level
transaction processing are halted.

The current [compute
budget](https://github.com/solana-labs/solana/blob/090e11210aa7222d8295610a6ccac4acda711bb9/program-runtime/src/compute_budget.rs#L26-L87)

can be found in the Solana Program Runtime.

For example, if the current budget is:

```rust
max_units: 1,400,000,
log_u64_units: 100,
create_program address units: 1500,
invoke_units: 1000,
max_invoke_depth: 4,
max_call_depth: 64,
stack_frame_size: 4096,
log_pubkey_units: 100,
...
```

Then the transaction

- Could execute 1,400,000 BPF instructions, if it did nothing else.
- Cannot exceed 4k of stack usage.
- Cannot exceed a BPF call depth of 64.
- Cannot exceed 4 levels of cross-program invocations.

Since the compute budget is consumed incrementally as the transaction executes,
the total budget consumption will be a combination of the various costs of the
operations it performs.

At runtime a program may log how much of the compute budget remains. See
[debugging](developing/on-chain-programs/debugging.md#monitoring-compute-budget-consumption)
for more information.

A transaction may set the maximum number of compute units it is allowed to
consume and the compute unit price by including a `SetComputeUnitLimit` and a
`SetComputeUnitPrice`
[Compute budget instructions](https://github.com/solana-labs/solana/blob/db32549c00a1b5370fcaf128981ad3323bbd9570/sdk/src/compute_budget.rs#L22)
respectively.

If no `SetComputeUnitLimit` is provided the limit will be calculated as the
product of the number of instructions in the transaction (excluding the [Compute
budget
instructions](https://github.com/solana-labs/solana/blob/db32549c00a1b5370fcaf128981ad3323bbd9570/sdk/src/compute_budget.rs#L22))
and the default per-instruction units, which is currently 200k.

Note that a transaction's prioritization fee is calculated by multiplying the
number of compute units by the compute unit price (measured in micro-lamports)
set by the transaction via compute budget instructions.  So transactions should
request the minimum amount of compute units required for execution to minimize
fees. Also note that fees are not adjusted when the number of requested compute
units exceeds the number of compute units actually consumed by an executed
transaction.

Compute Budget instructions don't require any accounts and don't consume any
compute units to process.  Transactions can only contain one of each type of
compute budget instruction, duplicate types will result in an error.

The `ComputeBudgetInstruction::set_compute_unit_limit` and
`ComputeBudgetInstruction::set_compute_unit_price` functions can be used to
create these instructions:

```rust
let instruction = ComputeBudgetInstruction::set_compute_unit_limit(300_000);
```

```rust
let instruction = ComputeBudgetInstruction::set_compute_unit_price(1);
```

## New Features

As Solana evolves, new features or patches may be introduced that changes the
behavior of the cluster and how programs run. Changes in behavior must be
coordinated between the various nodes of the cluster. If nodes do not
coordinate, then these changes can result in a break-down of consensus. Solana
supports a mechanism called runtime features to facilitate the smooth adoption
of changes.

Runtime features are epoch coordinated events where one or more behavior changes
to the cluster will occur. New changes to Solana that will change behavior are
wrapped with feature gates and disabled by default. The Solana tools are then
used to activate a feature, which marks it pending, once marked pending the
feature will be activated at the next epoch.

To determine which features are activated use the [Solana command-line
tools](cli/install-solana-cli-tools.md):

```bash
solana feature status
```

If you encounter problems, first ensure that the Solana tools version you are
using match the version returned by `solana cluster-version`. If they do not
match, [install the correct tool suite](cli/install-solana-cli-tools.md).
