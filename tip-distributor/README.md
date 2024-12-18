# Tip Distributor
This library and collection of binaries are responsible for generating and uploading merkle roots to the on-chain 
tip-distribution program found [here](https://github.com/jito-foundation/jito-programs/blob/submodule/tip-payment/programs/tip-distribution/src/lib.rs).

## Background
Each individual validator is assigned a new PDA per epoch where their share of tips, in lamports, will be stored. 
At the end of the epoch it's expected that validators take a commission and then distribute the rest of the funds
to their delegators such that delegators receive rewards proportional to their respective delegations. The distribution
mechanism is via merkle proofs similar to how airdrops work.

The merkle roots are calculated off-chain and uploaded to the validator's **TipDistributionAccount** PDA. Validators may
elect an account to upload the merkle roots on their behalf. Once uploaded, users can invoke the **claim** instruction
and receive the rewards they're entitled to. Once all funds are claimed by users the validator can close the account and
refunded the rent.

## Scripts

### stake-meta-generator

This script generates a JSON file identifying individual stake delegations to a validator, along with amount of lamports 
in each validator's **TipDistributionAccount**. All validators will be contained in the JSON list, regardless of whether 
the validator is a participant in the system; participant being indicative of running the jito-solana client to accept tips 
having initialized a **TipDistributionAccount** PDA account for the epoch.

One edge case that we've taken into account is the last validator in an epoch N receives tips but those tips don't get transferred
out into the PDA until some slot in epoch N + 1. Due to this we cannot rely on the bank's state at epoch N for lamports amount
in the PDAs. We use the bank solely to take a snapshot of delegations, but an RPC node to fetch the PDA lamports for more up-to-date data.

### merkle-root-generator
This script accepts a path to the above JSON file as one of its arguments, and generates a merkle-root into a JSON file.

### merkle-root-uploader
Uploads the root on-chain.

### claim-mev-tips
This reads the file outputted by `merkle-root-generator` and finds all eligible accounts to receive mev tips. Transactions
are created and sent to the RPC server.


## How it works?
In order to use this library as the merkle root creator one must follow the following steps:
1. Download a ledger snapshot containing the slot of interest, i.e. the last slot in an epoch. The Solana foundation has snapshots that can be found [here](https://console.cloud.google.com/storage/browser/mainnet-beta-ledger-us-ny5).
2. Download the snapshot onto your worker machine (where this script will run).
3. Run `solana-ledger-tool -l ${PATH_TO_LEDGER} create-snapshot ${YOUR_SLOT} ${WHERE_TO_CREATE_SNAPSHOT}`
   1. The snapshot created at `${WHERE_TO_CREATE_SNAPSHOT}` will have the highest slot of `${YOUR_SLOT}`, assuming you downloaded the correct snapshot.
4. Run `stake-meta-generator --ledger-path ${WHERE_TO_CREATE_SNAPSHOT} --tip-distribution-program-id ${PUBKEY} --out-path ${JSON_OUT_PATH} --snapshot-slot ${SLOT} --rpc-url ${URL}`
   1. Note: `${WHERE_TO_CREATE_SNAPSHOT}` must be the same in steps 3 & 4.
5. Run `merkle-root-generator --stake-meta-coll-path ${STAKE_META_COLLECTION_JSON} --rpc-url ${URL} --out-path ${MERKLE_ROOT_PATH}`
6. Run `merkle-root-uploader --out-path ${MERKLE_ROOT_PATH} --keypair-path ${KEYPAIR_PATH} --rpc-url ${URL} --tip-distribution-program-id ${PROGRAM_ID}`
7. Run `solana-claim-mev-tips --merkle-trees-path /solana/ledger/autosnapshot/merkle-tree-221615999.json --rpc-url ${URL} --tip-distribution-program-id ${PROGRAM_ID} --keypair-path ${KEYPAIR_PATH}`

Voila!
