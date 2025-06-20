---
title: Agave Validator Prerequisites
sidebar_position: 2
sidebar_label: Prerequisites
pagination_label: Prerequisites to run a Validator
---

Operating an Agave validator is an interesting and rewarding task. Generally speaking, it requires someone with a technical background but also involves community engagement and marketing.

## How to be a good Validator Operator

Here is a list of some of the requirements for being a good operator:

- Performant computer hardware and a fast internet connection
  - You can find a list of [hardware requirements here](./requirements.md)
  - Solana helps facilitate data-center server rentals through the [Solana server program](https://docs.solana.com/running-validator/validator-reqs)
- Knowledge of the Linux terminal
- Linux system administration
  - Accessing your machine via ssh and scp
  - Installing software (installing from source is encouraged)
  - Keeping your Linux distribution up to date
  - Managing users and system access
  - Understanding computer processes
  - Understanding networking basics
  - Formatting and mounting drives
  - Managing firewall rules (UFW/iptables)
- Hardware performance monitoring
- Cluster and node monitoring
- Quick response times in case of a validator issue
- Marketing and communications to attract delegators
- Customer support

Whether you decide to run a [validator](../what-is-a-validator.md) or an [RPC node](../what-is-an-rpc-node.md), you should consider all of these areas of expertise. A team of people is likely necessary for you to achieve your goals.

## Can I use my computer at home?

While anyone can join the network, you should make sure that your home computer and network meets the specifications in the [hardware requirements](./requirements.md) doc. Most home internet service providers do not provide consistent service that would allow your validator to perform well. If your home network or personal hardware is not performant enough to keep up with the Solana cluster, your validator will not be able to participate in consensus.

In addition to performance considerations, you will want to make sure that your home computer is resistant to outages caused by loss of power, flooding, fire, theft, etc. If you are just getting started on the testnet cluster and learning about being an operator, a home setup may be sufficient, but you will want to consider all of these factors when you start operating your validator on the mainnet-beta cluster.
