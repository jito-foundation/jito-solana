<p align="center">
  <a href="https://solana.com">
    <img alt="Solana" src="https://i.imgur.com/0vfIMHo.png" width="250" />
  </a>
</p>

[![Build status](https://badge.buildkite.com/3a7c88c0f777e1a0fddacc190823565271ae4c251ef78d83a8.svg)](https://buildkite.com/jito/jito-solana)

# About

This repository contains Jito's fork of the Solana validator.

We recommend checking out our [Gitbook](https://jito-foundation.gitbook.io/mev/jito-solana/building-the-software) for
more detailed instructions on building and running Jito-Solana.

---

## **1. Install rustc, cargo and rustfmt.**

```bash
$ curl https://sh.rustup.rs -sSf | sh
$ source $HOME/.cargo/env
$ rustup component add rustfmt
```

When building the master branch, please make sure you are using the latest stable rust version by running:

```bash
$ rustup update
```

When building a specific release branch, you should check the rust version in `ci/rust-version.sh` and if necessary,
install that version by running:

```bash
$ rustup install VERSION
```

Note that if this is not the latest rust version on your machine, cargo commands may require
an [override](https://rust-lang.github.io/rustup/overrides.html) in order to use the correct version.

On Linux systems you may need to install libssl-dev, pkg-config, zlib1g-dev, protobuf etc.

On Ubuntu:

```bash
$ sudo apt-get update
$ sudo apt-get install libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler
```

On Fedora:

```bash
$ sudo dnf install openssl-devel systemd-devel pkg-config zlib-devel llvm clang cmake make protobuf-devel protobuf-compiler perl-core
```

## **2. Download the source code.**

```bash
$ git clone https://github.com/jito-foundation/jito-solana.git
$ cd jito-solana
```

## **3. Build.**

```bash
$ ./cargo build
```

# Testing

**Run the test suite:**

```bash
$ ./cargo test
```

### Starting a local testnet

Start your own testnet locally, instructions are in the [online docs](https://docs.solanalabs.com/clusters/benchmark).

### Accessing the remote development cluster

* `devnet` - stable public cluster for development accessible via
  devnet.solana.com. Runs 24/7. Learn more about the [public clusters](https://docs.solanalabs.com/clusters)

# Benchmarking

First, install the nightly build of rustc. `cargo bench` requires the use of the
unstable features only available in the nightly build.

```bash
$ rustup install nightly
```

Run the benchmarks:

```bash
$ cargo +nightly bench
```

# Release Process

The release process for this project is described [here](RELEASE.md).

# Code coverage

To generate code coverage statistics:

```bash
$ scripts/coverage.sh
$ open target/cov/lcov-local/index.html
```

Why coverage? While most see coverage as a code quality metric, we see it primarily as a developer
productivity metric. When a developer makes a change to the codebase, presumably it's a *solution* to
some problem. Our unit-test suite is how we encode the set of *problems* the codebase solves. Running
the test suite should indicate that your change didn't *infringe* on anyone else's solutions. Adding a
test *protects* your solution from future changes. Say you don't understand why a line of code exists,
try deleting it and running the unit-tests. The nearest test failure should tell you what problem
was solved by that code. If no test fails, go ahead and submit a Pull Request that asks, "what
problem is solved by this code?" On the other hand, if a test does fail and you can think of a
better way to solve the same problem, a Pull Request with your solution would most certainly be
welcome! Likewise, if rewriting a test can better communicate what code it's protecting, please
send us that patch!

