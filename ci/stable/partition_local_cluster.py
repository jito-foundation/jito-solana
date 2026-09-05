#!/usr/bin/env python3
"""Balance the live local-cluster test list using measured execution times."""

import argparse
import json
from pathlib import Path
import re
import sys


def partition_tests(tests, durations, total):
    if total < 1:
        raise ValueError("partition count must be positive")
    partitions = [[] for _ in range(total)]
    loads = [0] * total
    # A missing timing affects balance only; new tests are still included.
    for name in sorted(tests, key=lambda name: (-durations.get(name, 60), name)):
        index = min(range(total), key=lambda index: (loads[index], index))
        partitions[index].append(name)
        loads[index] += durations.get(name, 60)
    return partitions, loads


def runnable_tests(test_list):
    suites = list(test_list["rust-suites"].values())
    if (
        len(suites) != 1
        or suites[0]["binary-id"] != "solana-local-cluster::local_cluster"
        or suites[0]["status"] != "listed"
    ):
        raise ValueError("expected the listed local_cluster integration test binary")
    tests = [
        name
        for name, case in suites[0]["testcases"].items()
        if not case["ignored"] and case["filter-match"]["status"] == "matches"
    ]
    if not tests:
        raise ValueError("local_cluster has no runnable tests")
    return tests


def filterset(tests):
    # This integration suite uses Rust identifiers. Fail rather than emit a
    # filter that could interpret an unfamiliar test name as DSL syntax.
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9:]*", name) for name in tests):
        raise ValueError("unsupported local_cluster test name")
    return " | ".join(f"test(={name})" for name in tests) or "none()"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", type=int)
    parser.add_argument("total", type=int)
    args = parser.parse_args()
    if not 1 <= args.current <= args.total:
        parser.error("expected 1 <= current <= total")

    # Successful attempts from https://buildkite.com/jito/jito-solana/builds/5586.
    durations = json.loads(
        Path(__file__).with_name("local-cluster-durations.json").read_text()
    )
    tests = runnable_tests(json.load(sys.stdin))
    partitions, loads = partition_tests(tests, durations, args.total)
    selected = partitions[args.current - 1]
    print(
        f"local-cluster {args.current}/{args.total}: {len(selected)}/{len(tests)} "
        f"tests, estimated {loads[args.current - 1]}s",
        file=sys.stderr,
    )
    print(filterset(selected))


if __name__ == "__main__":
    main()
