#!/usr/bin/env python3
"""Fake Cargo for ci/test-dcou.py: record arguments and return configured graphs."""
import json
import os
from pathlib import Path
import sys


def main():
    arguments = sys.argv[1:]
    with Path("cargo-calls.jsonl").open("a") as calls:
        calls.write(json.dumps(arguments) + "\n")

    # Ordinary builds succeed without creating binaries.
    if "--unit-graph" not in arguments:
        return 0

    # Development tools use dev-bins/Cargo.toml; production uses the workspace.
    if "--manifest-path" in arguments:
        group = "DEVELOPMENT"
    else:
        group = "PRODUCTION"

    print(os.environ[f"DCOU_TEST_{group}_GRAPH"])
    return int(os.environ[f"DCOU_TEST_{group}_STATUS"])


if __name__ == "__main__":
    sys.exit(main())
