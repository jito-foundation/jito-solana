#!/usr/bin/env python3
"""Temporary, retry-free LC2 comparison of two pinned source revisions.

Run inside the CI container: python3 ci/lc2-source-matrix.py
All source checkouts, archives and evidence stay under target/lc2-matrix.
"""

import collections
import datetime
import hashlib
import json
import os
from pathlib import Path
import random
import re
import signal
import subprocess
import sys
import time
import uuid


SOURCES = {
    "jito": (
        "c266f8b8039546fab8e9379e7fa4847bf03a53cf",
        "git@github.com:jito-foundation/jito-solana.git",
    ),
    "agave": (
        "aeeedea20b8ead8714d34dde073d9084c92ed628",
        "https://github.com/anza-xyz/agave.git",
    ),
}
TEST = "test_duplicate_shreds_broadcast_leader"
FILTER = f"package(=solana-local-cluster) & test(={TEST})"
PAIRS = 10
CONFIG = """[profile.ci]
retries = 0
test-threads = 1
slow-timeout = { period = "60s", terminate-after = 10 }
failure-output = "immediate"
success-output = "immediate"
"""


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def stop(process):
    # Supervise the whole process group so timed-out compilers cannot linger.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def execute(argv, cwd, log, env, timeout, check=True):
    argv = [str(arg) for arg in argv]
    record = {"argv": argv, "cwd": str(cwd), "log": str(log),
              "timeout_s": timeout, "started_at": time.time()}
    print(json.dumps({"event": "command_start", **record}), flush=True)
    started = time.monotonic()
    with log.open("wb") as output:
        process = subprocess.Popen(
            argv, cwd=cwd, env=env, stdout=output,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            while True:
                remaining = timeout - (time.monotonic() - started)
                if remaining <= 0:
                    stop(process)
                    record["timed_out"] = True
                    break
                try:
                    process.wait(timeout=min(60, remaining))
                    break
                except subprocess.TimeoutExpired:
                    print(json.dumps({"event": "command_running",
                                      "log": str(log),
                                      "elapsed_s": time.monotonic() - started}),
                          flush=True)
        except BaseException:
            stop(process)
            record["interrupted"] = True
            raise
        finally:
            record["elapsed_s"] = time.monotonic() - started
            record["process_exit_code"] = process.returncode
            record["exit_code"] = 124 if record.get("timed_out") else process.returncode
            save(log.with_suffix(".json"), record)
    print(json.dumps({"event": "command_end", **record}), flush=True)
    if check and record["exit_code"] != 0:
        raise RuntimeError(f"Command failed ({record['exit_code']}): {log}")
    return record


def main():
    repo = Path(__file__).resolve().parents[1]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    output = repo / "target" / "lc2-matrix" / f"{stamp}-{uuid.uuid4().hex[:8]}"
    output.mkdir(parents=True)
    config = output / "nextest.toml"
    config.write_text(CONFIG)
    env = os.environ.copy()
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    env.update(RUSTFLAGS="-D warnings", RUST_BACKTRACE="1",
               RUSTUP_TOOLCHAIN="1.98.0", CARGO_TERM_COLOR="never")
    # Record only known non-secret experiment settings.
    settings = {key: env.get(key) for key in (
        "RUSTFLAGS", "RUST_BACKTRACE", "RUSTUP_TOOLCHAIN", "RUST_LOG",
        "RUSTC_WRAPPER", "CARGO_BUILD_JOBS", "SOLANA_RAYON_THREADS",
        "SOLANA_MAX_RAYON_THREADS", "RAYON_NUM_THREADS", "BUILDKITE_JOB_ID",
    )}
    seed = int.from_bytes(os.urandom(8), "big")
    rng = random.Random(seed)
    schedule = []
    for pair in range(1, PAIRS + 1):
        labels = list(SOURCES)
        rng.shuffle(labels)
        schedule.extend({"pair": pair, "source": label} for label in labels)
    summary = {"output": str(output), "seed": seed, "settings": settings,
               "schedule": schedule, "sources": {}, "trials": []}
    save(output / "summary.json", summary)
    print(json.dumps({"event": "matrix_start", **summary}), flush=True)
    try:
        for args, label in [(["rustc", "--version"], "rustc"),
                            (["cargo", "nextest", "--version"], "nextest")]:
            execute(args, repo, output / f"{label}.log", env, 60)
        for command in ("archive", "run", "list"):
            execute(["cargo", "nextest", command, "--help"], repo,
                    output / f"nextest-{command}-help.log", env, 60)
        for label, (sha, remote) in SOURCES.items():
            source = output / label
            execute(["git", "clone", "--shared", "--no-checkout", repo, source],
                    repo, output / f"{label}-clone.log", env, 300)
            found = execute(["git", "cat-file", "-e", f"{sha}^{{commit}}"],
                            source, output / f"{label}-object.log", env, 60, False)
            if found["exit_code"]:
                execute(["git", "fetch", "--no-tags", remote, sha], source,
                        output / f"{label}-fetch.log", env, 300)
            execute(["git", "checkout", "--detach", sha], source,
                    output / f"{label}-checkout.log", env, 300)
            execute(["git", "rev-parse", "HEAD"], source,
                    output / f"{label}-head.log", env, 60)
            actual = (output / f"{label}-head.log").read_text().strip()
            if actual != sha:
                raise RuntimeError(f"Unexpected {label} HEAD: {actual}")
            if (source / ".gitmodules").exists():
                execute(["git", "submodule", "update", "--init", "--recursive", "--jobs", "4"],
                        source, output / f"{label}-submodules.log", env, 600)
                status_log = output / f"{label}-submodule-status.log"
                execute(["git", "submodule", "status", "--recursive"], source,
                        status_log, env, 60)
                if any(line[:1] in ("-", "+", "U")
                       for line in status_log.read_text().splitlines()):
                    raise RuntimeError(f"{label} submodule revision mismatch")
            manifest = (source / "local-cluster" / "Cargo.toml").read_text()
            package = re.search(r"(?ms)^\[package\]\s*(.*?)(?=^\[|\Z)", manifest)
            name = re.search(r'^name\s*=\s*"([^"]+)"', package[1], re.M) if package else None
            if not name or name[1] != "solana-local-cluster":
                raise RuntimeError(f"Unexpected package name in {source}")
            archive = output / f"{label}.tar.zst"
            provenance = {"sha": sha, "source": str(source),
                          "cargo_lock_sha256": digest(source / "Cargo.lock"),
                          "cargo_toml_sha256": digest(source / "Cargo.toml"),
                          "cargo_profile": "ci", "archive": str(archive)}
            summary["sources"][label] = provenance
            save(output / "summary.json", summary)
            execute(["cargo", "nextest", "archive", "--locked", "--cargo-profile", "ci",
                     "--package", name[1], "--test", "local_cluster",
                     "--target-dir", source / "target", "--config-file", config,
                     "--profile", "ci", "--archive-file", archive],
                    source, output / f"{label}-compile.log", env, 1800)
            if digest(source / "Cargo.lock") != provenance["cargo_lock_sha256"]:
                raise RuntimeError(f"{label} Cargo.lock changed during compilation")
            archive.chmod(0o444)
            provenance["archive_sha256"] = digest(archive)
            common = ["--archive-file", archive, "--workspace-remap", source,
                      "--config-file", config, "--profile", "ci", "-E", FILTER]
            execute(["cargo", "nextest", "list", *common, "--message-format", "json"],
                    source, output / f"{label}-tests.log", env, 120)
            listings = [json.loads(line) for line in (output / f"{label}-tests.log").read_text().splitlines()
                        if line.startswith('{"')]
            selected = [(suite["package-name"], test_name)
                        for listing in listings for suite in listing["rust-suites"].values()
                        for test_name, test in suite["testcases"].items()
                        if test["filter-match"]["status"] == "matches" and not test["ignored"]]
            if selected != [("solana-local-cluster", TEST)]:
                raise RuntimeError(f"Unexpected {label} selected tests: {selected}")
            provenance["selected_tests"] = selected
            save(output / "summary.json", summary)
        for number, trial in enumerate(schedule, 1):
            label = trial["source"]
            provenance = summary["sources"][label]
            source = Path(provenance["source"])
            result = execute([
                "cargo", "nextest", "run", "--archive-file", provenance["archive"],
                "--workspace-remap", source, "--config-file", config, "--profile", "ci",
                "--test-threads", "1", "--retries", "0", "--no-tests", "fail",
                "--failure-output", "immediate", "--success-output", "immediate",
                "--status-level", "all", "--final-status-level", "all", "-E", FILTER,
            ], source, output / f"trial-{number:02d}-{label}.log", env, 900, False)
            summary["trials"].append({**trial, **result, "sha": provenance["sha"],
                                      "archive_sha256": provenance["archive_sha256"]})
            save(output / "summary.json", summary)
        for provenance in summary["sources"].values():
            if digest(Path(provenance["archive"])) != provenance["archive_sha256"]:
                raise RuntimeError("Archive changed during the experiment")
        failures = sum(trial["exit_code"] != 0 for trial in summary["trials"])
        summary["failures"] = failures
        summary["complete"] = True
        save(output / "summary.json", summary)
        counts = collections.Counter((t["source"], t["exit_code"]) for t in summary["trials"])
        print(json.dumps({"event": "matrix_complete", "failures": failures,
                          "counts": [{"source": k[0], "exit_code": k[1], "count": v}
                                     for k, v in sorted(counts.items())],
                          "summary": str(output / "summary.json")}), flush=True)
        return 1 if failures else 0
    except BaseException as error:
        summary["complete"] = False
        summary["error"] = str(error)
        save(output / "summary.json", summary)
        print(json.dumps({"event": "matrix_stopped", "error": str(error),
                          "summary": str(output / "summary.json")}), flush=True)
        return 130 if isinstance(error, (KeyboardInterrupt, InterruptedError)) else 2


def interrupted(signum, frame):
    raise InterruptedError(f"Received signal {signum}")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
