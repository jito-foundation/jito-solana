#!/usr/bin/env python3
"""Temporary branch-only LC2 evidence collection; no shared runner changes."""
import collections
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import sys
import time

OUT = Path("target/lc2-diagnostic")
TEST = "test_duplicate_shreds_broadcast_leader"
FILTER = "test(/test_duplicate_shreds_(solitary|coalesced)_final_tick/)"


def capture(command):
    try:
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, check=False)
    except FileNotFoundError as error:
        return {"command": command, "error": str(error)}
    return {"command": command, "exit_code": result.returncode,
            "output": result.stdout}


def facts(label):
    OUT.mkdir(parents=True, exist_ok=True)
    data = {"time": time.time(), "label": label}
    for command in (["uname", "-a"], ["lscpu", "-J"], ["free", "-b"],
                    ["df", "-T", "."], ["git", "rev-parse", "HEAD"],
                    ["rustc", "--version"], ["cargo", "nextest", "--version"]):
        data[command[0] + " " + " ".join(command[1:])] = capture(command)
    for name in ("/proc/self/status", "/proc/loadavg", "/proc/meminfo",
                 "/proc/pressure/cpu", "/proc/pressure/memory",
                 "/proc/pressure/io", "/sys/fs/cgroup/cpu.max",
                 "/sys/fs/cgroup/cpuset.cpus.effective",
                 "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.events"):
        path = Path(name)
        if path.exists():
            data[name] = path.read_text()
    data["environment"] = {key: os.environ[key] for key in (
        "BUILDKITE_AGENT_NAME", "BUILDKITE_JOB_ID", "BUILDKITE_COMMIT",
        "CI_COMMIT", "RUSTFLAGS", "RUSTC_WRAPPER", "CI_HOST_SLOTS")
        if key in os.environ}
    (OUT / (label + "-facts.json")).write_text(json.dumps(data, indent=2))
    print(json.dumps({"event": "facts", "label": label, "facts": data}), flush=True)


def process_sample(parent):
    processes = {}
    for path in Path("/proc").glob("[0-9]*/status"):
        try:
            rows = dict(line.split(":", 1) for line in path.read_text().splitlines())
            processes[int(path.parent.name)] = rows
        except (OSError, ValueError):
            pass
    selected = {parent}
    while True:
        added = {pid for pid, rows in processes.items()
                 if int(rows.get("PPid", "0")) in selected} - selected
        if not added:
            break
        selected.update(added)
    result = []
    for pid in selected:
        if pid not in processes:
            continue
        rows = processes[pid]
        item = {key: rows.get(key, "").strip() for key in (
            "Name", "PPid", "Threads", "VmRSS", "Cpus_allowed_list")}
        item["pid"] = pid
        names = []
        for path in Path(f"/proc/{pid}/task").glob("*/comm"):
            try:
                names.append(path.read_text().strip())
            except OSError:
                pass
        item["thread_names"] = dict(collections.Counter(names))
        result.append(item)
    return {"time": time.time(), "processes": result}


def run_logged(label, command, timeout):
    path = OUT / (label + ".log")
    print(json.dumps({"event": "start", "label": label, "command": command}), flush=True)
    ledger_dir = OUT / (label + "-ledgers")
    os.environ["FARF_DIR"] = str(ledger_dir.resolve())
    start = time.monotonic()
    with path.open("w") as log, (OUT / (label + "-processes.jsonl")).open("w") as samples:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        while process.poll() is None:
            samples.write(json.dumps(process_sample(process.pid)) + "\n")
            samples.flush()
            if time.monotonic() - start > timeout:
                import signal
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                break
            time.sleep(10)
        code = process.wait()
    if code and ledger_dir.exists():
        with tarfile.open(OUT / (label + "-ledgers.tar.gz"), "w:gz") as archive:
            archive.add(ledger_dir, arcname=ledger_dir.name)
    text = path.read_text(errors="replace")
    selected = [line for line in text.splitlines() if re.search(
        r"lc2-(purge|dead)|duplicate-(batch|variants)|BlockAborted|ChainedBlockId|more than 10|FAIL|PASS|ABRT|error\[|^error:|test result:", line)]
    print("\n".join(selected[-120:]) or text[-12000:], flush=True)
    with gzip.open(str(path) + ".gz", "wt") as archive:
        archive.write(text)
    path.unlink()
    result = {"label": label, "exit_code": code,
              "elapsed": time.monotonic() - start}
    print(json.dumps({"event": "end", **result}), flush=True)
    return result


def run():
    os.environ["RUSTFLAGS"] = "-D warnings"
    os.environ["RUST_BACKTRACE"] = "1"
    os.environ["RUST_LOG"] = (
        "error,solana_turbine::broadcast_stage::broadcast_duplicates_run=info,"
        "solana_core::replay_stage=info,solana_core::repair=info,"
        "solana_local_cluster=info,solana_ledger::blockstore=info,"
        "solana_ledger::blockstore_processor=info")
    facts("container")
    common = ["cargo", "nextest", "run", "--profile", "ci", "--cargo-profile", "ci",
              "--retries", "0", "--no-fail-fast", "--test-threads", "1", "--success-output", "immediate",
              "--failure-output", "immediate"]
    results = [run_logged("broadcaster-regression", common + ["-p", "solana-turbine",
                          "--lib", "-E", FILTER], 1800)]
    for index in range(2):
        results.append(run_logged(f"cluster-{index + 1}", common + ["-p",
            "solana-local-cluster", "--test", "local_cluster", "-E",
            "test(/^" + TEST + "$/)"], 1800))
    (OUT / "results.json").write_text(json.dumps(results, indent=2))
    return int(any(result["exit_code"] for result in results))


def host(phase="baseline"):
    import fcntl
    # Serialize our diagnostics if both jobs land on the same physical host.
    lock = open("/tmp/jito-lc2-diagnostic.lock", "w")
    fcntl.flock(lock, fcntl.LOCK_EX)
    facts("host")
    command = (["python3", "ci/lc2-source-matrix.py"] if phase == "matrix"
               else ["python3", "ci/lc2-diagnostic.py", "run"])
    result = subprocess.run(["ci/docker-run-default-image.sh", *command], check=False)
    for path in Path("target/lc2-matrix").glob("*/*.log"):
        with path.open("rb") as source, gzip.open(str(path) + ".gz", "wb") as archive:
            import shutil
            shutil.copyfileobj(source, archive)
    subprocess.run(["buildkite-agent", "artifact", "upload",
                    "target/lc2-matrix/*/*.json;target/lc2-matrix/*/*.log.gz;target/lc2-matrix/*/*.toml"], check=False)
    subprocess.run(["buildkite-agent", "artifact", "upload",
                    "target/lc2-diagnostic/*.json;target/lc2-diagnostic/*.jsonl;target/lc2-diagnostic/*.gz"], check=False)
    return result.returncode


def pipeline(phase="baseline"):
    steps = []
    for index in range(2):
        steps.append({"label": "lc2-diagnostic " + phase + "-" + str(index + 1),
                      "command": "python3 ci/lc2-diagnostic.py host " + phase,
                      "agents": {"queue": "default"},
                      "timeout_in_minutes": 260 if phase == "matrix" else 65,
                      "retry": {"automatic": False}})
    print(json.dumps({"steps": steps}))
    return 0


if __name__ == "__main__":
    phase = sys.argv[2] if len(sys.argv) > 2 else "baseline"
    if phase not in ("baseline", "matrix"):
        raise SystemExit("Unknown experiment phase: " + phase)
    action = sys.argv[1]
    raise SystemExit(run() if action == "run" else {"pipeline": pipeline, "host": host}[action](phase))
