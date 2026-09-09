#!/usr/bin/env python3
"""Temporary LC2 startup-affinity contrast: one unfixed binary, ten pairs.

Run inside the CI container: python3 ci/lc2-source-matrix.py
All source checkouts, archives and evidence stay under target/lc2-matrix.
"""

import argparse
import collections
import datetime
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import re
import signal
import shutil
import xml.etree.ElementTree as ET
import subprocess
import sys
import time
import uuid


SOURCES = {
    "jito": (
        "c266f8b8039546fab8e9379e7fa4847bf03a53cf",
        "git@github.com:jito-foundation/jito-solana.git",
    ),
}
TEST = "test_duplicate_shreds_broadcast_leader"
FILTER = f"package(=solana-local-cluster) & test(={TEST})"
PAIRS = 10
HELPER_SPEC = importlib.util.spec_from_file_location(
    "lc2_validate_helpers", Path(__file__).with_name("lc2-validate.py"))
HELPERS = importlib.util.module_from_spec(HELPER_SPEC)
HELPER_SPEC.loader.exec_module(HELPERS)
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


def execute(argv, cwd, log, env, timeout, check=True, binary=None, affinity=None):
    argv = [str(arg) for arg in argv]
    record = {"argv": argv, "cwd": str(cwd), "log": str(log),
              "timeout_s": timeout, "started_at": time.time()}
    print(json.dumps({"event": "command_start", **record}), flush=True)
    started = time.monotonic()
    samples_path = log.with_name(log.stem + "-processes.jsonl.gz")
    samples = gzip.open(samples_path, "wt") if binary else None
    if binary:
        record["process_samples"] = str(samples_path)
    with log.open("wb") as output:
        process = subprocess.Popen(
            argv, cwd=cwd, env=env, stdout=output,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            next_sample = started
            while True:
                if samples and time.monotonic() >= next_sample:
                    sample = HELPERS.process_sample(process.pid, binary)
                    samples.write(json.dumps(sample) + "\n")
                    samples.flush()
                    for observed in sample["processes"]:
                        if affinity is not None:
                            masks = [observed["Cpus_allowed_list"],
                                     *observed["thread_affinity_counts"]]
                            if any(cpu_list(mask) != set(affinity) for mask in masks):
                                raise RuntimeError("Observed test/thread affinity changed")
                            identity = observed["nextest_environment"]
                            if (identity.get("NEXTEST_ATTEMPT") != "1"
                                    or not identity.get("NEXTEST_RUN_ID")):
                                raise RuntimeError("Missing test identity or unexpected retry")
                            record["observed_run_ids"] = sorted(set(
                                record.get("observed_run_ids", [])
                                + [identity["NEXTEST_RUN_ID"]]))
                            record["affinity_observations"] = (
                                record.get("affinity_observations", 0) + 1)
                    next_sample = time.monotonic() + 10
                remaining = timeout - (time.monotonic() - started)
                if remaining <= 0:
                    stop(process)
                    record["timed_out"] = True
                    break
                try:
                    process.wait(timeout=min(10 if binary else 60, remaining))
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
            if samples:
                samples.close()
            record["elapsed_s"] = time.monotonic() - started
            record["process_exit_code"] = process.returncode
            record["exit_code"] = 124 if record.get("timed_out") else process.returncode
            save(log.with_suffix(".json"), record)
    print(json.dumps({"event": "command_end", **record}), flush=True)
    if check and record["exit_code"] != 0:
        raise RuntimeError(f"Command failed ({record['exit_code']}): {log}")
    return record



def cpu_list(text):
    result = set()
    for part in text.strip().split(','):
        ends = part.split('-')
        result.update(range(int(ends[0]), int(ends[-1]) + 1))
    return result


def topology():
    """Select whole SMT cores evenly across the measured LLCs, never CPU ranges."""
    allowed = set(os.sched_getaffinity(0))
    if len(allowed) != 128:
        raise RuntimeError("Affinity experiment requires 128 available CPUs")
    if 'AMD EPYC 9575F 64-Core Processor' not in Path('/proc/cpuinfo').read_text():
        raise RuntimeError("Affinity experiment requires the predeclared 9575F host")
    rows = []
    for cpu in sorted(allowed):
        path = Path(f'/sys/devices/system/cpu/cpu{cpu}')
        caches = [cache for cache in (path / 'cache').glob('index*')
                  if (cache / 'level').read_text().strip() == '3'
                  and (cache / 'type').read_text().strip() == 'Unified']
        nodes = list(path.glob('node[0-9]*'))
        if len(caches) != 1 or len(nodes) != 1:
            raise RuntimeError('Missing or ambiguous LLC/NUMA topology')
        rows.append({'cpu': cpu,
            'socket': int((path / 'topology/physical_package_id').read_text()),
            'core': int((path / 'topology/core_id').read_text()),
            'siblings': sorted(cpu_list((path / 'topology/thread_siblings_list').read_text())),
            'node': int(nodes[0].name[4:]),
            'llc_cpus': sorted(cpu_list((caches[0] / 'shared_cpu_list').read_text()))})
    cores = {}
    for row in rows:
        key = (row['socket'], row['core'])
        if key in cores and any(cores[key][field] != row[field]
                                for field in ('siblings', 'llc_cpus', 'node')):
            raise RuntimeError('Inconsistent SMT/LLC/NUMA topology')
        cores[key] = row
    llcs = collections.defaultdict(list)
    for key, row in sorted(cores.items()):
        siblings = set(row['siblings'])
        members = {r['cpu'] for r in rows if (r['socket'], r['core']) == key}
        if len(siblings) != 2 or siblings != members or not siblings <= allowed:
            raise RuntimeError('Incomplete or unexpected SMT core')
        llcs[tuple(row['llc_cpus'])].append(row)
    if (len(cores) != 64 or len({r['socket'] for r in rows}) != 1
            or len({r['node'] for r in rows}) != 1 or len(llcs) != 8
            or any(len(group) != 8 for group in llcs.values())):
        raise RuntimeError('Unsupported topology; do not invent a replacement subset')
    for shared, group in llcs.items():
        if set(shared) != {cpu for core in group for cpu in core['siblings']}:
            raise RuntimeError('LLC sharing and physical-core membership disagree')
    subset = sorted(cpu for _, group in sorted(llcs.items())
                    for core in group[:3] for cpu in core['siblings'])
    if len(subset) != 48 or not set(subset) <= allowed:
        raise RuntimeError('Invalid 48-CPU subset')
    return {'rows': rows, 'arms': {'48': subset, '128': sorted(allowed)},
            'selection': 'First three physical core IDs per each of eight LLCs, '
                         'both SMT siblings; one socket and one NUMA node.'}


def source_state(source):
    def git(*args):
        return subprocess.check_output(['git', *args], cwd=source, text=True)
    return {'sha': git('rev-parse', 'HEAD').strip(),
            'tracked_status': git('status', '--porcelain', '--untracked-files=no'),
            'diff': git('diff', '--binary', 'HEAD'),
            'submodules': git('submodule', 'status', '--recursive'),
            'cargo_lock_sha256': digest(source / 'Cargo.lock')}


def gzip_file(path):
    target = Path(str(path) + '.gz')
    temporary = Path(str(target) + '.partial')
    with path.open('rb') as src, gzip.open(temporary, 'wb', compresslevel=1) as dst:
        shutil.copyfileobj(src, dst)
    temporary.replace(target)
    return {'path': str(target), 'sha256': digest(target),
            'uncompressed_sha256': digest(path), 'size': target.stat().st_size}


def native_result(log, junit, result):
    text = log.read_text(errors='replace')
    statuses = re.findall(
        r'^\s*(PASS|FAIL|TIMEOUT|LEAK|EXECFAIL|SIG[A-Z0-9]+)\s+\[[^\]]+\]\s+'
        r'(?:\(\s*1/1\)\s+)?solana-local-cluster::local_cluster '
        + re.escape(TEST) + r'\s*$', text, re.M)
    root = ET.parse(junit).getroot()
    suites = root.findall('testsuite')
    cases = root.findall('./testsuite/testcase')
    if (not statuses or len(set(statuses)) != 1 or len(suites) != 1 or len(cases) != 1
            or not root.get('uuid') or cases[0].get('name') != TEST
            or cases[0].find('skipped') is not None
            or suites[0].get('name') != 'solana-local-cluster::local_cluster'
            or cases[0].get('classname') != 'solana-local-cluster::local_cluster'):
        raise RuntimeError('Missing or ambiguous exact native/JUnit result')
    case = cases[0]
    passed = all(case.find(tag) is None for tag in ('failure', 'error', 'skipped'))
    if passed != (statuses[0] == 'PASS') or passed != (result['exit_code'] == 0):
        raise RuntimeError('Native/JUnit/command status disagreement')
    return {'run_id': root.get('uuid'), 'status': statuses[0], 'passed': passed,
            'suite': suites[0].get('name'), 'test': TEST,
            'seconds': float(case.get('time')), 'timestamp': case.get('timestamp'),
            'events': HELPERS.event_counts(text),
            'first_panic': next((line for line in text.splitlines()
                                 if 'panicked at' in line), None)}


def retain_trial(output, stem, ledger, junit, failed, arm, retained, env, invalid=False):
    evidence = {}
    log = output / (stem + '.log')
    if log.exists():
        evidence['log'] = gzip_file(log)
    if junit.exists():
        copied = output / (stem + '.xml')
        shutil.copyfile(junit, copied)
        evidence['junit'] = gzip_file(copied)
    if failed and (arm not in retained or invalid) and ledger.exists():
        archive = output / (stem + '-ledgers.tar.gz')
        temporary = Path(str(archive) + '.partial')
        execute(['tar', '-C', ledger.parent, '-czf', temporary, ledger.name],
                output, output / (stem + '-retain.log'), env, 180)
        temporary.replace(archive)
        evidence['ledgers'] = {'path': str(archive), 'sha256': digest(archive),
                              'size': archive.stat().st_size}
        retained.add(arm)
    evidence['ledgers_retained'] = 'ledgers' in evidence
    save(output / (stem + '-evidence.json'), evidence)
    # Each trial has a unique FARF_DIR; remove only after its retention finishes.
    if ledger.exists():
        shutil.rmtree(ledger)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-cpus", type=int, choices=(128,), default=128)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    output = repo / "target" / "lc2-matrix" / f"{stamp}-{uuid.uuid4().hex[:8]}"
    output.mkdir(parents=True)
    config = output / "nextest.toml"
    config.write_text(CONFIG)
    deadline = time.monotonic() + 245 * 60
    env = os.environ.copy()
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    env.update(RUSTFLAGS="-D warnings", RUST_BACKTRACE="1",
               RUSTUP_TOOLCHAIN="1.98.0", CARGO_TERM_COLOR="never",
               NEXTEST_USER_CONFIG_FILE="none", RUST_LOG=HELPERS.LOG_FILTER)
    inherited_thread_settings = {key: env.pop(key, None) for key in (
        "SOLANA_RAYON_THREADS", "SOLANA_MAX_RAYON_THREADS",
        "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS")}

    # Record only known non-secret experiment settings.
    settings = {key: env.get(key) for key in (
        "RUSTFLAGS", "RUST_BACKTRACE", "RUSTUP_TOOLCHAIN", "RUST_LOG",
        "RUSTC_WRAPPER", "CARGO_BUILD_JOBS", "SOLANA_RAYON_THREADS",
        "SOLANA_MAX_RAYON_THREADS", "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "BUILDKITE_JOB_ID",
    )}
    seed = int.from_bytes(os.urandom(8), "big")
    rng = random.Random(seed)
    schedule = []
    orders = [[48, 128], [128, 48]] * (PAIRS // 2)
    rng.shuffle(orders)
    for pair, arms in enumerate(orders, 1):
        schedule.extend({"pair": pair, "source": "jito", "cpus": arm} for arm in arms)
    summary = {"output": str(output), "seed": seed, "settings": settings,
               "expected_cpus": args.expected_cpus, "test_execution_started": False,
               "schedule": schedule, "sources": {}, "trials": [],
               "inherited_thread_settings_removed": inherited_thread_settings,
               "intervention": "combined startup affinity and CPU-derived pool sizing"}
    save(output / "summary.json", summary)
    print(json.dumps({"event": "matrix_start", **summary}), flush=True)
    try:
        summary["container_facts"] = HELPERS.facts(output, env)
        if (args.expected_cpus is not None and
                len(summary["container_facts"]["effective_cpus"]) != args.expected_cpus):
            summary.update(complete=False, reason="cpu_placement_mismatch", exit_code=78)
            save(output / "summary.json", summary)
            print(json.dumps({"event": "matrix_placement_mismatch", "exit_code": 78,
                              "summary": str(output / "summary.json")}), flush=True)
            return 78
        summary["topology"] = topology()
        save(output / "topology.json", summary["topology"])
        save(output / "summary.json", summary)
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
            clean_state = source_state(source)
            if clean_state["tracked_status"]:
                raise RuntimeError("Unfixed checkout was not clean before diagnostics")
            diagnostic_patch = repo / "ci/lc2-affinity-source.patch"
            execute(["git", "apply", "--check", diagnostic_patch], source,
                    output / "diagnostics-check.log", env, 60)
            execute(["git", "apply", diagnostic_patch], source,
                    output / "diagnostics-apply.log", env, 60)
            shutil.copyfile(diagnostic_patch, output / "source-diagnostics.patch")
            before_state = source_state(source)
            save(output / "source-before.json", before_state)
            manifest = (source / "local-cluster" / "Cargo.toml").read_text()
            package = re.search(r"(?ms)^\[package\]\s*(.*?)(?=^\[|\Z)", manifest)
            name = re.search(r'^name\s*=\s*"([^"]+)"', package[1], re.M) if package else None
            if not name or name[1] != "solana-local-cluster":
                raise RuntimeError(f"Unexpected package name in {source}")
            archive = output / f"{label}.tar.zst"
            provenance = {"sha": sha, "source": str(source),
                          "cargo_lock_sha256": digest(source / "Cargo.lock"),
                          "cargo_toml_sha256": digest(source / "Cargo.toml"),
                          "cargo_profile": "ci", "archive": str(archive),
                          "diagnostics_sha256": digest(diagnostic_patch)}
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
            extracted = output / f"{label}-extracted"
            extracted.mkdir()
            provenance["extracted"] = str(extracted)
            common = ["--archive-file", archive, "--workspace-remap", source,
                      "--extract-to", extracted,
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
            matched_suites = [suite for listing in listings
                              for suite in listing["rust-suites"].values()
                              if suite["package-name"] == "solana-local-cluster"
                              and TEST in suite["testcases"]
                              and suite["testcases"][TEST]["filter-match"]["status"] == "matches"]
            if len(matched_suites) != 1:
                raise RuntimeError(f"Unexpected {label} selected binary count")
            binary = Path(matched_suites[0]["binary-path"]).resolve()
            provenance["binary"] = str(binary)
            provenance["binary_sha256"] = digest(binary)
            provenance["selected_tests"] = selected
            save(output / "summary.json", summary)
        retained = set()
        for number, trial in enumerate(schedule, 1):
            if deadline - time.monotonic() < 650 + 300:
                raise RuntimeError("Overall evidence-preserving deadline reached")
            label, arm = trial["source"], trial["cpus"]
            provenance = summary["sources"][label]
            source = Path(provenance["source"])
            binary = Path(provenance["binary"])
            if (digest(binary) != provenance["binary_sha256"]
                    or digest(Path(provenance["archive"])) != provenance["archive_sha256"]
                    or source_state(source) != before_state):
                raise RuntimeError("Source, binary or archive changed before trial")
            affinity = summary["topology"]["arms"][str(arm)]
            stem = f"trial-{number:02d}-{arm}cpu"
            trial_config = output / (stem + '.toml')
            store = output / (stem + '-store')
            trial_config.write_text('[store]\ndir = ' + json.dumps(str(store)) + '\n'
                + CONFIG + '[profile.ci.junit]\npath = "junit.xml"\n'
                'store-success-output = true\nstore-failure-output = true\n')
            # A distinct FARF_DIR makes first-failure retention unambiguous.
            ledger = output / (stem + '-farf')
            ledger.mkdir()
            trial_env = {**env, 'FARF_DIR': str(ledger)}
            junit = store / 'ci/junit.xml'
            log = output / (stem + '.log')
            summary["test_execution_started"] = True
            save(output / 'summary.json', summary)
            result = None
            record = None
            trial_validated = False
            try:
                result = execute([
                    'taskset', '--cpu-list', ','.join(map(str, affinity)),
                    'cargo', 'nextest', 'run', '--archive-file', provenance['archive'],
                    '--workspace-remap', source, '--config-file', trial_config,
                    '--profile', 'ci', '--extract-to', provenance['extracted'],
                    '--extract-overwrite', '--test-threads', '1', '--retries', '0',
                    '--no-tests', 'fail', '--failure-output', 'immediate',
                    '--success-output', 'immediate', '--status-level', 'all',
                    '--final-status-level', 'all', '-E', FILTER,
                ], source, log, trial_env, 650, False, binary=binary, affinity=affinity)
                record = {**trial, **result, 'number': number, 'affinity': affinity,
                          'sha': provenance['sha'],
                          'archive_sha256': provenance['archive_sha256'],
                          'binary_sha256': provenance['binary_sha256']}
                summary['trials'].append(record)
                save(output / 'summary.json', summary)
                record['native'] = native_result(log, junit, result)
                if result.get('observed_run_ids') != [record['native']['run_id']]:
                    raise RuntimeError('Sampled test process and JUnit run UUID disagree')
                if (result['exit_code'] not in (0, 100) or result.get('timed_out')
                        or record['native']['status'] == 'EXECFAIL'
                        or not result.get('affinity_observations')):
                    raise RuntimeError('Trial infrastructure or affinity evidence invalid')
                if (digest(binary) != provenance['binary_sha256']
                        or digest(Path(provenance['archive'])) != provenance['archive_sha256']
                        or source_state(source) != before_state):
                    raise RuntimeError('Source, binary or archive changed during trial')
                trial_validated = True
            finally:
                # Unexpected evidence failures retain this trial's ledgers even
                # when an earlier native failure in the arm was already saved.
                evidence = retain_trial(output, stem, ledger, junit,
                    not trial_validated or result['exit_code'] != 0,
                    arm, retained, env, invalid=not trial_validated)
                if record is not None:
                    record['evidence'] = evidence
                    record['trial_validated'] = trial_validated
                    save(output / 'summary.json', summary)
            print(json.dumps({'event': 'affinity_trial_complete', 'number': number,
                              'pair': trial['pair'], 'cpus': arm,
                              'native': record['native']['status']}), flush=True)
        if len({t['native']['run_id'] for t in summary['trials']}) != 20:
            raise RuntimeError('Expected twenty unique native execution IDs')
        after_state = source_state(source)
        save(output / 'source-after.json', after_state)
        if before_state != after_state:
            raise RuntimeError('Source changed over the experiment')
        for provenance in summary["sources"].values():
            if digest(Path(provenance["archive"])) != provenance["archive_sha256"]:
                raise RuntimeError("Archive changed during the experiment")
        failures = sum(trial["exit_code"] != 0 for trial in summary["trials"])
        summary["failures"] = failures
        summary["complete"] = True
        save(output / "summary.json", summary)
        counts = collections.Counter((t["cpus"], t["exit_code"]) for t in summary["trials"])
        print(json.dumps({"event": "matrix_complete", "failures": failures,
                          "counts": [{"cpus": k[0], "exit_code": k[1], "count": v}
                                     for k, v in sorted(counts.items())],
                          "summary": str(output / "summary.json")}), flush=True)
        return 1 if failures else 0
    except BaseException as error:
        summary["complete"] = False
        summary["error"] = str(error)
        if summary["sources"]:
            try:
                save(output / "source-after.json", source_state(Path(
                    summary["sources"]["jito"]["source"])))
            except Exception as snapshot_error:
                summary["source_after_error"] = str(snapshot_error)
        save(output / "summary.json", summary)
        print(json.dumps({"event": "matrix_stopped", "error": str(error),
                          "summary": str(output / "summary.json")}), flush=True)
        return 130 if isinstance(error, (KeyboardInterrupt, InterruptedError)) else 2


def interrupted(signum, frame):
    raise InterruptedError(f"Received signal {signum}")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
