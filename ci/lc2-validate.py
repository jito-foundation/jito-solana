#!/usr/bin/env python3
"""Temporary LC2 validation: one immutable build, 100 sequential runs, no retries.

Run inside the CI image. All evidence stays in target/lc2-validate/<run-id>.
Exit 78 means CPU placement mismatch before compilation, never a test failure.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

TEST = 'test_duplicate_shreds_broadcast_leader'
FILTER = f'package(=solana-local-cluster) & test(={TEST})'
COUNT = 20
OVERALL_SECONDS = 240 * 60
LOG_FILTER = ('error,solana_core::replay_stage=warn,'
              'solana_local_cluster=info,local_cluster=info,lc2_diagnostic=info')
NEXTEST_TERMINAL_STATUS = re.compile(
    r'^\s*(PASS|FAIL|TIMEOUT|LEAK|EXECFAIL|SIG[A-Z0-9]+)\s+\[[^\]]+\]\s+'
    r'\[\s*(\d+)/(\d+)\]\s+\(\s*1/1\)\s+'
    rf'solana-local-cluster::local_cluster {re.escape(TEST)}\s*$')


def save(path, data):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(path)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def stop(process):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    process.wait()


def proc_status(path):
    return dict((k, v.strip()) for k, v in
                (line.split(':', 1) for line in path.read_text().splitlines()))


def proc_stat(text):
    # comm can contain spaces and parentheses; fields resume after the last ')'.
    pid, opening, rest = text.partition('(')
    name, closing, tail = rest.rpartition(')')
    fields = tail.split()
    if not opening or not closing or len(fields) < 37:
        raise ValueError('Incomplete task stat')
    return {'tid': int(pid), 'name': name, 'state': fields[0],
            'utime_ticks': int(fields[11]), 'stime_ticks': int(fields[12]),
            'starttime_ticks': int(fields[19]), 'last_cpu': int(fields[36])}


def task_sample(path):
    before = proc_stat(path.with_name('stat').read_text())
    row = proc_status(path)
    after = proc_stat(path.with_name('stat').read_text())
    if ((before['tid'], before['starttime_ticks']) !=
            (after['tid'], after['starttime_ticks']) or after['tid'] != int(row['Pid'])):
        raise ValueError('Task identity changed while sampling')
    return {**after, 'affinity': row['Cpus_allowed_list'],
            'voluntary_ctxt_switches': int(row['voluntary_ctxt_switches']),
            'nonvoluntary_ctxt_switches': int(row['nonvoluntary_ctxt_switches']),
            'observed_monotonic_ns': time.monotonic_ns()}


def process_sample(parent, binary):
    """Observe exact test descendants; kernel thread names do not identify stages."""
    started_at, started_ns = time.time(), time.monotonic_ns()
    pressure = {'observed_monotonic_ns': time.monotonic_ns()}
    for resource in ('cpu', 'io'):
        try:
            pressure[resource] = Path(f'/proc/pressure/{resource}').read_text()
        except OSError:
            pressure[resource] = None
    try:
        boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    except OSError:
        boot_id = None
    rows = {}
    for path in Path('/proc').glob('[0-9]*/status'):
        try:
            rows[int(path.parent.name)] = proc_status(path)
        except (OSError, ValueError):
            pass
    descendants = {parent}
    while True:
        added = {pid for pid, row in rows.items()
                 if int(row.get('PPid', 0)) in descendants} - descendants
        if not added:
            break
        descendants.update(added)
    selected = []
    for pid in sorted(descendants):
        try:
            process_path = Path(f'/proc/{pid}')
            if (process_path / 'exe').resolve(strict=True) != binary:
                continue
            identity = proc_stat((process_path / 'stat').read_text())
            row = proc_status(process_path / 'status')
            affinities, pools = Counter(), Counter()
            threads, thread_errors = [], 0
            for path in process_path.glob('task/*/status'):
                try:
                    thread = task_sample(path)
                    affinities[thread['affinity']] += 1
                    pools[(thread['name'], thread['affinity'])] += 1
                    threads.append(thread)
                except (OSError, ValueError, KeyError):
                    # Exited/reused tasks are omitted, never joined to another lifetime.
                    thread_errors += 1
            environment = {}
            for item in (process_path / 'environ').read_bytes().split(b'\0'):
                key, _, value = item.partition(b'=')
                if key in (b'NEXTEST_RUN_ID', b'NEXTEST_ATTEMPT', b'NEXTEST_STRESS_CURRENT', b'NEXTEST_STRESS_TOTAL'):
                    environment[key.decode()] = value.decode(errors='replace')
            after = proc_stat((process_path / 'stat').read_text())
            if ((identity['tid'], identity['starttime_ticks']) !=
                    (after['tid'], after['starttime_ticks']) or
                    (process_path / 'exe').resolve(strict=True) != binary):
                continue
            selected.append({'pid': pid, 'exe': str(binary),
                **{key: row.get(key) for key in ('Name', 'PPid', 'Threads', 'VmRSS',
                                                'Cpus_allowed_list')},
                'starttime_ticks': identity['starttime_ticks'],
                'thread_affinity_counts': dict(affinities),
                'pools': [{'name': key[0], 'affinity': key[1], 'count': count}
                          for key, count in sorted(pools.items())],
                'threads': threads, 'thread_observation_errors': thread_errors,
                'nextest_environment': environment})
        except (OSError, ValueError, KeyError):
            pass
    return {'time': time.time(), 'processes': selected,
            'sample_started_at': started_at, 'sample_started_monotonic_ns': started_ns,
            'sample_elapsed_ns': time.monotonic_ns() - started_ns,
            'boot_id': boot_id, 'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
            'host_pressure': pressure, 'pressure_total_unit': 'microseconds',
            'thread_counter_note': 'Cumulative ticks and context switches; join boot, '
                'process and task start identities. Exited unsampled tasks are missing.',
            'thread_name_note': 'Kernel comm names; generic Tokio names do not identify '
                'a specific runtime or stage.'}


def report_nextest_progress(stream, seen, counts, finished=False):
    """Observe complete native status lines; JUnit remains the acceptance source."""
    while True:
        offset = stream.tell()
        line = stream.readline()
        if not line:
            return
        if not finished and not line.endswith(b'\n'):
            stream.seek(offset)
            return
        native_line = line.decode(errors='replace').strip()
        match = NEXTEST_TERMINAL_STATUS.fullmatch(native_line)
        if match is None:
            continue
        status, iteration, total = match.groups()
        iteration, total = int(iteration), int(total)
        if total != COUNT or not 1 <= iteration <= total or iteration in seen:
            continue
        seen.add(iteration)
        counts[status] += 1
        print(json.dumps({'event': 'nextest_status', 'iteration': iteration,
                          'observed_completed': len(seen), 'total': total,
                          'observed_status_counts': dict(counts),
                          'native_status_line': native_line}), flush=True)


def execute(argv, repo, out, label, env, deadline, timeout, binary=None, check=True):
    argv = [str(arg) for arg in argv]
    log = out / (label + '.log')
    record = {'argv': argv, 'cwd': str(repo), 'started_at': time.time(),
              'timeout_s': timeout, 'log': str(log)}
    start = time.monotonic()
    end = min(deadline, start + timeout)
    if start >= end:
        raise TimeoutError('Overall validation deadline reached')
    print(json.dumps({'event': 'start', 'label': label, **record}), flush=True)
    samples = (out / 'processes.jsonl').open('a') if binary else None
    progress_stream = None
    seen, counts = set(), Counter()
    try:
        with log.open('wb') as stream:
            process = subprocess.Popen(argv, cwd=repo, env=env, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                if label == 'stress':
                    progress_stream = log.open('rb')
                next_sample, next_progress = start, start + 60
                while process.poll() is None:
                    now = time.monotonic()
                    if now >= end:
                        record['timed_out'] = True
                        stop(process)
                        break
                    if samples and now >= next_sample:
                        samples.write(json.dumps(process_sample(process.pid, binary)) + '\n')
                        samples.flush()
                        next_sample = now + 10
                    if now >= next_progress:
                        print(json.dumps({'event': 'running', 'label': label,
                                          'elapsed_s': now - start}), flush=True)
                        next_progress = now + 60
                    if progress_stream:
                        report_nextest_progress(progress_stream, seen, counts)
                    try:
                        process.wait(timeout=max(0, min(1, end - time.monotonic())))
                    except subprocess.TimeoutExpired:
                        pass
            except BaseException:
                record['interrupted'] = True
                stop(process)
                raise
            finally:
                if progress_stream:
                    report_nextest_progress(progress_stream, seen, counts, finished=True)
                record['elapsed_s'] = time.monotonic() - start
                record['process_exit_code'] = process.returncode
                record['exit_code'] = 124 if record.get('timed_out') else process.returncode
                save(out / (label + '.json'), record)
    finally:
        if progress_stream:
            progress_stream.close()
        if samples:
            samples.close()
    print(json.dumps({'event': 'end', 'label': label, **record}), flush=True)
    if check and record['exit_code']:
        raise RuntimeError(f'{label} failed with exit {record["exit_code"]}')
    return record


def source_state(repo, out, label, env, deadline):
    execute(['git', 'rev-parse', 'HEAD'], repo, out, label + '-sha', env, deadline, 60)
    execute(['git', 'diff', '--binary', 'HEAD', '--'], repo, out,
            label + '-dirty-diff', env, deadline, 60)
    execute(['git', 'status', '--porcelain=v1', '--untracked-files=all'], repo, out,
            label + '-status', env, deadline, 60)
    execute(['git', 'ls-files', '--others', '--exclude-standard', '-z'], repo, out,
            label + '-untracked', env, deadline, 60)
    untracked = {}
    for name in (out / (label + '-untracked.log')).read_bytes().split(b'\0'):
        if name:
            path = repo / os.fsdecode(name)
            untracked[os.fsdecode(name)] = sha256(path) if path.is_file() else None
    state = {'sha': (out / (label + '-sha.log')).read_text().strip(),
             'dirty_diff_sha256': sha256(out / (label + '-dirty-diff.log')),
             'tracked_clean': (out / (label + '-dirty-diff.log')).stat().st_size == 0,
             'untracked_sha256': untracked, 'cargo_lock_sha256': sha256(repo / 'Cargo.lock')}
    execute(['git', 'submodule', 'status', '--recursive'], repo, out,
            label + '-submodules', env, deadline, 60)
    state['submodules'] = (out / (label + '-submodules.log')).read_text()
    if any(line[:1] in ('-', '+', 'U') for line in state['submodules'].splitlines()):
        raise RuntimeError('Missing or mismatched pinned submodule; initialize it before validation')
    return state


def facts(out, env):
    result = {'time': time.time(), 'hostname': os.uname().nodename,
              'uname': list(os.uname()), 'cpu_count': os.cpu_count(),
              'effective_cpus': sorted(os.sched_getaffinity(0)),
              'environment': {key: env.get(key) for key in (
                  'BUILDKITE_AGENT_NAME', 'BUILDKITE_JOB_ID', 'BUILDKITE_COMMIT',
                  'BUILDKITE_BUILD_ID', 'BUILDKITE_BUILD_NUMBER', 'BUILDKITE_BUILD_URL',
                  'RUSTFLAGS', 'RUSTC_WRAPPER', 'RUST_BACKTRACE', 'RUSTUP_TOOLCHAIN',
                  'SOLANA_RAYON_THREADS', 'SOLANA_MAX_RAYON_THREADS', 'RAYON_NUM_THREADS',
                  'CARGO_BUILD_JOBS', 'RUST_LOG', 'TOKIO_WORKER_THREADS')}}
    for name in ('/proc/self/status', '/proc/meminfo', '/proc/cpuinfo', '/proc/loadavg',
                 '/proc/pressure/cpu', '/proc/pressure/memory', '/proc/pressure/io',
                 '/proc/self/cgroup', '/sys/fs/cgroup/cpuset.cpus.effective',
                 '/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/memory.max'):
        path = Path(name)
        if path.exists():
            result[name] = path.read_text()
    save(out / 'facts.json', result)
    return result


def event_counts(text):
    lines = text.splitlines()
    counts = {tag: sum(tag in line for line in lines) for tag in (
        'duplicate-batch', 'duplicate-variants', 'lc2-purge', 'lc2-dead',
        'lc2-empty-emitted', 'lc2-prefix-emitted', 'lc2-recovered',
        'BlockAborted(', 'ChainedBlockIdFailure(', 'more than 10 times')}
    counts['solitary_final_tick_batches'] = sum(
        'duplicate-batch' in line and re.search(r'\bentries=1\b', line) is not None
        for line in lines)
    counts['empty_prefix_emissions'] = sum(
        'lc2-prefix-emitted' in line and re.search(r'\bentries=0\b', line) is not None
        for line in lines) if counts['lc2-prefix-emitted'] else None
    purges, recoveries = {}, []
    for line in lines:
        if 'lc2-purge' in line:
            ledger = re.search(r'\bledger="([^"]+)"', line)
            slot = re.search(r'\bslot=(\d+)', line)
            expected = re.search(r'\bexpected=(\w+)', line)
            if ledger and slot and expected:
                purges[(ledger[1], int(slot[1]))] = expected[1]
        if 'lc2-recovered' in line:
            ledger = re.search(r'\bledger="([^"]+)"', line)
            slot = re.search(r'\bslot=(\d+)', line)
            recovered = re.search(r'\bhash=(\w+)', line)
            if ledger and slot and recovered:
                key = (ledger[1], int(slot[1]))
                recoveries.append({'ledger': key[0], 'slot': key[1],
                    'hash': recovered[1],
                    'matches_prior_purge': purges.get(key) == recovered[1]})
    counts['recovery_records'] = recoveries
    counts['verified_recovered_node_slots'] = len({(r['ledger'], r['slot'])
        for r in recoveries if r['matches_prior_purge']})
    return counts


def junit_summary(path):
    if not path.exists():
        return {'available': False, 'trials': []}
    root = ET.parse(path).getroot()
    trials = []
    for suite in root.findall('testsuite'):
        for case in suite.findall('testcase'):
            if case.get('name') != TEST:
                raise RuntimeError('JUnit includes an unexpected test')
            failure = case.find('failure')
            error = case.find('error')
            passed = failure is None and error is None and case.find('skipped') is None
            captured = '\n'.join(case.findtext(name, '') for name in ('system-out', 'system-err'))
            trials.append({'suite': suite.get('name'), 'run_id': root.get('uuid'),
                           'trial_id': root.get('uuid', '') + ':' + suite.get('name', ''),
                           'stress_iteration': int(suite.get('name').rsplit('@stress-', 1)[1]) + 1,
                           'test': case.get('name'), 'timestamp': case.get('timestamp'),
                           'seconds': float(case.get('time', '0')), 'passed': passed,
                           'events': event_counts(captured)})
    return {'available': True, 'run_id': root.get('uuid'), 'trials': trials,
            'passed': sum(t['passed'] for t in trials),
            'failed': sum(not t['passed'] for t in trials)}


def compress_evidence(out):
    files = []
    for path in sorted(out.rglob('*')):
        if path.is_file() and path.suffix in ('.log', '.jsonl', '.xml'):
            # Extracted Cargo/archive metadata remains in its immutable build tree.
            if 'extracted' in path.relative_to(out).parts:
                continue
            compressed = path.with_name(path.name + '.gz')
            with path.open('rb') as source, gzip.open(compressed, 'wb') as target:
                shutil.copyfileobj(source, target)
            files.append({'path': str(compressed), 'sha256': sha256(compressed),
                          'uncompressed_sha256': sha256(path), 'bytes': compressed.stat().st_size})
    save(out / 'compressed-artifacts.json', files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-cpus', type=int, choices=(48, 128), required=True)
    parser.add_argument('--expected-sha', help='Require this exact checked-out commit')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%SZ-') + uuid.uuid4().hex[:8]
    out = repo / 'target' / 'lc2-validate' / run_id
    out.mkdir(parents=True)
    deadline = time.monotonic() + OVERALL_SECONDS
    env = os.environ.copy()
    env.pop('CARGO_ENCODED_RUSTFLAGS', None)
    env.update(RUSTFLAGS='-D warnings', RUST_BACKTRACE='1', RUST_LOG=LOG_FILTER,
               CARGO_TERM_COLOR='never', NEXTEST_USER_CONFIG_FILE='none',
               FARF_DIR=str(out / 'ledgers'))
    summary = {'driver_run_id': run_id, 'output': str(out), 'requested_runs': COUNT,
               'expected_cpus': args.expected_cpus, 'complete': False, 'coverage_complete': False,
               'outcomes_complete': False, 'exit_code': 2,
               'driver_sha256': sha256(Path(__file__)), 'overall_timeout_s': OVERALL_SECONDS}
    save(out / 'summary.json', summary)
    print(json.dumps({'event': 'validation_start', **summary}), flush=True)
    try:
        if sys.platform != 'linux' or not hasattr(os, 'sched_getaffinity'):
            raise RuntimeError('Validation must run inside the Linux CI container')
        summary['facts'] = facts(out, env)
        if len(summary['facts']['effective_cpus']) != args.expected_cpus:
            summary.update(exit_code=78, reason='cpu_placement_mismatch', test_execution_started=False)
            return 78
        for argv, label in ((['rustc', '--version'], 'rustc'),
                            (['cargo', 'nextest', '--version'], 'nextest')):
            execute(argv, repo, out, label, env, deadline, 60)
        for command in ('archive', 'list', 'run'):
            execute(['cargo', 'nextest', command, '--help'], repo, out,
                    'nextest-' + command + '-help', env, deadline, 60)
        help_text = (out / 'nextest-run-help.log').read_text()
        if not all(flag in help_text for flag in ('--stress-count', '--fail-fast', '--retries')):
            raise RuntimeError('Installed nextest lacks required stress controls')
        summary['source'] = source_state(repo, out, 'source', env, deadline)
        summary['instrumentation'] = {}
        for relative in ('turbine/src/broadcast_stage/broadcast_duplicates_run.rs',
                         'core/src/replay_stage.rs',
                         'core/src/repair/cluster_slot_state_verifier.rs'):
            path = repo / relative
            if path.exists():
                source = path.read_text()
                summary['instrumentation'][relative] = {
                    'sha256': sha256(path),
                    'markers': {marker: marker in source for marker in (
                        'duplicate-batch', 'duplicate-variants', 'lc2-purge',
                        'lc2-recovered', 'lc2-prefix-emitted', 'lc2-empty-emitted')}}
        if not summary['source']['tracked_clean']:
            raise RuntimeError('Tracked source differs from HEAD; commit the reviewed source before validation')
        if args.expected_sha and summary['source']['sha'] != args.expected_sha:
            raise RuntimeError('Checked-out revision does not match --expected-sha')
        package = re.search(r'(?ms)^\[package\]\s*(.*?)(?=^\[|\Z)',
                            (repo / 'local-cluster/Cargo.toml').read_text())
        name = re.search(r'^name\s*=\s*"([^"]+)"', package[1], re.M) if package else None
        if not name or name[1] != 'solana-local-cluster':
            raise RuntimeError('Unexpected local-cluster Cargo package name')
        config = out / 'nextest.toml'
        config.write_text('[store]\ndir = ' + json.dumps(str(out / 'store')) + '''
[profile.ci]
retries = 0
test-threads = 1
slow-timeout = { period = "60s", terminate-after = 10 }
[profile.ci.junit]
path = "junit.xml"
store-success-output = true
store-failure-output = true
''')
        archive = out / 'local-cluster.tar.zst'
        execute(['cargo', 'nextest', 'archive', '--locked', '--cargo-profile', 'ci',
                 '-p', name[1], '--test', 'local_cluster', '--archive-file', archive,
                 '--config-file', config, '--profile', 'ci'],
                repo, out, 'compile', env, deadline, 1800)
        archive.chmod(0o444)
        summary['archive_sha256'] = sha256(archive)
        extracted = out / 'extracted'
        extracted.mkdir()
        common = ['--archive-file', archive, '--workspace-remap', repo,
                  '--extract-to', extracted, '--config-file', config, '--profile', 'ci',
                  '-E', FILTER]
        execute(['cargo', 'nextest', 'list', *common, '--message-format', 'json'],
                repo, out, 'selected-tests', env, deadline, 120)
        listing = next(json.loads(line) for line in (out / 'selected-tests.log').read_text().splitlines()
                       if line.startswith('{"rust-build-meta"'))
        selected = [(suite, test) for suite in listing['rust-suites'].values()
                    for test, data in suite['testcases'].items()
                    if data['filter-match']['status'] == 'matches' and not data['ignored']]
        if len(selected) != 1 or selected[0][1] != TEST or selected[0][0]['package-name'] != name[1]:
            raise RuntimeError('Selection is not exactly the requested LC2 test')
        binary = Path(selected[0][0]['binary-path']).resolve()
        summary['binary'] = {'path': str(binary), 'sha256': sha256(binary),
                             'suite': selected[0][0]['binary-id']}
        before = source_state(repo, out, 'pre-run-source', env, deadline)
        if before != summary['source']:
            raise RuntimeError('Source changed while compiling the archive')
        save(out / 'summary.json', summary)
        summary['test_execution_started'] = True
        result = execute(['cargo', 'nextest', 'run', *common, '--extract-overwrite',
            '--stress-count', str(COUNT), '--fail-fast', '--retries', '0', '--test-threads', '1',
            '--no-tests', 'fail', '--success-output', 'immediate', '--failure-output', 'immediate',
            '--status-level', 'all', '--final-status-level', 'none'],
            repo, out, 'stress', env, deadline, OVERALL_SECONDS, binary=binary, check=False)
        summary['stress'] = result
        summary['junit'] = junit_summary(out / 'store/ci/junit.xml')
        summary['coverage'] = {
            'block_aborted_log_mentions': sum(t['events']['BlockAborted(']
                for t in summary['junit']['trials']),
            'completed_tests_with_verified_recovery': sum(t['passed'] and
                t['events']['verified_recovered_node_slots'] > 0
                for t in summary['junit']['trials']),
            'observation_limit': 'Counts reflect captured logs and recorded source markers; '
                'solitary final ticks do not imply emitted empty prefixes.'}
        summary['binary_unchanged'] = sha256(binary) == summary['binary']['sha256']
        summary['archive_unchanged'] = sha256(archive) == summary['archive_sha256']
        trials = summary['junit']['trials']
        summary['coverage_complete'] = (len(trials) == COUNT and all(
            t['passed'] and t['events']['verified_recovered_node_slots'] >= 1
            and t['events']['BlockAborted('] == 0 for t in trials))
        summary['outcomes_complete'] = (result['exit_code'] == 0 and len(trials) == COUNT and
                               all(t['passed'] for t in trials) and
                               sorted(t['stress_iteration'] for t in trials) == list(range(1, COUNT + 1))
                               and bool(summary['junit'].get('run_id')) and summary['binary_unchanged']
                               and summary['archive_unchanged'])
        summary['complete'] = summary['outcomes_complete'] and summary['coverage_complete']
        if summary['outcomes_complete'] and not summary['coverage_complete']:
            summary['reason'] = 'recovery_coverage_incomplete'
        summary['exit_code'] = 0 if summary['complete'] else (result['exit_code'] or 2)
        return summary['exit_code']
    except BaseException as error:
        summary['error'] = str(error)
        summary['exit_code'] = 130 if isinstance(error, (KeyboardInterrupt, InterruptedError)) else 2
        return summary['exit_code']
    finally:
        # Retain native JUnit even after interruption; never synthesize missing passes.
        if 'junit' not in summary:
            try:
                summary['junit'] = junit_summary(out / 'store/ci/junit.xml')
            except Exception as error:
                summary['junit_error'] = str(error)
        save(out / 'summary.json', summary)
        compress_evidence(out)
        print(json.dumps({'event': 'validation_end', 'output': str(out),
                          'complete': summary['complete'], 'exit_code': summary['exit_code'],
                          'reason': summary.get('reason'), 'error': summary.get('error')}), flush=True)


def interrupted(signum, frame):
    raise InterruptedError(f'Received signal {signum}')


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
