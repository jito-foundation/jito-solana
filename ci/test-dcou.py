#!/usr/bin/env python3
"""Exercise the release installer's DCOU checks without compiling Rust."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

# In the fixture, this file is Cargo: record arguments and supply a graph.
if Path(sys.argv[0]).name == "cargo":
    args = sys.argv[1:]
    with open("calls", "a") as log:
        log.write(json.dumps(args) + "\n")
    if "--unit-graph" in args:
        group = "DEVELOPMENT" if "--manifest-path" in args else "PRODUCTION"
        graph, status = json.loads(os.environ[f"DCOU_TEST_{group}"])
        print(graph)
        sys.exit(status)
    sys.exit(0)

root = Path(__file__).resolve().parent.parent
clean = '{"units":[{"features":[]}]}'
dcou = '{"units":[{"features":[]},{"features":["dev-context-only-utils"]}]}'
with tempfile.TemporaryDirectory(prefix="dcou-test-") as directory:
    fixture = Path(directory)
    (fixture / "scripts").mkdir()
    for name in ("cargo-install-all.sh", "agave-build-lists.sh"):
        shutil.copy2(root / "scripts" / name, fixture / "scripts" / name)
    shutil.copyfile(__file__, fixture / "cargo")
    # Installing fake binaries adds no coverage; leave copying to other tests.
    (fixture / "cp").write_text("#!/bin/sh\nexit 0\n")
    for name in ("cargo", "cp"):
        (fixture / name).chmod(0o755)
    exclusions = subprocess.check_output([
        "bash", "-c", 'source scripts/agave-build-lists.sh; '
        'printf "%s\\n" "${DCOU_TAINTED_PACKAGES[@]}"',
    ], cwd=fixture, text=True).splitlines()

    def run(*flags, production=(clean, 0), development=(dcou, 0), ok=True):
        (fixture / "calls").write_text("")
        result = subprocess.run([
            str(fixture / "scripts/cargo-install-all.sh"),
            "--no-spl-token", "--no-build-platform-tools", *flags,
            str(fixture / "install"),
        ], env={**os.environ, "PATH": f"{fixture}:{os.environ['PATH']}",
                "DCOU_TEST_PRODUCTION": json.dumps(production),
                "DCOU_TEST_DEVELOPMENT": json.dumps(development)},
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert (result.returncode == 0) == ok, (flags, production, development, result.stdout)
        calls = [json.loads(line) for line in (fixture / "calls").read_text().splitlines()]
        if not ok or "--dcou-check-only" in flags:
            assert calls and all("--unit-graph" in call for call in calls), result.stdout
        if not ok:
            assert ("Failed to obtain DCOU unit graph." in result.stdout
                    or "Invalid DCOU unit graph" in result.stdout), result.stdout
        return calls

    for profile in ("release", "release-with-debug", "release-with-lto"):
        calls = run(*([] if profile == "release" else [f"--{profile}"]))
        assert len(calls) == 4, calls
        for check, build in zip(calls[:2], calls[2:]):
            assert "--unit-graph" in check, check
            assert [arg for arg in check if arg not in (
                "-Z", "unstable-options", "--unit-graph")] == build, calls
            assert build[build.index("--profile") + 1] == profile, build
        production, development = calls[2:]
        assert "--workspace" in production and "--manifest-path" not in production
        assert "--workspace" not in development
        assert development[development.index("--manifest-path") + 1] == "dev-bins/Cargo.toml"
        assert [production[i + 1] for i, arg in enumerate(production)
                if arg == "--exclude"] == exclusions, production

    run("--dcou-check-only")
    run("--dcou-check-only", "--no-build-dcou-bins")
    bad_graphs = [dcou, "not-json", "", '{"units":[]}', '{"units":[{}]}',
                  '{"units":[{"features":"dev-context-only-utils"}]}', clean + clean]
    # A Cargo error must fail even when stdout contains a valid clean graph.
    for response in [(graph, 0) for graph in bad_graphs] + [(clean, 42)]:
        run(production=response, ok=False)
        run("--no-build-dcou-bins", production=response, ok=False)
    for response in [(clean, 0), (clean, 42), ("not-json", 0), ("", 0)]:
        run(development=response, ok=False)
    run("--dcou-check-only", production=(dcou, 0), ok=False)

print("DCOU regression tests passed.")
