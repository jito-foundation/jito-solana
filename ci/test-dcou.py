#!/usr/bin/env python3
"""Test the release installer's DCOU checks without compiling Rust.

A fake Cargo records every invocation and returns a graph chosen by each test.
That lets us compare the check and build commands, and exercise error handling.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
CLEAN_GRAPH = json.dumps({"units": [{"features": []}]})
DCOU_GRAPH = json.dumps({
    "units": [
        {"features": []},
        {"features": ["dev-context-only-utils"]},
    ],
})


class DcouInstallerTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory(prefix="dcou-test-")
        self.addCleanup(temporary_directory.cleanup)
        self.fixture = Path(temporary_directory.name)
        (self.fixture / "scripts").mkdir()

        for name in ("cargo-install-all.sh", "agave-build-lists.sh"):
            shutil.copy2(
                REPO_ROOT / "scripts" / name,
                self.fixture / "scripts" / name,
            )
        shutil.copyfile(
            REPO_ROOT / "ci/fixtures/dcou/cargo.py",
            self.fixture / "cargo",
        )

        # There are no compiled binaries to install in this fixture.
        (self.fixture / "cp").write_text("#!/bin/sh\nexit 0\n")
        for name in ("cargo", "cp"):
            (self.fixture / name).chmod(0o755)

        self.calls_file = self.fixture / "cargo-calls.jsonl"
        self.expected_exclusions = subprocess.check_output(
            [
                "bash",
                "-c",
                'source scripts/agave-build-lists.sh; '
                'printf "%s\\n" "${DCOU_TAINTED_PACKAGES[@]}"',
            ],
            cwd=self.fixture,
            text=True,
        ).splitlines()

    def run_installer(
        self,
        *flags,
        production_graph=CLEAN_GRAPH,
        development_graph=DCOU_GRAPH,
        production_status=0,
        development_status=0,
        expect_success=True,
    ):
        """Run the real installer with fake Cargo responses and return its calls."""
        self.calls_file.write_text("")
        environment = os.environ.copy()
        environment.update({
            "PATH": f"{self.fixture}:{environment['PATH']}",
            "DCOU_TEST_PRODUCTION_GRAPH": production_graph,
            "DCOU_TEST_PRODUCTION_STATUS": str(production_status),
            "DCOU_TEST_DEVELOPMENT_GRAPH": development_graph,
            "DCOU_TEST_DEVELOPMENT_STATUS": str(development_status),
        })
        result = subprocess.run(
            [
                str(self.fixture / "scripts/cargo-install-all.sh"),
                "--no-spl-token",
                "--no-build-platform-tools",
                *flags,
                str(self.fixture / "install"),
            ],
            cwd=self.fixture,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        calls = [
            json.loads(line)
            for line in self.calls_file.read_text().splitlines()
        ]

        if expect_success:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertRegex(
                result.stdout,
                r"Failed to obtain DCOU unit graph\.|Invalid DCOU unit graph",
            )
            self.assert_only_graph_checks(calls)
        return calls

    def assert_only_graph_checks(self, calls):
        self.assertTrue(calls, "The installer never called Cargo")
        for call in calls:
            self.assertIn("--unit-graph", call, f"Unexpected compilation: {call}")

    def assert_matching_build_arguments(self, check, build):
        self.assertIn("--unit-graph", check)
        graph_options = ("-Z", "unstable-options", "--unit-graph")
        checked_build_arguments = [
            argument for argument in check if argument not in graph_options
        ]
        self.assertEqual(
            checked_build_arguments,
            build,
            "The DCOU check and actual compile must use the same arguments",
        )

    def test_release_profiles_check_the_same_arguments_they_compile(self):
        profiles = [
            ("release", []),
            ("release-with-debug", ["--release-with-debug"]),
            ("release-with-lto", ["--release-with-lto"]),
        ]
        for profile, flags in profiles:
            with self.subTest(profile=profile):
                calls = self.run_installer(*flags)
                self.assertEqual(len(calls), 4, calls)
                (
                    production_check,
                    development_check,
                    production_build,
                    development_build,
                ) = calls

                self.assert_matching_build_arguments(production_check, production_build)
                self.assert_matching_build_arguments(
                    development_check, development_build
                )
                for build in (production_build, development_build):
                    self.assertEqual(build[build.index("--profile") + 1], profile)

                self.assertIn("--workspace", production_build)
                self.assertNotIn("--manifest-path", production_build)
                exclusions = [
                    production_build[index + 1]
                    for index, argument in enumerate(production_build)
                    if argument == "--exclude"
                ]
                self.assertEqual(exclusions, self.expected_exclusions)

                self.assertNotIn("--workspace", development_build)
                manifest_option = development_build.index("--manifest-path")
                self.assertEqual(
                    development_build[manifest_option + 1], "dev-bins/Cargo.toml"
                )

    def test_check_only_never_compiles(self):
        for flags in ([], ["--no-build-dcou-bins"]):
            with self.subTest(flags=flags):
                calls = self.run_installer("--dcou-check-only", *flags)
                self.assert_only_graph_checks(calls)

    def test_production_rejects_dcou(self):
        for flags in ([], ["--no-build-dcou-bins"], ["--dcou-check-only"]):
            with self.subTest(flags=flags):
                self.run_installer(
                    *flags,
                    production_graph=DCOU_GRAPH,
                    expect_success=False,
                )

    def test_production_rejects_invalid_graphs(self):
        invalid_graphs = {
            "invalid JSON": "not-json",
            "empty output": "",
            "no units": '{"units":[]}',
            "missing features": '{"units":[{}]}',
            "features is not an array": json.dumps({
                "units": [{"features": "dev-context-only-utils"}],
            }),
            "multiple JSON documents": CLEAN_GRAPH + CLEAN_GRAPH,
        }
        for description, graph in invalid_graphs.items():
            for flags in ([], ["--no-build-dcou-bins"]):
                with self.subTest(graph=description, flags=flags):
                    self.run_installer(
                        *flags,
                        production_graph=graph,
                        expect_success=False,
                    )

    def test_cargo_errors_are_rejected_even_with_valid_json(self):
        for flags in ([], ["--no-build-dcou-bins"]):
            with self.subTest(group="production", flags=flags):
                self.run_installer(
                    *flags,
                    production_graph=CLEAN_GRAPH,
                    production_status=42,
                    expect_success=False,
                )

        with self.subTest(group="development"):
            self.run_installer(
                development_graph=CLEAN_GRAPH,
                development_status=42,
                expect_success=False,
            )

    def test_development_requires_a_valid_graph_with_dcou(self):
        invalid_responses = {
            "DCOU is missing": CLEAN_GRAPH,
            "invalid JSON": "not-json",
            "empty output": "",
        }
        for description, graph in invalid_responses.items():
            with self.subTest(graph=description):
                self.run_installer(
                    development_graph=graph,
                    expect_success=False,
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
