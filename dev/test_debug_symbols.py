#!/usr/bin/env python3
"""Bounded regression tests for ./f and the Dockerfile's symbol contract.

Run with: python3 dev/test_debug_symbols.py
Uses temporary repositories and mock Docker; never touches real build outputs.
The ELF checks additionally require a native C compiler and GNU binutils.
"""

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "dev/Dockerfile").read_text()


def run(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, **kwargs)


def docker_function(name):
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}", DOCKERFILE, re.M | re.S)
    if match is None:
        raise AssertionError(f"Cannot find {name} in dev/Dockerfile")
    return match.group(0)


def clean_flag_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CFLAGS", "CXXFLAGS"}
        and not key.startswith(("CARGO_TARGET_", "CARGO_PROFILE_RELEASE_WITH_DEBUG_"))
    }


class WrapperTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="jito-symbol-cli-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.repo = Path(cls.temp.name) / "repo"
        cls.repo.mkdir()
        for relative in (
            "f", "Cargo.toml", "rust-toolchain.toml", "dev/Dockerfile",
            "scripts/read-cargo-variable.sh",
        ):
            destination = cls.repo / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
        for args in (
            ["init", "--quiet"],
            ["config", "user.name", "Debug symbols fixture"],
            ["config", "user.email", "fixture@example.invalid"],
            ["add", "."],
            ["-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Fixture"],
            ["tag", "fixture-tag"],
        ):
            subprocess.run(["git", "-C", str(cls.repo), *args], check=True,
                           capture_output=True)
        cls.sha = subprocess.check_output(
            ["git", "-C", str(cls.repo), "rev-parse", "HEAD"], text=True
        ).strip()
        cls.mock_bin = Path(cls.temp.name) / "mock-bin"
        cls.mock_bin.mkdir()
        docker = cls.mock_bin / "docker"
        docker.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys\n"
            "with open(os.environ['DOCKER_CALLS'], 'a') as log:\n"
            "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "if sys.argv[1:3] == ['buildx', 'build']:\n"
            "    args = sys.argv[3:]\n"
            "    output = args[args.index('--output') + 1].split('dest=', 1)[1]\n"
            "    loose = pathlib.Path(output) / 'docker-output'\n"
            "    loose.mkdir(parents=True)\n"
            "    (loose / 'agave-validator').write_text('fixture binary')\n"
        )
        docker.chmod(0o755)

    def setUp(self):
        self.calls_path = Path(self.temp.name) / "calls.jsonl"
        self.calls_path.unlink(missing_ok=True)
        self.env = os.environ.copy()
        self.env["PATH"] = f"{self.mock_bin}:{self.env['PATH']}"
        self.env["DOCKER_CALLS"] = str(self.calls_path)
        self.env.pop("CI_COMMIT", None)

    def invoke(self, *args):
        # f moves mock loose exports into the temporary repository each time.
        output = Path(tempfile.mkdtemp(prefix="output-", dir=self.temp.name))
        self.calls_path.unlink(missing_ok=True)
        result = run(["bash", str(self.repo / "f"), "--output", str(output), *args],
                     cwd=self.repo, env=self.env)
        calls = [json.loads(line) for line in self.calls_path.read_text().splitlines()] \
            if self.calls_path.exists() else []
        return result, calls

    def successful_build(self, *args):
        result, calls = self.invoke(*args)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        builds = [call[2:] for call in calls if call[:2] == ["buildx", "build"]]
        self.assertEqual(len(builds), 1)
        build_args = builds[0]
        values = dict(
            build_args[index + 1].split("=", 1)
            for index, flag in enumerate(build_args) if flag == "--build-arg"
        )
        return result, build_args, values

    def test_symbol_alias_and_profile_matrix(self):
        cases = (
            ([], "release", False),
            (["--profile", "debug"], "debug", False),
            (["--profile", "release-with-lto"], "release-with-lto", False),
            (["--debug-symbols"], "release-with-debug", True),
            (["--debug-symbols", "--debug-symbols"], "release-with-debug", True),
            (["--profile", "release", "--debug-symbols"], "release-with-debug", True),
            (["--debug-symbols", "--profile", "release"], "release-with-debug", True),
            (["--profile", "release-with-debug"], "release-with-debug", True),
            (["--profile", "release-with-debug", "--debug-symbols"], "release-with-debug", True),
            (["--debug-symbols", "--profile", "release-with-debug"], "release-with-debug", True),
        )
        for args, profile, symbols in cases:
            with self.subTest(args=args):
                result, _, values = self.successful_build("--tag", "same-tag", *args)
                self.assertEqual(values["BUILD_PROFILE"], profile)
                self.assertEqual(values["CHANNEL_OR_TAG"], "same-tag")
                self.assertEqual(values["TARBALL_BASENAME"],
                                 "jito-solana-release-same-tag" +
                                 ("-debug-symbols" if symbols else ""))
                self.assertEqual("debug symbols  : full, embedded" in result.stdout, symbols)
                self.assertEqual(values["CI_COMMIT"], self.sha)

    def test_conflicts_fail_before_checkout_or_docker(self):
        before = run(["git", "worktree", "list", "--porcelain"], cwd=self.repo).stdout
        for profile in ("debug", "release-with-lto"):
            for args in (["--profile", profile, "--debug-symbols"],
                         ["--debug-symbols", "--profile", profile]):
                with self.subTest(args=args):
                    result, calls = self.invoke(*args, "--checkout", "missing-ref")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("incompatible", result.stderr)
                    self.assertNotIn("creating worktree", result.stdout)
                    self.assertEqual(calls, [])
        after = run(["git", "worktree", "list", "--porcelain"], cwd=self.repo).stdout
        self.assertEqual(after, before)

    def test_unknown_profile_still_rejected(self):
        result, calls = self.invoke("--debug-symbols", "--profile", "invalid")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid --profile", result.stderr)
        self.assertEqual(calls, [])

    def test_custom_tag_platform_features_and_cache_arguments_preserved(self):
        _, args, values = self.successful_build(
            "--debug-symbols", "--tag", "branch/tag", "--basename", "custom",
            "--tip-router", "--no-val-bins", "--platform", "linux/arm64",
            "--no-cache", "--pull-cache", "registry/pull:cache",
            "--push-cache", "registry/push:cache", "--progress", "quiet",
        )
        self.assertEqual(values["CHANNEL_OR_TAG"], "branch/tag")
        self.assertEqual(values["TARBALL_BASENAME"], "custom-branch_tag-debug-symbols")
        self.assertEqual(values["INCLUDE_TIP_ROUTER"], "1")
        self.assertEqual(values["INCLUDE_VAL_BINS"], "0")
        self.assertEqual(args[args.index("--platform") + 1], "linux/arm64")
        self.assertEqual(args[args.index("--progress") + 1], "quiet")
        self.assertIn("--no-cache", args)
        self.assertIn("type=registry,ref=registry/pull:cache", args)
        self.assertIn("type=registry,ref=registry/push:cache,mode=max", args)
        self.assertTrue((self.repo / "docker-output/agave-validator").exists())

    def test_checkout_uses_current_dockerfile_and_cleans_worktree(self):
        before = run(["git", "worktree", "list", "--porcelain"], cwd=self.repo).stdout
        _, args, values = self.successful_build("--debug-symbols", "--checkout", "fixture-tag")
        self.assertEqual(values["CHANNEL_OR_TAG"], "fixture-tag")
        self.assertEqual(values["CI_COMMIT"], self.sha)
        self.assertEqual(values["TARBALL_BASENAME"],
                         "jito-solana-release-fixture-tag-debug-symbols")
        self.assertEqual(args[args.index("--file") + 1], str(self.repo / "dev/Dockerfile"))
        self.assertNotEqual(args[-1], ".")
        self.assertFalse(Path(args[-1]).exists())
        after = run(["git", "worktree", "list", "--porcelain"], cwd=self.repo).stdout
        self.assertEqual(after, before)

    def test_help_documents_alias(self):
        result, calls = self.invoke("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("--debug-symbols", result.stdout)
        self.assertIn("-debug-symbols before _<target>", result.stdout)
        self.assertEqual(calls, [])


class FlagTests(unittest.TestCase):
    def configured(self, values=None, profile="release-with-debug", target="x86_64-unknown-linux-gnu"):
        env = clean_flag_env()
        env.update(BUILD_PROFILE=profile, TARGET_TRIPLE=target)
        env.update(values or {})
        dump = (
            "import json, os; print(json.dumps({k:v for k,v in os.environ.items() "
            "if k.startswith(('CARGO_TARGET_', 'CARGO_PROFILE_RELEASE_WITH_DEBUG_')) "
            "or k in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CFLAGS', 'CXXFLAGS')}))"
        )
        result = run(
            ["bash", "-euo", "pipefail", "-c", docker_function("configure_debug_symbols") +
             "\nconfigure_debug_symbols\n" + shlex.quote(sys.executable) + " -c " + shlex.quote(dump)],
            env=env,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_profile_contract_and_target_flags(self):
        key = "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS"
        flags = self.configured({key: "--cfg fixture -Cforce-frame-pointers=no"})
        self.assertEqual(flags[key], "--cfg fixture -Cforce-frame-pointers=no -Cforce-frame-pointers=yes")
        self.assertNotIn("RUSTFLAGS", flags)
        self.assertNotIn("CARGO_ENCODED_RUSTFLAGS", flags)
        self.assertEqual(flags["CARGO_PROFILE_RELEASE_WITH_DEBUG_DEBUG"], "2")
        self.assertEqual(flags["CARGO_PROFILE_RELEASE_WITH_DEBUG_STRIP"], "none")
        self.assertEqual(flags["CARGO_PROFILE_RELEASE_WITH_DEBUG_SPLIT_DEBUGINFO"], "off")
        self.assertEqual(flags["CFLAGS"], "-g -fno-omit-frame-pointer")
        self.assertEqual(flags["CXXFLAGS"], "-g -fno-omit-frame-pointer")

    def test_arm_target_variable(self):
        flags = self.configured(target="aarch64-unknown-linux-gnu")
        self.assertEqual(flags["CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS"].strip(),
                         "-Cforce-frame-pointers=yes")
        self.assertNotIn("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS", flags)

    def test_plain_rustflags_preserve_active_source_including_empty(self):
        for original in ("", "--cfg fixture -Cforce-frame-pointers=no"):
            with self.subTest(original=original):
                flags = self.configured({"RUSTFLAGS": original})
                self.assertEqual(flags["RUSTFLAGS"],
                                 original + (" " if original else "") + "-Cforce-frame-pointers=yes")
                self.assertNotIn("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS", flags)

    def test_encoded_rustflags_preserve_boundaries_and_win_even_when_empty(self):
        for original in ("", '--cfg\x1ffixture="has space"\x1f-Cforce-frame-pointers=no'):
            with self.subTest(original=original):
                flags = self.configured({"CARGO_ENCODED_RUSTFLAGS": original,
                                         "RUSTFLAGS": "--cfg inactive"})
                self.assertEqual(flags["CARGO_ENCODED_RUSTFLAGS"],
                                 original + ("\x1f" if original else "") + "-Cforce-frame-pointers=yes")
                self.assertEqual(flags["RUSTFLAGS"], "--cfg inactive")
                self.assertNotIn("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS", flags)

    def test_native_flags_preserved(self):
        flags = self.configured({"CFLAGS": "-O2 -DFIXTURE_C -fomit-frame-pointer",
                                 "CXXFLAGS": "-O3 -DFIXTURE_CXX"})
        self.assertEqual(flags["CFLAGS"],
                         "-O2 -DFIXTURE_C -fomit-frame-pointer -g -fno-omit-frame-pointer")
        self.assertEqual(flags["CXXFLAGS"], "-O3 -DFIXTURE_CXX -g -fno-omit-frame-pointer")

    def test_other_profiles_do_not_modify_flags(self):
        original = {"RUSTFLAGS": "--cfg keep", "CARGO_ENCODED_RUSTFLAGS": "",
                    "CFLAGS": "-O1", "CXXFLAGS": "-O2"}
        for profile in ("release", "debug", "release-with-lto"):
            with self.subTest(profile=profile):
                self.assertEqual(self.configured(original, profile=profile), original)


@unittest.skipUnless(all(shutil.which(tool) for tool in ("cc", "readelf", "objcopy", "strip")),
                     "ELF tests need a C compiler and GNU binutils")
class ElfGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="jito-symbol-elf-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.scratch = Path(cls.temp.name)
        source = cls.scratch / "fixture.c"
        source.write_text("int application_function(int value) { return value + 1; }\n"
                          "int main(void) { return application_function(0); }\n")
        cls.elf = cls.scratch / "fixture"
        result = run(["cc", "-g", "-fno-omit-frame-pointer", "-Wl,--build-id=sha1",
                      str(source), "-o", str(cls.elf)])
        if result.returncode:
            raise unittest.SkipTest("Could not compile GNU ELF fixture: " + result.stderr)

    def setUp(self):
        self.bin_dir = Path(tempfile.mkdtemp(prefix="bin-", dir=self.scratch))
        self.binary = self.bin_dir / "agave-validator"
        shutil.copy2(self.elf, self.binary)
        shutil.copy2(self.elf, self.bin_dir / "solana")
        shutil.copy2(self.elf, self.bin_dir / "agave-ledger-tool")

    def guard(self, profile="release-with-debug"):
        env = os.environ.copy()
        env["BUILD_PROFILE"] = profile
        return run(["bash", "-euo", "pipefail", "-c",
                    docker_function("validate_debug_symbols") +
                    '\nAGAVE_BINS_END_USER=(solana)\n'
                    'AGAVE_BINS_VAL_OP=(agave-validator)\n'
                    'AGAVE_BINS_DCOU=(agave-ledger-tool)\n'
                    'validate_debug_symbols "$1"',
                    "fixture", str(self.bin_dir)], env=env)

    def test_valid_elf_and_non_elf_helper(self):
        (self.bin_dir / "helper.sh").write_text("#!/bin/sh\nexit 0\n")
        result = self.guard()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_debug_sections(self):
        for section in (".debug_info", ".debug_line"):
            with self.subTest(section=section):
                shutil.copy2(self.elf, self.binary)
                subprocess.run(["objcopy", f"--remove-section={section}", str(self.binary)],
                               check=True, capture_output=True)
                result = self.guard()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(section, result.stderr)
                self.assertIn("agave-validator", result.stderr)

    def test_stripped_elf(self):
        subprocess.run(["strip", "--strip-all", str(self.binary)], check=True)
        result = self.guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(".symtab", result.stderr)

    def test_missing_build_id(self):
        subprocess.run(["objcopy", "--remove-section=.note.gnu.build-id", str(self.binary)],
                       check=True, capture_output=True)
        result = self.guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("GNU build ID", result.stderr)
        self.assertIn("agave-validator", result.stderr)

    def test_malformed_elf_rejected(self):
        self.binary.write_bytes(b"\x7fELFbroken")
        result = self.guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot read ELF metadata", result.stderr)

    def test_each_binary_group_is_checked(self):
        for name in ("solana", "agave-ledger-tool"):
            with self.subTest(name=name):
                binary = self.bin_dir / name
                subprocess.run(["strip", "--strip-debug", str(binary)], check=True)
                result = self.guard()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)
                shutil.copy2(self.elf, binary)

    def test_missing_selected_binary_rejected(self):
        self.binary.unlink()
        result = self.guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("agave-validator", result.stderr)

    def test_non_symbol_profiles_skip_guard(self):
        self.binary.write_bytes(b"\x7fELFbroken")
        for profile in ("release", "debug", "release-with-lto"):
            with self.subTest(profile=profile):
                result = self.guard(profile)
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
