import json
from pathlib import Path
import unittest

from partition_local_cluster import filterset, partition_tests, runnable_tests


class PartitionTests(unittest.TestCase):
    def test_complete_disjoint_and_deterministic_with_new_tests(self):
        tests = ["slow", "medium", "new_test", "fast"]
        durations = {"slow": 100, "medium": 50, "fast": 10, "deleted_test": 999}
        partitions, loads = partition_tests(tests, durations, 2)
        self.assertEqual(partitions, [["slow", "fast"], ["new_test", "medium"]])
        self.assertEqual(loads, [110, 110])
        self.assertEqual(
            partition_tests(list(reversed(tests)), durations, 2),
            (partitions, loads),
        )
        self.assertCountEqual([name for part in partitions for name in part], tests)

    def test_more_partitions_than_tests(self):
        partitions, loads = partition_tests(["only_test"], {}, 3)
        self.assertEqual(partitions, [["only_test"], [], []])
        self.assertEqual(loads, [60, 0, 0])
        self.assertEqual(filterset(partitions[1]), "none()")

    def test_invalid_partition_count(self):
        with self.assertRaises(ValueError):
            partition_tests(["test"], {}, 0)

    def test_exact_filter_does_not_match_similarly_named_tests(self):
        self.assertEqual(
            filterset(["test_restart", "tests::test_restart_other"]),
            "test(=test_restart) | test(=tests::test_restart_other)",
        )
        with self.assertRaises(ValueError):
            filterset(["test) | all("])

    def test_preserves_ignored_and_filtered_tests(self):
        suite = {
            "binary-id": "solana-local-cluster::local_cluster",
            "status": "listed",
            "testcases": {
                "active": {"ignored": False, "filter-match": {"status": "matches"}},
                "ignored": {"ignored": True, "filter-match": {"status": "matches"}},
                "filtered": {"ignored": False, "filter-match": {"status": "mismatch"}},
            },
        }
        self.assertEqual(runnable_tests({"rust-suites": {"suite": suite}}), ["active"])
        suite["status"] = "skipped"
        with self.assertRaises(ValueError):
            runnable_tests({"rust-suites": {"suite": suite}})

    def test_empty_inventory_fails(self):
        with self.assertRaises(ValueError):
            runnable_tests({"rust-suites": {}})

    def test_recorded_suite_balance_and_inventory(self):
        durations = json.loads(
            Path(__file__).with_name("local-cluster-durations.json").read_text()
        )
        partitions, loads = partition_tests(durations, durations, 10)
        self.assertCountEqual(
            [name for part in partitions for name in part], durations
        )
        self.assertLessEqual(max(loads), 380)


if __name__ == "__main__":
    unittest.main()
