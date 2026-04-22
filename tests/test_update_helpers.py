import json
import os
import sys
import tempfile
import unittest

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_ROOT = os.path.join(PROJECT_ROOT, "src")

if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from src.apply_updates_to_pure import _selected_for_update, get_csv_files, get_json_files


class ApplyUpdateHelperTests(unittest.TestCase):
    def test_selected_for_update_accepts_case_and_whitespace(self):
        frame = pd.DataFrame(
            {
                "to_be_updated": ["X", " x ", "y", None, "x"],
                "value": [1, 2, 3, 4, 5],
            }
        )

        selected = _selected_for_update(frame)

        self.assertEqual([1, 2, 5], selected["value"].tolist())

    def test_get_csv_files_reads_only_update_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pd.DataFrame({"to_be_updated": ["X"]}).to_csv(
                os.path.join(tmpdir, "person_update.csv"),
                index=False,
            )
            pd.DataFrame({"ignored": [1]}).to_csv(
                os.path.join(tmpdir, "not_in_scope.csv"),
                index=False,
            )

            csv_files = get_csv_files(tmpdir)

            self.assertEqual(["person_update.csv"], sorted(csv_files.keys()))

    def test_get_csv_files_can_be_limited_to_target_filenames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pd.DataFrame({"to_be_updated": ["X"]}).to_csv(
                os.path.join(tmpdir, "person_update.csv"),
                index=False,
            )
            pd.DataFrame({"to_be_updated": ["X"]}).to_csv(
                os.path.join(tmpdir, "other_update.csv"),
                index=False,
            )

            csv_files = get_csv_files(tmpdir, allowed_filenames={"other_update.csv"})

            self.assertEqual(["other_update.csv"], sorted(csv_files.keys()))

    def test_get_json_files_reads_json_documents(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = os.path.join(tmpdir, "one.json")
            second = os.path.join(tmpdir, "two.json")

            with open(first, "w", encoding="utf-8") as handle:
                json.dump({"id": 1}, handle)
            with open(second, "w", encoding="utf-8") as handle:
                json.dump({"id": 2}, handle)

            json_files = get_json_files(tmpdir)

            self.assertEqual(2, len(json_files))
            self.assertIn({"id": 1}, json_files)
            self.assertIn({"id": 2}, json_files)

    def test_get_json_files_can_be_limited_to_target_filenames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = os.path.join(tmpdir, "one.json")
            second = os.path.join(tmpdir, "two.json")

            with open(first, "w", encoding="utf-8") as handle:
                json.dump({"id": 1}, handle)
            with open(second, "w", encoding="utf-8") as handle:
                json.dump({"id": 2}, handle)

            json_files = get_json_files(tmpdir, allowed_filenames={"two.json"})

            self.assertEqual([{"id": 2}], json_files)
