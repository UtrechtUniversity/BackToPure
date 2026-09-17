import logging
import os
import tempfile
import unittest

from src.logging_config import setup_logging


class SetupLoggingTests(unittest.TestCase):
    def test_setup_logging_creates_log_file_and_handlers(self):
        previous_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                logger = setup_logging("job-test", level=logging.INFO)
                logger.info("hello world")

                log_path = os.path.join(tmpdir, "logs", "job-test.log")
                self.assertTrue(os.path.exists(log_path))
                self.assertEqual(2, len(logger.handlers))

                with open(log_path, "r", encoding="utf-8") as handle:
                    contents = handle.read()

                self.assertIn("INFO hello world", contents)
            finally:
                os.chdir(previous_cwd)
