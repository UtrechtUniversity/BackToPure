import threading
import time
import unittest

import fulltext_ratelimit as rl


class TokenBucketTests(unittest.TestCase):
    def test_first_acquire_is_immediate(self):
        bucket = rl.TokenBucket(rate_per_second=10)
        started = time.monotonic()
        bucket.acquire()
        self.assertLess(time.monotonic() - started, 0.05)

    def test_second_acquire_waits_for_the_rate(self):
        bucket = rl.TokenBucket(rate_per_second=20, capacity=1)
        bucket.acquire()
        started = time.monotonic()
        bucket.acquire()
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.04)

    def test_acquire_is_thread_safe(self):
        bucket = rl.TokenBucket(rate_per_second=200, capacity=1)
        errors = []

        def worker():
            try:
                bucket.acquire()
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual([], errors)


class RegistryTests(unittest.TestCase):
    def test_same_host_shares_one_bucket(self):
        registry = rl.RateLimiterRegistry(rates={}, default_rate=5)
        self.assertIs(registry.bucket_for("a.org"), registry.bucket_for("a.org"))

    def test_different_hosts_get_different_buckets(self):
        registry = rl.RateLimiterRegistry(rates={}, default_rate=5)
        self.assertIsNot(registry.bucket_for("a.org"), registry.bucket_for("b.org"))

    def test_per_host_rate_overrides_the_default(self):
        registry = rl.RateLimiterRegistry(rates={"slow.org": 0.5}, default_rate=5)
        self.assertEqual(0.5, registry.bucket_for("slow.org").rate_per_second)
        self.assertEqual(5, registry.bucket_for("other.org").rate_per_second)

    def test_default_registry_throttles_the_hosts_we_actually_hit(self):
        registry = rl.default_registry()
        for host in ("doi.org", "onlinelibrary.wiley.com", "dspace.library.uu.nl"):
            with self.subTest(host=host):
                self.assertLessEqual(registry.bucket_for(host).rate_per_second, 2)
