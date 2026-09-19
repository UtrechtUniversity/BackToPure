"""Per-host request throttling for full-text candidate validation.

Ported from doi_resolver (backend/app/utils/ratelimit.py), with asyncio
primitives replaced by threading ones because this job validates candidates
through a ThreadPoolExecutor.

Why per host rather than a global limit: over a 300-work sample of UU
publications, 44% of candidate URLs were on doi.org and the top three hosts
were 58% of all fetches. A global pool would hammer a handful of servers --
one of which (dspace.library.uu.nl) is UU's own repository. Throttling
arrives as 429s and Cloudflare challenges, which are served as HTML and are
correctly rejected by the validator, so it would show up in the report as
"no full text available" for papers that are perfectly available.
"""
import logging
import threading
import time
from urllib.parse import urlparse

from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

DEFAULT_RATE_PER_SECOND = 2.0

HOST_RATES = {
    "doi.org": 2.0,
    "dspace.library.uu.nl": 1.0,
    "onlinelibrary.wiley.com": 0.5,
    "link.springer.com": 0.5,
    "www.cambridge.org": 0.5,
    "pubs.acs.org": 0.5,
    "academic.oup.com": 0.5,
    "www.nature.com": 1.0,
}


class TokenBucket:
    """Allows rate_per_second acquisitions per second, bursting up to capacity."""

    def __init__(self, rate_per_second, capacity=None):
        self.rate_per_second = float(rate_per_second)
        self.capacity = float(capacity if capacity is not None else max(rate_per_second, 1.0))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity,
                    self._tokens + (now - self._updated) * self.rate_per_second,
                )
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self.rate_per_second
            # sleep outside the lock so other threads can refill and proceed
            time.sleep(wait)


class RateLimiterRegistry:
    """One bucket per host, created on first use."""

    def __init__(self, rates, default_rate):
        self._rates = dict(rates or {})
        self._default_rate = float(default_rate)
        self._buckets = {}
        self._lock = threading.Lock()

    def bucket_for(self, host):
        key = (host or "").lower()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self._rates.get(key, self._default_rate))
                self._buckets[key] = bucket
            return bucket

    def acquire_for_url(self, url):
        self.bucket_for(urlparse(url or "").netloc.lower()).acquire()


def default_registry():
    return RateLimiterRegistry(rates=HOST_RATES, default_rate=DEFAULT_RATE_PER_SECOND)
