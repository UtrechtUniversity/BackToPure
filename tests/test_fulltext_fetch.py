import unittest
from unittest.mock import MagicMock, patch

import fulltext_fetch as ff


def _response(status=200, headers=None, content=b""):
    response = MagicMock()
    response.status_code = status
    response.headers = headers or {}
    response.content = content
    # Streaming: iter_content yields the whole body as one chunk by default,
    # which is enough for these tests (they only assert on the final bytes).
    response.iter_content = MagicMock(return_value=iter([content]) if content else iter([]))
    return response


class PreflightTests(unittest.TestCase):
    def test_pdf_content_type_passes(self):
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "application/pdf"})

        result = ff.preflight(session, "https://a.org/x.pdf")

        self.assertTrue(result.ok)

    def test_html_landing_page_is_rejected(self):
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "text/html; charset=utf-8"})

        result = ff.preflight(session, "https://a.org/x")

        self.assertFalse(result.ok)
        self.assertIn("HTML", result.detail)

    def test_forbidden_is_rejected_as_protected(self):
        session = MagicMock()
        session.head.return_value = _response(status=403, headers={})

        result = ff.preflight(session, "https://a.org/x")

        self.assertFalse(result.ok)
        self.assertEqual(403, result.http_status)
        self.assertFalse(result.transport_failure)

    def test_unknown_content_type_is_allowed_through(self):
        """Deliberate: some publishers send octet-stream for real PDFs."""
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "application/octet-stream"})

        self.assertTrue(ff.preflight(session, "https://a.org/x").ok)

    def test_head_failure_does_not_block_the_download(self):
        session = MagicMock()
        session.head.side_effect = Exception("HEAD unsupported")

        self.assertTrue(ff.preflight(session, "https://a.org/x").ok)

    def test_500_is_a_transport_failure(self):
        session = MagicMock()
        session.head.return_value = _response(status=500, headers={})

        result = ff.preflight(session, "https://a.org/x")

        self.assertFalse(result.ok)
        self.assertTrue(result.transport_failure)

    def test_429_is_reported_as_rate_limited_and_a_transport_failure(self):
        """A 429 must never be reported as 'no full text available' -- that is
        exactly the failure the per-host rate limiter exists to prevent."""
        session = MagicMock()
        session.head.return_value = _response(status=429, headers={})
        registry = MagicMock()

        result = ff.preflight(session, "https://a.org/x", registry=registry)

        self.assertFalse(result.ok)
        self.assertTrue(result.transport_failure)
        self.assertIn("rate limit", result.detail.lower())

    def test_429_retries_once_and_honours_retry_after(self):
        session = MagicMock()
        session.head.side_effect = [
            _response(status=429, headers={"Retry-After": "0"}),
            _response(status=200, headers={"content-type": "application/pdf"}),
        ]
        registry = MagicMock()

        with patch.object(ff.time, "sleep") as mock_sleep:
            result = ff.preflight(session, "https://a.org/x", registry=registry)

        self.assertTrue(result.ok)
        self.assertEqual(2, session.head.call_count)
        self.assertEqual(2, registry.acquire_for_url.call_count)
        # Retry-After: 0 means no wait is owed; the retry still happens.
        mock_sleep.assert_not_called()

    def test_429_retry_still_rate_limited_stays_a_transport_failure(self):
        session = MagicMock()
        session.head.return_value = _response(status=429, headers={})
        registry = MagicMock()

        with patch.object(ff.time, "sleep"):
            result = ff.preflight(session, "https://a.org/x", registry=registry)

        self.assertFalse(result.ok)
        self.assertTrue(result.transport_failure)
        self.assertEqual(2, session.head.call_count)


class DownloadTests(unittest.TestCase):
    PDF = b"%PDF-1.7\n stub"

    def test_valid_pdf_returns_bytes_and_filename(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/pdf"}, content=self.PDF
        )

        payload, filename = ff.download_and_validate(session, "https://a.org/article.pdf")

        self.assertEqual(self.PDF, payload)
        self.assertEqual("article.pdf", filename)

    def test_magic_bytes_accepted_without_pdf_content_type(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/octet-stream"}, content=self.PDF
        )

        payload, _filename = ff.download_and_validate(session, "https://a.org/x.pdf")

        self.assertEqual(self.PDF, payload)

    def test_html_body_is_rejected(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "text/html"}, content=b"<html>go away</html>"
        )

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/x")
        self.assertIn("non-PDF", str(ctx.exception))

    def test_oversize_is_rejected(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/pdf"}, content=self.PDF + b"x" * 100
        )

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/x.pdf", max_bytes=10)
        self.assertIn("size limit", str(ctx.exception))

    def test_oversize_aborts_without_consuming_the_whole_stream(self):
        """The point of streaming: a huge/unbounded body must be rejected as
        soon as the cap is exceeded, not read into memory in full first."""
        session = MagicMock()
        chunks_yielded = []

        def _chunks():
            for _ in range(1000):
                chunks_yielded.append(1)
                yield b"x" * 1024

        response = _response(status=200, headers={"content-type": "application/pdf"})
        response.iter_content = MagicMock(return_value=_chunks())
        session.get.return_value = response

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/big.pdf", max_bytes=5000)

        self.assertIn("size limit", str(ctx.exception))
        # Aborted well before all 1000 chunks (1MB) were pulled from the stream.
        self.assertLess(len(chunks_yielded), 1000)

    def test_error_status_serving_html_is_named_as_a_challenge(self):
        session = MagicMock()
        session.get.return_value = _response(
            status=403, headers={"content-type": "text/html"}, content=b"<html/>"
        )

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/x.pdf")
        self.assertIn("HTML", str(ctx.exception))

    def test_rate_limiter_is_consulted_before_the_request(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/pdf"}, content=self.PDF
        )
        registry = MagicMock()

        ff.download_and_validate(session, "https://a.org/x.pdf", registry=registry)

        registry.acquire_for_url.assert_called_once_with("https://a.org/x.pdf")

    def test_429_raises_a_rate_limit_error_flagged_as_transport_failure(self):
        session = MagicMock()
        session.get.return_value = _response(status=429, headers={})
        registry = MagicMock()

        with patch.object(ff.time, "sleep"):
            with self.assertRaises(ff.CandidateFetchError) as ctx:
                ff.download_and_validate(session, "https://a.org/x.pdf", registry=registry)

        self.assertTrue(ctx.exception.transport_failure)
        self.assertEqual(429, ctx.exception.status_code)
        self.assertIn("rate limit", str(ctx.exception).lower())
        self.assertEqual(2, session.get.call_count)

    def test_5xx_raises_error_flagged_as_transport_failure(self):
        session = MagicMock()
        session.get.return_value = _response(status=503, headers={})

        with self.assertRaises(ff.CandidateFetchError) as ctx:
            ff.download_and_validate(session, "https://a.org/x.pdf")

        self.assertTrue(ctx.exception.transport_failure)
        self.assertEqual(503, ctx.exception.status_code)

    def test_4xx_raises_error_not_flagged_as_transport_failure(self):
        session = MagicMock()
        session.get.return_value = _response(status=404, headers={})

        with self.assertRaises(ff.CandidateFetchError) as ctx:
            ff.download_and_validate(session, "https://a.org/x.pdf")

        self.assertFalse(ctx.exception.transport_failure)
        self.assertEqual(404, ctx.exception.status_code)


if __name__ == "__main__":
    unittest.main()
