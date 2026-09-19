import unittest
from unittest.mock import MagicMock

import fulltext_fetch as ff


def _response(status=200, headers=None, content=b""):
    response = MagicMock()
    response.status_code = status
    response.headers = headers or {}
    response.content = content
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

    def test_unknown_content_type_is_allowed_through(self):
        """Deliberate: some publishers send octet-stream for real PDFs."""
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "application/octet-stream"})

        self.assertTrue(ff.preflight(session, "https://a.org/x").ok)

    def test_head_failure_does_not_block_the_download(self):
        session = MagicMock()
        session.head.side_effect = Exception("HEAD unsupported")

        self.assertTrue(ff.preflight(session, "https://a.org/x").ok)


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


if __name__ == "__main__":
    unittest.main()
