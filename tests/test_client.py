"""
Regression tests for the transient-failure handling of accounting API fetches.

Covers the failure that motivated `XeroClient._fetch_accounting_page`: a job died
with an unhandled `HTTPStatusException` (504 Gateway Time-out) raised by the Xero
API in the middle of paginating an endpoint, which surfaced as an opaque internal
error. The tests assert both that such a blip is now retried and that nothing else
about the fetch changed - client errors still fail immediately, a persistent
failure still propagates, and a successful fetch still performs exactly one call.
"""
import unittest
from unittest import mock

from tenacity import wait_none
from urllib3.exceptions import ProtocolError

from xero_python.accounting.models import Journal, Journals
from xero_python.exceptions import AccountingBadRequestException, HTTPStatusException, NotFoundException

from xero.client import RETRY_MAX_ATTEMPTS, XeroClient, _is_transient_error


def _http_error(status: int, reason: str = "Error") -> HTTPStatusException:
    return HTTPStatusException(status=status, reason=reason)


def _journals(count: int) -> Journals:
    return Journals(journals=[Journal(journal_id=f"id-{i}") for i in range(count)])


class _NoSleepRetryMixin(unittest.TestCase):
    """Builds a client without touching OAuth, and removes the retry backoff sleep."""

    def setUp(self):
        super().setUp()
        self.client = object.__new__(XeroClient)
        self.client._api_client = mock.Mock()
        patcher = mock.patch.object(XeroClient._fetch_accounting_page.retry, "wait", wait_none())
        patcher.start()
        self.addCleanup(patcher.stop)


class TestIsTransientError(unittest.TestCase):
    def test_server_errors_are_transient(self):
        for status in (500, 502, 503, 504, 520):
            with self.subTest(status=status):
                self.assertTrue(_is_transient_error(_http_error(status)))

    def test_rate_limit_is_transient(self):
        self.assertTrue(_is_transient_error(_http_error(429)))

    def test_client_errors_are_not_transient(self):
        for status in (400, 401, 403, 404, 405, 412):
            with self.subTest(status=status):
                self.assertFalse(_is_transient_error(_http_error(status)))

    def test_transport_errors_are_transient(self):
        self.assertTrue(_is_transient_error(ProtocolError("connection broken")))

    def test_unrelated_exceptions_are_not_transient(self):
        self.assertFalse(_is_transient_error(ValueError("nope")))

    def test_status_less_exception_is_not_transient(self):
        self.assertFalse(_is_transient_error(HTTPStatusException()))

    def test_exception_whose_status_cannot_be_read_is_not_transient(self):
        """
        `AccountingBadRequestException.status` parses the response body and raises on a
        non-JSON one. Such an exception must propagate untouched rather than be retried.
        """
        error = AccountingBadRequestException(http_resp=mock.Mock(text="<html>not json</html>"))
        with self.assertRaises(Exception):
            _ = error.status

        self.assertFalse(_is_transient_error(error))

    def test_non_integer_status_is_not_transient(self):
        error = HTTPStatusException()
        with mock.patch.object(type(error), "status", mock.PropertyMock(return_value="Validation Exception")):
            self.assertFalse(_is_transient_error(error))


class TestFetchAccountingPageRetry(_NoSleepRetryMixin):
    """`_fetch_accounting_page` is the only place the defensive retry was added."""

    def test_successful_fetch_performs_exactly_one_call(self):
        """The happy path must be untouched: no extra calls, value passed through."""
        page = _journals(2)
        getter = mock.Mock(return_value=page)

        result = self.client._fetch_accounting_page(getter, "tenant-1", offset=0)

        self.assertIs(page, result)
        getter.assert_called_once_with("tenant-1", offset=0)

    def test_transient_gateway_timeout_is_retried_then_succeeds(self):
        """The reported failure: a single 504 must no longer kill the job."""
        page = _journals(1)
        getter = mock.Mock(side_effect=[_http_error(504, "Gateway Time-out"), page])

        result = self.client._fetch_accounting_page(getter, "tenant-1", offset=100)

        self.assertIs(page, result)
        self.assertEqual(2, getter.call_count)
        getter.assert_called_with("tenant-1", offset=100)

    def test_persistent_transient_error_reraises_original_exception(self):
        """Retries are bounded and never swallow a real failure."""
        error = _http_error(504, "Gateway Time-out")
        getter = mock.Mock(side_effect=error)

        with self.assertRaises(HTTPStatusException) as ctx:
            self.client._fetch_accounting_page(getter, "tenant-1", offset=0)

        self.assertIs(error, ctx.exception)
        self.assertEqual(504, ctx.exception.status)
        self.assertEqual(RETRY_MAX_ATTEMPTS, getter.call_count)

    def test_client_error_is_not_retried(self):
        """A 404 must keep failing immediately, exactly as before the retry existed."""
        error = NotFoundException(status=404, reason="Not Found")
        getter = mock.Mock(side_effect=error)

        with self.assertRaises(NotFoundException) as ctx:
            self.client._fetch_accounting_page(getter, "tenant-1", offset=0)

        self.assertIs(error, ctx.exception)
        getter.assert_called_once_with("tenant-1", offset=0)

    def test_unexpected_exception_is_not_retried(self):
        getter = mock.Mock(side_effect=ValueError("boom"))

        with self.assertRaises(ValueError):
            self.client._fetch_accounting_page(getter, "tenant-1", offset=0)

        self.assertEqual(1, getter.call_count)


class TestPaginationWithTransientError(_NoSleepRetryMixin):
    """End-to-end over `get_accounting_object`, the code path that failed in production."""

    def _journals_pages(self, results):
        """
        Drive the offset-paginated `Journals` endpoint with a stubbed getter.

        Returns the generator plus the list that records the offset of every call.
        """
        pending = list(results)
        calls = []

        def get_journals(xero_tenant_id, if_modified_since=None, offset=None, payments_only=None):
            calls.append(offset)
            result = pending.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        patcher = mock.patch("xero.client.AccountingApi")
        accounting_api = patcher.start()
        self.addCleanup(patcher.stop)
        accounting_api.return_value.get_journals = get_journals

        pages = self.client.get_accounting_object(tenant_id="tenant-1", model_name="Journals")
        return pages, calls

    def test_pagination_completes_without_errors(self):
        """Baseline: offsets advance by 100 and an empty page terminates the stream."""
        pages, calls = self._journals_pages([_journals(2), _journals(1), _journals(0)])

        self.assertEqual([2, 1], [len(page) for page in pages])
        self.assertEqual([0, 100, 200], calls)

    def test_transient_error_mid_pagination_recovers_and_yields_all_pages(self):
        """A 504 on a later page is retried at the same offset; no page is lost or duplicated."""
        pages, calls = self._journals_pages([
            _journals(2),
            _http_error(504, "Gateway Time-out"),
            _journals(3),
            _journals(0),
        ])

        self.assertEqual([2, 3], [len(page) for page in pages])
        # The failed offset is retried, then pagination continues from where it was.
        self.assertEqual([0, 100, 100, 200], calls)

    def test_client_error_mid_pagination_propagates_immediately(self):
        """A non-transient error still aborts the download after the pages already yielded."""
        pages, calls = self._journals_pages([_journals(2), NotFoundException(status=404, reason="Not Found")])

        yielded = []
        with self.assertRaises(NotFoundException):
            for page in pages:
                yielded.append(page)

        self.assertEqual([2], [len(page) for page in yielded])
        self.assertEqual([0, 100], calls)


if __name__ == "__main__":
    unittest.main()
