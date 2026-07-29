from dataclasses import dataclass
import inspect
import logging
from http.client import RemoteDisconnected
from typing import Dict, Iterable, List

from tenacity import (retry, wait_exponential, stop_after_attempt, retry_if_exception_type, retry_if_exception,
                      before_sleep_log)
from urllib3.exceptions import ProtocolError

from keboola.component.dao import OauthCredentials, TableDefinition

from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.api_client.serializer import serialize

from xero_python.exceptions.http_status_exceptions import OAuth2InvalidGrantError, HTTPStatusException

# Always import utility to monkey patch BaseModel
from .utility import XeroException, get_accounting_model, EnhancedBaseModel

from ratelimit import limits, sleep_and_retry

# Bounded retry of a single accounting API fetch on transient upstream failures.
RETRY_MAX_ATTEMPTS = 3
# Non-5xx statuses that still signal a temporary condition (any 5xx is retried as well).
RETRYABLE_NON_SERVER_STATUS = frozenset({429})


def _is_transient_error(exception: BaseException) -> bool:
    """
    Whether an exception raised by an accounting API call is worth retrying.

    Only transport-level errors and server-side/rate-limit HTTP statuses qualify.
    Client errors (400/401/403/404, ...) are never retried, so they keep failing
    immediately exactly as before. Anything whose status cannot be determined is
    treated as non-retryable, so the original exception propagates untouched.
    """
    if isinstance(exception, (ProtocolError, RemoteDisconnected)):
        return True
    if not isinstance(exception, HTTPStatusException):
        return False
    try:
        status = exception.status
    except Exception:
        # Some Xero exceptions derive the status from the response body, which can
        # fail to parse. Never let that mask the exception the caller should see.
        return False
    if not isinstance(status, int):
        return False
    return status >= 500 or status in RETRYABLE_NON_SERVER_STATUS


@dataclass
class Table:
    data: List[Dict]
    table_definition: TableDefinition


class XeroClient:
    def __init__(self, oauth_credentials: OauthCredentials) -> None:
        self._oauth_token_dict = oauth_credentials.data

        oauth2_token_obj = OAuth2Token(client_id=oauth_credentials.appKey,
                                       client_secret=oauth_credentials.appSecret)
        oauth2_token_obj.update_token(**self._oauth_token_dict)
        self._api_client = ApiClient(Configuration(oauth2_token=oauth2_token_obj),
                                     oauth2_token_getter=self.get_xero_oauth2_token_dict,
                                     oauth2_token_saver=self._set_xero_oauth2_token_dict)

        self._available_tenant_ids = None

    def get_xero_oauth2_token_dict(self) -> Dict:
        return self._oauth_token_dict

    def _set_xero_oauth2_token_dict(self, new_token: Dict) -> None:
        self._oauth_token_dict = new_token

    def refresh_available_tenant_ids(self) -> None:
        identity_api = IdentityApi(self._api_client)
        available_tenants = []
        try:
            for connection in identity_api.get_connections():
                tenant = serialize(connection)
                available_tenants.append(tenant.get("tenantId"))
        except (OAuth2InvalidGrantError, HTTPStatusException) as oauth_err:
            raise XeroException(oauth_err) from oauth_err
        self._available_tenant_ids = available_tenants

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10),
           stop=stop_after_attempt(3),
           retry=retry_if_exception_type((HTTPStatusException, ProtocolError, RemoteDisconnected)))
    def force_refresh_token(self):
        try:
            self._api_client.refresh_oauth2_token()
        except (HTTPStatusException, ProtocolError) as error:
            raise XeroException(
                "Failed to authenticate the client, please reauthorize the component") from error

    def get_available_tenant_ids(self):
        if not self._available_tenant_ids:
            self.refresh_available_tenant_ids()
        return self._available_tenant_ids

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10),
           stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
           retry=retry_if_exception(_is_transient_error),
           before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
           reraise=True)
    def _fetch_accounting_page(self, getter, tenant_id: str, **kwargs):
        """
        Perform a single accounting API fetch, retrying transient upstream failures.

        On success the getter's return value is passed through untouched. Once the
        attempts are exhausted the original exception is re-raised unchanged, so a
        persistent failure still fails the job.
        """
        return getter(tenant_id, **kwargs)

    def get_accounting_object(self, tenant_id: str, model_name: str, **kwargs) -> Iterable[List[EnhancedBaseModel]]:
        accounting_api = AccountingApi(self._api_client)
        model: EnhancedBaseModel = get_accounting_model(model_name)
        getter_name = model.get_download_method_name()
        if getter_name:
            getter = sleep_and_retry(limits(calls=50, period=60)(getattr(accounting_api, getter_name)))
            getter_signature = inspect.signature(getter)
            used_kwargs = {k: v for k, v in kwargs.items()
                           if k in getter_signature.parameters and v is not None}
            if 'page' in getter_signature.parameters:
                used_kwargs['page'] = 1
                while True:
                    accounting_object = self._fetch_accounting_page(getter, tenant_id, **used_kwargs)
                    if accounting_object.is_empty_list():
                        break
                    yield accounting_object.to_list()
                    used_kwargs['page'] = used_kwargs['page'] + 1
            elif 'offset' in getter_signature.parameters:
                used_kwargs['offset'] = 0
                while True:
                    accounting_object = self._fetch_accounting_page(getter, tenant_id, **used_kwargs)
                    if accounting_object.is_empty_list():
                        break
                    yield accounting_object.to_list()
                    used_kwargs['offset'] = used_kwargs['offset'] + 100
            else:
                yield self._fetch_accounting_page(getter, tenant_id, **used_kwargs).to_list()
        else:
            raise XeroException(
                f"Requested model ({model_name}) getter function not found.")

    def get_serialized_accounting_object(self, model_name: str, **kwargs) -> Dict:
        return serialize(self.get_accounting_object(model_name, **kwargs))
