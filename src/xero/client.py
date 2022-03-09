import time
import logging

from requests import HTTPError
from typing import Dict, Iterable, List, Tuple

# from keboola.http_client import HttpClient
from keboola.component.dao import OauthCredentials
from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token, TokenApi
from xero_python.accounting.models.accounts import Accounts

# TOKEN_URL = "https://identity.xero.com/connect/token?="


class XeroClientException(Exception):
    pass


class XeroClient:
    def __init__(self, oauth_credentials: OauthCredentials) -> None:
        self.oauth_token_dict = oauth_credentials.data

        oauth2_token_obj = OAuth2Token(client_id=oauth_credentials.appKey,
                                       client_secret=oauth_credentials.appSecret)
        oauth2_token_obj.update_token(**self.oauth_token_dict)
        self.api_client = ApiClient(Configuration(oauth2_token=oauth2_token_obj),
                                    oauth2_token_getter=self._obtain_xero_oauth2_token,
                                    oauth2_token_saver=self._store_xero_oauth2_token)

    def _obtain_xero_oauth2_token(self) -> Dict:
        return self.oauth_token_dict

    def _store_xero_oauth2_token(self, new_token: Dict) -> None:
        self.oauth_token_dict = new_token

    def _get_tenants(self) -> List[str]:
        identity_api = IdentityApi(self.api_client)
        available_tenants = []
        for connection in identity_api.get_connections():
            tenant = serialize(connection)
            available_tenants.append(tenant.get("tenantId"))

        return available_tenants

    @property
    def refresh_token(self) -> str:
        return self.oauth_token_dict['refresh_token']

    def get_accounts(self, modified_since: str = None, **kwargs) -> Tuple[str, Iterable[Accounts]]:
        accounting_api = AccountingApi(self.api_client)
        tenant_ids = self._get_tenants()
        for tenant_id in tenant_ids:
            api_response = accounting_api.get_accounts(
                tenant_id, modified_since, **kwargs)
            yield (tenant_id, api_response)
