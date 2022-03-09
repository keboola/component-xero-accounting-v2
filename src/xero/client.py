from typing import Dict, Iterable, List, Tuple

from keboola.component.dao import OauthCredentials
from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.accounting.models.accounts import Accounts


class XeroClientException(Exception):
    pass


class XeroClient:
    def __init__(self, oauth_credentials: OauthCredentials) -> None:
        self._oauth_token_dict = oauth_credentials.data
        oauth2_token_obj = OAuth2Token(client_id=oauth_credentials.appKey,
                                       client_secret=oauth_credentials.appSecret)
        oauth2_token_obj.update_token(**self._oauth_token_dict)
        self._api_client = ApiClient(Configuration(oauth2_token=oauth2_token_obj),
                                     oauth2_token_getter=self.get_xero_oauth2_token_dict,
                                     oauth2_token_saver=self._set_xero_oauth2_token_dict)

    def get_xero_oauth2_token_dict(self) -> Dict:
        return self._oauth_token_dict

    def _set_xero_oauth2_token_dict(self, new_token: Dict) -> None:
        self._oauth_token_dict = new_token

    def _get_tenants(self) -> List[str]:
        identity_api = IdentityApi(self._api_client)
        available_tenants = []
        for connection in identity_api.get_connections():
            tenant = serialize(connection)
            available_tenants.append(tenant.get("tenantId"))

        return available_tenants

    def force_refresh_token(self):
        self._api_client.refresh_oauth2_token()

    def get_accounts(self, modified_since: str = None, **kwargs) -> Iterable[Tuple[str, Accounts]]:
        accounting_api = AccountingApi(self._api_client)
        tenant_ids = self._get_tenants()
        for tenant_id in tenant_ids:
            api_response = accounting_api.get_accounts(
                tenant_id, modified_since, **kwargs)
            yield (tenant_id, api_response)
