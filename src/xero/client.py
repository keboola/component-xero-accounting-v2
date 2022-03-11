import logging
from typing import Dict, Iterable, List, Tuple

from keboola.component.dao import OauthCredentials
from keboola.component.exceptions import UserException

from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
import xero_python.accounting.models as xero_models


class XeroClientException(Exception):
    pass


class XeroClient:
    def __init__(self, oauth_credentials: OauthCredentials, tenant_id: str = None) -> None:
        self._oauth_token_dict = oauth_credentials.data
        oauth2_token_obj = OAuth2Token(client_id=oauth_credentials.appKey,
                                       client_secret=oauth_credentials.appSecret)
        oauth2_token_obj.update_token(**self._oauth_token_dict)
        self._api_client = ApiClient(Configuration(oauth2_token=oauth2_token_obj),
                                     oauth2_token_getter=self.get_xero_oauth2_token_dict,
                                     oauth2_token_saver=self._set_xero_oauth2_token_dict)
        self.tenant_id = tenant_id
        tenants_available = self._get_tenants()
        if tenant_id is None:
            self.tenant_id = tenants_available[0]
            logging.warning(
                f'Tenant ID not specified, using first available: {self.tenant_id}.')
        else:
            if self.tenant_id not in tenants_available:
                raise UserException(f"Specified Tenant ID ({self.tenant_id}) is not accessible,"
                                    " please, check if you granted sufficient credentials.")

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

    @staticmethod
    def get_account_field_names() -> List[str]:
        return list(xero_models.Account.attribute_map.values())

    def get_accounts(self, modified_since: str = None, **kwargs) -> Dict:
        accounting_api = AccountingApi(self._api_client)
        tenant_accounts: xero_models.Accounts = accounting_api.get_accounts(
            self.tenant_id, modified_since, **kwargs)
        accounts_dict = serialize(tenant_accounts)
        return accounts_dict
