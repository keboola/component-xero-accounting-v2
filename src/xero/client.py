import time

from requests import HTTPError
from typing import Dict, Iterable, List

from keboola.http_client import HttpClient
from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration, OAuth2Token

TOKEN_URL = "https://identity.xero.com/connect/token?="


class XeroClientException(Exception):
    pass


class XeroClient(HttpClient):
    def __init__(self, client_id: str, client_secret: str, refresh_token: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.tenant_ids = []
        self.jwt_token = {}

        oauth_token = OAuth2Token(client_id=client_id, client_secret=client_secret)
        self.xero_client = ApiClient(Configuration(oauth2_token=oauth_token))
        self.api_instance = None
        super().__init__(TOKEN_URL)

    def login(self) -> None:
        """
            Set api client oauth2_token_getter to method for refreshing the token
            Init the api instance for the Accounting API
            Fetch all tenants to get data for
        """
        self.jwt_token = self.xero_client.oauth2_token_getter(self._obtain_xero_oauth2_token)
        self.xero_client.oauth2_token_saver(self._store_xero_oauth2_token)
        self.api_instance = AccountingApi(self.xero_client)
        self.tenant_ids = self._get_tenants()

    def _obtain_xero_oauth2_token(self) -> Dict:
        new_token = self._update_refresh_token()
        new_token["expires_at"] = time.time() + 1800
        return new_token

    def _update_refresh_token(self) -> Dict:
        payload = {'grant_type': 'refresh_token',
                   'refresh_token': self.refresh_token,
                   'client_id': self.client_id,
                   'client_secret': self.client_secret}
        try:
            return self.post(data=payload)
        except HTTPError as http_error:
            raise XeroClientException("Failed to authenticate, invalid refresh token. "
                                      "Re-authenticate the component") from http_error

    def _store_xero_oauth2_token(self, new_token: Dict) -> None:
        self.jwt_token = new_token

    def _get_tenants(self) -> List[str]:
        identity_api = IdentityApi(self.xero_client)
        available_tenants = []
        for connection in identity_api.get_connections():
            tenant = serialize(connection)
            available_tenants.append(tenant.get("tenantId"))

        return available_tenants

    def get_accounts(self, last_modified: str = None, **kwargs) -> Iterable:
        for tenant_id in self.tenant_ids:
            api_response = self.api_instance.get_accounts(tenant_id, last_modified, **kwargs)
            yield api_response
