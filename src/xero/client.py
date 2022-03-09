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
        # self.client_id = oauth_credentials.appKey
        # self.client_secret = oauth_credentials.appSecret
        # self.refresh_token = refresh_token
        # self.scope = oauth_data_dict['scope']
        self.oauth_token_dict = oauth_credentials.data

        self.oauth2_token_obj = OAuth2Token(client_id=oauth_credentials.appKey,
                                            client_secret=oauth_credentials.appSecret)
        self.oauth2_token_obj.update_token(**self.oauth_token_dict)
        self.api_client = ApiClient(Configuration(oauth2_token=self.oauth2_token_obj),
                                     oauth2_token_getter=self._obtain_xero_oauth2_token,
                                     oauth2_token_saver=self._store_xero_oauth2_token)
        
        self.accounting_api = None
        self.tenant_ids = []
        # super().__init__(TOKEN_URL)

    def login(self) -> None:
        """
            Set api client oauth2_token_getter to method for refreshing the token
            Init the api instance for the Accounting API
            Fetch all tenants to get data for
        """
        # self.jwt_token = self.xero_client.oauth2_token_getter(self._obtain_xero_oauth2_token)
        # self.xero_client.oauth2_token_saver(self._store_xero_oauth2_token)
        
        self.accounting_api = AccountingApi(self.api_client)
        self.tenant_ids = self._get_tenants()

    def _obtain_xero_oauth2_token(self) -> Dict:
        new_token = self._update_refresh_token()
        new_token["expires_at"] = time.time() + 1800
        return new_token

    def _update_refresh_token(self) -> Dict:
        # payload = {'grant_type': 'refresh_token',
        #            'refresh_token': self.refresh_token,
        #            'client_id': self.client_id,
        #            'client_secret': self.client_secret}
        try:
            # self.oauth2_token_obj.refresh_access_token
            # token_dict_alt = 
            try:
                self.oauth2_token_obj.refresh_access_token(self.api_client)
            except Exception as e:
                logging.exception(e)
            token_api = TokenApi(self.api_client,
                                 self.oauth2_token_obj.client_id, self.oauth2_token_obj.client_secret)
            token_dict = token_api.refresh_token(self.oauth2_token_obj.refresh_token,
                                                 self.oauth_token_dict['scope'])
            # token_dict = self.post(data=payload)
            # self.refresh_token = token_dict['refresh_token']
            return token_dict
        except HTTPError as http_error:
            raise XeroClientException("Failed to authenticate, invalid refresh token. "
                                      "Re-authenticate the component") from http_error

    def _store_xero_oauth2_token(self, new_token: Dict) -> None:
        self.oauth_token_dict = new_token

    def _get_tenants(self) -> List[str]:
        identity_api = IdentityApi(self.api_client)
        available_tenants = []
        for connection in identity_api.get_connections():
            tenant = serialize(connection)
            available_tenants.append(tenant.get("tenantId"))

        return available_tenants
    
    def get_refresh_token(self) -> str:
        return self.oauth_token_dict['refresh_token']

    def get_accounts(self, modified_since: str = None, **kwargs) -> Tuple[str, Iterable[Accounts]]:
        for tenant_id in self.tenant_ids:
            api_response = self.accounting_api.get_accounts(tenant_id, modified_since, **kwargs)
            yield (tenant_id, api_response)
