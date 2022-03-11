import logging
from typing import Dict, List

from keboola.component.dao import OauthCredentials
from keboola.component.exceptions import UserException

from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
import xero_python.accounting.models
from xero_python.models import BaseModel


class XeroClientException(Exception):
    pass


def _get_accounting_model(model_name: str) -> BaseModel:
    try:
        model: BaseModel = getattr(xero_python.accounting.models, model_name)
    except Exception as e:
        raise XeroClientException(
            f"Requested model ({model_name}) not found.") from e
    return model


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
    def get_field_names(model_name: str) -> List[str]:
        return list(_get_accounting_model(model_name).attribute_map.values())

    def get_serialized_accounting_object(self, model_name: str, **kwargs) -> Dict:
        # TODO: handle paging where needed - some endpoints require paging, e. g. Quotes
        accounting_api = AccountingApi(self._api_client)
        model: BaseModel = _get_accounting_model(model_name)
        inv_map = {v: k for k, v in model.attribute_map.items()}
        try:
            data_getter = getattr(
                accounting_api, f'get_{inv_map[model_name]}')
        except Exception as e:
            raise XeroClientException(
                f"Requested model ({model_name}) not found.") from e
        data_dict = serialize(data_getter(self.tenant_id, **kwargs))
        return data_dict
