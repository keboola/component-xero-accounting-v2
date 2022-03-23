from dataclasses import dataclass
import logging
from typing import Dict, MutableSet, List, Tuple, Union
from enum import Enum

from keboola.component.dao import OauthCredentials, SupportedDataTypes, TableMetadata, TableDefinition
from keboola.component.exceptions import UserException
from keboola.component import ComponentBase

from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
import xero_python.accounting.models
from xero_python.models import BaseModel

from xero_python.exceptions.http_status_exceptions import OAuth2InvalidGrantError, HTTPStatusException


class XeroClientException(Exception):
    pass


def _get_accounting_model(model_name: str) -> Union[BaseModel, None]:
    # try:
    #     model: BaseModel = getattr(xero_python.accounting.models, model_name)
    #     # TODO do not use Exception, exactly specify which exception
    # except Exception as e:
    #     raise XeroClientException(
    #         f"Requested model ({model_name}) not found.") from e
    # return model
    return getattr(xero_python.accounting.models, model_name, None)

@dataclass
class Table:
    # table_name: str
    # primary_key: MutableSet[str]
    # field_types: Dict[str, SupportedDataTypes]
    data: List[Dict]
    # table_metadata: TableMetadata
    table_definition: TableDefinition

    # def __eq__(self, other):
    #     return other and self.table_name == other.table_name and self.primary_key == other.primary_key

    # def __ne__(self, other):
    #     return not self.__eq__(other)

    # def __hash__(self):
    #   return hash((self.table_name, self.primary_key))



class XeroClient:
    def __init__(self, oauth_credentials: OauthCredentials, tenant_id: str = None, component: ComponentBase = None) -> None:
        self._oauth_token_dict = oauth_credentials.data
        self.tenant_id = tenant_id
        self.component = component

        oauth2_token_obj = OAuth2Token(client_id=oauth_credentials.appKey,
                                       client_secret=oauth_credentials.appSecret)
        oauth2_token_obj.update_token(**self._oauth_token_dict)
        self._api_client = ApiClient(Configuration(oauth2_token=oauth2_token_obj),
                                     oauth2_token_getter=self.get_xero_oauth2_token_dict,
                                     oauth2_token_saver=self._set_xero_oauth2_token_dict)

    def update_tenants(self):
        tenants_available = self._get_tenants()
        if self.tenant_id is None:
            # TODO the previous component took all tenants and fetched all data for each tenant
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

        try:
            for connection in identity_api.get_connections():
                tenant = serialize(connection)
                available_tenants.append(tenant.get("tenantId"))
        except OAuth2InvalidGrantError as oauth_err:
            raise XeroClientException(oauth_err) from oauth_err

        return available_tenants

    def force_refresh_token(self):
        try:
            self._api_client.refresh_oauth2_token()
        except HTTPStatusException as http_error:
            raise XeroClientException(
                "Failed to authenticate the client, please reauthorize the component") from http_error

    @staticmethod
    def get_field_names(model_name: str) -> Union[List[str], None]:
        model = _get_accounting_model(model_name)
        return list(model.attribute_map.values()) if model else None

    def get_serialized_accounting_object(self, model_name: str, **kwargs) -> Dict:
        # TODO: handle paging where needed - some endpoints require paging, e. g. Quotes
        return serialize(self.get_accounting_object(model_name, **kwargs))
    
    def get_accounting_object(self, model_name: str, **kwargs) -> Dict:
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
        return data_getter(self.tenant_id, **kwargs)
    
    def parse_accounting_object_into_tables(self, root_object: BaseModel, **kwargs) -> List[Table]:
        TERMINAL_TYPE_MAPPING = {'str': {'type': SupportedDataTypes.STRING},
                                 'int': {'type': SupportedDataTypes.INTEGER},
                                 'float': {'type': SupportedDataTypes.NUMERIC, 'length': '38,8'},
                                 'bool': {'type': SupportedDataTypes.BOOLEAN}}
        tables: Dict[str, Table] = {}
        def parse_object(object: BaseModel, parent_object: BaseModel = None) -> Tuple[str, str]:
            class_name = object.__class__.__name__
            # id_attr_name = f'{class_name.lower()}_id'
            # id_field_name = object.attribute_map.get(id_attr_name)
            id_field_name = f'{class_name}ID'
            id_attr_name = {v: k for k, v in object.attribute_map.items()}.get(id_field_name, '_')
            id = (id_attr_name, getattr(object, id_attr_name, None))
            record = {}
            primary_key = {id_field_name}
            # field_types = {object.attribute_map[attr_name]: object.openapi_types[attr_name]
            #                for attr_name in object.attribute_map
            #                if object.openapi_types[attr_name] in TERMINAL_TYPE_NAMES} 
            field_types = {}
            for attr_name in object.attribute_map:
                type_name: str = object.openapi_types[attr_name] 
                if type_name in TERMINAL_TYPE_MAPPING:
                    field_types[object.attribute_map[attr_name]] = object.openapi_types[attr_name]
                elif type_name.startswith('list'):
                    pass
                elif type_name.startswith('date') or issubclass(_get_accounting_model(type_name), Enum):
                    field_types[object.attribute_map[attr_name]] = 'str'
                elif issubclass(_get_accounting_model(type_name), BaseModel):
                    pass
                else:
                    raise XeroClientException(f'Unexpected type encountered: {type_name}.')
            if parent_object:
                parent_class_name = parent_object.__class__.__name__
                parent_id_field_name = f'{parent_class_name}ID'
                parent_id_attr_name = {v: k for k, v in parent_object.attribute_map.items()}.get(parent_id_field_name, '_')
                parent_id_val = getattr(parent_object, parent_id_attr_name, None)
                if parent_id_val:
                    record[parent_id_field_name] = parent_id_val
                    field_types[parent_id_field_name] = 'str'
                    primary_key.add(parent_id_field_name)
            for attribute_name in object.attribute_map:
                attribute_value = getattr(object, attribute_name)
                if isinstance(attribute_value, List):
                    for sub_object in attribute_value:
                        assert isinstance(sub_object, BaseModel)
                        parse_object(sub_object, parent_object=object)
                elif isinstance(attribute_value, BaseModel):
                    sub_id_attr_name, sub_id_val = parse_object(attribute_value)
                    sub_id_field_name = attribute_value.attribute_map[sub_id_attr_name]
                    record[sub_id_field_name] = sub_id_val
                    field_types[sub_id_field_name] = attribute_value.openapi_types[sub_id_attr_name]
                elif attribute_value:
                    field_name = object.attribute_map[attribute_name]
                    record[field_name] = serialize(attribute_value)
                    # field_types[field_name] = object.openapi_types[attribute_name]
            if len(record) > 0:
                table = tables.get(class_name)
                # if tables.get(class_name):
                #     table = tables[class_name]
                if table is None:
                    # table = Table(table_name=class_name,
                    #               primary_key=primary_key,
                    #               field_types=field_types,
                    #               data=[],
                    #               table_metadata=TableMetadata())
                    table_definiton = self.component.create_out_table_definition(name=f'{class_name}.csv',
                                                                   primary_key=list(primary_key),
                                                                   columns=list(field_types.keys()))
                    table = Table(data=[],
                                  table_definition=table_definiton)
                    for _field_name, source_field_type in field_types.items():
                        output_type = TERMINAL_TYPE_MAPPING[source_field_type]
                        table.table_definition.table_metadata.add_column_data_type(column=_field_name,
                                                                                   source_data_type=source_field_type,
                                                                                   data_type=output_type['type'],
                                                                                   length=output_type.get('length'))
                    tables[class_name] = table
                table.data.append(record)
            return id
        # for attribute_name in object.attribute_map:
        #     attribute_type = object.openapi_types[attribute_name]
        #     if attribute_type.startswith('list['):
        #         tables[object.attribute_map[attribute_name]] = []
                
        #     else:
        parse_object(root_object)
        pass
        return list(tables.values())
