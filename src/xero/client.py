from dataclasses import dataclass
import logging
from typing import Dict, List, Tuple, Union, Mapping
from enum import Enum

from keboola.component.dao import OauthCredentials, SupportedDataTypes, TableDefinition
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

# Configuration variables
TERMINAL_TYPE_MAPPING = {'str': {'type': SupportedDataTypes.STRING},
                         'int': {'type': SupportedDataTypes.INTEGER},
                         'float': {'type': SupportedDataTypes.NUMERIC, 'length': '38,8'},
                         'bool': {'type': SupportedDataTypes.BOOLEAN}}


class XeroClientException(Exception):
    pass


def _flatten_dict(d: Mapping, parent_key: str = '', sep: str = '_') -> Dict:
    # TODO: delete unless needed
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, Mapping):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def _get_accounting_model(model_name: str) -> Union[BaseModel, None]:
    return getattr(xero_python.accounting.models, model_name, None)


@dataclass
class Table:
    data: List[Dict]
    table_definition: TableDefinition


class XeroClient:
    def __init__(self, oauth_credentials: OauthCredentials, tenant_id: str = None,
                 component: ComponentBase = None) -> None:
        self._oauth_token_dict = oauth_credentials.data
        self.tenant_id = tenant_id
        self.component = component

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

    @staticmethod
    def get_field_names(model_name: str) -> Union[List[str], None]:
        model = _get_accounting_model(model_name)
        return list(model.attribute_map.values()) if model else None

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

    def get_serialized_accounting_object(self, model_name: str, **kwargs) -> Dict:
        # TODO: handle paging where needed - some endpoints require paging, e. g. Quotes
        return serialize(self.get_accounting_object(model_name, **kwargs))

    def parse_accounting_object_into_tables(self, root_object: BaseModel, **kwargs) -> List[Table]:
        tables: Dict[str, Table] = {}

        def resolve_serialized_type(type_name: str, struct: bool = False) -> Union[str, None]:
            # TODO: add special case for date/timestamp as metadata
            # TODO: resolve down to Keboola supported type with length
            # TODO: create table definition independently of data
            if type_name in TERMINAL_TYPE_MAPPING:
                r = type_name
            elif type_name.startswith('list'):
                r = None
            elif type_name.startswith('date') or issubclass(_get_accounting_model(type_name), Enum):
                r = 'str'
            elif issubclass(_get_accounting_model(type_name), BaseModel):
                r = None
            else:
                raise XeroClientException(
                    f'Unexpected type encountered: {type_name}.')
            if struct and r is None:
                raise XeroClientException(
                    f'Unexpected type encountered in struct: {type_name}.')
            return r

        def parse_object(object: BaseModel, parent_object: BaseModel = None) -> Tuple[str, str]:
            class_name = object.__class__.__name__
            id_field_name = f'{class_name}ID'
            id_attr_name = {v: k for k, v in object.attribute_map.items()}.get(
                id_field_name, '_')
            id = (id_attr_name, getattr(object, id_attr_name, None))
            record = {}
            primary_key = {id_field_name}
            field_types = {}
            for attribute_name in object.attribute_map:
                resolved_type_name = resolve_serialized_type(
                    object.openapi_types[attribute_name])
                if resolved_type_name:
                    field_types[object.attribute_map[attribute_name]
                                ] = resolved_type_name
                # if type_name in TERMINAL_TYPE_MAPPING:
                #     field_types[object.attribute_map[attribute_name]
                #                 ] = object.openapi_types[attribute_name]
                # elif type_name.startswith('list'):
                #     pass
                # elif type_name.startswith('date') or issubclass(_get_accounting_model(type_name), Enum):
                #     field_types[object.attribute_map[attribute_name]] = 'str'
                # elif issubclass(_get_accounting_model(type_name), BaseModel):
                #     pass
                # else:
                #     raise XeroClientException(
                #         f'Unexpected type encountered: {type_name}.')
            if parent_object:
                parent_class_name = parent_object.__class__.__name__
                parent_id_field_name = f'{parent_class_name}ID'
                parent_id_attr_name = {v: k for k, v in parent_object.attribute_map.items()}.get(
                    parent_id_field_name, '_')
                parent_id_val = getattr(
                    parent_object, parent_id_attr_name, None)
                if parent_id_val:
                    record[parent_id_field_name] = parent_id_val
                    field_types[parent_id_field_name] = 'str'
                    primary_key.add(parent_id_field_name)
            def parse_struct(struct: BaseModel, prefix: str):
                for struct_attr_name in struct.attribute_map:
                    struct_attr_val = getattr(struct, struct_attr_name)
                    struct_field_name = struct.attribute_map[struct_attr_name]
                    parent_field_name = f'{prefix}_{struct_field_name}'
                    if isinstance(struct_attr_val, List):
                        raise XeroClientException(
                                f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
                    elif isinstance(struct_attr_val, BaseModel):
                        if f'{field_name}ID' in struct.attribute_map.values():
                            raise XeroClientException(
                                f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
                        else:
                            parse_struct(struct_attr_val, parent_field_name)
                    elif struct_attr_val:
                        record[parent_field_name] = serialize(
                            struct_attr_val) if struct_attr_val else None
                        field_types[parent_field_name] = resolve_serialized_type(
                            struct.openapi_types[struct_attr_name], struct=True)
                pass
            for attribute_name in object.attribute_map:
                attribute_value = getattr(object, attribute_name)
                field_name = object.attribute_map[attribute_name]
                if isinstance(attribute_value, List):
                    for sub_object in attribute_value:
                        assert isinstance(sub_object, BaseModel)
                        parse_object(sub_object, parent_object=object)
                elif isinstance(attribute_value, BaseModel):
                    if f'{field_name}ID' in attribute_value.attribute_map.values(): # check if struct or full object
                        sub_id_attr_name, sub_id_val = parse_object(
                            attribute_value)
                        assert isinstance(sub_id_val, str)
                        sub_id_field_name = attribute_value.attribute_map.get(
                            sub_id_attr_name)
                        record[sub_id_field_name] = sub_id_val
                        field_types[sub_id_field_name] = attribute_value.openapi_types[sub_id_attr_name]
                    else:
                        parse_struct(attribute_value, field_name)
                elif attribute_value:
                    record[field_name] = serialize(attribute_value)
            if len(record) > 0:  # TODO: ignore ID only records
                table = tables.get(class_name)
                if table is None:
                    table_definiton = self.component.create_out_table_definition(name=f'{class_name}.csv',
                                                                                 primary_key=list(
                                                                                     primary_key),
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
                else:
                    extra_columns = sorted(set(field_types.keys()) -
                                           set(table.table_definition.columns))
                    if extra_columns:
                        table.table_definition.columns.extend(extra_columns)
                        for extra_column in extra_columns:
                            output_type = TERMINAL_TYPE_MAPPING[field_types[extra_column]]
                            table.table_definition.table_metadata.add_column_data_type(column=extra_column,
                                                                                   source_data_type=field_types[extra_column],
                                                                                   data_type=output_type['type'],
                                                                                   length=output_type.get('length'))  
                table.data.append(record)
            return id
        parse_object(root_object)
        return list(tables.values())
