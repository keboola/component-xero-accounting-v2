from dataclasses import dataclass
import inspect
import logging
from typing import Dict, Iterable, List, Union
from enum import Enum
import hashlib
import json

from keboola.component.dao import OauthCredentials, TableDefinition
from keboola.component.exceptions import UserException
from keboola.component import ComponentBase

from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.models import BaseModel
from xero_python.api_client.serializer import serialize

from xero_python.exceptions.http_status_exceptions import OAuth2InvalidGrantError, HTTPStatusException

from xero.table_definition_factory import TableDefinitionFactory

# Always import utility to monkey patch BaseModel
from .utility import XeroException, get_accounting_model, TERMINAL_TYPE_MAPPING


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
            raise XeroException(oauth_err) from oauth_err
        return available_tenants

    def force_refresh_token(self):
        try:
            self._api_client.refresh_oauth2_token()
        except HTTPStatusException as http_error:
            raise XeroException(
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

    def get_accounting_object(self, model_name: str, **kwargs) -> Iterable[List[BaseModel]]:
        accounting_api = AccountingApi(self._api_client)
        model: BaseModel = get_accounting_model(model_name)
        getter_name = model.get_download_method_name()
        if getter_name:
            getter = getattr(accounting_api, getter_name)
            getter_signature = inspect.signature(getter)
            used_kwargs = {k: v for k, v in kwargs.items()
                           if k in getter_signature.parameters and v is not None}
            if 'page' in getter_signature.parameters:
                used_kwargs['page'] = 1
                while True:
                    accounting_object = getter(self.tenant_id, **used_kwargs)
                    if accounting_object.is_empty_list():
                        break
                    yield accounting_object.to_list()
                    used_kwargs['page'] = used_kwargs['page'] + 1
            else:
                yield getter(self.tenant_id, **used_kwargs).to_list()
        else:
            raise XeroException(
                f"Requested model ({model_name}) getter function not found.")

    def get_serialized_accounting_object(self, model_name: str, **kwargs) -> Dict:
        return serialize(self.get_accounting_object(model_name, **kwargs))

    def parse_accounting_object_list_into_tables(self, accounting_object_list: List[BaseModel]) -> Dict[str, Table]:
        tables: Dict[str, Table] = {}

        def resolve_serialized_type(type_name: str, struct: bool = False) -> Union[str, None]:
            # TODO: separate table definition creation to an independent XeroClient method independent of data
            # TODO: add special case for date/timestamp as metadata
            # TODO: resolve down to Keboola supported type with length
            if type_name in TERMINAL_TYPE_MAPPING:
                r = type_name
            elif type_name.startswith('list'):
                r = None
            elif type_name.startswith('date') or issubclass(get_accounting_model(type_name), Enum):
                r = 'str'
            elif issubclass(get_accounting_model(type_name), BaseModel):
                r = None
            else:
                raise XeroException(
                    f'Unexpected type encountered: {type_name}.')
            if struct and r is None:
                raise XeroException(
                    f'Unexpected type encountered in struct: {type_name}.')
            return r

        def parse_object(accounting_object: BaseModel,
                         table_name_prefix: str = None,
                         parent_id_field_name: str = None,
                         parent_id_field_value: str = None) -> None:
            table_name = accounting_object.__class__.__name__
            row_dict = {}
            field_types = {}
            id_field_value = accounting_object.get_id_value()
            if id_field_value:
                id_field_name = accounting_object.get_id_field_name()
            else:
                id_field_name = f'{table_name}ID'
                id_field_value = hashlib.md5(json.dumps(serialize(accounting_object), sort_keys=True
                                                        ).encode('utf-8')).hexdigest()
                row_dict[id_field_name] = id_field_value
                field_types[id_field_name] = 'str'
            primary_key = {id_field_name}
            for attribute_name in accounting_object.attribute_map:
                resolved_type_name = resolve_serialized_type(
                    accounting_object.openapi_types[attribute_name])
                if resolved_type_name:
                    field_types[accounting_object.attribute_map[attribute_name]
                                ] = resolved_type_name
            if parent_id_field_name:
                # TODO: add delete_where parameters to manifest file (those with the same parent_id_field values) when using incremental load
                # e.g.: subtables of Contact need to have the rows with ContactID equal to those of one of the processed Contacts deleted first
                if parent_id_field_value:
                    table_name = f'{table_name_prefix}_{table_name}'
                    row_dict[parent_id_field_name] = parent_id_field_value
                    field_types[parent_id_field_name] = 'str'
                    primary_key.add(parent_id_field_name)
                else:
                    raise XeroException(
                        "Parent object must have defined ID if specified.")

            def parse_struct(struct: BaseModel, prefix: str):
                for struct_attr_name in struct.attribute_map:
                    struct_attr_val = getattr(struct, struct_attr_name)
                    struct_field_name = struct.get_field_name(struct_attr_name)
                    field_name_inside_parent = f'{prefix}_{struct_field_name}'
                    if isinstance(struct_attr_val, List):
                        raise XeroException(
                            f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
                    elif isinstance(struct_attr_val, BaseModel):
                        if struct.is_downloadable():
                            raise XeroException(
                                f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
                        else:
                            parse_struct(struct_attr_val,
                                         field_name_inside_parent)
                    elif struct_attr_val:
                        row_dict[field_name_inside_parent] = serialize(
                            struct_attr_val)
                        field_types[field_name_inside_parent] = resolve_serialized_type(
                            struct.openapi_types[struct_attr_name], struct=True)
            for attribute_name in accounting_object.attribute_map:
                attribute_value = getattr(accounting_object, attribute_name)
                field_name = accounting_object.attribute_map[attribute_name]
                if isinstance(attribute_value, List):
                    for sub_object in attribute_value:
                        if isinstance(sub_object, List):
                            raise XeroException(
                                f'Unexpected type encountered: list within list in {field_name} field within object'
                                f' of type {table_name}.')
                        elif isinstance(sub_object, BaseModel):
                            parse_object(sub_object, table_name_prefix=table_name,
                                         parent_id_field_name=id_field_name, parent_id_field_value=id_field_value)
                        elif sub_object:
                            raise XeroException(
                                f'Unexpected type encountered: {type(sub_object)} within list in {field_name} field within object'
                                f' of type {table_name}.')
                elif isinstance(attribute_value, BaseModel):
                    if attribute_value.is_downloadable():
                        sub_id_field_name = attribute_value.get_id_field_name()
                        sub_id_val = attribute_value.get_id_value()
                        row_dict[sub_id_field_name] = sub_id_val
                        field_types[sub_id_field_name] = 'str'
                    else:
                        parse_struct(attribute_value, prefix=field_name)
                elif attribute_value:
                    row_dict[field_name] = serialize(attribute_value)
            if len(row_dict) > 0:
                table = tables.get(table_name)
                if table is None:
                    table_definiton = self.component.create_out_table_definition(name=f'{table_name}.csv',
                                                                                 primary_key=list(
                                                                                     primary_key),
                                                                                 columns=list(field_types.keys()))
                    table = Table(data=[],
                                  table_definition=table_definiton)
                    for _field_name, source_field_type in field_types.items():
                        output_type = TERMINAL_TYPE_MAPPING[source_field_type]
                        table.table_definition.table_metadata.add_column_data_type(column=_field_name,
                                                                                   source_data_type=source_field_type,
                                                                                   data_type=output_type.type,
                                                                                   length=output_type.length)
                    tables[table_name] = table
                else:
                    extra_columns = sorted(set(field_types.keys()) -
                                           set(table.table_definition.columns))
                    if extra_columns:
                        table.table_definition.columns.extend(extra_columns)
                        for extra_column in extra_columns:
                            output_type = TERMINAL_TYPE_MAPPING[field_types[extra_column]]
                            table.table_definition.table_metadata.add_column_data_type(column=extra_column,
                                                                                       source_data_type=field_types[
                                                                                           extra_column],
                                                                                       data_type=output_type.type,
                                                                                       length=output_type.length)
                table.data.append(row_dict)
            return id

        for o in accounting_object_list:
            parse_object(o)
        return tables

    def get_table_definitions(self, model_name: str) -> Dict[str, TableDefinition]:
        return TableDefinitionFactory(model_name, self.component).get_table_definitions()
