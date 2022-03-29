from dataclasses import dataclass
import inspect
import logging
from typing import Any, Dict, Iterable, List, Union, Callable
from enum import Enum
import hashlib
import json

from keboola.component.dao import OauthCredentials, SupportedDataTypes, TableDefinition
from keboola.component.exceptions import UserException
from keboola.component import ComponentBase

from xero_python.identity import IdentityApi
from xero_python.accounting import AccountingApi
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.models import BaseModel
from xero_python.api_client.serializer import serialize, serialize_routing

from xero_python.exceptions.http_status_exceptions import OAuth2InvalidGrantError, HTTPStatusException

# Always import utility to monkey patch BaseModel
from .utility import get_accounting_model, get_element_type_name

# Configuration variables
TERMINAL_TYPE_MAPPING = {'str': {'type': SupportedDataTypes.STRING},
                         'int': {'type': SupportedDataTypes.INTEGER},
                         'float': {'type': SupportedDataTypes.NUMERIC, 'length': '38,8'},
                         'bool': {'type': SupportedDataTypes.BOOLEAN},
                         'date': {'type': SupportedDataTypes.DATE},
                         'datetime': {'type': SupportedDataTypes.TIMESTAMP}}


class XeroClientException(Exception):
    pass


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

    def get_accounting_object(self, model_name: str, **kwargs) -> Iterable[BaseModel]:
        accounting_api = AccountingApi(self._api_client)
        model: BaseModel = get_accounting_model(model_name)
        getter_name = model.get_download_method_name()
        if getter_name:
            getter = getattr(accounting_api, getter_name)
            getter_signature = inspect.signature(getter)
            used_kwargs = {k: v for k, v in kwargs.items(
            ) if k in getter_signature.parameters}
            if 'page' in getter_signature.parameters:
                used_kwargs['page'] = 1
                while True:
                    accounting_object = getter(self.tenant_id, **used_kwargs)
                    if accounting_object.is_empty_list():
                        break
                    yield accounting_object
                    used_kwargs['page'] = used_kwargs['page'] + 1
            else:
                yield getter(self.tenant_id, **used_kwargs)
        else:
            raise XeroClientException(
                f"Requested model ({model_name}) getter function not found.")

    def get_serialized_accounting_object(self, model_name: str, **kwargs) -> Dict:
        return serialize(self.get_accounting_object(model_name, **kwargs))

    def parse_accounting_object_into_tables(self, root_object: BaseModel) -> Dict[str, Table]:
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
                raise XeroClientException(
                    f'Unexpected type encountered: {type_name}.')
            if struct and r is None:
                raise XeroClientException(
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
                if parent_id_field_value:
                    table_name = f'{table_name_prefix}_{table_name}'
                    row_dict[parent_id_field_name] = parent_id_field_value
                    field_types[parent_id_field_name] = 'str'
                    primary_key.add(parent_id_field_name)
                else:
                    raise XeroClientException(
                        "Parent object must have defined ID if specified.")

            def parse_struct(struct: BaseModel, prefix: str):
                for struct_attr_name in struct.attribute_map:
                    struct_attr_val = getattr(struct, struct_attr_name)
                    struct_field_name = struct.get_field_name(struct_attr_name)
                    field_name_inside_parent = f'{prefix}_{struct_field_name}'
                    if isinstance(struct_attr_val, List):
                        raise XeroClientException(
                            f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
                    elif isinstance(struct_attr_val, BaseModel):
                        if struct.is_downloadable():
                            raise XeroClientException(
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
                            raise XeroClientException(
                                f'Unexpected type encountered: list within list in {field_name} field within object'
                                f' of type {table_name}.')
                        elif isinstance(sub_object, BaseModel):
                            parse_object(sub_object, table_name_prefix=table_name,
                                         parent_id_field_name=id_field_name, parent_id_field_value=id_field_value)
                            if sub_object.is_downloadable():
                                pass  # TODO: warn that full object may be downloadable via different endpoint?
                        elif sub_object:
                            raise XeroClientException(
                                f'Unexpected type encountered: {type(sub_object)} within list in {field_name} field within object'
                                f' of type {table_name}.')
                elif isinstance(attribute_value, BaseModel):
                    if attribute_value.is_downloadable():
                        # TODO?: log warning to suggest downloading the appropriate endpoint to the user?
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
                                                                                   data_type=output_type['type'],
                                                                                   length=output_type.get('length'))
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
                                                                                       data_type=output_type['type'],
                                                                                       length=output_type.get('length'))
                table.data.append(row_dict)
            return id

        assert root_object.is_wrapped_list()
        for o in root_object.to_list():
            parse_object(o)
        return tables

    # TODO: Table Definition from model definition
    def get_table_definitions(self, model_name: str) -> Dict[str, TableDefinition]:
        root_model: BaseModel = get_accounting_model(model_name)
        list_attr_name: Union[str, None] = root_model.get_list_attribute_name()
        if list_attr_name:
            model_name = get_element_type_name(
                root_model.openapi_types[list_attr_name])
            root_model = get_accounting_model(model_name)

        def resolve_attribute_type(attribute_type: str) -> str:
            # TODO: make common with parse_accounting_object_into_tables method
            if attribute_type in TERMINAL_TYPE_MAPPING:
                r = attribute_type
            elif attribute_type.startswith("datetime"):
                r = "datetime"
            elif attribute_type.startswith("date"):
                r = "date"
            elif attribute_type.startswith("list"):
                r = 'list'
            elif issubclass(get_accounting_model(attribute_type), Enum):
                r = 'str'
            elif issubclass(get_accounting_model(attribute_type), BaseModel):
                model: BaseModel = get_accounting_model(attribute_type)
                if model.is_downloadable():
                    r = 'downloadable_object'
                else:
                    r = 'struct'
            else:
                raise XeroClientException(
                    f'Unexpected type encountered: {attribute_type}.')
            # if in_struct and r not in TERMINAL_TYPE_MAPPING:
            #     raise XeroClientException(
            #         f'Unexpected type encountered in struct: {attribute_type}.')
            return r
        table_defs: Dict[str, TableDefinition] = {}
        model_name: str
        def add_table_def_of(model: BaseModel,
                             table_name_prefix: str = None,
                             parent_id_field_name: str = None):
            table_name: str = model.__name__
            field_types = {}
            id_field_name = model.get_id_field_name()
            if not id_field_name:
                id_field_name = f'{table_name}ID'
                field_types[id_field_name] = TERMINAL_TYPE_MAPPING['str']
            primary_key = {id_field_name}
            if parent_id_field_name:
                table_name = f'{table_name_prefix}_{table_name}'
                field_types[parent_id_field_name] = TERMINAL_TYPE_MAPPING['str']
                primary_key.add(parent_id_field_name)

            def add_field_types_of_struct(struct: BaseModel, prefix: str):
                for struct_attr_name, struct_attr_type_name in struct.openapi_types.items():
                    struct_field_name = struct.get_field_name(struct_attr_name)
                    field_name_inside_parent = f'{prefix}_{struct_field_name}'
                    resolved_struct_attr_type_name = resolve_attribute_type(
                        struct_attr_type_name)
                    if resolved_struct_attr_type_name:
                        if resolved_struct_attr_type_name in TERMINAL_TYPE_MAPPING:
                            field_types[field_name_inside_parent] = TERMINAL_TYPE_MAPPING[resolved_struct_attr_type_name]
                        elif resolved_struct_attr_type_name == 'struct':
                            struct_attr_model: BaseModel = get_accounting_model(
                                struct_attr_type_name)
                            add_field_types_of_struct(
                                struct_attr_model, field_name_inside_parent)
                        else:
                            raise XeroClientException(
                                f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
                    else:
                        raise XeroClientException(
                            f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
            for attr_name, attr_type_name in model.openapi_types.items():
                attr_type_name: str
                field_name = model.attribute_map[attr_name]
                resolved_type = resolve_attribute_type(attr_type_name)
                if resolved_type:
                    if resolved_type in TERMINAL_TYPE_MAPPING:
                        field_types[field_name] = TERMINAL_TYPE_MAPPING[resolved_type]
                    elif resolved_type == 'downloadable_object':
                        sub_id_field_name = get_accounting_model(
                            attr_type_name).get_id_field_name()
                        field_types[sub_id_field_name] = TERMINAL_TYPE_MAPPING['str']
                    elif resolved_type == 'struct':
                        add_field_types_of_struct(
                            get_accounting_model(attr_type_name), prefix=field_name)
                    elif resolved_type == 'list':
                        element_type_name = get_element_type_name(
                            attr_type_name)
                        element_resolved_type = resolve_attribute_type(
                            element_type_name)
                        if element_resolved_type:
                            if element_resolved_type == 'struct':
                                add_table_def_of(get_accounting_model(element_type_name), table_name_prefix=table_name,
                                                 parent_id_field_name=id_field_name)
                            elif element_resolved_type == 'downloadable_object':
                                # TODO: warn that full object may be downloadable via different endpoint?
                                if element_type_name != model_name:  # This prevents infinite recrusion here (Contacts <-> ContactGroups)
                                    add_table_def_of(get_accounting_model(element_type_name),
                                                    table_name_prefix=table_name, parent_id_field_name=id_field_name)
                            else:
                                raise XeroClientException(
                                    f"Unexpected attribute type encountered: {attr_type_name}.")
                        else:
                            raise XeroClientException(
                                f"Unexpected attribute type encountered: {attr_type_name}.")
                    else:
                        raise XeroClientException(
                            f"Unexpected attribute type encountered: {attr_type_name}.")
                else:
                    raise XeroClientException(
                        f"Unexpected attribute type encountered: {attr_type_name}.")
            if len(field_types) > 0:
                table_defs[table_name] = self.component.create_out_table_definition(name=f'{table_name}.csv',
                                                                                    primary_key=list(
                                                                                        primary_key),
                                                                                    columns=list(field_types.keys()))
                for _field_name, field_type in field_types.items():
                    table_defs[table_name].table_metadata.add_column_data_type(column=_field_name,
                                                                               data_type=field_type['type'],
                                                                               length=field_type.get('length'))

        add_table_def_of(root_model)
        return table_defs
