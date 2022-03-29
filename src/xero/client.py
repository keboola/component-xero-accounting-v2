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


def _get_accounting_model(model_name: str) -> Union[BaseModel, None]:
    return getattr(xero_python.accounting.models, model_name, None)

# Decorator that adds the decorated function as a method of cls


def add_as_a_method_of(cls):
    def decorator(func):
        setattr(cls, func.__name__, func)
        return func
    return decorator

# Adding methods to BaseModel class


@add_as_a_method_of(BaseModel)
def get_field_names(self: BaseModel) -> List[str]:
    return list(self.attribute_map.values())


@add_as_a_method_of(BaseModel)
def get_field_name(self: BaseModel, attr_name: str) -> Union[str, None]:
    return self.attribute_map.get(attr_name)


@add_as_a_method_of(BaseModel)
def get_attr_name(self: BaseModel, field_name: str) -> Union[str, None]:
    inv_map = {v: k for k, v in self.attribute_map.items()}
    return inv_map.get(field_name)


@add_as_a_method_of(BaseModel)
def get_field_value(self: BaseModel, field_name: str, default=None) -> Any:
    attr_name = self.get_attr_name(field_name)
    if attr_name:
        return getattr(self, attr_name, default)
    else:
        return default


@add_as_a_method_of(BaseModel)
def get_id_field_name(self: BaseModel) -> Union[str, None]:
    return f'{self.__class__.__name__}ID'


@add_as_a_method_of(BaseModel)
def get_id_attribute_name(self: BaseModel) -> Union[str, None]:
    return self.get_attr_name(self.get_id_field_name())


@add_as_a_method_of(BaseModel)
def get_id_value(self: BaseModel) -> Union[str, None]:
    id_value = self.get_field_value(self.get_id_field_name())
    if id_value:
        assert isinstance(id_value, str)
    return id_value


@add_as_a_method_of(BaseModel)
def has_id(self: BaseModel) -> Union[str, None]:
    return self.get_id_attribute_name() is not None


@add_as_a_method_of(BaseModel)
def get_download_method_name(self: BaseModel) -> Union[Callable, None]:
    id_attr_name = self.get_id_attribute_name()
    getter_name = None
    if id_attr_name:
        getter_name = f'get_{id_attr_name.replace("_id", "")}'
    else:
        if len(self.attribute_map) == 1:
            getter_name = f'get_{self.get_attr_name(self.__class__.__name__)}'
    if getter_name and hasattr(AccountingApi, getter_name):
        return getter_name
    else:
        return None


@add_as_a_method_of(BaseModel)
def is_downloadable(self: BaseModel) -> bool:
    return self.get_download_method_name() is not None


@add_as_a_method_of(BaseModel)
def to_list(self: BaseModel) -> List[BaseModel]:
    attr_list = list(self.attribute_map.keys())
    assert len(attr_list) == 1
    attr_to_parse = attr_list[0]
    contained_list = getattr(self, attr_to_parse)
    return contained_list


@add_as_a_method_of(BaseModel)
def is_empty_list(self: BaseModel) -> bool:
    return len(self.to_list()) == 0


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

    def get_accounting_object(self, model_name: str, **kwargs) -> Iterable[BaseModel]:
        accounting_api = AccountingApi(self._api_client)
        model: BaseModel = _get_accounting_model(model_name)()
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

    def parse_accounting_object_into_tables(self, root_object: BaseModel) -> List[Table]:
        tables: Dict[str, Table] = {}

        def resolve_serialized_type(type_name: str, struct: bool = False) -> Union[str, None]:
            # TODO: separate table definition creation to an independent XeroClient method independent of data
            # TODO: add special case for date/timestamp as metadata
            # TODO: resolve down to Keboola supported type with length
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
                            struct_attr_val) if struct_attr_val else None
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

        for o in root_object.to_list():
            parse_object(o)
        return list(tables.values())
