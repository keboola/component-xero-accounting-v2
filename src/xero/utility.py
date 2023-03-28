from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Union, Callable
from keboola.component.dao import SupportedDataTypes
from xero_python.models import BaseModel
from xero_python.accounting import AccountingApi
import xero_python.accounting.models
from xero_python.api_client.serializer import LIST_DATA_TYPE


class XeroException(Exception):
    pass


@dataclass
class KeboolaTypeSpec:
    type: SupportedDataTypes
    length: str = None


# Configuration variables
TERMINAL_TYPE_MAPPING = {'str': KeboolaTypeSpec(type=SupportedDataTypes.STRING),
                         'int': KeboolaTypeSpec(type=SupportedDataTypes.INTEGER),
                         'float': KeboolaTypeSpec(type=SupportedDataTypes.NUMERIC, length='38,8'),
                         'bool': KeboolaTypeSpec(type=SupportedDataTypes.BOOLEAN),
                         'date': KeboolaTypeSpec(type=SupportedDataTypes.DATE),
                         'datetime': KeboolaTypeSpec(type=SupportedDataTypes.TIMESTAMP)}


def get_element_type_name(type_str: str) -> Union[str, None]:
    match = LIST_DATA_TYPE.search(type_str)
    if match:
        return match.group(1)
    else:
        return None


def resolve_attribute_type(type_name: str) -> str:
    if type_name in TERMINAL_TYPE_MAPPING:
        r = type_name
    elif type_name.startswith("datetime"):
        r = "datetime"
    elif type_name.startswith("date"):
        r = "date"
    elif type_name.startswith("list"):
        r = 'list'
    elif issubclass(get_accounting_model(type_name), Enum):
        r = 'str'
    elif issubclass(get_accounting_model(type_name), BaseModel):
        model: BaseModel = get_accounting_model(type_name)
        if model.is_downloadable():
            r = 'downloadable_object'
        else:
            r = 'struct'
    else:
        raise XeroException(
            f'Unexpected type encountered: {type_name}.')
    return r


def get_accounting_model(model_name: str) -> Union[BaseModel, None]:
    return getattr(xero_python.accounting.models, model_name, None)


def add_as_a_method_of(cls):
    def decorator(func):
        setattr(cls, func.__name__, func)
        return func

    return decorator


# Adding methods to BaseModel class (monkey patching)


class EnhancedBaseModel(BaseModel):
    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_field_names(cls: BaseModel) -> List[str]:
        return list(cls.attribute_map.values())

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_field_name(cls: BaseModel, attr_name: str) -> Union[str, None]:
        return cls.attribute_map.get(attr_name)

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_attr_name(cls: BaseModel, field_name: str) -> Union[str, None]:
        inv_map = {v: k for k, v in cls.attribute_map.items()}
        return inv_map.get(field_name)

    @add_as_a_method_of(BaseModel)
    def get_field_value(self: BaseModel, field_name: str, default=None) -> Any:
        attr_name = self.get_attr_name(field_name)
        if attr_name:
            return getattr(self, attr_name, default)
        else:
            return default

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_id_field_name(cls: BaseModel) -> Union[str, None]:
        id_field_name = f'{cls.__name__}ID'
        if id_field_name in cls.get_field_names():
            return id_field_name
        else:
            return None

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_id_attribute_name(self: BaseModel) -> Union[str, None]:
        return self.get_attr_name(self.get_id_field_name())

    @add_as_a_method_of(BaseModel)
    def get_id_value(self: BaseModel) -> Union[str, None]:
        id_value = self.get_field_value(self.get_id_field_name())
        if id_value:
            assert isinstance(id_value, str)
        return id_value

    @add_as_a_method_of(BaseModel)
    @classmethod
    def has_id(cls: BaseModel) -> Union[str, None]:
        return cls.get_id_attribute_name() is not None

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_download_method_name(cls: BaseModel) -> Union[Callable, None]:
        id_attr_name = cls.get_id_attribute_name()
        getter_name = None
        if id_attr_name:
            getter_name = f'get_{id_attr_name.replace("_id", "")}'
        else:
            if len(cls.attribute_map) == 1:
                getter_name = f'get_{cls.get_attr_name(cls.__name__)}'
        if getter_name and hasattr(AccountingApi, getter_name):
            return getter_name
        else:
            return None

    @add_as_a_method_of(BaseModel)
    @classmethod
    def is_downloadable(cls: BaseModel) -> bool:
        return cls.get_download_method_name() is not None

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_list_attribute_name(cls: BaseModel) -> Union[str, None]:
        attr_list = list(cls.attribute_map.keys())
        attr_name = attr_list[0]
        attr_type = cls.openapi_types[attr_name]
        if len(attr_list) == 1 and LIST_DATA_TYPE.match(attr_type):
            return attr_name
        else:
            return None

    @add_as_a_method_of(BaseModel)
    def to_list(self: BaseModel) -> List[BaseModel]:
        return getattr(self, self.get_list_attribute_name())

    @add_as_a_method_of(BaseModel)
    @classmethod
    def is_wrapped_list(cls: BaseModel) -> bool:
        return cls.get_list_attribute_name() is not None

    @add_as_a_method_of(BaseModel)
    def is_empty_list(self: BaseModel) -> bool:
        return len(self.to_list()) == 0

    @add_as_a_method_of(BaseModel)
    @classmethod
    def get_contained_model(cls: BaseModel) -> BaseModel:
        list_attr_name: Union[str, None] = cls.get_list_attribute_name()
        if list_attr_name:
            model_name = get_element_type_name(
                cls.openapi_types[list_attr_name])
            return get_accounting_model(model_name)
        else:
            return cls
