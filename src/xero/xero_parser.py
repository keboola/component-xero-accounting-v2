from typing import Any, Dict, List, Tuple
import hashlib
import json

from xero_python.api_client.serializer import serialize

from .utility import XeroException, TERMINAL_TYPE_MAPPING, resolve_attribute_type, \
    EnhancedBaseModel


class XeroParser:
    def __init__(self) -> None:
        self.parsed_data = None

    def parse_data(self, xero_object_data) -> Dict[str, List[Dict]]:
        self.parsed_data = {}
        self._parse_data(xero_object_data)
        return self.parsed_data

    def _parse_data(self, accounting_object_list: List[EnhancedBaseModel]) -> None:
        for accounting_object in accounting_object_list:
            self._add_data_from_object(accounting_object)

    def _add_data_from_object(self, xero_object_data: EnhancedBaseModel, table_name_prefix: str = None,
                              parent_id_field_name: str = None, parent_id_field_value: str = None) -> None:

        table_name = self._create_table_name(xero_object_data, table_name_prefix)
        id_field_name, id_field_value = self._get_xero_object_id_name_and_value(xero_object_data)
        row_dict = {id_field_name: id_field_value}
        row_dict |= self._get_parent_id_name_and_value(parent_id_field_name, parent_id_field_value)
        row_dict |= self._parse_fields(xero_object_data, table_name, id_field_name, id_field_value)

        if not self.parsed_data.get(table_name):
            self.parsed_data[table_name] = []
        self.parsed_data[table_name].append(row_dict)

    def _parse_fields(self, xero_object_data: EnhancedBaseModel, table_name: str, id_field_name: str,
                      id_field_value: str) -> Dict:
        field_data = {}
        for attribute_name, attribute_type_name in xero_object_data.openapi_types.items():
            attribute_value = getattr(xero_object_data, attribute_name)
            if attribute_value is not None:
                field_name = xero_object_data.get_field_name(attribute_name)
                attribute_dict = self._get_data_from_attribute(
                    value=attribute_value, type_name=attribute_type_name, field_name=field_name,
                    table_name=table_name, id_field_name=id_field_name, id_field_value=id_field_value)
                field_data = field_data | attribute_dict
        return field_data

    def _get_data_from_attribute(self, value, type_name: str, field_name: str, table_name: str,
                                 id_field_name: str, id_field_value: str) -> Dict[str, Any]:
        resolved_type = resolve_attribute_type(type_name)
        if resolved_type == 'list':
            for element in value:
                element_type_name = element.__class__.__name__
                element_resolved_type_name = resolve_attribute_type(
                    element_type_name)
                if element_resolved_type_name in ('struct', 'downloadable_object'):
                    self._add_data_from_object(element, table_name_prefix=table_name,
                                               parent_id_field_name=id_field_name, parent_id_field_value=id_field_value)

                elif element is not None:
                    raise XeroException(
                        f'Unexpected type encountered: {type_name(element)}'
                        f' within list in {field_name} field within object'
                        f' of type {table_name}.')
            return {}

        elif resolved_type == 'downloadable_object':
            sub_id_field_name = value.get_id_field_name()
            sub_id_val = value.get_id_value()
            return {sub_id_field_name: sub_id_val}
        elif resolved_type == 'struct':
            return self._flatten_struct(value, prefix=field_name)
        elif resolved_type in TERMINAL_TYPE_MAPPING:
            return {field_name: serialize(value)}

    def _flatten_struct(self, struct: EnhancedBaseModel, prefix: str) -> Dict[str, Any]:
        flattened_struct = {}
        for struct_attr_name, struct_attr_type_name in struct.openapi_types.items():
            struct_attr_val = getattr(struct, struct_attr_name)
            if struct_attr_val is not None:
                resolved_type = resolve_attribute_type(struct_attr_type_name)
                struct_field_name = struct.get_field_name(struct_attr_name)
                field_name_inside_parent = f'{prefix}_{struct_field_name}'
                if resolved_type == 'struct':
                    flattened_struct = flattened_struct | self._flatten_struct(
                        struct_attr_val, prefix=field_name_inside_parent)
                elif resolved_type in TERMINAL_TYPE_MAPPING:
                    flattened_struct[field_name_inside_parent] = serialize(
                        struct_attr_val)
                else:
                    raise XeroException(
                        f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
        return flattened_struct

    @staticmethod
    def _generate_hash_id(data_to_hash: bytes) -> str:
        return hashlib.md5(data_to_hash).hexdigest()

    @staticmethod
    def _dump_xero_object_data(accounting_object: EnhancedBaseModel) -> bytes:
        return json.dumps(serialize(accounting_object), sort_keys=True).encode('utf-8')

    def _get_xero_object_id_name_and_value(self, xero_object_data: EnhancedBaseModel) -> Tuple[str, str]:
        table_name = xero_object_data.__class__.__name__
        id_field_value = xero_object_data.get_id_value()
        if id_field_value:
            id_field_name = xero_object_data.get_id_field_name()
        else:
            id_field_name = f'{table_name}ID'
            id_field_value = self._generate_hash_id(self._dump_xero_object_data(xero_object_data))

        return id_field_name, id_field_value

    @staticmethod
    def _create_table_name(xero_object_data: EnhancedBaseModel, table_name_prefix: str) -> str:
        table_name = xero_object_data.__class__.__name__
        if table_name_prefix:
            table_name = f'{table_name_prefix}_{table_name}'
        return table_name

    @staticmethod
    def _get_parent_id_name_and_value(parent_id_field_name: str, parent_id_field_value: str) -> Dict:
        if parent_id_field_name:
            if parent_id_field_value is None:
                raise XeroException("Parent object must have defined ID if specified.")
            else:
                return {parent_id_field_name: parent_id_field_value}
        return {}
