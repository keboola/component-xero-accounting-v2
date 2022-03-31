from typing import Any, Union, Dict, List
import hashlib
import json

from xero_python.api_client.serializer import serialize

from .utility import XeroException, TERMINAL_TYPE_MAPPING, resolve_attribute_type, EnhancedBaseModel, KeboolaDeleteWhereSpec, TableData


class TableDataFactory:
    def __init__(self, accounting_object_list: List[EnhancedBaseModel]) -> None:
        self.accounting_object_list = accounting_object_list

        self._tables_data: Union[Dict[str, TableData], None] = None

    def get_table_definitions(self) -> Dict[str, TableData]:
        if not self._tables_data:
            self._tables_data = {}
            self._add_data_from_object_list(self.accounting_object_list)
        return self._tables_data

    def _add_data_from_object_list(self, accounting_object_list: List[EnhancedBaseModel], table_name_prefix: str = None,
                                   parent_id_field_name: str = None, parent_id_field_value: str = None) -> None:
        for accounting_object in accounting_object_list:
            self._add_data_from_object(accounting_object, table_name_prefix=table_name_prefix,
                                       parent_id_field_name=parent_id_field_name, parent_id_field_value=parent_id_field_value)

    def _add_data_from_object(self, accounting_object: EnhancedBaseModel, table_name_prefix: str = None,
                              parent_id_field_name: str = None, parent_id_field_value: str = None) -> None:
        table_name = accounting_object.__class__.__name__
        row_dict = {}
        id_field_value = accounting_object.get_id_value()
        if id_field_value:
            id_field_name = accounting_object.get_id_field_name()
        else:
            id_field_name = f'{table_name}ID'
            id_field_value = hashlib.md5(json.dumps(serialize(accounting_object), sort_keys=True
                                                    ).encode('utf-8')).hexdigest()
            row_dict[id_field_name] = id_field_value
        if parent_id_field_name:
            if parent_id_field_value:
                table_name = f'{table_name_prefix}_{table_name}'
                row_dict[parent_id_field_name] = parent_id_field_value
            else:
                raise XeroException(
                    "Parent object must have defined ID if specified.")
        for attribute_name, attribute_type_name in accounting_object.openapi_types.items():
            attribute_value = getattr(accounting_object, attribute_name)
            if attribute_value:
                field_name = accounting_object.get_field_name(attribute_name)
                row_dict = row_dict | self._get_data_from_attribute(
                    value=attribute_value, type_name=attribute_type_name, field_name=field_name,
                    table_name=table_name, id_field_name=id_field_name, id_field_value=id_field_value)
        if len(row_dict) > 0:
            table_data = self._tables_data.get(table_name)
            if table_data is None:
                if parent_id_field_name:
                    table_data = TableData(to_add=[],
                                           to_delete=KeboolaDeleteWhereSpec(column=parent_id_field_name))
                else:
                    table_data = TableData()
                self._tables_data[table_name] = table_data
            table_data.to_add.append(row_dict)
            if parent_id_field_name:
                table_data.to_delete.values.add(id_field_value)

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
                    return {}
                elif element:
                    raise XeroException(
                        f'Unexpected type encountered: {type_name(element)} within list in {field_name} field within object'
                        f' of type {table_name}.')
        elif resolved_type == 'downloadable_object':
            sub_id_field_name = value.get_id_field_name()
            sub_id_val = value.get_id_value()
            return {sub_id_field_name: sub_id_val}
        elif resolved_type == 'struct':
            return TableDataFactory._flatten_struct(value, prefix=field_name)
        elif resolved_type in TERMINAL_TYPE_MAPPING:
            return {field_name: serialize(value)}

    @staticmethod
    def _flatten_struct(struct: EnhancedBaseModel, prefix: str) -> Dict[str, Any]:
        flattened_struct = {}
        for struct_attr_name, struct_attr_type_name in struct.openapi_types.items():
            struct_attr_val = getattr(struct, struct_attr_name)
            if struct_attr_val:
                resolved_type = resolve_attribute_type(struct_attr_type_name)
                struct_field_name = struct.get_field_name(struct_attr_name)
                field_name_inside_parent = f'{prefix}_{struct_field_name}'
                if resolved_type == 'struct':
                    flattened_struct = flattened_struct | TableDataFactory._flatten_struct(
                        struct_attr_val, prefix=field_name_inside_parent)
                elif resolved_type in TERMINAL_TYPE_MAPPING:
                    flattened_struct[field_name_inside_parent] = serialize(
                        struct_attr_val)
                else:
                    raise XeroException(
                        f'Unexpected type encountered in struct: {struct.openapi_types[struct_attr_name]}.')
        return flattened_struct
