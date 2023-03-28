from typing import Union, Dict
from keboola.component import ComponentBase
from keboola.component.dao import TableDefinition

from .utility import KeboolaTypeSpec, XeroException, get_accounting_model, get_element_type_name,\
    TERMINAL_TYPE_MAPPING, resolve_attribute_type, EnhancedBaseModel


class TableDefinitionFactory:
    def __init__(self, input_model_name: str, component: ComponentBase) -> None:
        self.input_model: EnhancedBaseModel = get_accounting_model(input_model_name)
        self.root_model: EnhancedBaseModel = self.input_model.get_contained_model()
        self.component = component

        self._table_defs: Union[Dict[str, TableDefinition], None] = None

    def get_table_definitions(self) -> Dict[str, TableDefinition]:
        if not self._table_defs:
            self._table_defs = {}
            self.add_table_def_of(self.root_model)
        return self._table_defs

    def add_table_def_of(self, model: EnhancedBaseModel,
                         table_name_prefix: str = None,
                         parent_id_field_name: str = None) -> None:
        table_name: str = model.__name__
        field_types: Dict[str, KeboolaTypeSpec] = {}
        id_field_name = model.get_id_field_name()
        if not id_field_name:
            id_field_name = f'{table_name}ID'
            field_types[id_field_name] = TERMINAL_TYPE_MAPPING['str']
        primary_key = {id_field_name}
        if parent_id_field_name:
            table_name = f'{table_name_prefix}_{table_name}'
            field_types[parent_id_field_name] = TERMINAL_TYPE_MAPPING['str']
            primary_key.add(parent_id_field_name)
        for attr_name, type_name in model.openapi_types.items():
            field_types = field_types | self._get_field_types_of_attribute(
                type_name=type_name, field_name=model.get_field_name(
                    attr_name),
                table_name_prefix=table_name, parent_id_field_name=id_field_name)
        if len(field_types) > 0:
            self._table_defs[table_name] = self.component.create_out_table_definition(name=f'{table_name}.csv',
                                                                                      primary_key=list(primary_key),
                                                                                      columns=list(field_types.keys()))
            for _field_name, field_type in field_types.items():
                self._table_defs[table_name].table_metadata.add_column_data_type(column=_field_name,
                                                                                 data_type=field_type.type,
                                                                                 length=field_type.length)

    def _get_field_types_of_attribute(self, type_name: str, field_name: str,
                                      table_name_prefix: str, parent_id_field_name: str) -> Dict[str, KeboolaTypeSpec]:
        resolved_type = resolve_attribute_type(type_name)
        if resolved_type in TERMINAL_TYPE_MAPPING:
            return {field_name: TERMINAL_TYPE_MAPPING[resolved_type]}
        elif resolved_type == 'downloadable_object':
            sub_id_field_name = get_accounting_model(
                type_name).get_id_field_name()
            return {sub_id_field_name: TERMINAL_TYPE_MAPPING['str']}
        elif resolved_type == 'struct':
            return TableDefinitionFactory._get_field_types_of_struct(
                get_accounting_model(type_name), prefix=field_name)
        elif resolved_type == 'list':
            element_type_name = get_element_type_name(
                type_name)
            # This prevents infinite recursion (Contacts <-> ContactGroups)
            if element_type_name != self.root_model.__name__:
                element_resolved_type = resolve_attribute_type(
                    element_type_name)
                if element_resolved_type in ('struct', 'downloadable_object'):
                    self.add_table_def_of(get_accounting_model(element_type_name), table_name_prefix=table_name_prefix,
                                          parent_id_field_name=parent_id_field_name)
                    return {}
            else:
                return {}
        else:
            raise XeroException(
                f"Unexpected attribute type encountered: {type_name}.")

    @staticmethod
    def _get_field_types_of_struct(struct: EnhancedBaseModel, prefix: str) -> Dict[str, KeboolaTypeSpec]:
        field_types = {}
        for struct_attr_name, struct_attr_type_name in struct.openapi_types.items():
            struct_attr_handled = False
            struct_field_name = struct.get_field_name(struct_attr_name)
            field_name_inside_parent = f'{prefix}_{struct_field_name}'
            resolved_struct_attr_type_name = resolve_attribute_type(
                struct_attr_type_name)
            if resolved_struct_attr_type_name:
                if resolved_struct_attr_type_name in TERMINAL_TYPE_MAPPING:
                    field_types[field_name_inside_parent] = TERMINAL_TYPE_MAPPING[resolved_struct_attr_type_name]
                    struct_attr_handled = True
                elif resolved_struct_attr_type_name == 'struct':
                    struct_attr_model: EnhancedBaseModel = get_accounting_model(
                        struct_attr_type_name)
                    field_types = field_types | TableDefinitionFactory._get_field_types_of_struct(
                        struct_attr_model, field_name_inside_parent)
                    struct_attr_handled = True
            if not struct_attr_handled:
                raise XeroException(
                    f'Unexpected type encountered in struct: {struct_attr_type_name}.')
        return field_types
