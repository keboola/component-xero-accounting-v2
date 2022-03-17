import json

from typing import Dict


class EndpointDefinitionError(Exception):
    pass


class EndpointDefinition:
    def __init__(self, path_to_json="", endpoint_name=""):
        endpoint_definitions = self._get_endpoint_definitions_from_json(path_to_json)
        endpoint_definition = endpoint_definitions.get(endpoint_name)
        if not endpoint_definition:
            valid_endpoint_definitions = list(endpoint_definitions.keys())
            raise EndpointDefinitionError(
                f"Endpoint '{endpoint_name}' not found, only found definitions for {valid_endpoint_definitions}")
        self.parent_table = endpoint_definition.get("parent_table")
        self.child_table_definitions = endpoint_definition.get("child_table_definitions")
        self.table_primary_keys = endpoint_definition.get("table_primary_keys")
        self.root_node = endpoint_definition.get("root_node")
        self.all_tables = self._get_all_table_definitions()

    @staticmethod
    def _get_endpoint_definitions_from_json(path_to_json):
        try:
            with open(path_to_json, 'r') as f:
                return json.load(f)
        except FileNotFoundError as file_not_found_error:
            raise EndpointDefinitionError(
                "Path to endpoint definition is invalid, file does not exist") from file_not_found_error

    def _get_all_table_definitions(self) -> Dict:
        all_tables = self.parent_table
        if self.child_table_definitions:
            all_tables = {**self.parent_table, **self.child_table_definitions}
        return all_tables

    @property
    def parent_table_name(self):
        return list(self.parent_table.keys())[0]

    @property
    def all_table_names(self):
        return list(self.all_tables.keys())

    @property
    def child_table_names(self):
        return list(self.child_table_definitions.keys())

    def is_child_table(self, table_name) -> bool:
        if table_name in self.child_table_names:
            return True
        return False

    def get_table_primary_key_objects(self, table_name):
        table_dict = self.table_primary_keys.get(table_name)
        if not table_dict:
            raise EndpointDefinitionError(f"Table {table_name} has no defined primary keys")
        return list(table_dict.keys())

    def get_table_primary_key_names(self, table_name):
        table_primary_key_names = []
        for primary_key_object in self.table_primary_keys[table_name]:
            table_primary_key_names.append(self.get_table_primary_key_name(table_name, primary_key_object))
        return table_primary_key_names

    def get_table_primary_key_name(self, table_name, object_name):
        return self.table_primary_keys.get(table_name).get(object_name)
