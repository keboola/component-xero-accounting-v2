import logging
from typing import List, Dict
from typing import Optional

from .table_key import Key
from .endpoint_definition import EndpointDefinition


class JSONParserError(Exception):
    pass


class JSONParser:
    def __init__(self, endpoint_definition: EndpointDefinition) -> None:

        """
        Class for parsing JSON data. An input JSON is transformed into 1 or multiple lists of dictionaries of depth 1.

        Args:
            parent_table:
                Dict : Parent table definition e.g. {"parent_element": "parent_table_name.csv"}
            child_table_definitions:
                Optional[dict]: Child table definitions containing the key value pairs of child tables.
                Keys being the object name (with all parent objects separated by periods),
                and the value being the name of the output table. e.g. {"order.order-items": "order_items.csv"}
            table_primary_keys:
                Optional[dict]: definition of all primary keys of all tables. Key value pairs where
                Keys being the object name (with all parent objects separated by periods) and values being
                key value pairs of objects that should be primary keys of the table and values being the resulting
                names of the columns in the CSV
                e.g. {"order.order-item" : {"order.id" : "order_id", "order.order-items.item_id" : "item_id"}}
            root_node:
                Optional[str] : name of any root nodes of the data e.g. if data is:
                {"root_el": {"orders": {"order": [{}]}}}, then root_node should be root_el.orders.order
        Raises:
            JSONParserError - on parsing errors.
        """
        self.endpoint_definition = endpoint_definition
        if not endpoint_definition.child_table_definitions:
            self.endpoint_definition.child_table_definitions = {}

    def parse_data(self, data: Dict) -> Dict:
        """

        Args:
            data (Dict): A dictionary containing the data to be parsed. If the root node is root_el.orders.order,
            then the data should be {"root_el": {"orders": {"order": [{**ORDER_DATA**},{**ORDER_DATA**}]}}}

        Returns:
            parsed_data (Dict) : dictionary of parsed data with key value pairs, where keys are names of csv files, and
            values are lists of flat dictionaries.

        """
        data_to_parse = self._get_data_to_parse(data, self.endpoint_definition.root_node)
        parsed_data = {}
        for row in data_to_parse:
            parsed_row = self._parse_row_to_tables(row)
            for table_name in parsed_row:
                if table_name not in parsed_data:
                    parsed_data[table_name] = []
                parsed_data[table_name].extend(parsed_row[table_name])
        return parsed_data

    @property
    def root_node(self):
        return self.endpoint_definition.root_node

    @property
    def table_primary_keys(self):
        return self.endpoint_definition.table_primary_keys

    def _initialize_tables(self) -> Dict[str, List]:
        table_data = {}
        for table in self.endpoint_definition.parent_table:
            table_data[self.endpoint_definition.parent_table[table]] = []
        for table in self.endpoint_definition.child_table_definitions:
            table_data[self.endpoint_definition.child_table_definitions[table]] = []
        return table_data

    def _parse_row_to_tables(self, data_object: Dict) -> Dict:
        table_data = self._initialize_tables()
        warnings = {}

        def _parse_list_of_dicts(data: List[Dict], primary_keys: List[Key], object_name: str) -> None:
            for index, datum in enumerate(data):
                _parse_nested_dict(datum, object_name, foreign_keys=primary_keys, table_index=index)

        def _parse_nested_dict(data: Dict,
                               parent_object: str = "",
                               table_index: int = 0,
                               foreign_keys: Optional[List[Key]] = None,
                               parent_prefix: str = "") -> None:

            if not foreign_keys:
                foreign_keys = []

            for index, column in enumerate(data):
                _parse_column(data, parent_object, table_index, foreign_keys, parent_prefix, column)

        def _parse_column(data: Dict,
                          parent_object: str,
                          table_index: int,
                          foreign_keys: Optional[List[Key]],
                          parent_prefix: str,
                          column: str) -> None:
            if self._is_object_child_table(parent_object, column):
                _process_child_table(data, parent_object, foreign_keys, column)
            elif self._is_object_dict(data[column]):
                new_parent_prefix = self.get_joined_name(parent_prefix, column)
                primary_keys = self._get_primary_keys(data, parent_object, parent_object)
                _parse_nested_dict(data[column],
                                   parent_object,
                                   table_index,
                                   foreign_keys=primary_keys,
                                   parent_prefix=new_parent_prefix)
            elif self._is_list_of_dicts(data[column]):
                warnings[column] = f'Warning : Possible table "{column}" will be ignored as it is not specified ' \
                                   f"in the configuration of the parser. Table parent object : '{parent_object}'" \
                                   f"Sample data : {data[column]}"
            else:
                _parse_object(parent_object, parent_prefix, column, data, table_index, foreign_keys)

        def _process_child_table(data: Dict,
                                 parent_object: str,
                                 foreign_keys: Optional[List[Key]],
                                 column: str) -> None:
            object_name = self.get_joined_name(parent_object, column, ".")
            if not isinstance(data[column], List):
                data[column] = [data[column]]
            for foreign_key in foreign_keys:
                foreign_key.name = self.endpoint_definition.get_table_primary_key_name(object_name,
                                                                                       foreign_key.object_name)
            primary_keys = self._get_primary_keys(data, parent_object, object_name)
            primary_keys.extend(foreign_keys)
            _parse_list_of_dicts(data[column], primary_keys, object_name)

        def _parse_object(parent_object: str,
                          parent_prefix: str,
                          column: str,
                          data: Dict,
                          table_index: int,
                          foreign_keys: List[Key]) -> None:

            table_name = self.endpoint_definition.all_tables[parent_object]

            if len(table_data[table_name]) <= table_index:
                table_data[table_name].append({})

            _add_foreign_keys_to_table(table_name, foreign_keys)
            column_name = self.get_joined_name(parent_prefix, column, "_")
            table_data[table_name][table_index][column_name] = data[column]

        def _add_foreign_keys_to_table(table_name: str, foreign_keys: List[Key]):
            table_size = len(table_data[table_name])
            for foreign_key in foreign_keys:
                table_data[table_name][table_size - 1][foreign_key.name] = foreign_key.value

        parent_table = self.endpoint_definition.parent_table_name
        _parse_nested_dict(data_object, parent_table)
        self._log_warnings(warnings)
        return table_data

    @staticmethod
    def _log_warnings(warnings: Dict):
        for warning in warnings:
            logging.warning(warnings[warning])

    def _is_object_child_table(self, parent_object: str, object_name: str) -> bool:
        object_name = self.get_joined_name(parent_object, object_name, ".")
        return self.endpoint_definition.is_child_table(object_name)

    @staticmethod
    def _is_object_dict(object_):
        return isinstance(object_, Dict)

    @staticmethod
    def _is_list_of_dicts(object_) -> bool:
        if not isinstance(object_, List):
            return False
        _is_list_of_dicts = all(isinstance(i, dict) for i in object_)
        return _is_list_of_dicts

    @staticmethod
    def _get_data_to_parse(data: Dict, root_node: str) -> List[Dict]:
        root_nodes = root_node.split(".")
        if len(root_nodes) == 1 and not root_nodes[0]:
            raise JSONParserError("Invalid root node, could not parse JSON file")
        try:
            for root_node in root_nodes:
                data = data.get(root_node)
        except AttributeError as attr_err:
            raise JSONParserError("Invalid root node, could not parse JSON file") from attr_err

        if not data:
            raise JSONParserError("Invalid root node or empty JSON. Could not find data based on root node")

        if not isinstance(data, List):
            raise JSONParserError(
                "Invalid root node. Data extracted from JSON using the root node should be a list of dictionaries.")
        return data

    @staticmethod
    def get_joined_name(parent: str, child: str, delimiter="_") -> str:
        joined_name = child
        if parent:
            joined_name = delimiter.join([parent, child])
        return joined_name

    def _get_primary_keys(self, data: Dict, parent_object: str, table_object) -> List[Key]:
        primary_keys = []
        primary_key_objects = self.endpoint_definition.get_table_primary_key_objects(parent_object)
        key_prefix = "".join([parent_object, "."])
        for object_name in primary_key_objects:
            child_object = object_name.replace(key_prefix, "")
            if child_object in data:
                value = data[child_object]
                name = self.endpoint_definition.get_table_primary_key_name(table_object, object_name)
                new_key = Key(object_name, name, value)
                primary_keys.append(new_key)
        return primary_keys
