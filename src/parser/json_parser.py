import logging
from typing import List, Dict
from typing import Tuple
from typing import Optional


class JSONParserError(Exception):
    pass


class JSONParser:
    def __init__(self,
                 parent_table: Dict[str, str],
                 table_primary_keys: Dict[str, Dict[str, str]] = None,
                 child_table_definitions: Dict[str, str] = None,
                 root_node: str = "") -> None:

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
        self.parent_table = parent_table
        self.child_table_definitions = child_table_definitions
        if not child_table_definitions:
            self.child_table_definitions = {}
        self.table_primary_keys = table_primary_keys
        if not table_primary_keys:
            self.table_primary_keys = {}
        self.root_node = root_node

        self.all_tables = parent_table
        if child_table_definitions:
            self.all_tables = {**parent_table, **child_table_definitions}

    def parse_data(self, data: Dict) -> Dict:
        """

        Args:
            data (Dict): A dictionary containing the data to be parsed. If the root node is root_el.orders.order,
            then the data should be {"root_el": {"orders": {"order": [{**ORDER_DATA**},{**ORDER_DATA**}]}}}

        Returns:
            parsed_data (Dict) : dictionary of parsed data with key value pairs, where keys are names of csv files, and
            values are lists of flat dictionaries.

        """
        data_to_parse = data
        if isinstance(data, Dict):
            data_to_parse = self._get_data_to_parse(data, self.root_node)
        parsed_data = {}
        for row in data_to_parse:
            parsed_row = self._parse_row_to_tables(row)
            for key in parsed_row:
                if key not in parsed_data:
                    parsed_data[key] = []
                parsed_data[key].extend(parsed_row[key])
        return parsed_data

    def _parse_row_to_tables(self, data_object: Dict) -> Dict:
        table_data = {}
        warnings = {}

        for table in self.parent_table:
            table_data[self.parent_table[table]] = []

        for table in self.child_table_definitions:
            table_data[self.child_table_definitions[table]] = []

        def _parse_list_of_dicts(column: str,
                                 data: Dict,
                                 primary_keys: List[str],
                                 primary_key_data: List[str],
                                 object_name: str) -> None:

            for index, d in enumerate(data[column]):
                _parse_nested_dict(d, object_name, foreign_key_data=primary_key_data, foreign_keys=primary_keys,
                                   table_index=index)

        def _get_primary_key_values(data: Dict, parent_object: str) -> Tuple[List[str], List[str]]:
            primary_keys = list(self.table_primary_keys[parent_object].keys())
            available_primary_keys = []
            key_prefix = "".join([parent_object, "."])
            primary_key_data = []
            for primary_key in primary_keys:
                if primary_key.replace(key_prefix, "") in data:
                    primary_key_data.append(data[primary_key.replace(key_prefix, "")])
                    available_primary_keys.append(self.table_primary_keys[parent_object][primary_key])
            return available_primary_keys, primary_key_data

        def _parse_nested_dict(data: Dict,
                               parent_object: str = "",
                               table_index: int = 0,
                               foreign_keys: Optional[List[str]] = None,
                               foreign_key_data: Optional[List[str]] = None) -> None:

            if not foreign_keys or not foreign_key_data:
                foreign_key_data = []
                foreign_keys = []
            primary_keys, primary_key_data = _get_primary_key_values(data, parent_object)

            for index, column in enumerate(data):
                if self._is_object_child_table(parent_object, column):
                    object_name = column
                    if parent_object:
                        object_name = ".".join([parent_object, column])
                    if not isinstance(data[column], List):
                        data[column] = [data[column]]
                    primary_keys.extend(foreign_keys)
                    primary_key_data.extend(foreign_key_data)
                    _parse_list_of_dicts(column, data, primary_keys, primary_key_data, object_name)
                elif isinstance(data[column], Dict):
                    table_name = self.all_tables[parent_object]
                    _flatten_simple_dict(data[column], table_name, table_index, column, foreign_keys, foreign_key_data)
                elif self._is_list_of_dicts(data[column]):
                    warnings[column] = f'Warning : Possible table "{column}" will be ignored as it is not specified ' \
                                       "in the configuration of the parser."
                else:
                    _parse_object(parent_object, column, data, index, foreign_keys, foreign_key_data)

        def _parse_object(parent_object: str,
                          column: str, data: Dict,
                          index: int, foreign_keys: List[str],
                          foreign_key_data: List[str]) -> None:

            table_name = self.all_tables[parent_object]

            if index == 0:
                table_data[table_name].append({})
            table_size = len(table_data[table_name])
            if foreign_keys and foreign_key_data:
                for i, foreign_key in enumerate(foreign_keys):
                    table_data[table_name][table_size - 1][foreign_key] = foreign_key_data[i]
            table_data[table_name][table_size - 1][column] = data[column]

        def _flatten_simple_dict(data: Dict,
                                 table_name: str,
                                 index: int,
                                 parent_key: str,
                                 foreign_keys: List[str],
                                 foreign_key_data: List[str]) -> None:
            for d_key in data:
                new_key = parent_key + "_" + d_key
                if len(table_data[table_name]) < index + 1:
                    table_data[table_name].append({})
                table_data[table_name][index][new_key] = data[d_key]

            if foreign_keys and foreign_key_data:
                for i, foreign_key in enumerate(foreign_keys):
                    table_data[table_name][index][foreign_key] = foreign_key_data[i]

        parent_table = list(self.parent_table.keys())[0]
        _parse_nested_dict(data_object, parent_table)
        for warning in warnings:
            logging.warning(warnings[warning])
        return table_data

    def _is_object_child_table(self, parent_object: str, object_name: str) -> bool:
        if parent_object:
            object_name = ".".join([parent_object, object_name])
        if object_name in list(self.all_tables.keys()):
            return True
        return False

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
