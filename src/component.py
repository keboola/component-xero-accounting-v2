import logging
import dateparser
import tempfile
import os

from collections import OrderedDict
from typing import Dict
from csv_tools import CachedOrthogonalDictWriter
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from json_parser import JSONParser, EndpointDefinition

from xero import XeroClient

# configuration variables
KEY_MODIFIED_SINCE = 'modified_since'
KEY_ENDPOINTS = 'endpoints'
KEY_TENANT_ID = 'tenant_id'

KEY_STATE_OAUTH_TOKEN_DICT = "#oauth_token_dict"
KEY_STATE_ENDPOINT_COLUMNS = "endpoint_columns"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_ENDPOINTS]
REQUIRED_IMAGE_PARS = []

ENDPOINT_DEFINITION_PATH = os.path.join(os.path.dirname(__file__), "endpoint_definition", "endpoint_definitions.json")


class Component(ComponentBase):
    def __init__(self):
        self.client = None
        self.tables = {}
        self._writer_cache = dict()
        self.new_state = {}
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters
        modified_since = dateparser.parse(
            params.get(KEY_MODIFIED_SINCE)).isoformat()
        endpoints = params.get(KEY_ENDPOINTS)
        tenant_id = params.get(KEY_TENANT_ID)

        oauth_credentials = self.configuration.oauth_credentials

        state = self.get_state_file()
        oauth_token_dict = state.get(KEY_STATE_OAUTH_TOKEN_DICT)
        if oauth_token_dict and oauth_token_dict['expires_at'] > oauth_credentials.data['expires_at']:
            oauth_credentials.data = oauth_token_dict

        self.client = XeroClient(oauth_credentials, tenant_id=tenant_id)
        self.client.force_refresh_token()
        self.client.update_tenants()

        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = self.client.get_xero_oauth2_token_dict()
        self.write_state_file(self.new_state)

        endpoint_columns = state.get(KEY_STATE_ENDPOINT_COLUMNS) if state.get(KEY_STATE_ENDPOINT_COLUMNS) else {}

        for endpoint in endpoints:
            logging.info(f"Fetching data for endpoint : {endpoint}")
            endpoint_definition = EndpointDefinition(ENDPOINT_DEFINITION_PATH, endpoint)
            parser = JSONParser(endpoint_definition)

            self.create_tables_from_endpoint_definition(endpoint_definition, endpoint_columns)
            fieldnames = endpoint_columns if endpoint_columns else {}

            if endpoint == 'Accounts':
                self.download_accounts(parser, modified_since=modified_since, fieldnames=fieldnames)
            elif endpoint == 'Quotes':
                self.download_quotes(parser, fieldnames)

        # important to update the table columns to the same values as the final columns of the Orthogonal Writer
        self.update_table_definitions()
        self._close_writers()
        self.write_table_manifests()
        self.update_endpoint_columns_in_state()
        self.write_state_file(self.new_state)

    def download_accounts(self, parser: JSONParser, modified_since: str = None, fieldnames: Dict = None):
        # TODO : simplify the functions
        # model_name = parser.root_node
        # table_name = parser.parent_table[model_name]
        # field_names = self.client.get_field_names(
        #     model_name[:-1])  # Leaving out the 's' at the end
        # primary_key = [
        #     value for value in parser.table_primary_keys[model_name].values()]
        # table_def = self.create_out_table_definition(table_name,
        #                                              # destination=f"{self.out_bucket}.{self.table_name}",
        #                                              primary_key=primary_key,
        #                                              #  columns=field_names,
        #                                              is_sliced=False,
        #                                              #  incremental=self.incremental_flag
        #                                              )
        # self.write_manifest(table_def)
        #
        # accounts_dict = self.client.get_serialized_accounting_object(
        #     model_name, if_modified_since=modified_since)
        # for table_name, list_of_rows in parser.parse_data(accounts_dict).items():
        #     table_path = os.path.join(self.tables_out_path, table_name)
        #     with open(table_path, 'w') as f:
        #         writer = csv.DictWriter(f, fieldnames=field_names)
        #         writer.writeheader()
        #         writer.writerows(list_of_rows)
        model_name = parser.root_node
        accounts_dict = self.client.get_serialized_accounting_object(model_name, if_modified_since=modified_since)
        parsed_data = parser.parse_data(accounts_dict)
        self.save_parsed_data(parsed_data, fieldnames)

    def download_quotes(self, parser: JSONParser, fieldnames):
        model_name = parser.root_node
        quotes_dict = self.client.get_serialized_accounting_object(
            model_name)
        parsed_data = parser.parse_data(quotes_dict)
        self.save_parsed_data(parsed_data, fieldnames)

    def save_parsed_data(self, parsed_data, fieldnames):
        for data_name in parsed_data:
            table_fieldnames = fieldnames.get(data_name) if fieldnames.get(data_name) else []
            table_fieldnames.extend(self.tables[data_name].columns)
            table_fieldnames = self.get_no_duplicate_list(table_fieldnames)
            writer = self._get_writer_from_cache(self.tables[data_name], table_fieldnames)
            writer.writerows(parsed_data[data_name])

    def create_tables_from_endpoint_definition(self, endpoint_definition, endpoint_columns):
        for table in endpoint_definition.all_tables:
            table_name = endpoint_definition.all_tables[table]
            primary_keys = endpoint_definition.get_table_primary_key_names(table)
            table_columns = primary_keys.copy()
            object_name = table_name.replace(".csv", "")
            columns_from_state = endpoint_columns.get(object_name) if endpoint_columns.get(object_name) else []
            table_columns.extend(columns_from_state)
            table_columns = self.get_no_duplicate_list(table_columns)
            self.tables[table_name] = self.create_out_table_definition(table_name, primary_key=primary_keys,
                                                                       incremental=True, columns=table_columns)
            self._get_writer_from_cache(self.tables[table_name], self.tables[table_name].columns)

    @staticmethod
    def get_no_duplicate_list(list_):
        # list(set()) rearranges the list
        return list(OrderedDict.fromkeys(list_))

    def _get_writer_from_cache(self, out_table, fieldnames):
        if not self._writer_cache.get(out_table.name):
            # init writer if not in cache
            self._writer_cache[out_table.name] = CachedOrthogonalDictWriter(out_table.full_path,
                                                                            fieldnames,
                                                                            temp_directory=tempfile.mkdtemp(),
                                                                            table_name=out_table.name)

        return self._writer_cache[out_table.name]

    def _close_writers(self):
        for wr in self._writer_cache.values():
            wr.close()

    def update_table_definitions(self):
        for wr in self._writer_cache.values():
            table_name = wr.table_name
            columns = wr.fieldnames
            self.update_table_columns(table_name, columns)

    def update_table_columns(self, table_name, columns):
        for table in self.tables:
            if table_name == table:
                self.tables[table].columns = columns

    def write_table_manifests(self):
        for table in self.tables:
            self.write_manifest(self.tables[table])

    def update_endpoint_columns_in_state(self):
        endpoint_columns = {}
        for table in self.tables:
            table_name = self.tables[table].name.replace(".csv", "")
            endpoint_columns[table_name] = self.tables[table].columns
        self.new_state[KEY_STATE_ENDPOINT_COLUMNS] = endpoint_columns


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
