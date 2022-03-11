import json
import logging
import dateparser
import importlib.resources
import csv
import os

import inflection

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from parser.json_parser import JSONParser

from xero import XeroClient

# configuration variables
KEY_MODIFIED_SINCE = 'modified_since'
KEY_ENDPOINTS = 'endpoints'
KEY_TENANT_ID = 'tenant_id'

KEY_STATE_OAUTH_TOKEN_DICT = "#oauth_token_dict"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_ENDPOINTS]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        endpoint_definitions = self.get_endpoint_definitions()

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

        self.write_state_file(
            {KEY_STATE_OAUTH_TOKEN_DICT: self.client.get_xero_oauth2_token_dict()})

        for endpoint in endpoints:
            logging.info(f"Fetching data for endpoint : {endpoint}")
            endpoint_def = endpoint_definitions[endpoint]
            if endpoint == 'Accounts':
                parser = JSONParser(**endpoint_def)
                self.download_accounts(parser, modified_since)

    def download_accounts(self, parser: JSONParser, modified_since: str = None):
        model_name = parser.root_node
        table_name = parser.parent_table[model_name]
        field_names = self.client.get_field_names(
            inflection.singularize(model_name))  # Leaving out the 's' at the end
        primary_key = [
            value for value in parser.table_primary_keys[model_name].values()]
        table_def = self.create_out_table_definition(table_name,
                                                     # destination=f"{self.out_bucket}.{self.table_name}",
                                                     primary_key=primary_key,
                                                     #  columns=field_names,
                                                     is_sliced=False,
                                                     #  incremental=self.incremental_flag
                                                     )
        self.write_manifest(table_def)

        accounts_dict = self.client.get_serialized_accounting_object(
            model_name, if_modified_since=modified_since)
        for table_name, list_of_rows in parser.parse_data(accounts_dict).items():
            table_path = os.path.join(self.tables_out_path, table_name)
            with open(table_path, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(list_of_rows)

    @staticmethod
    def get_endpoint_definitions():
        with importlib.resources.open_text("parser", "endpoint_definitions.json") as ed_f:
            return json.load(ed_f)


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
