import json
import logging
import dateparser
import importlib.resources
import csv
import os

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from parser.json_parser import JSONParser

from xero import XeroClient

# configuration variables
KEY_MODIFIED_SINCE = 'modified_since'
KEY_ENDPOINTS = 'endpoints'

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

        oauth_credentials = self.configuration.oauth_credentials

        state = self.get_state_file()
        # TODO: only override if state token not expired
        if state.get(KEY_STATE_OAUTH_TOKEN_DICT):
            oauth_credentials.data = state.get(KEY_STATE_OAUTH_TOKEN_DICT)

        self.client = XeroClient(oauth_credentials)
        self.client.force_refresh_token()

        self.write_state_file(
            {KEY_STATE_OAUTH_TOKEN_DICT: self.client.get_xero_oauth2_token_dict()})

        for endpoint in endpoints:
            logging.info(f"Fetching data for endpoint : {endpoint}")
            endpoint_def = endpoint_definitions[endpoint]
            if endpoint == 'Accounts':
                self.download_accounts(endpoint_def, modified_since)

    def download_accounts(self, endpoint_def, modified_since=None):
        parser = JSONParser(**endpoint_def)
        table_name = endpoint_def["parent_table"]["accounts"]
        primary_key = [
            value for value in endpoint_def["table_primary_keys"]["accounts"].values()]
        all_accounts_list: list[dict] = []
        for tenant_id, accounts_dict in self.client.get_accounts(modified_since):
            all_accounts_list.extend(
                parser.parse_data(accounts_dict)[table_name])

        table_path = os.path.join(self.tables_out_path, table_name)
        # os.makedirs(table_path, exist_ok=True)
        field_names = list(all_accounts_list[0].keys())
        with open(table_path, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(all_accounts_list)
        table_def = self.create_out_table_definition(table_name,
                                                     # destination=f"{self.out_bucket}.{self.table_name}",
                                                     primary_key=primary_key,
                                                     columns=field_names,
                                                     is_sliced=False,
                                                     #  incremental=self.incremental_flag
                                                     )
        self.write_manifest(table_def)
        # print(all_accounts_dict)

        # TODO implement endpoints
        # TODO parse endpoints
        # TODO write endpoints to storage

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
