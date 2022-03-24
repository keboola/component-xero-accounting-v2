import logging
import dateparser
import os
import csv

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.interface import register_csv_dialect

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


class Component(ComponentBase):
    def __init__(self):
        self.client = None
        self.tables = {}
        self._writer_cache = dict()
        self.new_state = {}
        super().__init__()
        register_csv_dialect()

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

        self.client = XeroClient(
            oauth_credentials, tenant_id=tenant_id, component=self)
        self.client.force_refresh_token()
        self.client.update_tenants()

        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = self.client.get_xero_oauth2_token_dict()
        self.write_state_file(self.new_state)

        for endpoint in endpoints:
            logging.info(f"Fetching data for endpoint : {endpoint}")
            accounting_object = self.client.get_accounting_object(
                endpoint)  # TODO: use modified since
            tables = self.client.parse_accounting_object_into_tables(
                accounting_object)
            for table in tables:
                self.write_manifest(table.table_definition)
                with open(os.path.join(self.tables_out_path, table.table_definition.name), 'w') as f:
                    # TODO: use UUID slices to avoid conflicts and for pagination
                    csv_writer = csv.DictWriter(
                        f, dialect='kbc', fieldnames=table.table_definition.columns)
                    csv_writer.writerows(table.data)


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
