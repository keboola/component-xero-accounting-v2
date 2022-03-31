import logging
from typing import Dict, List, Union
import dateparser
import os
import csv

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.interface import register_csv_dialect

from xero import XeroClient
from xero.utility import KeboolaDeleteWhereSpec

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

        params: Dict = self.configuration.parameters
        modified_since = params.get(KEY_MODIFIED_SINCE)
        if modified_since:
            modified_since = dateparser.parse(modified_since).isoformat()
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

        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = self.client.get_xero_oauth2_token_dict()
        # TODO: state should be saved even on subsequent run failure
        self.write_state_file(self.new_state)

        self.client.update_tenants()

        for endpoint in endpoints:
            logging.info(f"Fetching data for endpoint : {endpoint}")
            page_number = 1
            table_defs = self.client.get_table_definitions(endpoint)
            tables_to_define: List[str] = []
            delete_where_specs: Dict[str,
                                     Union[KeboolaDeleteWhereSpec, None]] = {}
            for accounting_object_list in self.client.get_accounting_object(
                    endpoint, if_modified_since=modified_since):
                tables_data = self.client.parse_accounting_object_list_into_tables(
                    accounting_object_list)
                for table_name, table_data in tables_data.items():
                    table_def = table_defs[table_name]
                    if page_number == 1:
                        tables_to_define.append(table_name)
                        delete_where_specs[table_name] = table_data.to_delete
                    elif delete_where_specs[table_name]:
                        delete_where_specs[table_name].values.update(
                            table_data.to_delete.values)
                    base_path = os.path.join(
                        self.tables_out_path, table_def.name)
                    os.makedirs(base_path, exist_ok=True)
                    with open(os.path.join(base_path, f'{endpoint}_{page_number}.csv'), 'w') as f:
                        csv_writer = csv.DictWriter(
                            f, dialect='kbc', fieldnames=table_def.columns)
                        csv_writer.writerows(table_data.to_add)
                page_number += 1
            for table_name in tables_to_define:
                table_def = table_defs[table_name]
                delete_where_spec = delete_where_specs[table_name]
                if delete_where_spec:
                    table_def.set_delete_where_from_dict({'column': delete_where_spec.column,
                                                          'operator': delete_where_spec.operator,
                                                          'values': list(delete_where_spec.values)})
                self.write_manifest(table_def)

        self.client.force_refresh_token()
        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = self.client.get_xero_oauth2_token_dict()
        self.write_state_file(self.new_state)


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
