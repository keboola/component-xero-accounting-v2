import logging
from typing import Dict, List, Union
import dateparser
import os
import csv

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.interface import register_csv_dialect
from keboola.utils.helpers import comma_separated_values_to_list

from xero.client import XeroClient
from xero.utility import KeboolaDeleteWhereSpec
from xero.table_data_factory import TableDataFactory
from xero.table_definition_factory import TableDefinitionFactory

# configuration variables
KEY_MODIFIED_SINCE = 'modified_since'
KEY_ENDPOINTS = 'endpoints'
KEY_TENANT_IDS = 'tenant_ids'
KEY_DESTINATION_OPTIONS = 'destination'
KEY_LOAD_TYPE = 'load_type'

KEY_STATE_OAUTH_TOKEN_DICT = "#oauth_token_dict"
KEY_STATE_ENDPOINT_COLUMNS = "endpoint_columns"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_ENDPOINTS]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self, data_path_override: str = None):
        self.incremental_load = None
        self.client = None
        self.tables = {}
        self._writer_cache = {}
        self.new_state = {}
        super().__init__(data_path_override=data_path_override, required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

        register_csv_dialect()

    def run(self):
        params: Dict = self.configuration.parameters
        endpoints: List[str] = params[KEY_ENDPOINTS]

        destination = params.get(KEY_DESTINATION_OPTIONS, {})
        load_type = destination.get(KEY_LOAD_TYPE, "full_load")
        self.incremental_load = load_type == "incremental_load"

        modified_since = params.get(KEY_MODIFIED_SINCE)
        if modified_since:
            modified_since = dateparser.parse(modified_since).isoformat()
        tenant_ids_to_download: Union[List[str], None] = comma_separated_values_to_list(params.get(KEY_TENANT_IDS))

        oauth_credentials = self.configuration.oauth_credentials

        state = self.get_state_file()
        oauth_token_dict = state.get(KEY_STATE_OAUTH_TOKEN_DICT)
        if oauth_token_dict and oauth_token_dict['expires_at'] > oauth_credentials.data['expires_at']:
            oauth_credentials.data = oauth_token_dict

        self.client = XeroClient(oauth_credentials)

        # TODO: state should be saved even on subsequent run failure
        self.refresh_and_save_state()

        available_tenant_ids = self.client.get_available_tenant_ids()
        if not tenant_ids_to_download:
            tenant_ids_to_download = available_tenant_ids
            logging.warning(
                f'Tenant IDs not specified, using all available: {available_tenant_ids}.')
        else:
            unavailable_tenants = set(
                tenant_ids_to_download) - set(available_tenant_ids)
            if unavailable_tenants:
                unavailable_tenants_str = ', '.join(unavailable_tenants)
                raise UserException(f"Some tenants to be downloaded (IDs: {unavailable_tenants_str})"
                                    f" are not accessible,"
                                    f" please, check if you granted sufficient credentials.")
        for endpoint in endpoints:
            self.download_endpoint(endpoint_name=endpoint, tenant_ids=tenant_ids_to_download,
                                   if_modified_since=modified_since)
        self.refresh_and_save_state()

    def refresh_and_save_state(self):
        self.client.force_refresh_token()
        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = self.client.get_xero_oauth2_token_dict()
        self.write_state_file(self.new_state)

    def download_endpoint(self, endpoint_name: str, tenant_ids: List[str], **kwargs):
        logging.info(f"Fetching data for endpoint : {endpoint_name}")
        page_number = 1
        table_defs = TableDefinitionFactory(
            endpoint_name, self).get_table_definitions()
        tables_to_define: List[str] = []
        delete_where_specs: Dict[str,
                                 Union[KeboolaDeleteWhereSpec, None]] = {}
        for tenant_id in tenant_ids:
            for accounting_object_list in self.client.get_accounting_object(tenant_id=tenant_id,
                                                                            model_name=endpoint_name, **kwargs):
                tables_data = TableDataFactory(accounting_object_list).get_table_definitions()
                for table_name, table_data in tables_data.items():
                    table_def = table_defs[table_name]
                    if page_number == 1:
                        tables_to_define.append(table_name)
                        delete_where_specs[table_name] = table_data.to_delete
                    elif delete_where_specs[table_name]:
                        delete_where_specs[table_name].values.update(
                            table_data.to_delete.values)
                    base_path = os.path.join(self.tables_out_path, table_def.name)
                    os.makedirs(base_path, exist_ok=True)
                    with open(os.path.join(base_path, f'{tenant_id}_{endpoint_name}_{page_number}.csv'), 'w') as f:
                        csv_writer = csv.DictWriter(f, dialect='kbc', fieldnames=table_def.columns)
                        csv_writer.writerows(table_data.to_add)
                page_number += 1

        for table_name in tables_to_define:
            table_def = table_defs[table_name]
            delete_where_spec = delete_where_specs[table_name]
            table_def.incremental = self.incremental_load
            if delete_where_spec and table_def.incremental:
                table_def.set_delete_where_from_dict({'column': delete_where_spec.column,
                                                      'operator': delete_where_spec.operator,
                                                      'values': list(delete_where_spec.values)})
            self.write_manifest(table_def)


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
