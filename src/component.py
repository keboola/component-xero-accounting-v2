import json
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
from xero.utility import KeboolaDeleteWhereSpec, XeroException
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

        self._init_client()

        try:
            available_tenant_ids = self.client.get_available_tenant_ids()
        except XeroException as xero_exc:
            raise UserException from xero_exc

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
        self.refresh_token_and_save_state()

    def refresh_token_and_save_state(self):
        try:
            self.client.force_refresh_token()
        except XeroException as xero_exc:
            raise UserException("Failed to authorize the component. Please reauthorize the component. "
                                "\n Due to the functioning of the XERO authorization, if a component fails,"
                                " the component must be reauthorized.") from xero_exc
        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = json.dumps(self.client.get_xero_oauth2_token_dict())
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

    def _init_client(self):
        logging.info("Authorizing Client")

        state = self.get_state_file()
        state_authorization_params = state.get(KEY_STATE_OAUTH_TOKEN_DICT)

        if self._is_valid_state_auth(state_authorization_params):
            logging.info("Authorizing Client from state")
            self._init_client_from_state(state_authorization_params)
        else:
            logging.info("Authorizing Client from oauth")
            self._init_client_from_config()
        logging.info("Client Authorized")

    def _init_client_from_state(self, state_authorization_params):
        oauth_credentials = self.configuration.oauth_credentials
        oauth_credentials.data = self._load_state_oauth(state_authorization_params)
        self.client = XeroClient(oauth_credentials)
        try:
            self.refresh_token_and_save_state()
            self.client.get_available_tenant_ids()
        except (UserException, XeroException):
            logging.warning("Authorizing Client from state failed, trying from oauth")
            self._init_client_from_config()

    @staticmethod
    def _load_state_oauth(state_authorization_params):
        if isinstance(state_authorization_params, str):
            return json.loads(state_authorization_params)
        elif isinstance(state_authorization_params, dict):
            return state_authorization_params
        else:
            raise UserException("Invalid state, please contact support")

    def _init_client_from_config(self):
        oauth_credentials = self.configuration.oauth_credentials
        if isinstance(oauth_credentials.data.get("scope"), str):
            oauth_credentials.data["scope"] = oauth_credentials.data["scope"].split(" ")
        self.client = XeroClient(oauth_credentials)
        try:
            self.refresh_token_and_save_state()
            self.client.get_available_tenant_ids()
        except (UserException, XeroException) as xero_exception:
            raise UserException(xero_exception) from xero_exception

    @staticmethod
    def _is_valid_state_auth(state_authorization_params):
        if state_authorization_params:
            if "access_token" in state_authorization_params and "scope" in state_authorization_params \
                    and "expires_in" in state_authorization_params and "token_type" in state_authorization_params:
                return True
        return False


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.warning("During the component fail, the authorization is invalidated due to the functioning of the "
                        "XERO authorization. If The authroization is invalid, you must reauthorize the component")
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
