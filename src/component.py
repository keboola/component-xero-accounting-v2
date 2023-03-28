import json
import logging
from typing import Dict, List, Set, Union
import dateparser
import os
import csv

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.dao import TableDefinition
from keboola.component.interface import register_csv_dialect
from keboola.utils.helpers import comma_separated_values_to_list

from xero.client import XeroClient
from xero.utility import XeroException
from xero.xero_parser import XeroParser
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
        modified_since = self._get_modified_since()

        self._init_client()

        available_tenant_ids = self._get_available_tenant_ids()
        tenant_ids_to_download = self._get_tenants_to_download(available_tenant_ids)

        for endpoint in endpoints:
            self.download_endpoint(endpoint_name=endpoint, tenant_ids=tenant_ids_to_download,
                                   if_modified_since=modified_since)
        self.refresh_token_and_save_state()

    def refresh_token_and_save_state(self) -> None:
        self._refresh_client_token()
        self.new_state[KEY_STATE_OAUTH_TOKEN_DICT] = json.dumps(self.client.get_xero_oauth2_token_dict())
        self.write_state_file(self.new_state)

    def _refresh_client_token(self) -> None:
        try:
            self.client.force_refresh_token()
        except XeroException as xero_exc:
            raise UserException("Failed to authorize the component. Please reauthorize the component. "
                                "\n Due to the functioning of the XERO authorization, if a component fails,"
                                " the component must be reauthorized.") from xero_exc

    def download_endpoint(self, endpoint_name: str, tenant_ids: List[str], **kwargs) -> None:
        logging.info(f"Fetching data for endpoint : {endpoint_name}")
        saved_tables: Set[str] = set()
        for tenant_id in tenant_ids:
            for pagen_num, page in enumerate(self.client.get_accounting_object(tenant_id=tenant_id,
                                                                               model_name=endpoint_name,
                                                                               **kwargs)):
                parsed_data = XeroParser().parse_data(page)
                self.save_parsed_data(parsed_data, pagen_num, tenant_id, endpoint_name)
                saved_tables.update(list(parsed_data.keys()))

        for table_name in saved_tables:
            table_def = self._get_table_definition_of_endpoint_data_by_name(endpoint_name, table_name)
            table_def.incremental = self.incremental_load
            self.write_manifest(table_def)

    def save_parsed_data(self, parsed_data: Dict[str, List[Dict]], pagen_num: int, tenant_id: str,
                         endpoint_name: str) -> None:
        for table_name, table_data in parsed_data.items():
            table_def = self._get_table_definition_of_endpoint_data_by_name(endpoint_name, table_name)
            base_path = os.path.join(self.tables_out_path, table_def.name)
            os.makedirs(base_path, exist_ok=True)
            with open(os.path.join(base_path, f'{tenant_id}_{endpoint_name}_{pagen_num}.csv'), 'w') as f:
                csv_writer = csv.DictWriter(f, dialect='kbc', fieldnames=table_def.columns)
                csv_writer.writerows(table_data)

    def _get_table_definition_of_endpoint_data_by_name(self, endpoint_name: str, table_name: str) -> TableDefinition:
        all_table_definitions = self._get_all_table_definitions_of_endpoint_data(endpoint_name)
        table_definition = all_table_definitions.get(table_name)
        if not table_definition:
            raise KeyError(f"Failed to get Table Definition of table {table_name}. Please contact support")
        return table_definition

    def _get_all_table_definitions_of_endpoint_data(self, endpoint_name: str) -> Dict[str, TableDefinition]:
        return TableDefinitionFactory(endpoint_name, self).get_table_definitions()

    def _init_client(self) -> None:
        logging.info("Authorizing Client")

        state = self.get_state_file()
        state_authorization_params = state.get(KEY_STATE_OAUTH_TOKEN_DICT)

        if self._state_contains_authorization_parameters(state_authorization_params):
            logging.info("Authorizing Client from state")
            self._init_client_from_state(state_authorization_params)
        else:
            logging.info("Authorizing Client from oauth")
            self._init_client_from_config()
        logging.info("Client Authorized")

    def _init_client_from_state(self, state_authorization_params: Union[str, Dict]) -> None:
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
    def _load_state_oauth(state_authorization_params: Union[str, Dict]) -> Dict:
        if isinstance(state_authorization_params, str):
            return json.loads(state_authorization_params)
        elif isinstance(state_authorization_params, dict):
            return state_authorization_params
        else:
            raise UserException("Invalid state, please contact support")

    def _init_client_from_config(self) -> None:
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
    def _state_contains_authorization_parameters(state_authorization_params: Dict) -> bool:
        if state_authorization_params:
            if "access_token" in state_authorization_params and "scope" in state_authorization_params \
                    and "expires_in" in state_authorization_params and "token_type" in state_authorization_params:
                return True
        return False

    def _get_modified_since(self) -> str:
        modified_since = self.configuration.parameters.get(KEY_MODIFIED_SINCE)
        if modified_since:
            modified_since = dateparser.parse(modified_since).isoformat()
        return modified_since

    def _get_available_tenant_ids(self) -> List[str]:
        try:
            return self.client.get_available_tenant_ids()
        except XeroException as xero_exc:
            raise UserException from xero_exc

    def _get_tenants_to_download(self, available_tenant_ids: List[str]) -> List[str]:
        tenant_ids_to_download = comma_separated_values_to_list(self.configuration.parameters.get(KEY_TENANT_IDS))

        if not tenant_ids_to_download:
            tenant_ids_to_download = available_tenant_ids
            logging.info(f'Tenant IDs not specified, using all available: {available_tenant_ids}.')

        self._validate_tenants_to_download(tenant_ids_to_download, available_tenant_ids)
        return tenant_ids_to_download

    @staticmethod
    def _validate_tenants_to_download(tenant_ids_to_download: List[str], available_tenant_ids: List[str]) -> None:
        unavailable_tenants = set(tenant_ids_to_download) - set(available_tenant_ids)
        if unavailable_tenants:
            unavailable_tenants_str = ', '.join(unavailable_tenants)
            raise UserException(f"Some tenants to be downloaded (IDs: {unavailable_tenants_str})"
                                f" are not accessible, please, check if you granted sufficient credentials.")


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
