"""
Template Component main class.

"""
import json
import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from xero import XeroClient

# configuration variables
KEY_LAST_MODIFIED = 'last_modified'
KEY_ENDPOINTS = 'endpoints'

KEY_STATE_REFRESH_TOKEN = "#refresh_token"

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

        params = self.configuration.parameters
        # last_modified = params.get(KEY_LAST_MODIFIED)
        endpoints = params.get(KEY_ENDPOINTS)

        oauth_credentials = self.configuration.oauth_credentials
        client_id = oauth_credentials.appKey
        client_secret = oauth_credentials.appSecret
        refresh_token = oauth_credentials.data.get("refresh_token")

        state = self.get_state_file()
        if state.get(KEY_STATE_REFRESH_TOKEN):
            refresh_token = state.get(KEY_STATE_REFRESH_TOKEN)

        client = XeroClient(client_id, client_secret, refresh_token)
        client.login()

        self.write_state_file({KEY_STATE_REFRESH_TOKEN: client.refresh_token})

        for endpoint in endpoints:
            logging.info(f"Fetching data for endpoint : {endpoint}")
            # TODO implement endpoints
            # TODO parse endpoints
            # TODO write endpoints to storage

        # endpoint_definitions = self.get_endpoint_definitions()
        # account_data_parser = JSONParser(**endpoint_definitions.get("account"))
        # for account_data in client.get_accounts("2020-02-06T12:17:43.202-08:00"):
        #     parsed_data = account_data_parser.parse_data(account_data.to_dict())

    @staticmethod
    def get_endpoint_definitions():
        with open("endpoint_definitions/endpoint_definitions.json", 'r') as f:
            return json.load(f)


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
