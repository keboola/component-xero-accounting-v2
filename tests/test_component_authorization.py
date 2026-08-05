"""
Regression tests for the handling of a configuration without a Xero authorization.

Covers the failure that motivated `Component._get_oauth_credentials`: a job died with
an unhandled `AttributeError: 'NoneType' object has no attribute 'data'` because
`Configuration.oauth_credentials` is `None` when the configuration carries no
`authorization.oauth_api.credentials` section. The component exited with code 2, so a
configuration problem the user can fix surfaced as an opaque internal error.

The tests assert both that the missing authorization is now reported as a `UserException`
(exit code 1, actionable message) on every path that reads the credentials, and that
nothing about an authorized configuration changed - the credentials are handed through
untouched and the client is still built exactly as before.
"""
import json
import unittest
from unittest import mock

from keboola.component.dao import OauthCredentials
from keboola.component.exceptions import UserException

from component import KEY_STATE_OAUTH_TOKEN_DICT, Component

STATE_TOKEN = {
    "access_token": "token",
    "scope": ["accounting.transactions"],
    "expires_in": 1800,
    "token_type": "Bearer",
}


def _credentials(data: dict = None) -> OauthCredentials:
    return OauthCredentials(id="", created="", data=data if data is not None else {}, oauthVersion="2.0", appKey="",
                            appSecret="")


class _StubComponent(Component):
    """
    A Component whose data-dir-backed `configuration` and state file are stubbed out, so the
    authorization handling can be driven without a KBC data dir. Mirrors the OAuth-free client
    construction used in test_client.py.
    """

    def __init__(self, oauth_credentials, state: dict = None):
        self._oauth_credentials = oauth_credentials
        self._state = state if state is not None else {}
        self.client = None
        self.new_state = {}
        self.refresh_token_and_save_state = mock.Mock()

    @property
    def configuration(self):
        return mock.Mock(oauth_credentials=self._oauth_credentials)

    def get_state_file(self) -> dict:
        return self._state


class TestMissingAuthorization(unittest.TestCase):
    """A configuration with no authorization must fail as a user error, not an internal one."""

    def test_get_oauth_credentials_raises_user_exception(self):
        with self.assertRaises(UserException) as raised:
            _StubComponent(None)._get_oauth_credentials()
        self.assertIn("not authorized", str(raised.exception))

    def test_init_client_from_state_raises_user_exception_not_attribute_error(self):
        """The exact failing path: state holds a token, but the configuration has no authorization."""
        with self.assertRaises(UserException):
            _StubComponent(None)._init_client_from_state(json.dumps(STATE_TOKEN))

    def test_init_client_from_config_raises_user_exception_not_attribute_error(self):
        with self.assertRaises(UserException):
            _StubComponent(None)._init_client_from_config()

    def test_init_client_via_state_branch_raises_user_exception(self):
        component = _StubComponent(None, state={KEY_STATE_OAUTH_TOKEN_DICT: json.dumps(STATE_TOKEN)})
        with self.assertRaises(UserException):
            component._init_client()

    def test_init_client_via_config_branch_raises_user_exception(self):
        with self.assertRaises(UserException):
            _StubComponent(None, state={})._init_client()


class TestAuthorizedConfigurationUnchanged(unittest.TestCase):
    """An authorized configuration must behave exactly as it did before the guard was added."""

    def test_present_credentials_are_returned_untouched(self):
        credentials = _credentials({"access_token": "token"})
        self.assertIs(_StubComponent(credentials)._get_oauth_credentials(), credentials)

    def test_init_client_from_state_still_loads_state_token_and_builds_client(self):
        credentials = _credentials()
        component = _StubComponent(credentials)
        with mock.patch("component.XeroClient") as client_class:
            component._init_client_from_state(json.dumps(STATE_TOKEN))
        client_class.assert_called_once_with(credentials)
        self.assertEqual(credentials.data, STATE_TOKEN)
        component.refresh_token_and_save_state.assert_called_once_with()

    def test_init_client_from_config_still_splits_scope_and_builds_client(self):
        credentials = _credentials({"access_token": "token", "scope": "accounting.transactions accounting.settings"})
        component = _StubComponent(credentials)
        with mock.patch("component.XeroClient") as client_class:
            component._init_client_from_config()
        client_class.assert_called_once_with(credentials)
        self.assertEqual(credentials.data["scope"], ["accounting.transactions", "accounting.settings"])


if __name__ == "__main__":
    unittest.main()
