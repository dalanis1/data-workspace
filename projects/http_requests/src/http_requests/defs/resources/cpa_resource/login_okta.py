import re
import uuid
import base64
import httpx
from dagster import ConfigurableResource

class Login(ConfigurableResource):
    email: str
    password: str
    product: int

    def get_session_token(self):
        """Get the session token from Okta by authenticating the user."""
        auth_payload = {
            "username": self.email,
            "password": self.password,
            "options": {
                "warnBeforePasswordExpired": True,
                "multiOptionalFactorEnroll": True
            }
        }
        response = httpx.post("https://login.cpavision.mx/api/v1/authn", json = auth_payload, verify = False)
        response.raise_for_status()
        return response.json().get("sessionToken")

    def get_client_parameters(self):
        """
            Get the client parameters based on the product number.
        """
        response = httpx.post(
            "https://access.cpavision.mx/system/getClientID",
            params = {"producto": self.product},
            verify = False
        )
        response.raise_for_status()
        data = response.json()
        return {
            "client_id": DecodingRequests.unhash_client_id(data.get("client_id")),
            "redirect_uri": data.get("redirect_Token")
        }

    def oauth_cpa_okta(self, session: httpx.Client):
        """ Perform OAuth authentication with Okta for CPA Vision. """
        payload = self.get_client_parameters()
        state = DecodingRequests.get_state()
        payload.update({
            "response_type": "id_token token code",
            "scope": "openid",
            "state": state,
            "nonce": "TEST",
            "sessionToken": self.get_session_token(),
            "response_mode": "form_post"
        })
        response = session.get("https://login.cpavision.mx/oauth2/ausdw4dezmhhvT9uP4x6/v1/authorize", params = payload, timeout = 100)
        response.raise_for_status()
        return DecodingRequests.extract_tokens_from_html(response.text)

    def get_session_app(self, session: httpx.Client):
        """ Send a POST request to end login endpoint with the extracted tokens to establish a session. """
        payload = self.oauth_cpa_okta(session)
        response = session.post("https://supply.cpavision.mx/login-okta", data = payload)
        response.raise_for_status()
        return session

class DecodingRequests():
    state:str
    @staticmethod
    def unhash_client_id(client_id):
        """ Unhash the client ID to get the original product number. """
        return base64.b64decode(client_id).decode("utf-8")

    @staticmethod
    def get_state():
        """ Create an aleatory unique identifier """
        return uuid.uuid4().hex

    @staticmethod
    def extract_tokens_from_html(html_content):
        """
            Extract tokens from the HTML content returned by the authorization request.
        """
        token_keys = ["state", "code", "id_token", "access_token", "token_type", "expires_in", "scope"]
        dict_tokens = {}
        for key in token_keys:
            pattern = rf'name="{key}"\s+value="([^"]+)"'
            match = re.search(pattern, html_content)
            if match:
                dict_tokens[key] = match.group(1)
        return dict_tokens
