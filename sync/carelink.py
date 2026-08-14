"""cl2ns - Carelink Cloud API Client

Lightweight, async client for Medtronic Carelink Cloud API.
"""

import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone
import aiofiles
import httpx

_LOGGER = logging.getLogger("cl2ns.carelink")

DISCOVERY_URL = "https://clcloud.minimed.eu/connect/carepartner/v13/discover/android/3.6"
TOKEN_EXPIRY_MARGIN_SEC = 600  # 10 minutes margin before token expiration
HTTP_TIMEOUT_SEC = 30.0
USER_AGENT = "Dalvik/2.1.0 (Linux; U; Android 10; Nexus 5X Build/QQ3A.200805.001)"


class CarelinkError(Exception):
    """Base exception for Carelink API errors."""
    pass


class CarelinkAuthError(CarelinkError):
    """Authentication or token refresh failure."""
    pass


class CarelinkClient:
    """Carelink API Client with automatic token management and persistence."""

    def __init__(
        self,
        access_token: str = None,
        refresh_token: str = None,
        client_id: str = None,
        client_secret: str = None,
        mag_identifier: str = None,
        patient_id: str = None,
        data_dir: str = "/data",
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.mag_identifier = mag_identifier
        self.patient_id = patient_id
        self.data_dir = data_dir
        self.token_file_path = os.path.join(data_dir, "carelink_tokens.json")

        self.username = None
        self.country = None
        self.role = None
        self.token_expires_at = None
        self.config = None
        self._http_client = None
        self._initialized = False

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=HTTP_TIMEOUT_SEC,
                follow_redirects=True,
            )
        return self._http_client

    async def close(self):
        """Close the HTTP client session."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    async def load_tokens(self) -> bool:
        """Load stored tokens from data directory if available."""
        if not os.path.exists(self.token_file_path):
            return False

        try:
            async with aiofiles.open(self.token_file_path, "r") as f:
                data = json.loads(await f.read())

            self.access_token = data.get("access_token", self.access_token)
            self.refresh_token = data.get("refresh_token", self.refresh_token)
            self.client_id = data.get("client_id", self.client_id)
            self.client_secret = data.get("client_secret", self.client_secret)
            self.mag_identifier = data.get("mag-identifier", self.mag_identifier)
            _LOGGER.debug(f"Loaded credentials from {self.token_file_path}")
            return True
        except Exception as e:
            _LOGGER.warning(f"Failed loading token file {self.token_file_path}: {e}")
            return False

    async def save_tokens(self):
        """Persist refreshed tokens to data directory atomically."""
        os.makedirs(self.data_dir, exist_ok=True)
        payload = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
        }
        if self.client_secret:
            payload["client_secret"] = self.client_secret
        if self.mag_identifier:
            payload["mag-identifier"] = self.mag_identifier

        tmp_file = self.token_file_path + ".tmp"
        try:
            async with aiofiles.open(tmp_file, "w") as f:
                await f.write(json.dumps(payload, indent=2))
            os.replace(tmp_file, self.token_file_path)
            _LOGGER.debug(f"Saved token payload to {self.token_file_path}")
        except Exception as e:
            _LOGGER.error(f"Failed saving tokens to file: {e}")

    def _parse_jwt(self, token: str) -> dict:
        """Decode JWT payload without verifying signature."""
        try:
            parts = token.split(".")
            if len(parts) < 2:
                raise ValueError("Invalid JWT token structure")

            b64_str = parts[1]
            padding = 4 - (len(b64_str) % 4)
            if padding and padding < 4:
                b64_str += "=" * padding

            decoded_bytes = base64.b64decode(b64_str)
            return json.loads(decoded_bytes.decode("utf-8"))
        except Exception as e:
            raise CarelinkAuthError(f"Failed decoding JWT access token: {e}") from e

    def _extract_token_metadata(self):
        """Extract expiration timestamp, username, and country from JWT payload."""
        if not self.access_token:
            raise CarelinkAuthError("No access token provided")

        payload = self._parse_jwt(self.access_token)
        exp_ts = payload.get("exp")
        if not exp_ts:
            raise CarelinkAuthError("JWT token missing 'exp' field")

        self.token_expires_at = datetime.fromtimestamp(exp_ts, tz=timezone.utc)
        token_details = payload.get("token_details", {})
        self.username = token_details.get("preferred_username")
        self.country = token_details.get("country", "EU")

        _LOGGER.debug(
            f"Token parsed: user={self.username}, country={self.country}, "
            f"expires_at={self.token_expires_at.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )

    async def _discover_config(self):
        """Discover Carelink endpoints based on country/region."""
        client = await self._get_client()
        resp = await client.get(DISCOVERY_URL)
        if resp.status_code != 200:
            raise CarelinkError(f"Discovery API returned HTTP {resp.status_code}")

        data = resp.json()
        region = None
        for country_map in data.get("supportedCountries", []):
            if self.country.upper() in country_map:
                region = country_map[self.country.upper()].get("region")
                break

        if not region:
            raise CarelinkError(f"Unsupported country code: {self.country}")

        config = None
        for cp in data.get("CP", []):
            if cp.get("region") == region:
                config = cp
                break

        if not config:
            raise CarelinkError(f"No region configuration found for region {region}")

        # Fetch SSO configuration to get token_url
        sso_key = config.get("UseSSOConfiguration", "Auth0SSOConfiguration")
        sso_url = config.get(sso_key)
        if not sso_url:
            raise CarelinkError(f"SSO URL key '{sso_key}' not found in discovery configuration")

        sso_resp = await client.get(sso_url)
        if sso_resp.status_code != 200:
            raise CarelinkError(f"SSO config returned HTTP {sso_resp.status_code}")

        sso_config = sso_resp.json()
        is_auth0 = "Auth0" in sso_key

        if is_auth0:
            if "issuer" in sso_config and sso_config["issuer"]:
                sso_base = sso_config["issuer"].rstrip("/")
            elif "server" in sso_config:
                srv = sso_config["server"]
                sso_base = f"https://{srv['hostname']}:{srv['port']}/{srv['prefix']}".rstrip("/")
            else:
                auth_ep = sso_config.get("system_endpoints", {}).get("authorization_endpoint_path", "")
                if auth_ep.startswith("http"):
                    sso_base = auth_ep.rsplit("/", 1)[0]
                else:
                    raise CarelinkError("Cannot resolve Auth0 base URL")
        else:
            srv = sso_config["server"]
            sso_base = f"https://{srv['hostname']}:{srv['port']}/{srv['prefix']}".rstrip("/")

        token_path = sso_config.get("system_endpoints", {}).get("token_endpoint_path", "/oauth/token")
        config["token_url"] = sso_base + token_path
        self.config = config
        _LOGGER.debug(f"Resolved Carelink token endpoint: {config['token_url']}")

    async def _refresh_token(self):
        """Execute OAuth 2.0 refresh_token grant."""
        if not self.config or "token_url" not in self.config:
            await self._discover_config()

        client = await self._get_client()
        token_url = self.config["token_url"]
        _LOGGER.info("Refreshing Carelink access token...")

        body = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
        }
        if self.client_secret:
            body["client_secret"] = self.client_secret

        headers = {}
        if self.mag_identifier:
            headers["mag-identifier"] = self.mag_identifier

        resp = await client.post(token_url, data=body, headers=headers)
        if resp.status_code != 200:
            raise CarelinkAuthError(f"Token refresh failed HTTP {resp.status_code}: {resp.text}")

        res_json = resp.json()
        self.access_token = res_json["access_token"]
        self.refresh_token = res_json.get("refresh_token", self.refresh_token)

        self._extract_token_metadata()
        await self.save_tokens()
        _LOGGER.info(f"Token successfully refreshed (expires at {self.token_expires_at.strftime('%H:%M:%S UTC')}).")

    async def ensure_valid_token(self):
        """Check token expiry and refresh if expiring within margin."""
        if not self.access_token or not self.token_expires_at:
            self._extract_token_metadata()

        now = datetime.now(timezone.utc)
        margin = timedelta(seconds=TOKEN_EXPIRY_MARGIN_SEC)

        if now + margin >= self.token_expires_at:
            _LOGGER.info("Access token near expiration. Triggering refresh...")
            await self._refresh_token()

    async def authenticate(self):
        """Initialize session and fetch user profile/role."""
        await self.load_tokens()
        self._extract_token_metadata()
        await self.ensure_valid_token()
        await self._discover_config()

        # Fetch user role via /users/me
        client = await self._get_client()
        user_url = self.config["baseUrlCareLink"] + "/users/me"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json, text/plain, */*",
        }
        if self.mag_identifier:
            headers["mag-identifier"] = self.mag_identifier

        resp = await client.get(user_url, headers=headers)
        if resp.status_code in (401, 403):
            _LOGGER.info("Initial /users/me call returned 401/403. Attempting token refresh...")
            await self._refresh_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = await client.get(user_url, headers=headers)

        if resp.status_code != 200:
            raise CarelinkError(f"Failed fetching user profile HTTP {resp.status_code}")

        user_data = resp.json()
        user_role = user_data.get("role", "PATIENT")
        self.role = "carepartner" if user_role in ("CARE_PARTNER", "CARE_PARTNER_OUS") else "patient"

        # If care partner role and no patient_id specified, resolve active patient
        if self.role == "carepartner" and not self.patient_id:
            patients_url = self.config["baseUrlCareLink"] + "/links/patients"
            patients_resp = await client.get(patients_url, headers=headers)
            if patients_resp.status_code == 200:
                for p in patients_resp.json():
                    if p.get("status") == "ACTIVE":
                        self.patient_id = p.get("username")
                        _LOGGER.info(f"Resolved active Care Partner patient ID: {self.patient_id}")
                        break

        self._initialized = True
        _LOGGER.info(f"Carelink session initialized (User: {self.username}, Role: {self.role}).")

    async def fetch_recent_data(self) -> dict:
        """Fetch recent pump/sensor data from Carelink /display/message."""
        if not self._initialized:
            await self.authenticate()
        else:
            await self.ensure_valid_token()

        client = await self._get_client()
        display_url = self.config["baseUrlCumulus"] + "/display/message"

        payload_obj = {"username": self.username, "role": self.role}
        if self.role == "carepartner" and self.patient_id:
            payload_obj["patientId"] = self.patient_id

        request_body = json.dumps(payload_obj)

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if self.mag_identifier:
            headers["mag-identifier"] = self.mag_identifier

        resp = await client.post(display_url, data=request_body, headers=headers)
        if resp.status_code in (401, 403):
            _LOGGER.warning("Data fetch returned 401/403. Refreshing token and retrying...")
            await self._refresh_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
            resp = await client.post(display_url, data=request_body, headers=headers)

        if resp.status_code != 200:
            raise CarelinkError(f"Data fetch failed HTTP {resp.status_code}: {resp.text}")

        data = resp.json()
        return data.get("patientData", data)
