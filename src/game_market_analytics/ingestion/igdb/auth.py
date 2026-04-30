"""Authentication helpers for IGDB through Twitch client credentials."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TWITCH_TOKEN_ENDPOINT = "https://id.twitch.tv/oauth2/token"


class IGDBAuthError(RuntimeError):
    """Raised when IGDB credentials cannot be exchanged for an access token."""


@dataclass(frozen=True)
class IGDBAccessToken:
    """Twitch access token payload used by IGDB requests."""

    access_token: str
    token_type: str
    expires_in: int | None = None


def validate_igdb_credentials(client_id: str | None, client_secret: str | None) -> None:
    """Validate that required IGDB/Twitch credentials are present."""

    if not client_id:
        raise IGDBAuthError("IGDB_CLIENT_ID is required.")
    if not client_secret:
        raise IGDBAuthError("IGDB_CLIENT_SECRET is required.")


def request_igdb_access_token(
    *,
    client_id: str | None,
    client_secret: str | None,
    token_endpoint: str = TWITCH_TOKEN_ENDPOINT,
    timeout_seconds: float = 30.0,
) -> IGDBAccessToken:
    """Request a Twitch app access token for IGDB API calls."""

    validate_igdb_credentials(client_id, client_secret)
    form_data = urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    request = Request(
        token_endpoint,
        data=form_data,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "game-market-analytics/0.1",
        },
    )

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        raise IGDBAuthError(f"IGDB token request failed with HTTP {exc.code}.") from exc
    except URLError as exc:
        raise IGDBAuthError(f"IGDB token request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise IGDBAuthError("IGDB token request timed out.") from exc

    try:
        parsed_payload: Any = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise IGDBAuthError("IGDB token response was not valid JSON.") from exc

    if not isinstance(parsed_payload, dict):
        raise IGDBAuthError("IGDB token response did not contain a JSON object.")

    access_token = parsed_payload.get("access_token")
    token_type = parsed_payload.get("token_type", "bearer")
    expires_in = parsed_payload.get("expires_in")
    if not isinstance(access_token, str) or not access_token:
        raise IGDBAuthError("IGDB token response did not include an access token.")
    if not isinstance(token_type, str) or not token_type:
        raise IGDBAuthError("IGDB token response did not include a token type.")
    if expires_in is not None and not isinstance(expires_in, int):
        expires_in = None

    return IGDBAccessToken(
        access_token=access_token,
        token_type=token_type,
        expires_in=expires_in,
    )
