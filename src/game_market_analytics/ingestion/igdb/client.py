"""Minimal IGDB API client for controlled reference ingestion."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from game_market_analytics.ingestion.igdb.auth import IGDBAccessToken


IGDB_API_BASE_URL = "https://api.igdb.com/v4"


class IGDBClientError(RuntimeError):
    """Raised when an IGDB API request cannot be completed or parsed."""


class IGDBClient:
    """Small synchronous IGDB client using APICALYPSE request bodies."""

    def __init__(
        self,
        *,
        client_id: str,
        access_token: IGDBAccessToken,
        api_base_url: str = IGDB_API_BASE_URL,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not client_id:
            raise IGDBClientError("IGDB client ID is required.")
        if not access_token.access_token:
            raise IGDBClientError("IGDB access token is required.")

        self.client_id = client_id
        self.access_token = access_token
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def post(self, endpoint: str, query: str) -> list[dict[str, Any]]:
        """POST an APICALYPSE query to an IGDB endpoint."""

        normalized_endpoint = endpoint.strip("/")
        url = f"{self.api_base_url}/{normalized_endpoint}"
        request = Request(
            url,
            data=query.encode("utf-8"),
            method="POST",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token.access_token}",
                "Client-ID": self.client_id,
                "Content-Type": "text/plain",
                "User-Agent": "game-market-analytics/0.1",
            },
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            raise IGDBClientError(
                f"IGDB request to endpoint '{normalized_endpoint}' failed with HTTP {exc.code}."
            ) from exc
        except URLError as exc:
            raise IGDBClientError(
                f"IGDB request to endpoint '{normalized_endpoint}' failed: {exc.reason}"
            ) from exc
        except TimeoutError as exc:
            raise IGDBClientError(
                f"IGDB request to endpoint '{normalized_endpoint}' timed out."
            ) from exc

        try:
            parsed_payload: Any = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise IGDBClientError(
                f"IGDB response from endpoint '{normalized_endpoint}' was not valid JSON."
            ) from exc

        if not isinstance(parsed_payload, list):
            raise IGDBClientError(
                f"IGDB response from endpoint '{normalized_endpoint}' did not contain a JSON array."
            )
        if not all(isinstance(item, dict) for item in parsed_payload):
            raise IGDBClientError(
                f"IGDB response from endpoint '{normalized_endpoint}' contained non-object items."
            )

        return parsed_payload
