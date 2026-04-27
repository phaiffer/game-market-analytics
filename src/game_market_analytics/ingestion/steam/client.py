"""Minimal Steam Web API client for the application catalog."""

from __future__ import annotations

import json
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


STEAM_APP_LIST_ENDPOINT = "https://partner.steam-api.com/IStoreService/GetAppList/v1/"
SteamApiKeyAuthLocation = Literal["query", "header"]


class SteamClientError(RuntimeError):
    """Raised when the Steam app catalog cannot be fetched or parsed."""


class SteamClient:
    """Small client for Steam app catalog requests."""

    def __init__(
        self,
        api_key: str,
        app_list_endpoint: str = STEAM_APP_LIST_ENDPOINT,
        timeout_seconds: float = 30.0,
        max_results: int = 50_000,
        api_key_auth_location: SteamApiKeyAuthLocation = "query",
    ) -> None:
        if not api_key:
            raise SteamClientError("STEAM_API_KEY is required for the official Steam app catalog endpoint.")
        if api_key_auth_location not in ("query", "header"):
            raise SteamClientError("STEAM_API_KEY_AUTH_LOCATION must be either 'query' or 'header'.")

        self.api_key = api_key
        self.app_list_endpoint = app_list_endpoint
        self.timeout_seconds = timeout_seconds
        self.max_results = max_results
        self.api_key_auth_location = api_key_auth_location

    def fetch_app_list(self) -> dict[str, Any]:
        """Fetch all raw Steam app list pages."""

        pages: list[dict[str, Any]] = []
        last_appid: int | None = None

        while True:
            page_payload = self._fetch_app_list_page(last_appid=last_appid)
            pages.append(page_payload)

            apps = page_payload.get("response", {}).get("apps", [])
            if not isinstance(apps, list) or not apps:
                break

            next_last_appid = _last_appid(apps)
            if next_last_appid is None or next_last_appid == last_appid:
                break

            last_appid = next_last_appid
            if len(apps) < self.max_results:
                break

        return {
            "source": "steam",
            "endpoint": self.app_list_endpoint,
            "page_count": len(pages),
            "pages": pages,
        }

    def _fetch_app_list_page(self, *, last_appid: int | None = None) -> dict[str, Any]:
        query_params: dict[str, str | int] = {
            "max_results": self.max_results,
            "include_games": "true",
            "include_dlc": "true",
            "include_software": "true",
            "include_videos": "true",
            "include_hardware": "true",
        }
        headers = {"User-Agent": "game-market-analytics/0.1"}
        if self.api_key_auth_location == "query":
            query_params["key"] = self.api_key
        else:
            headers["x-webapi-key"] = self.api_key

        if last_appid is not None:
            query_params["last_appid"] = last_appid

        url = f"{self.app_list_endpoint}?{urlencode(query_params)}"
        request = Request(url, headers=headers)

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            if exc.code == 403:
                raise SteamClientError(
                    "Steam app list request failed with HTTP 403. "
                    f"Authentication was sent using {self.api_key_auth_location} auth. "
                    "Check for a missing or invalid API key, a revoked key, "
                    "an authorization mismatch for the requested endpoint, "
                    "or a request format mismatch."
                ) from exc
            raise SteamClientError(
                f"Steam app list request failed with HTTP {exc.code} "
                f"using {self.api_key_auth_location} auth."
            ) from exc
        except URLError as exc:
            raise SteamClientError(f"Steam app list request failed: {exc.reason}") from exc
        except TimeoutError as exc:
            raise SteamClientError("Steam app list request timed out.") from exc

        try:
            parsed_payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SteamClientError("Steam app list response was not valid JSON.") from exc

        if not isinstance(parsed_payload, dict):
            raise SteamClientError("Steam app list response did not contain a JSON object.")

        return parsed_payload


def _last_appid(apps: list[Any]) -> int | None:
    last_app = apps[-1]
    if not isinstance(last_app, dict):
        return None

    appid = last_app.get("appid")
    return appid if isinstance(appid, int) else None
