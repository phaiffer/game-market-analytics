"""Minimal Steam review API client."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


STEAM_REVIEWS_ENDPOINT_TEMPLATE = "https://store.steampowered.com/appreviews/{app_id}"


class SteamReviewClientError(RuntimeError):
    """Raised when Steam reviews cannot be fetched or parsed."""


class SteamReviewClient:
    """Small client for Steam app review requests."""

    def __init__(
        self,
        endpoint_template: str = STEAM_REVIEWS_ENDPOINT_TEMPLATE,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.endpoint_template = endpoint_template
        self.timeout_seconds = timeout_seconds

    def fetch_review_page(
        self,
        *,
        app_id: int,
        cursor: str,
        language: str,
        review_type: str,
        filter_value: str = "recent",
        num_per_page: int = 100,
    ) -> dict[str, Any]:
        """Fetch one raw Steam review page."""

        query_params: dict[str, str | int] = {
            "json": 1,
            "cursor": cursor,
            "filter": filter_value,
            "language": language,
            "review_type": review_type,
            "purchase_type": "all",
            "num_per_page": num_per_page,
            "filter_offtopic_activity": 0,
        }
        url = f"{self.endpoint_template.format(app_id=app_id)}?{urlencode(query_params)}"
        request = Request(url, headers={"User-Agent": "game-market-analytics/0.1"})

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            raise SteamReviewClientError(
                f"Steam review request for app_id={app_id} failed with HTTP {exc.code}."
            ) from exc
        except URLError as exc:
            raise SteamReviewClientError(
                f"Steam review request for app_id={app_id} failed: {exc.reason}"
            ) from exc
        except TimeoutError as exc:
            raise SteamReviewClientError(
                f"Steam review request for app_id={app_id} timed out."
            ) from exc

        try:
            parsed_payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SteamReviewClientError(
                f"Steam review response for app_id={app_id} was not valid JSON."
            ) from exc

        if not isinstance(parsed_payload, dict):
            raise SteamReviewClientError(
                f"Steam review response for app_id={app_id} did not contain a JSON object."
            )

        return parsed_payload
