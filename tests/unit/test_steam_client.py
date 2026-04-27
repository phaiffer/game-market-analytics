import json
from io import BytesIO
from urllib.error import HTTPError

from game_market_analytics.ingestion.steam import client as steam_client_module
from game_market_analytics.ingestion.steam.client import SteamClient, SteamClientError


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args) -> None:
        return None

    def read(self) -> bytes:
        return BytesIO(json.dumps(self._payload).encode("utf-8")).read()


def test_fetch_app_list_paginates_until_last_page(monkeypatch) -> None:
    calls = []
    responses = [
        {"response": {"apps": [{"appid": 10}, {"appid": 20}]}},
        {"response": {"apps": [{"appid": 30}]}},
    ]

    def fake_urlopen(request, timeout):
        calls.append(request.full_url)
        return FakeResponse(responses[len(calls) - 1])

    monkeypatch.setattr(steam_client_module, "urlopen", fake_urlopen)

    client = SteamClient(
        api_key="test-key",
        app_list_endpoint="https://example.test/IStoreService/GetAppList/v1/",
        max_results=2,
    )
    payload = client.fetch_app_list()

    assert payload["page_count"] == 2
    assert len(payload["pages"]) == 2
    assert "key=test-key" in calls[0]
    assert "max_results=2" in calls[0]
    assert "last_appid=20" in calls[1]


def test_fetch_app_list_supports_header_auth_without_query_key(monkeypatch) -> None:
    requests = []

    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse({"response": {"apps": []}})

    monkeypatch.setattr(steam_client_module, "urlopen", fake_urlopen)

    client = SteamClient(
        api_key="test-key",
        app_list_endpoint="https://example.test/IStoreService/GetAppList/v1/",
        api_key_auth_location="header",
    )
    client.fetch_app_list()

    request = requests[0]
    assert "key=test-key" not in request.full_url
    assert request.get_header("X-webapi-key") == "test-key"


def test_http_403_error_explains_auth_mode_without_exposing_key(monkeypatch) -> None:
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 403, "Forbidden", hdrs=None, fp=None)

    monkeypatch.setattr(steam_client_module, "urlopen", fake_urlopen)

    client = SteamClient(
        api_key="secret-test-key",
        app_list_endpoint="https://example.test/IStoreService/GetAppList/v1/",
        api_key_auth_location="header",
    )

    try:
        client.fetch_app_list()
    except SteamClientError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected SteamClientError")

    assert "HTTP 403" in message
    assert "header auth" in message
    assert "missing or invalid API key" in message
    assert "secret-test-key" not in message
