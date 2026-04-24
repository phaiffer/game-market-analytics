import json
from io import BytesIO

from game_market_analytics.ingestion.steam import client as steam_client_module
from game_market_analytics.ingestion.steam.client import SteamClient


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
