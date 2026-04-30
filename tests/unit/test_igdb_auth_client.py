import json
from io import BytesIO
from urllib.error import HTTPError

from game_market_analytics.ingestion.igdb import auth as igdb_auth_module
from game_market_analytics.ingestion.igdb import client as igdb_client_module
from game_market_analytics.ingestion.igdb.auth import (
    IGDBAccessToken,
    IGDBAuthError,
    request_igdb_access_token,
    validate_igdb_credentials,
)
from game_market_analytics.ingestion.igdb.client import IGDBClient, IGDBClientError


class FakeResponse:
    def __init__(self, payload) -> None:
        self._payload = payload

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args) -> None:
        return None

    def read(self) -> bytes:
        return BytesIO(json.dumps(self._payload).encode("utf-8")).read()


def test_validate_igdb_credentials_requires_both_values() -> None:
    try:
        validate_igdb_credentials("client-id", None)
    except IGDBAuthError as exc:
        assert "IGDB_CLIENT_SECRET" in str(exc)
    else:
        raise AssertionError("Expected IGDBAuthError")


def test_request_igdb_access_token_uses_client_credentials_without_logging_secret(
    monkeypatch,
) -> None:
    requests = []

    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse(
            {
                "access_token": "test-access-token",
                "token_type": "bearer",
                "expires_in": 1000,
            }
        )

    monkeypatch.setattr(igdb_auth_module, "urlopen", fake_urlopen)

    token = request_igdb_access_token(
        client_id="client-id",
        client_secret="secret-value",
        token_endpoint="https://example.test/token",
    )

    assert token.access_token == "test-access-token"
    body = requests[0].data.decode("utf-8")
    assert "client_id=client-id" in body
    assert "client_secret=secret-value" in body
    assert "grant_type=client_credentials" in body


def test_request_igdb_access_token_error_does_not_expose_secret(monkeypatch) -> None:
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 401, "Unauthorized", hdrs=None, fp=None)

    monkeypatch.setattr(igdb_auth_module, "urlopen", fake_urlopen)

    try:
        request_igdb_access_token(
            client_id="client-id",
            client_secret="secret-value",
            token_endpoint="https://example.test/token",
        )
    except IGDBAuthError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected IGDBAuthError")

    assert "HTTP 401" in message
    assert "secret-value" not in message


def test_igdb_client_posts_apicalypse_query(monkeypatch) -> None:
    requests = []

    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse([{"id": 1, "name": "Dota 2"}])

    monkeypatch.setattr(igdb_client_module, "urlopen", fake_urlopen)

    client = IGDBClient(
        client_id="client-id",
        access_token=IGDBAccessToken(access_token="access-token", token_type="bearer"),
        api_base_url="https://example.test/v4",
    )
    payload = client.post("games", 'search "Dota 2"; fields id,name; limit 10;')

    assert payload == [{"id": 1, "name": "Dota 2"}]
    request = requests[0]
    assert request.full_url == "https://example.test/v4/games"
    assert request.data.decode("utf-8") == 'search "Dota 2"; fields id,name; limit 10;'
    assert request.get_header("Authorization") == "Bearer access-token"
    assert request.get_header("Client-id") == "client-id"


def test_igdb_client_rejects_non_array_response(monkeypatch) -> None:
    def fake_urlopen(request, timeout):
        return FakeResponse({"id": 1})

    monkeypatch.setattr(igdb_client_module, "urlopen", fake_urlopen)

    client = IGDBClient(
        client_id="client-id",
        access_token=IGDBAccessToken(access_token="access-token", token_type="bearer"),
        api_base_url="https://example.test/v4",
    )

    try:
        client.post("games", "fields id;")
    except IGDBClientError as exc:
        assert "JSON array" in str(exc)
    else:
        raise AssertionError("Expected IGDBClientError")
