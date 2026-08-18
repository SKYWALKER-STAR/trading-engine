from __future__ import annotations

import asyncio
from typing import Any

from trading_engine.infra.binance_futures_user_data_stream import BinanceFuturesUserDataStream


class _FakeListenKeys:
    def __init__(self) -> None:
        self.created = 0
        self.closed: list[str] = []

    def create(self, rest_api_url: str, api_key: str, timeout_seconds: float) -> str:
        self.created += 1
        return "listen-key-1"

    def keepalive(
        self, rest_api_url: str, api_key: str, listen_key: str, timeout_seconds: float
    ) -> None:
        return None

    def close(
        self, rest_api_url: str, api_key: str, listen_key: str, timeout_seconds: float
    ) -> None:
        self.closed.append(listen_key)


class _FakeWebSocket:
    def __init__(self) -> None:
        self.endpoints: list[str] = []

    async def consume(self, endpoint: str, handler: Any) -> None:
        self.endpoints.append(endpoint)
        await handler({"e": "ACCOUNT_UPDATE"})


def test_stream_session_creates_connects_and_closes_listen_key() -> None:
    listen_keys = _FakeListenKeys()
    websocket = _FakeWebSocket()
    messages: list[dict[str, Any]] = []
    stream = BinanceFuturesUserDataStream(
        rest_api_url="https://fapi.example.test",
        websocket_stream_url="wss://fstream.example.test/ws",
        api_key="key",
        listen_key_transport=listen_keys,
        websocket_transport=websocket,
    )

    asyncio.run(stream._run_session(messages.append))

    assert messages == [{"e": "ACCOUNT_UPDATE"}]
    assert websocket.endpoints == ["wss://fstream.example.test/ws/listen-key-1"]
    assert listen_keys.created == 1
    assert listen_keys.closed == ["listen-key-1"]
