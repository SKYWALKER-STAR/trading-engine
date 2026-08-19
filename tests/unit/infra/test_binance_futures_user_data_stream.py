from __future__ import annotations

import asyncio
import json
from os import getenv
from typing import Any

import pytest

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


class _LiveWebSocketProbe:
    """Open a real WebSocket and optionally capture one account event."""

    def __init__(self, receive_timeout_seconds: float) -> None:
        self.receive_timeout_seconds = receive_timeout_seconds
        self.connected = False

    async def consume(self, endpoint: str, handler: Any) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("install the runtime extras to run this test") from exc

        async with websockets.connect(
            endpoint,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        ) as ws:
            self.connected = True
            try:
                raw_message = await asyncio.wait_for(
                    ws.recv(),
                    timeout=self.receive_timeout_seconds,
                )
            except TimeoutError:
                return

            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")
            payload = json.loads(raw_message)
            assert isinstance(payload, dict)
            await handler(payload)


def test_stream_connects_to_real_binance_user_data_stream() -> None:
    """Probe Binance only when the non-destructive live test is explicitly enabled."""
    if getenv("RUN_BINANCE_LIVE_USER_STREAM_TEST") != "1":
        pytest.skip(
            "set RUN_BINANCE_LIVE_USER_STREAM_TEST=1 to enable the live user-stream test"
        )

    api_key = getenv("BINANCE_API_KEY", "")
    assert api_key, "BINANCE_API_KEY is required"

    receive_timeout_seconds = float(
        getenv("BINANCE_LIVE_USER_STREAM_RECEIVE_TIMEOUT_SECONDS", "5.0")
    )
    assert receive_timeout_seconds > 0

    websocket = _LiveWebSocketProbe(receive_timeout_seconds)
    messages: list[dict[str, Any]] = []
    stream = BinanceFuturesUserDataStream(
        rest_api_url=getenv("BINANCE_FUTURES_REST_API_URL", "https://fapi.binance.com"),
        websocket_stream_url=getenv(
            "BINANCE_FUTURES_USER_STREAM_URL", "wss://fstream.binance.com/ws"
        ),
        api_key=api_key,
        request_timeout_seconds=10.0,
        websocket_transport=websocket,
    )

    asyncio.run(stream._run_session(messages.append))

    assert websocket.connected
    if messages:
        assert isinstance(messages[0].get("e"), str)
        print(f"Received Binance user data event: {messages[0]}")
