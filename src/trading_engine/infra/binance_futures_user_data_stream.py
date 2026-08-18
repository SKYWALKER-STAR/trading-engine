from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.request import Request, urlopen

from trading_engine.common.logger import get_logger


LOGGER = get_logger(__name__)


class BinanceListenKeyTransport(Protocol):
    def create(self, rest_api_url: str, api_key: str, timeout_seconds: float) -> str: ...

    def keepalive(
        self, rest_api_url: str, api_key: str, listen_key: str, timeout_seconds: float
    ) -> None: ...

    def close(
        self, rest_api_url: str, api_key: str, listen_key: str, timeout_seconds: float
    ) -> None: ...


class BinanceWebSocketTransport(Protocol):
    async def consume(
        self,
        endpoint: str,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None: ...


class BinanceHttpListenKeyTransport:
    """Manage USD-M Futures listen keys through Binance's REST API."""

    def create(self, rest_api_url: str, api_key: str, timeout_seconds: float) -> str:
        payload = self._request("POST", rest_api_url, api_key, None, timeout_seconds)
        listen_key = payload.get("listenKey")
        if not isinstance(listen_key, str) or not listen_key:
            raise RuntimeError("Binance did not return a listenKey")
        return listen_key

    def keepalive(
        self, rest_api_url: str, api_key: str, listen_key: str, timeout_seconds: float
    ) -> None:
        self._request("PUT", rest_api_url, api_key, listen_key, timeout_seconds)

    def close(
        self, rest_api_url: str, api_key: str, listen_key: str, timeout_seconds: float
    ) -> None:
        self._request("DELETE", rest_api_url, api_key, listen_key, timeout_seconds)

    @staticmethod
    def _request(
        method: str,
        rest_api_url: str,
        api_key: str,
        listen_key: str | None,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        endpoint = f"{rest_api_url.rstrip('/')}/fapi/v1/listenKey"
        if listen_key is not None:
            endpoint = f"{endpoint}?listenKey={listen_key}"
        request = Request(
            endpoint,
            method=method,
            headers={"X-MBX-APIKEY": api_key},
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
        if not raw:
            return {}
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected Binance listenKey response")
        return payload


class BinanceWebSocketsTransport:
    async def consume(
        self,
        endpoint: str,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets is not installed. Install the runtime extras") from exc

        async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
            async for raw_message in ws:
                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode("utf-8")
                payload = json.loads(raw_message)
                if isinstance(payload, dict):
                    await handler(payload)


@dataclass(slots=True)
class BinanceFuturesUserDataStream:
    """Long-running USD-M Futures user stream with listen-key renewal and reconnects."""

    rest_api_url: str
    websocket_stream_url: str
    api_key: str
    keepalive_seconds: float = 1800.0
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    request_timeout_seconds: float = 10.0
    listen_key_transport: BinanceListenKeyTransport | None = None
    websocket_transport: BinanceWebSocketTransport | None = None

    def run_forever(self, handler: Callable[[dict[str, Any]], None]) -> None:
        asyncio.run(self._run_forever(handler))

    async def _run_forever(self, handler: Callable[[dict[str, Any]], None]) -> None:
        retry_seconds = self.reconnect_initial_seconds
        while True:
            try:
                await self._run_session(handler)
                retry_seconds = self.reconnect_initial_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception(
                    "Binance user data stream disconnected",
                    extra={"retry_seconds": retry_seconds},
                )
                await asyncio.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2, self.reconnect_max_seconds)

    async def _run_session(self, handler: Callable[[dict[str, Any]], None]) -> None:
        listen_keys = self.listen_key_transport or BinanceHttpListenKeyTransport()
        websocket = self.websocket_transport or BinanceWebSocketsTransport()
        listen_key = await asyncio.to_thread(
            listen_keys.create,
            self.rest_api_url,
            self.api_key,
            self.request_timeout_seconds,
        )
        endpoint = f"{self.websocket_stream_url.rstrip('/')}/{listen_key}"
        keepalive_task = asyncio.create_task(self._keepalive(listen_keys, listen_key))

        async def dispatch(payload: dict[str, Any]) -> None:
            handler(payload)

        LOGGER.info("Binance user data stream connected")
        consume_task = asyncio.create_task(websocket.consume(endpoint, dispatch))
        try:
            done, pending = await asyncio.wait(
                {consume_task, keepalive_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
        finally:
            consume_task.cancel()
            keepalive_task.cancel()
            await asyncio.gather(consume_task, keepalive_task, return_exceptions=True)
            try:
                await asyncio.to_thread(
                    listen_keys.close,
                    self.rest_api_url,
                    self.api_key,
                    listen_key,
                    self.request_timeout_seconds,
                )
            except Exception:
                LOGGER.exception("Failed to close Binance listen key")

    async def _keepalive(
        self, transport: BinanceListenKeyTransport, listen_key: str
    ) -> None:
        while True:
            await asyncio.sleep(self.keepalive_seconds)
            await asyncio.to_thread(
                transport.keepalive,
                self.rest_api_url,
                self.api_key,
                listen_key,
                self.request_timeout_seconds,
            )
            LOGGER.debug("Renewed Binance user data stream listen key")
