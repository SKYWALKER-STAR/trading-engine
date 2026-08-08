from __future__ import annotations

from os import getenv
from typing import Any

from trading_engine.common.logger import get_logger


LOGGER = get_logger(__name__)


class ClickHouseMarketDataSource:
    """Fetches the latest market row from ClickHouse."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
        custom_query: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._table = table
        self._custom_query = custom_query
        self._client: Any | None = None

    @classmethod
    def from_env(cls) -> "ClickHouseMarketDataSource":
        table = getenv("CLICKHOUSE_TABLE", "v_usdt_futures_trend_score_calc")
        LOGGER.info("Using ClickHouse table: %s", table)
        return cls(
            host=getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(getenv("CLICKHOUSE_PORT", "8123")),
            user=getenv("CLICKHOUSE_USER", "codex"),
            password=getenv("CLICKHOUSE_PASSWORD", "dZiRUCj2w0LJ"),
            database=getenv("CLICKHOUSE_DATABASE", "binance"),
            table=getenv("CLICKHOUSE_TABLE", "v_usdt_futures_trend_score_calc"),
            custom_query=getenv("CLICKHOUSE_QUERY"),
        )

    def fetch_latest(self, symbol: str | None = None) -> dict[str, Any] | None:
        client = self._get_client()

        if self._custom_query:
            query = self._custom_query
            parameters: dict[str, Any] = {}
        else:
            where_clause = ""
            parameters = {}
            if symbol is not None:
                where_clause = "WHERE symbol = %(symbol)s"
                parameters["symbol"] = symbol

            query = (
                "SELECT symbol, interval, open_time, close, "
                "ema_12, ema_26, rsi_14, adx_14, plus_di, minus_di, taker_buy_ratio, funding_rate, "
                "score_ema, score_dmi_adx, score_rsi, score_flow, score_funding "
                f"FROM {self._table} "
                f"{where_clause} "
                "ORDER BY open_time DESC LIMIT 1"
            )

        LOGGER.debug("Executing ClickHouse query {%s}", query)
        result = client.query(query, parameters=parameters)
        LOGGER.debug(
            "ClickHouse query executed, %d rows returned",
            len(result.result_rows),
            extra={"symbol": symbol, "query": query},
        )
        LOGGER.debug("ClickHouse query result: %s", result.result_rows, extra={"symbol": symbol, "query": query})
        if not result.result_rows:
            LOGGER.debug("No market data found from ClickHouse query", extra={"symbol": symbol})
            return None

        row = result.result_rows[0]
        LOGGER.debug("Fetched latest market row from ClickHouse", extra={"symbol": row[0]})
        return dict(zip(result.column_names, row, strict=True))

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            import clickhouse_connect
        except ImportError as exc:
            raise RuntimeError(
                "clickhouse-connect is not installed. Install with: pip install clickhouse-connect"
            ) from exc

        self._client = clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            username=self._user,
            password=self._password,
            database=self._database,
        )
        LOGGER.info("Initialized ClickHouse client", extra={"host": self._host, "database": self._database})
        return self._client
