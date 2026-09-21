from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from trading_engine.contracts.messages import EngineEvent
from trading_engine.contracts.serde import decode_event, encode_event


@dataclass
class WorkflowChange:
    state: dict[str, Any] | None
    events: list[tuple[str, EngineEvent[Any], str]] = field(default_factory=list)
    due_at: float | None = None


class RedisWorkflowStore:
    """Commit an aggregate, input receipt, outgoing events and work schedule together.

    All keys share an account hash tag. Callbacks must have no external side effects:
    WATCH conflicts cause a fresh read and recomputation. Inbox receipts do not expire.
    """

    def __init__(self, client: Any, prefix: str, account_id: str = "default") -> None:
        if any(c in account_id or c in prefix for c in "{}"):
            raise ValueError("account_id and prefix cannot contain Redis hash-tag braces")
        self.client = client
        self.base = f"{prefix.rstrip(':')}:{{{account_id}}}"
        self.outbox = f"{self.base}:outbox"
        self.tasks = f"{self.base}:tasks"

    def state_key(self, identity: str) -> str:
        return f"{self.base}:state:{identity}"

    def get(self, identity: str) -> dict[str, Any] | None:
        raw = self.client.get(self.state_key(identity))
        return None if raw is None else json.loads(raw)

    def transact(
        self,
        identity: str,
        input_id: str | None,
        decide: Callable[[dict[str, Any] | None], WorkflowChange],
    ) -> bool:
        from redis.exceptions import WatchError

        state_key = self.state_key(identity)
        inbox_key = None if input_id is None else f"{self.base}:inbox:{input_id}"
        for _ in range(32):
            with self.client.pipeline() as pipe:
                try:
                    pipe.watch(state_key, *([] if inbox_key is None else [inbox_key]))
                    if inbox_key is not None and pipe.exists(inbox_key):
                        return False
                    raw = pipe.get(state_key)
                    current = None if raw is None else json.loads(raw)
                    change = decide(current)
                    # Serialize and validate before queuing any writes. These keys are
                    # owned exclusively by this store; never reuse them for other types.
                    encoded = None
                    if change.state is not None:
                        change.state["revision"] = (current or {}).get("revision", 0) + 1
                        encoded = json.dumps(change.state, allow_nan=False)
                    outgoing = [
                        {"topic": topic, "key": key,
                         "body": encode_event(event).decode("utf-8"),
                         "aggregate": identity,
                         "revision": str((change.state or {}).get("revision", 0))}
                        for topic, event, key in change.events
                    ]
                    for key, expected in ((self.outbox, "stream"), (self.tasks, "zset")):
                        kind = pipe.type(key)
                        if kind not in ("none", expected):
                            raise ValueError(f"Workflow key {key} has unexpected type {kind}")
                    pipe.multi()
                    if encoded is not None:
                        pipe.set(state_key, encoded)
                    for message in outgoing:
                        pipe.xadd(self.outbox, message)
                    if change.due_at is None:
                        pipe.zrem(self.tasks, identity)
                    else:
                        pipe.zadd(self.tasks, {identity: change.due_at})
                    if inbox_key is not None:
                        pipe.set(inbox_key, identity)
                    pipe.execute()
                    return True
                except WatchError:
                    continue
        raise RuntimeError("Workflow contention limit reached; input offset must not be committed")

    def due(self, now: float) -> list[str]:
        return self.client.zrangebyscore(self.tasks, "-inf", now, start=0, num=100)


class OutboxRelay:
    """One serial relay per store, protected by a renewable Redis lock.

    Read the oldest entry and delete it only after Kafka acknowledges. A crash
    after publishing leaves the same serialized event for replay. No trimming.
    """

    def __init__(self, store: RedisWorkflowStore, publisher: Any, observer: Any = None) -> None:
        self.store = store
        self.publisher = publisher
        self.observer = observer

    def run_once(self, limit: int = 100) -> int:
        lock = self.store.client.lock(f"{self.store.base}:relay-lock", timeout=60)
        if not lock.acquire(blocking=False):
            return 0
        count = 0
        try:
            for _ in range(limit):
                lock.extend(60, replace_ttl=True)
                rows = self.store.client.xrange(self.store.outbox, count=1)
                if not rows:
                    break
                entry_id, fields = rows[0]
                self.publisher.publish(fields["topic"], decode_event(fields["body"]), fields["key"])
                if not lock.owned():
                    raise RuntimeError("Outbox relay lease expired; leave event for replay")
                self.store.client.xdel(self.store.outbox, entry_id)
                if self.observer is not None:
                    try:
                        self.observer(decode_event(fields["body"]))
                    except Exception:
                        from trading_engine.common.logger import get_logger
                        get_logger(__name__).exception("Optional outbox observer failed")
                count += 1
        finally:
            if lock.owned():
                lock.release()
        return count


def create_workflow_store(prefix: str, redis_url: str, account_id: str) -> RedisWorkflowStore:
    import redis

    client = redis.Redis.from_url(
        redis_url, decode_responses=True, socket_timeout=10, socket_connect_timeout=10,
    )
    return RedisWorkflowStore(client, prefix, account_id)
