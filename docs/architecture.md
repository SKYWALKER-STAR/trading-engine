# Architecture Notes

## Layering

- `domain`: pure models and events.
- `strategy`: strategy-specific models, protocols, rules, and engine behavior.
- `infra`: adapter implementations such as event bus.
- `app`: composition root and bootstrapping.

## Extensibility Path

Each engine should run as an independent process and exchange only versioned Kafka messages.
In-process buses remain useful for unit tests, but they are not the integration boundary.

Planned event flow:

1. Strategy engine emits `strategy.signal.generated.v1`.
2. Position engine consumes strategy signals and emits `trade.action.requested.v1`.
3. Trade engine turns requested actions into venue orders and emits `trade.order.update.received.v1`.
4. Position engine consumes order updates and emits `position.state.changed.v1`.
5. Risk engine consumes strategy signals together with position events and produces risk actions in its own process.

## Why This Structure

- Low coupling: engines communicate via explicit events and protocols.
- High testability: each layer can be unit-tested with fakes.
- Operational flexibility: engines can scale, replay, and deploy independently.
- Traceability: envelope metadata carries correlation and causation across processes.

## Message Contracts

Contract ownership lives in `trading_engine.contracts`.
Every cross-engine payload must be wrapped in a shared envelope with:

- `event_id`
- `event_type`
- `schema_version`
- `occurred_at`
- `producer`
- `correlation_id`
- `causation_id`

Current payload families:

- `StrategySignalPayload`
- `PositionSignalCommand`
- `TradeActionPayload`
- `OrderUpdatePayload`
- `PositionStatePayload`

Boundary rule:

- `PositionManager` must not depend on `strategy` package models directly
- Kafka/application adapters are responsible for mapping external events into `PositionSignalCommand`

Versioning rule:

- backward-compatible field additions keep the same topic suffix and increment only `schema_version`
- breaking changes require a new topic suffix, such as `.v2`


## PositionManager

```
                PositionManager
                      │
          ┌───────────┼
          ↓           ↓
      StateMachine   Redis
          │        当前状态
          │
           Kafka Contracts
             ↓
        trade.action.requested.v1
```

##### PositionManager State Machine
     
StrategyEngine

LONG
 │
 ↓
PositionEngine

FLAT → OPEN_LONG
 │
 ↓
TradeEngine
 │
 ↓
Binance

RiskEngine 订阅:
- strategy.signal.generated.v1
- position.state.changed.v1

NEW
 │
 ↓
PositionManager

OPENING_LONG
 │
 │ Binance FILLED
 ↓
LONG