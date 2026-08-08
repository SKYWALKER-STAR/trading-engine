# Architecture Notes

## Layering

- `domain`: pure models and events.
- `strategy`: strategy-specific models, protocols, rules, and engine behavior.
- `infra`: adapter implementations such as event bus.
- `app`: composition root and bootstrapping.

## Extensibility Path

The current strategy engine emits domain events through an `EventBus` contract.
Future engines can subscribe to these events without changing strategy internals.

Planned event flow:

1. Strategy engine emits `SignalGenerated`.
2. Risk engine evaluates and emits `RiskDecision`.
3. Position engine converts to target exposure and emits `TargetPosition`.
4. Execution engine turns targets into venue orders and emits execution events.

## Why This Structure

- Low coupling: engines communicate via explicit events and protocols.
- High testability: each layer can be unit-tested with fakes.
- Operational flexibility: same contracts can run in-process first and move to queue/RPC later.
