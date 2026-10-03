# Architecture

Client sends one newline-delimited JSON subscribe/publish/metrics request. Subscribe returns replay or snapshot at a cursor, followed by ordered live events. Hub mutations run in one event loop. Overflow replaces queued data with resync_required and disconnects the subscriber. Reconnect reconciles via cursor.

## Tradeoff

The lab optimizes local reproducibility and an inspectable failure boundary. The architecture is intentionally small enough to explain during an interview.

## Evidence boundaries

WebSocket/browser integration and durable multi-process event log remain extensions.
