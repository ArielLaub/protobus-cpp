# Compatibility with the TypeScript, Python and Go ports

protobus-cpp speaks the protobus wire protocol exactly as
[protobus](https://github.com/ArielLaub/protobus) (TypeScript, 2.5),
[protobus-py](https://github.com/ArielLaub/protobus-py) (2.0) and
[protobus-go](https://github.com/ArielLaub/protobus-go) (2.0) do. Services and
clients in all four languages can share one broker, one schema and even one
queue: replicas of a service in different languages compete for its requests
and climb one retry ladder together.

This is tested, not assumed: the [cross-language suite](../crosslang/README.md)
runs C++ against the other ports' real libraries over a real broker, in both
directions, and runs `interop.Flaky` in all four languages at once on one
queue.

## The contract is the `.proto`

A schema is shared verbatim. It needs no C++ options and no import for the
built-in custom types: the CLI and the `MessageFactory` add it. Each proto
package becomes a C++ namespace (`package Calculator` → `Calculator::`).

| Proto | TypeScript | Python | Go | C++ |
|---|---|---|---|---|
| `int64`, `uint64`, … | decimal `string` | `int` | `int64`, `uint64` | `int64_t`, `uint64_t` |
| `bigint` (custom) | `bigint` | `int` | `*pbtypes.Bigint` | `::bigint` (`protobus::toUint256` → `Uint256`) |
| `timestamp` (custom) | `Date` | aware `datetime` (UTC) | `*pbtypes.Timestamp` | `::timestamp` (`protobus::toTimePoint`) |
| `bytes` | `Buffer` | `bytes` | `[]byte` | `std::string` |
| enum | value name (`string`) | value name | generated enum type | generated enum |
| unset scalar | its default | its default | its default | its default (proto3) |

The wire bytes are identical in every case; only the in-language
representation differs.

`bigint` is an unsigned integer up to 2^256-1 carried as exactly 32
big-endian bytes; `timestamp` is signed milliseconds since the epoch. Both are
one-field messages (`message bigint { optional bytes value = 1; }`) declared at
the root of the type namespace. Every port refuses a negative or oversized
`bigint` and refuses to decode one wider than 32 bytes.

## Topology

Identical names, flags and arguments; the exchange names come from the same
environment variables.

| | Name | Type / flags |
|---|---|---|
| RPC exchange | `proto.bus` (`BUS_EXCHANGE_NAME`) | topic, durable |
| Reply exchange | `proto.bus.callback` (`CALLBACKS_EXCHANGE_NAME`) | direct, durable |
| Event exchange | `proto.bus.events` (`EVENTS_EXCHANGE_NAME`) | topic, durable |
| Stream-cancel exchange | `proto.bus.cancel` (`CANCEL_EXCHANGE_NAME`) | fanout, durable |
| Service queue | `<Service>` bound `REQUEST.<Service>.*` | durable; arguments only when configured (`x-message-ttl`, `x-max-priority`) |
| Retry | `<Service>.Retry` (TTL, DLX → `proto.bus`), `<Service>.Retry.Exchange` (topic, `#`), `<Service>.DLQ` | durable |
| Event queue | `<Service>.Events` | durable |
| Event retry (opt-in) | `<Service>.Events.Retry`, `.Events.Retry.Exchange`, `.Events.Redelivery`, `.Events.DLQ` | durable |
| Reply queue | server-named, bound to `proto.bus.callback` under its own name | exclusive, auto-delete |
| Cancel queue | server-named, bound to `proto.bus.cancel` | exclusive, auto-delete |

RabbitMQ compares integer queue arguments by class, so the different integer
widths the four AMQP clients pick for the same TTL are equivalent; the
mixed-replica test declares one service's queues from all four languages.

## Messages

- Requests, replies and events travel in the protobus envelopes
  (`RequestContainer`, `ResponseContainer`, `EventContainer`). protobus-cpp
  encodes them byte for byte as TypeScript does (the golden vectors in
  `tests/unit/wire_test.cc`, shared with protobus-go), including the empty
  fields TypeScript writes explicitly.
- Every publish carries a `messageId` (a UUID unless the caller sets one),
  `contentType: application/octet-stream` and a `correlationId`; requests and
  events are persistent. Unary requests are published `mandatory`.
- Streaming replies carry `x-protobus-seq` (from 0) and `x-protobus-final`;
  the last frame is final, an empty stream is one empty final frame, and a
  failure is the final frame. Cancellation is an empty message on
  `proto.bus.cancel` carrying the stream's correlation id.
- Retry and dead-letter copies carry `x-retry-count`,
  `x-original-routing-key` (written from the delivered routing key, which
  every port's retry hop preserves; C++ never routes by the header), `x-first-failure-time`, `x-last-error` (the
  error's class and code, never an unhandled error's message), and on the DLQ
  `x-original-queue` and `x-dlq-time`; they keep `contentType`,
  `contentEncoding`, `priority`, `timestamp`, `type` and `appId`, and drop
  `expiration` and `userId`.
- Readers accept every encoding peers produce: integer headers of any width or
  as text, `x-protobus-final` as a boolean, number or text.
- A mandatory publish from C++ whose `messageId` is shared with another
  publish still awaiting its confirm on the same channel (or that has none)
  also carries `x-protobus-publish-tag`, a per-publish token that tells the
  broker's return for it apart. Every port ignores or copies unknown headers.

## Errors

A service error crosses as `ResponseError{method, message, code}`. In C++ a
remote error is a `protobus::RemoteError`; the codes are shared:
`HANDLED_ERROR` (default for a `HandledError`), `PROTOCOL_ERROR` (a request the
service will not run), `INTERNAL_ERROR` (an unhandled error when
`PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`), `PROCESSING_TIMEOUT`, and any code a
service chooses.

## Where the ports differ

These are deliberate, and none changes what is on the wire. C++ follows the
TypeScript reference except where all three other ports' experience, or C++
itself, argued otherwise.

| | C++ | TypeScript 2.5 | Python 2.0 | Go 2.0 |
|---|---|---|---|---|
| Who declares the core exchanges | every process, for what it publishes to | services only | every process | every process |
| Processing timeout, after the last retry | caller answered: `PROCESSING_TIMEOUT` | caller waits for its own timeout | caller answered | caller answered |
| Settlement publish fails | message requeued after 1 s | left unacknowledged | requeued after 1 s | requeued after 1 s |
| Retry/DLQ copies | `mandatory` | not mandatory | `mandatory` | `mandatory` |
| Channel lost on a live connection | listener rebuilt; dispatcher reopens on next publish | not recovered | listener rebuilt | component rebuilt |
| A downstream `RemoteError` escaping a handler | retried, like any unhandled error | retried | retried | answered with the downstream code |
| `HandledError` from an event handler with event retry on | dropped | dropped | dropped | dead-lettered |
| Streaming call | request published at call time; failures surface at the first `next()` | starts at call time | starts at iteration | starts when ranged over |
| Processing timeout on streams | covers obtaining the generator | covers obtaining the iterator | applied | not applied |
| Explicit priority 0 | sent | sent | sent | not sent (equivalent at the broker) |
| Handlers run | on worker threads, up to the prefetch, in parallel | on the event loop | on the event loop | on goroutines, in parallel |
| Closing the context | fails pending calls and streams at once, then waits for running handlers, up to `SHUTDOWN_DRAIN_TIMEOUT_MS` | closes at once | closes at once | `Close` at once; `Shutdown` drains first |
| Logging | `ILogger` / structured `Log`, as TypeScript | `Logger` / `Log` | `logging` | `log/slog` |
