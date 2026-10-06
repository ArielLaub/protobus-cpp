# Configuration

protobus-cpp reads the same environment variables as the TypeScript, Python
and Go ports, with the same defaults and the same parsing rules, so a mixed
deployment is configured in one vocabulary. Each `protobus::Config` getter
reads its variable when called, so a change takes effect without a restart of
anything that has not cached a value.

An integer must be all digits and positive; anything else (`6oo000`, `-5`,
`0`, `1.5`) keeps the default rather than becoming a surprising zero. A
boolean is one of `1`/`true`/`yes`/`on` or `0`/`false`/`no`/`off`,
case-insensitive.

## Exchanges

Part of the wire protocol: every process on one bus, whatever its language,
must agree on them.

| Variable | Default | |
|---|---|---|
| `BUS_EXCHANGE_NAME` | `proto.bus` | RPC requests (topic) |
| `CALLBACKS_EXCHANGE_NAME` | `proto.bus.callback` | RPC replies (direct) |
| `EVENTS_EXCHANGE_NAME` | `proto.bus.events` | events (topic) |
| `CANCEL_EXCHANGE_NAME` | `proto.bus.cancel` | stream cancellation (fanout) |

## Timeouts and bounds

| Variable | Default | |
|---|---|---|
| `MESSAGE_PROCESSING_TIMEOUT` | 600000 ms | Cap on one attempt at a unary request |
| `RPC_CALL_TIMEOUT_MS` | 600000 ms | How long a caller waits for its reply |
| `STREAM_IDLE_TIMEOUT_MS` | 60000 ms | The longest gap a stream's caller tolerates between chunks |
| `PUBLISH_CONFIRM_TIMEOUT_MS` | 30000 ms | How long a publish waits for the broker's confirm, counted from the publish call, including any wait for a slot; expiry is ambiguous once sent, `PublishBacklogError` before |
| `CONNECTION_READY_TIMEOUT_MS` | 30000 ms | How long a publish waits for a reconnection |
| `AMQP_HEARTBEAT_SECONDS` | 30 | AMQP heartbeat interval; bounds how long a dead peer goes unnoticed |
| `DEFAULT_PREFETCH` | 1 | Prefetch of consumers that set none of their own (event listeners) |
| `MAX_OUTSTANDING_CONFIRMS` | 256 | Publishes the broker has not yet answered, on one channel; further publishes wait for a slot. A publish whose confirm timed out keeps its slot until the broker answers or the channel closes; a channel whose every slot is held that way is closed and replaced |
| `MAX_PARKED_PUBLISHES` | 4096 | Publishes waiting for a slot on one channel; one more fails at once with `PublishBacklogError` |
| `STREAM_MAX_BUFFERED_CHUNKS` | 1024 | Chunks buffered for one streaming call |
| `STREAM_MAX_BUFFERED_BYTES` | 64 MiB | Bytes buffered for one streaming call |
| `STREAM_MAX_TOTAL_BUFFERED_BYTES` | 256 MiB | Bytes buffered across all of a process's streaming calls |
| `SHUTDOWN_DRAIN_TIMEOUT_MS` | 30000 ms | How long a graceful shutdown, or closing a `Context`, waits for in-flight work |
| `SHUTDOWN_EXIT_GRACE_MS` | 5000 ms | How long after a shutdown `RunnableService` lets the process leave on its own |

## Errors and logging

| Variable | Default | |
|---|---|---|
| `PROTOBUS_EXPOSE_INTERNAL_ERRORS` | true | Send an unhandled error's message to the caller. See [Errors](errors.md#what-the-caller-sees) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` or `silent` |

### Logging

Framework lines go through a replaceable sink, with the level filter applied
first:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/protobus.h>

class MyLogger : public protobus::ILogger {
 public:
  void info(const std::string& m) override {}
  void warn(const std::string& m) override {}
  void debug(const std::string& m) override {}
  void error(const std::string& m) override {}
};

void configureLogging() {
  protobus::setLogger(std::make_shared<MyLogger>());
  protobus::setLogLevel(protobus::LogLevel::Warn);
}
```

A sink implementing `protobus::IStructuredLogger` also receives
`protobus::LogRecord`s, the structured form: an operation, a message and
low-cardinality fields (message type, ids, queue, error code, outcome).
Payloads, headers and URLs are never in a record. The only way to log payload
material is `setDiagnosticsSerializer()`, which decides what of it is safe to
log; without one it is never even assembled.

A broker URL's password is replaced with `***` wherever protobus logs the URL.

## The broker URL

`Context::init(url)` takes a RabbitMQ URI: `amqp://user:pass@host:port/vhost`
or `amqps://...` for TLS. Percent-escapes are decoded (a vhost is often
`%2f`). Query parameters:

| Parameter | |
|---|---|
| `heartbeat` | Seconds; wins over `AMQP_HEARTBEAT_SECONDS`, and `0` disables heartbeats |
| `connection_timeout` | Milliseconds to connect and log in (default 30000) |
| `channel_max`, `frame_max` | Proposed to the broker |
| `cacertfile` | CA bundle for `amqps`; the system's when absent |
| `certfile`, `keyfile` | A client certificate |
| `verify=verify_none` | Skip verifying the broker's certificate. For tests only |

## Reconnection

A lost connection is re-established in the background: capped exponential
backoff with up to 30% jitter, configured with `ContextOptions`:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/protobus.h>

void connect(protobus::Context& context, const std::string& url) {
  protobus::ContextOptions options;
  options.reconnection.maxRetries = 0;           // 0 retries forever; default 10
  options.reconnection.initialDelayMs = 1000;    // default 1000
  options.reconnection.maxDelayMs = 30000;       // default 30000
  options.reconnection.backoffMultiplier = 2;    // default 2
  context.init(url, {}, options);
}
```

A reconnection is only reported once every component is back: the
connection reopens, and each service, listener and dispatcher reopens its
channel, redeclares its queues and bindings and resumes consuming, in
registration order. One that fails fails the attempt, and the backoff tries
again. Meanwhile:

- a call or publish waits for the reconnection, up to
  `CONNECTION_READY_TIMEOUT_MS`, then fails with `NotReadyError`;
- a call already awaiting its reply fails with `DisconnectedError`, and a
  stream in progress likewise: its request may or may not have been
  processed (closing the `Context`, or `Connection::disconnect()`, does the
  same, without reconnecting);
- unacknowledged deliveries go back to their queues, and are redelivered.

After `maxRetries` consecutive failures the connection gives up: waiting calls
fail with `NotReadyError` naming the reason, and `Connection::onError`
listeners hear it. `Connection::onReconnecting`, `onReconnected` and
`onDisconnected` report each step.

A channel lost while the connection stays up (closed by a broker-side error)
is replaced too: a listener rebuilds its channel at once, a dispatcher on its
next publish. So is a consumer the broker cancels on a live connection
(`basic.cancel`, as when its queue is deleted): the listener redeclares its
queue, bindings and retry topology and consumes again, backing off from
100 ms to 30 s while that keeps failing. A listener being stopped or closed is
not brought back.
