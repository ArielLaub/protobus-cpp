# Errors

## Answers and failures

What a handler throws decides what happens to the request.

**A `protobus::HandledError` is an answer.** The service decided to tell the
caller something: a validation failure, a business rule. The caller receives
it at once, as a `RemoteError` carrying its message and code, and it is never
retried.

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/protobus.h>

class ValidationError : public protobus::HandledError {
 public:
  explicit ValidationError(const std::string& message) : HandledError(message, "VALIDATION_ERROR") {}
};

void check(int quantity) {
  if (quantity <= 0) throw ValidationError("quantity must be positive");
  if (quantity > 100) throw protobus::HandledError("too many", "LIMIT_EXCEEDED");
}
```

The code defaults to `HANDLED_ERROR`. Subclass `HandledError` for errors of
your own; `protobus::isHandledError(e)` recognises them.

**Anything else is a failure**: an unexpected exception, or a processing
timeout. It is treated as infrastructure going wrong, so the request is
retried, and dead-lettered once its retries are spent.

## The retry ladder

```
REQUEST.Billing.Invoices.pay ──► Billing.Invoices ──► handler throws
                                      ▲                      │
                                      │                      ▼
                       (x-message-ttl expires)    Billing.Invoices.Retry.Exchange
                                      │                      │ (#)
                    proto.bus ◄───────┴──── Billing.Invoices.Retry
```

1. The failed request is republished to `<Service>.Retry.Exchange` under its
   original routing key, with `x-retry-count` incremented, then acknowledged.
2. It waits on `<Service>.Retry` for `retryDelayMs`, the queue's
   `x-message-ttl`.
3. On expiry the broker dead-letters it back to `proto.bus`, still under its
   original routing key, and the service's queue receives it again: perhaps
   on another replica, perhaps in another language.
4. After `maxRetries` retries it is published to `<Service>.DLQ` instead, and
   the caller is answered with the final error.

The caller waits through the ladder, so its timeout must allow for it
(3 retries 5 seconds apart by default).

`maxRetries = 0` turns the ladder off: a failure is answered at once and the
request rejected without requeue.

### What a dead letter carries

The retry and dead-letter copies are republished, keeping `contentType`,
`contentEncoding`, `priority`, `timestamp`, `type`, `appId`, `messageId` and
`correlationId`, and adding headers:

| Header | |
|---|---|
| `x-retry-count` | Retries so far |
| `x-original-routing-key` | `REQUEST.<Service>.<method>` |
| `x-first-failure-time` | Milliseconds since the epoch |
| `x-last-error` | The error's class and code, e.g. `std::runtime_error` or `TimeoutError[PROCESSING_TIMEOUT]`; never an unhandled error's message, which routinely quotes the data that caused it |
| `x-original-queue` | On the DLQ: the service's queue |
| `x-dlq-time` | On the DLQ: milliseconds since the epoch |

`expiration` is dropped (it would race the retry queue's TTL, or expire the
evidence on the DLQ) and so is `userId` (RabbitMQ checks it against the
republishing connection's user).

To replay dead letters, move them back to `proto.bus` under their
`x-original-routing-key`, for instance with RabbitMQ's shovel.

## What the caller sees

A failure reaches the caller as a `protobus::RemoteError` with the service's
message and code:

| Thrown in the handler | `code()` | `what()` |
|---|---|---|
| `HandledError(message, code)` | `code` | `message` |
| an rpc the service does not implement, or a malformed request | `PROTOCOL_ERROR` | the reason |
| a processing timeout | `PROCESSING_TIMEOUT` | `message <id> exceeded the <n>ms processing timeout` |
| anything else | the exception's code (a protobus error's), else empty | the exception's message |

With `PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`, the last row becomes
`INTERNAL_ERROR` with `internal service error (correlationId <id>)`, and the
real message stays in the service's own log. Turn it off for a service whose
callers relay errors to untrusted clients. A `HandledError` always crosses: it
is the service's answer.

A service that calls another and lets its `RemoteError` escape fails like any
other exception: it is retried. Catch it and throw a `HandledError` to answer
with it instead.

## Error codes

| Code | Raised as | |
|---|---|---|
| `HANDLED_ERROR` | `HandledError` | default code of a handled error |
| `PROTOCOL_ERROR` | `ProtocolError`, `InvalidMethodError` | a request the service will not run |
| `INTERNAL_ERROR` | `InternalServiceError` | a hidden unhandled error |
| `PROCESSING_TIMEOUT` | `TimeoutError` | an attempt overran `processingTimeoutMs` |
| `RPC_TIMEOUT` | `RpcTimeoutError` | no reply in time (caller side) |
| `NOT_READY` | `NotReadyError` | the connection is not carrying traffic |
| `PUBLISH_NACKED` | `PublishNackedError` | the broker refused a publish |
| `UNROUTABLE` | `UnroutableError` | a mandatory publish matched no queue |
| `PUBLISH_CONFIRM_TIMEOUT` | `PublishConfirmTimeoutError` | no confirm in time (ambiguous) |
| `CHANNEL_CLOSED` | `ChannelClosedError` | the channel closed unconfirmed (ambiguous) |

The first four cross the wire in a `ResponseError` and are shared by every
port. Every protobus exception derives from `protobus::Error`, with `name()`
(the class name the TypeScript port uses) and `code()`.

## Requests the service cannot understand

A request that does not decode, or names a method this service does not serve,
is answered `PROTOCOL_ERROR` and never retried: the same bytes fail the same
way on every redelivery. See [Security](security.md) for the checks.
