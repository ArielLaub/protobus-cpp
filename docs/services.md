# Services

A service is a class deriving from the `<Service>Base` that code generation
writes for each `service` in a schema, itself a `protobus::RunnableService`
and so a `protobus::MessageService`. Each rpc is a virtual method:

| Schema | C++ |
|---|---|
| `rpc add(AddRequest) returns (AddResponse);` | `AddResponse add(const AddRequest&, protobus::CallContext&)` |
| `rpc tick(TickRequest) returns (stream Tick);` | `protobus::Generator<Tick> tick(const TickRequest&, protobus::CallContext&)` |

An rpc named like a C++ keyword or a member of the base classes (`delete`,
`init`, `close`, ...) gets a trailing underscore in C++ (`delete_`); on the
wire it keeps its name. Client streaming is not part of the protobus protocol,
and such rpcs are not generated.

## On the broker

A service named `Billing.Invoices` owns:

| Object | Purpose |
|---|---|
| `Billing.Invoices` | its request queue, bound to `REQUEST.Billing.Invoices.*` on `proto.bus`; every replica consumes from it |
| `Billing.Invoices.Retry`, `Billing.Invoices.Retry.Exchange` | the retry ladder (see [Errors](errors.md)) |
| `Billing.Invoices.DLQ` | requests whose retries are spent |
| `Billing.Invoices.Events` | the events the service subscribes to |

and one exclusive, auto-delete queue on `proto.bus.cancel` per process, which
hears stream cancellations.

## Ownership and lifecycle

A service is owned by a `std::shared_ptr`. Every delivery holds a reference
while its handler runs, so a service released mid-request is destroyed when
that request is done, never under a running handler. `init()` refuses a
service that is not shared-owned.

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include "Calculator.protobus.h"

class CalculatorService : public Calculator::ServiceBase {
 public:
  using ServiceBase::ServiceBase;
};

void run(protobus::Context& context) {
  // By hand:
  auto service = std::make_shared<CalculatorService>(context);
  service->init();  // declare the queues and start consuming
  // ...
  service->stopConsuming();                       // stop taking new work
  context.connection().drainInFlight(30000);      // let the work in hand finish
  service->close();                               // close the service's channels
}
```

`RunnableService::start<T>(context, options, postInit)` does the same and
adds the process lifecycle: on SIGINT or SIGTERM (or
`RunnableService::requestShutdown()`) it stops every started service, drains
in-flight work for up to `SHUTDOWN_DRAIN_TIMEOUT_MS`, calls each service's
`cleanup()`, and closes the connection. `RunnableService::wait()` blocks until
then and returns the exit code. If the process is still running
`SHUTDOWN_EXIT_GRACE_MS` after the shutdown, it is made to exit.

`cleanup()` never runs while a handler of that service's connection is still
running. When the drain deadline passes with handlers running, the connection
is closed (their messages go back to the queue) and:

- with the exit on (the default), the shutdown waits up to
  `SHUTDOWN_EXIT_GRACE_MS` more for them; if they finish, cleanup runs and
  `wait()` returns as usual, and if they do not, the process exits with the
  shutdown's code, without cleaning up beneath them and without returning to
  a `main()` whose teardown would destroy what they still use;
- with the exit off (`setExitAfterShutdown(false)`), `wait()` returns at once
  and the cleanup runs when the last of those handlers finishes, so keep the
  `Context` alive until then.

A `Context` must outlive every service built on it. Closing or destroying it
disconnects, then waits up to `SHUTDOWN_DRAIN_TIMEOUT_MS` for handlers still
running.

## Options

`protobus::MessageServiceOptions`, passed to the constructor:

| Field | Default | |
|---|---|---|
| `maxConcurrent` | 1 | Deliveries handled in parallel by this process: the queue's prefetch |
| `retry.maxRetries` | 3 | Retry hops before the DLQ; 0 disables retries |
| `retry.retryDelayMs` | 5000 | Delay between hops: the retry queue's `x-message-ttl` |
| `retry.messageTtlMs` | none | `x-message-ttl` on the request queue itself |
| `lateAck` | true | Acknowledge after the handler; `false` acknowledges on delivery (see below) |
| `processingTimeoutMs` | `MESSAGE_PROCESSING_TIMEOUT` | Cap on one attempt at a unary request |
| `maxPriority` | none | Declare the request queue as a priority queue |
| `eventRetry` | off | Retry for the service's event subscriptions (see [Events](events.md)) |
| `serializeHandlers` | false | Run the service's handlers (requests, stream steps, events) one at a time, so its state needs no locking (see [Threading](threading.md)) |

Queue arguments are fixed when a queue is first declared. Changing
`retryDelayMs` for a service that has run before fails its start with
`RetryQueueMismatchError`: drain and delete `<Service>.Retry`, or keep the
original delay. Likewise `maxPriority` and `messageTtlMs` on the request queue
need the queue to be recreated.

## The call context

Every handler receives a `protobus::CallContext`:

| Field | |
|---|---|
| `actor` | The caller's free-text identity, from the request envelope. Not authenticated |
| `correlationId` | This request's correlation id |
| `method` | The contract method, `<package>.<Service>.<method>` |
| `signal` | An `AbortSignal` that fires on the processing timeout and, for a stream, when the caller cancels |
| `routingKey` | The routing key the broker delivered on |
| `messageId` | Stable across redeliveries and retries: deduplicate on it |
| `redelivered` | The broker has delivered this message before |
| `headers` | The AMQP headers, read-only |

## Concurrency

Handlers run on worker threads, up to `maxConcurrent` at once per process, so
an implementation must be safe for concurrent use when `maxConcurrent` is
above 1, and its event handlers run on the event listener's own consumer,
concurrently with its rpcs. The worker pool grows as needed: a handler that
calls another service and waits for its reply does not starve the reply's own
delivery. `serializeHandlers` runs them one at a time instead; see
[Threading and lifetimes](threading.md).

## The processing timeout

A unary attempt that runs longer than `processingTimeoutMs` is failed: the
handler's `signal` fires, the delivery is settled as a failure (retried, then
dead-lettered, and the caller answered `PROCESSING_TIMEOUT` once retries are
spent), and the handler's eventual result is discarded. C++ cannot stop a
running function, so the handler itself runs on until it returns; a handler
doing long work should check `signal.aborted()`, or wait with
`signal.waitFor(duration)` instead of sleeping. Graceful shutdown waits for
such handlers too.

For a stream the timeout covers obtaining the generator, which returns at
once: a stream is bounded instead by the caller's idle timeout and by
cancellation, as in the TypeScript port.

## Early acknowledgement

`lateAck = false` acknowledges each request on delivery. That gives
at-most-once delivery: a request whose process dies mid-handler is lost, and
retries and dead-lettering are off. The caller is still answered when the
handler throws.

## Priority

`maxPriority` declares the request queue with `x-max-priority`, so a caller's
`CallOptions::priority` decides the order in which queued requests are taken.
`Config::RECOMMENDED_MAX_PRIORITY` (2) gives the three named levels
`PRIORITY_NORMAL`, `PRIORITY_HIGH` and `PRIORITY_CONTROL`; RabbitMQ keeps
structures per level, so keep the range small. Priority only reorders what is
still queued, so it needs late acknowledgement (the default): with
`lateAck = false` the broker pushes the whole backlog to the consumer, and the
constructor refuses the combination.

## Instance-named services

Several instances can serve one contract under names of their own. Override
`ServiceName()`:

<!-- doc-check: compile proto=examples/combat/proto -->
```cpp
#include "player.protobus.h"

class Player : public Combat::PlayerBase {
 public:
  Player(protobus::Context& context, std::string id) : PlayerBase(context), id_(std::move(id)) {}
  // Serves the contract Combat.Player under REQUEST.Combat.Player.<id>.*
  std::string ServiceName() const override { return "Combat.Player." + id_; }

 private:
  std::string id_;
};
```

The contract is found by trimming trailing segments off the name until one
names a service in a loaded schema. A proxy addresses an instance the same way:
`Combat::PlayerProxy player(context, "Combat.Player.player6")`. The envelope
carries the contract method (`Combat.Player.shoot`), which the service
validates; the routing key carries the instance (`REQUEST.Combat.Player.player6.shoot`).

## Without code generation

A service over schemas loaded at runtime derives from `MessageService`
directly and registers handlers over dynamic messages:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/protobus.h>

class DynamicCalculator : public protobus::MessageService {
 public:
  explicit DynamicCalculator(protobus::Context& ctx) : MessageService(ctx) {
    registerMethod("add", [this](const google::protobuf::Message& request, protobus::CallContext&) {
      const auto* d = request.GetDescriptor();
      const auto* r = request.GetReflection();
      const int sum = r->GetInt32(request, d->FindFieldByName("a")) + r->GetInt32(request, d->FindFieldByName("b"));
      auto response = context().factory().newMessage("Calculator.AddResponse");
      response->GetReflection()->SetInt32(response.get(), response->GetDescriptor()->FindFieldByName("result"), sum);
      return response;
    });
  }
  std::string ServiceName() const override { return "Calculator.Service"; }
};
```

The schema comes from `Context::init(url, {"./proto"})`, from code compiled in,
or from `ProtoFileName()`/`Proto()`, which a service overrides to supply its
own. `registerStreamingMethod` does the same for streams. An rpc the contract
declares but nothing registers answers `PROTOCOL_ERROR`.

## A service that calls itself

`protobus::ProxiedService<Proxy, Base>` gives a service a typed proxy to its
own name, created in `init()`:

<!-- doc-check: compile proto=examples/combat/proto -->
```cpp
#include "player.protobus.h"

class Player : public protobus::ProxiedService<Combat::PlayerProxy, Combat::PlayerBase> {
 public:
  using ProxiedService::ProxiedService;
  void poke() { proxy().getStatus({}); }
};
```

With `serializeHandlers` on, a handler must not wait on its own service: the
call could only run once the handler returns. It throws `std::logic_error`
instead of deadlocking; call the method directly.
