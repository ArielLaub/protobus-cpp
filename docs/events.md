# Events

An event is a protobuf message published on the `proto.bus.events` topic
exchange. Every queue bound to its topic receives a copy, so one publish
reaches every subscribing service, each through its own queue, and replicas
of one service share their copy as they share requests.

## Publishing

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <chrono>

#include "Calculator.protobus.h"

void publish(protobus::Context& context) {
  Calculator::Calculated event;
  event.set_operation("add");
  *event.mutable_at() = protobus::makeTimestamp(std::chrono::system_clock::now());

  context.publishEvent(event);                            // topic EVENT.Calculator.Calculated
  context.publishEvent(event, "EVENT.calculator.audit");  // a topic of your own
}
```

The type is the message's full name; `publishEvent(type, message, topic)`
names it explicitly and refuses a message of another type. The call returns
once the broker has confirmed the event. An event with no subscriber is
normal: events are not published `mandatory`, so nothing fails.

A service publishes the same way with `publishEvent(...)`.

## Subscribing

A service subscribes through its own events queue, `<Service>.Events`, after
`init()`:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <iostream>

#include "Calculator.protobus.h"

void subscribe(protobus::MessageService& service) {
  // Typed: the event type selects the default topic, EVENT.Calculator.Calculated.
  service.subscribeEvent<Calculator::Calculated>(
      [](const Calculator::Calculated& event, const std::string& type, const std::string& topic) {
        std::cout << event.operation() << std::endl;
      });

  // A topic pattern of your own.
  service.subscribeEvent<Calculator::Calculated>(
      [](const Calculator::Calculated& event, const std::string& type, const std::string& topic) {},
      "EVENT.calculator.*");

  // Dynamic: the handler receives a message of the factory's type.
  service.subscribeEvent(
      "Calculator.Calculated",
      [](const google::protobuf::Message& event, const std::string& type, const std::string& topic) {});
}
```

Any process can also subscribe with an `EventListener` of its own:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include "Calculator.protobus.h"

std::shared_ptr<protobus::EventListener> audit(protobus::Context& context) {
  auto listener = std::make_shared<protobus::EventListener>(context.connectionPtr(), context.factoryPtr());
  // A named queue survives restarts and is shared by replicas; "" declares an
  // exclusive, auto-delete queue for this process alone.
  listener->init(nullptr, "Audit.Events");
  listener->subscribeAll([](const google::protobuf::Message& event, const std::string& type,
                            const std::string& topic) {});
  listener->start();
  return listener;
}
```

### Topics

Topics are dot-separated words. A pattern matches with `*` for exactly one
word and `#` for zero or more:

| Pattern | Matches | Does not match |
|---|---|---|
| `ORDERS.*.CREATED` | `ORDERS.US.CREATED` | `ORDERS.US.123.CREATED` |
| `ORDERS.#` | `ORDERS`, `ORDERS.US.123.CREATED` | `INVOICES.X` |
| `ORDERS.US.*.SHIPPED` | `ORDERS.US.123.SHIPPED` | `ORDERS.EU.456.SHIPPED` |

The broker matches a pattern to deliver an event; the listener then matches
the routing key the broker delivered on, not the topic the publisher wrote
into the event, to pick handlers. The body is the publisher's to write, and
trusting it would let a publisher reach handlers its routing key never could.
`subscribeAll`'s handler runs first, then every handler whose pattern
matches, each once, one after another.

A typed handler receiving an event of another type (two types published on
one topic) fails with `InvalidMessageError`.

### Concurrency

A listener handles `DEFAULT_PREFETCH` (1) events at a time, on its own
consumer, concurrently with the service's requests.

## When a handler fails

By default, a handler that throws loses its event: the delivery is rejected
without requeue. That is deliberate: one event that always fails cannot then
stall the listener behind its own prefetch.

`MessageServiceOptions::eventRetry` (or the `EventListener` constructor's
`EventRetryOptions`) gives events the ladder requests climb:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/protobus.h>

protobus::MessageServiceOptions withEventRetry() {
  protobus::MessageServiceOptions options;
  options.eventRetry.maxRetries = 3;
  options.eventRetry.retryDelayMs = 5000;
  return options;
}
```

A failing event then waits on `<Service>.Events.Retry` and comes back, up to
`maxRetries` times, then lands on `<Service>.Events.DLQ` with the same
metadata headers as a request (see [Errors](errors.md)). The expired event
returns through `<Service>.Events.Redelivery`, an exchange bound only to this
listener's queue, so a retry reaches this subscriber and not every other one.
A retried event runs every handler that matched it again, including the ones
that succeeded the first time.

A `HandledError` thrown from an event handler is not retried: the event is
dropped.

Retry needs a named queue: an anonymous one disappears with the connection.
