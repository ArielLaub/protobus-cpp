# ProtoBus for C++

**RabbitMQ-native microservices for C++, with Protocol Buffers on the wire.**

[![CI](https://github.com/ArielLaub/protobus-cpp/actions/workflows/ci.yml/badge.svg)](https://github.com/ArielLaub/protobus-cpp/actions/workflows/ci.yml)
[![C++](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus&logoColor=white)](https://en.cppreference.com/w/cpp/20)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-%E2%89%A53.8-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com)

Define a service in a `.proto` file, implement the class protobus generates for
it, and call it from anywhere on the bus as if it were local. ProtoBus turns
each service into **one durable RabbitMQ queue with N processes competing for
it**, so load balancing, failover, backpressure, retries and dead-lettering are
the broker's job, not your program's.

This is the C++ port of [protobus](https://github.com/ArielLaub/protobus)
(TypeScript), [protobus-py](https://github.com/ArielLaub/protobus-py) (Python)
and [protobus-go](https://github.com/ArielLaub/protobus-go) (Go), designed
after the TypeScript reference class for class. The four are
**wire-compatible**: a C++ service serves TypeScript, Python and Go callers and
the other way round, with streaming, events, custom types and error codes
included. See [Compatibility](docs/compatibility.md).

**Status: new.** 2.0.0 is the first release of the C++ port. It is at feature
parity with the TypeScript port, and its CI runs it against the TypeScript,
Python and Go ports' real libraries, in both directions, on RabbitMQ.

---

## Install

You need a C++20 compiler with coroutine support, CMake 3.24+, and three
libraries: [protobuf](https://github.com/protocolbuffers/protobuf) (with
`protoc`), [rabbitmq-c](https://github.com/alanxz/rabbitmq-c) and, for the test
suites, GoogleTest. CI builds with GCC 13 against Ubuntu 24.04's packages
(protobuf 3.21, rabbitmq-c 0.11) and with Apple Clang against Homebrew's
current releases.

```bash
# Debian / Ubuntu
sudo apt-get install g++ cmake ninja-build libprotobuf-dev libprotoc-dev protobuf-compiler librabbitmq-dev
# macOS
brew install cmake ninja protobuf rabbitmq-c
```

Build and install the library, the `protobus-cpp` code generator and its CMake
package:

```bash
git clone https://github.com/ArielLaub/protobus-cpp.git && cd protobus-cpp
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DPROTOBUS_BUILD_TESTS=OFF
cmake --build build && sudo cmake --install build
```

Or vendor it with `add_subdirectory(protobus-cpp)` or `FetchContent`; the
`protobus::protobus` target and `protobus_generate()` work the same way.

You also need a RabbitMQ 3.8+ broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

---

## Quick start

Four steps to a working RPC. The code is the
[`examples/calculator`](examples/calculator) example, trimmed.

### 1. Describe the service

```protobuf
// proto/Calculator.proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
}

message AddResponse {
  int32 result = 1;
}

message DivideRequest {
  double dividend = 1;
  double divisor = 2;
}

message DivideResponse {
  double quotient = 1;
}
```

The package plus the service name is the service's name on the bus:
`Calculator.Service`. The file is an ordinary protobus schema, shared as-is
with TypeScript, Python and Go. It needs no C++ options.

### 2. Generate the C++ code

```cmake
# CMakeLists.txt
cmake_minimum_required(VERSION 3.24)
project(app LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)

find_package(protobus CONFIG REQUIRED)

add_executable(calculator services/calculator/main.cc)
protobus_generate(TARGET calculator PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
target_link_libraries(calculator PRIVATE protobus::protobus)
```

`protobus_generate()` runs the `protobus-cpp` CLI over every `.proto` in the
directory at build time. Each package becomes a C++ namespace (`package
Calculator` becomes `Calculator::`), holding protoc's message classes plus:

- `ServiceBase`, the class your implementation derives from, with a virtual
  method per rpc;
- `ServiceProxy`, a typed client.

### 3. Implement and run the service

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
// services/calculator/main.cc
#include <cstdlib>

#include "Calculator.protobus.h"

class CalculatorService : public Calculator::ServiceBase {
 public:
  using ServiceBase::ServiceBase;

  Calculator::AddResponse add(const Calculator::AddRequest& request, protobus::CallContext&) override {
    Calculator::AddResponse response;
    response.set_result(request.a() + request.b());
    return response;
  }

  Calculator::DivideResponse divide(const Calculator::DivideRequest& request, protobus::CallContext&) override {
    if (request.divisor() == 0) {
      // A HandledError is an answer, not a failure: never retried.
      throw protobus::HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
    }
    Calculator::DivideResponse response;
    response.set_quotient(request.dividend() / request.divisor());
    return response;
  }
};

int main() {
  protobus::Context context;
  context.init(std::getenv("AMQP_URL"));

  protobus::MessageServiceOptions options;
  options.maxConcurrent = 8;
  protobus::RunnableService::start<CalculatorService>(context, options);
  // Serves until SIGINT/SIGTERM, then drains in-flight work and closes.
  return protobus::RunnableService::wait();
}
```

Every handler receives a `protobus::CallContext` with the caller's actor, the
message id to deduplicate on, and an `AbortSignal` that fires when the
processing timeout expires. An rpc you do not override answers
`PROTOCOL_ERROR`.

`protobus-cpp generate:service Calculator` writes a skeleton like this one for
you.

### 4. Call it

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
// cmd/client/main.cc
#include <cstdlib>
#include <iostream>

#include "Calculator.protobus.h"

int main() {
  protobus::Context context;
  context.init(std::getenv("AMQP_URL"));

  Calculator::ServiceProxy calculator(context);
  calculator.init();

  Calculator::AddRequest request;
  request.set_a(5);
  request.set_b(3);
  protobus::CallOptions options;
  options.timeoutMs = 10000;
  std::cout << "5 + 3 = " << calculator.add(request, options).result() << std::endl;
}
```

```
$ ./build/calculator &
$ ./build/client
5 + 3 = 8
```

Without `timeoutMs` the call is bounded by `RPC_CALL_TIMEOUT_MS` (10 minutes
by default, as in the other ports).

The full walkthrough adds events, error handling and a unit test:
**[Getting Started](docs/getting-started.md)**.

---

## Why ProtoBus

### RabbitMQ only, on purpose

ProtoBus is built for one broker, so the things a broker is good at stay in the
broker instead of being reimplemented above it:

| Concern | Where it lives |
|---|---|
| Load balancing | competing consumers on one queue |
| Routing | topic exchange bindings (`REQUEST.<Service>.*`) |
| Redelivery on consumer loss | late ack: an unacked delivery returns to the queue |
| Retry delay | the retry queue's `x-message-ttl`, drained by a dead-letter exchange |
| Persistence | durable queues, persistent messages |
| Dead letters | a real `<Service>.DLQ` |
| Priority | native queue priorities |

A request goes publisher → exchange → queue → consumer. Nothing tracks live
instances, so nothing holds a stale one, and a consumer that dies mid-request
leaves its delivery unacked for the next consumer to take.

If you may need to swap RabbitMQ for another broker, use a transport-agnostic
framework instead. That is a real feature and protobus does not have it.

### Protocol Buffers, not JSON

- **Contract-first.** The `.proto` file is the interface between teams and
  languages, and the generated C++ types fail the build when the two drift
  apart.
- **Versioning by field number.** Adding a field does not break an old peer.

### A small protocol, a small port

The protocol protobus adds on top of AMQP is small and documented: five
envelope messages, a routing-key convention, an error encoding and two
streaming headers. Because queueing, consumer distribution and retry delays
belong to the broker, a port adapts that protocol to another AMQP client
rather than reimplementing messaging. This port depends on
[rabbitmq-c](https://github.com/alanxz/rabbitmq-c) for AMQP and
[protobuf](https://github.com/protocolbuffers/protobuf) for the wire, and
nothing else at runtime.

---

## Features

- **Retries and dead-lettering.** An unhandled exception or a processing
  timeout parks the request on `<Service>.Retry` and redelivers it; after
  `maxRetries` (3 by default, 5 seconds apart) it lands on `<Service>.DLQ`,
  and the caller is answered with the final error. A `HandledError` is an
  answer and is never retried. See [Errors](docs/errors.md).
- **Server streaming.** A method declared `returns (stream T)` is implemented
  as a C++20 coroutine returning `protobus::Generator<T>` (each `co_yield` is a
  chunk) and consumed with a range-for loop. Leaving the loop, destroying the
  stream or firing an `AbortSignal` stops the producer on the server. See
  [Streaming](docs/streaming.md).
- **Events.** `publishEvent` publishes on a topic exchange; `subscribe`
  receives events by message type, topic pattern (`*`, `#`) or both, typed or
  dynamic, with optional per-listener retries and a dead-letter queue. See
  [Events](docs/events.md).
- **Custom types.** `bigint` (unsigned 256-bit) and `timestamp` (milliseconds
  since the epoch) are built in and need no import in the schema; they appear
  in C++ as `::bigint` and `::timestamp`, with `protobus::Uint256` and
  `std::chrono` helpers. Declare your own with `--custom-type`. See
  [Code generation](docs/codegen.md).
- **Priority.** `maxPriority` makes a service queue a priority queue, and
  `CallOptions::priority` lets a control message overtake a bulk backlog.
- **Instance-named services.** Many instances can serve one contract under
  their own names (`Combat.Player.player6`), each addressed by its own proxy.
- **Processing timeout.** `processingTimeoutMs` caps one attempt at a unary
  request; the handler's signal fires and the attempt counts as failed.
- **Reconnection.** A lost connection is re-established with capped
  exponential backoff, and every service, listener and dispatcher is restored
  before the connection reports itself ready. Calls made meanwhile wait for it;
  calls in flight when it dropped fail with `DisconnectedError`.
- **Publisher confirms.** Every publish waits for the broker's confirm. A
  failure is a `PublishError` that says whether the outcome is ambiguous, and
  `CallOptions::messageId` makes a republish safe to deduplicate.
- **Dynamic API.** `MessageFactory` loads `.proto` files at runtime;
  `ServiceProxy::call`, `ServiceProxy::callStream` and
  `MessageService::registerMethod` serve and call them with no generated code.
- **An in-memory broker.** `protobus::testing::MemoryBroker` runs services and
  clients in-process, with retries, dead-lettering, priorities and connection
  loss modelled. See [Testing](docs/testing.md).
- **Graceful shutdown.** `RunnableService` stops intake on SIGINT/SIGTERM, lets
  in-flight work finish within `SHUTDOWN_DRAIN_TIMEOUT_MS`, runs your
  `cleanup()` (never beneath a handler still running), then closes.

### A short tour

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <iostream>

#include "Calculator.protobus.h"

void tour(protobus::Context& context) {
  // Options on a service: concurrency, retries, a processing timeout and a
  // priority queue.
  protobus::MessageServiceOptions options;
  options.maxConcurrent = 16;
  options.retry.maxRetries = 5;
  options.retry.retryDelayMs = 10000;
  options.processingTimeoutMs = 30000;
  options.maxPriority = protobus::Config::RECOMMENDED_MAX_PRIORITY;

  // Options on a call.
  Calculator::ServiceProxy calculator(context);
  calculator.init();
  Calculator::AddRequest request;
  protobus::CallOptions call;
  call.priority = protobus::Config::PRIORITY_HIGH;
  call.timeoutMs = 5000;
  call.actor = "billing-job";
  calculator.add(request, call);

  // Events: subscribe by type, publish from anywhere on the bus.
  auto listener = std::make_shared<protobus::EventListener>(context.connectionPtr(), context.factoryPtr());
  listener->init(nullptr, "");
  listener->subscribe<Calculator::Calculated>(
      [](const Calculator::Calculated& event, const std::string& type, const std::string& topic) {
        std::cout << event.operation() << std::endl;
      });
  listener->start();

  Calculator::Calculated event;
  event.set_operation("add");
  *event.mutable_at() = protobus::makeTimestamp(std::chrono::system_clock::now());
  context.publishEvent(event);
}
```

<!-- doc-check: compile proto=examples/tokenstream/proto -->
```cpp
#include <iostream>

#include "chat.protobus.h"

// Streaming, server side: a coroutine; each co_yield is one chunk.
class Assistant : public Chat::AssistantBase {
 public:
  using AssistantBase::AssistantBase;

  protobus::Generator<Chat::Token> generate(const Chat::GenerateRequest& request,
                                            protobus::CallContext& context) override {
    int i = 0;
    for (const char* word : {"hello", "from", "c++"}) {
      if (context.signal.aborted()) co_return;  // the caller has gone: stop producing
      Chat::Token token;
      token.set_index(i++);
      token.set_text(word);
      co_yield token;
    }
  }
};

// Client side: a range-for loop over the generated method's stream.
void read(Chat::AssistantProxy& assistant) {
  Chat::GenerateRequest request;
  request.set_prompt("hi");
  for (const auto& token : assistant.generate(request)) {
    std::cout << token.text() << " ";
    if (token.index() == 1) break;  // tells the server to stop producing
  }
}
```

---

## Concurrency

Each unacknowledged delivery runs on a worker thread of the connection's own,
so handlers run truly in parallel and must be safe for concurrent use. The
number in flight is bounded by the consumer's prefetch: `maxConcurrent` for a
service (default 1, one request at a time), `DEFAULT_PREFETCH` for event
handling (also 1). A streaming handler holds its slot for the life of its
stream. The worker pool grows as needed, so a handler that calls another
service and waits for its reply cannot starve the reply's own delivery.

A `Context`, its proxies and its dispatchers are safe for concurrent use:
create one context per process and share it. A service is owned by a
`std::shared_ptr` (`std::make_shared`, or `RunnableService::start`), and a
`Context` outlives every service and proxy built on it. See
[Architecture](docs/architecture.md).

---

## Wire compatibility

protobus-cpp speaks the protobus wire protocol exactly as TypeScript protobus
2.5, protobus-py 2.0 and protobus-go 2.0 do: the same exchanges, queues,
envelopes (byte for byte), headers and error codes, and the same environment
variables for configuration. Replicas of one service in different languages
can share its queue and climb one retry ladder together. The
[cross-language suite](crosslang/README.md) runs C++ against the other ports'
real libraries over a real broker, in both directions.

The type mapping, the topology and the few deliberate behavioural differences
are in **[Compatibility](docs/compatibility.md)**.

---

## Documentation

Full index: **[docs/](docs/README.md)**

| Start | |
|---|---|
| [Getting Started](docs/getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](docs/services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](docs/clients.md) | Calling services: options, timeouts, errors, the dynamic proxy |
| [Streaming](docs/streaming.md) | Server streaming and cancellation |
| [Events](docs/events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](docs/errors.md) | The error model, retries and dead letters |

| Reference | |
|---|---|
| [Configuration](docs/configuration.md) | Environment variables and reconnection |
| [Code generation](docs/codegen.md) | The CLI, the protoc plugin, `protobus_generate()`, custom types |
| [Testing](docs/testing.md) | The in-memory broker and the suites |
| [Compatibility](docs/compatibility.md) | The wire contract and how the ports differ |
| [Architecture](docs/architecture.md) | Threads, ownership and reconnection |
| [Security](docs/security.md) | Dispatch checks, error exposure, logging |

---

## Examples

| Example | Shows |
|---|---|
| [`examples/calculator`](examples/calculator) | A service, a client, a handled error and an event |
| [`examples/tokenstream`](examples/tokenstream) | Streaming tokens, cancelled three ways, with the server's own count |
| [`examples/combat`](examples/combat) | Six instances of one service playing a game over RPC and events |

```bash
docker compose up -d --wait   # RabbitMQ on 127.0.0.1:25672
cmake -S . -B build -G Ninja && cmake --build build
AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ ./build/examples/calculator
```

---

## License

MIT. See [LICENSE](LICENSE).
