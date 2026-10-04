# Getting started

From an empty directory to a service, a client, an event and a unit test. It
assumes protobus-cpp is installed (see the [README](../README.md#install)) and
a broker is running:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

## 1. Lay out the project

```
app/
├── CMakeLists.txt
├── proto/
│   └── Calculator.proto
├── services/calculator/main.cc
├── cmd/client/main.cc
└── tests/calculator_test.cc
```

## 2. Write the schema

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

// Published after every successful calculation.
message Calculated {
  string operation = 1;
  timestamp at = 2;
}
```

`timestamp` is a protobus custom type: milliseconds since the epoch, shared by
every port. It needs no import.

## 3. Build it

```cmake
cmake_minimum_required(VERSION 3.24)
project(app LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)

find_package(protobus CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)

add_library(calculator_proto STATIC)
protobus_generate(TARGET calculator_proto PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
target_link_libraries(calculator_proto PUBLIC protobus::protobus)

add_executable(calculator services/calculator/main.cc)
target_link_libraries(calculator PRIVATE calculator_proto)

add_executable(client cmd/client/main.cc)
target_link_libraries(client PRIVATE calculator_proto)

add_executable(calculator_test tests/calculator_test.cc)
target_link_libraries(calculator_test PRIVATE calculator_proto GTest::gtest_main)
```

Generating into a library target once and linking it everywhere keeps one copy
of the generated code. `protobus_generate()` puts `Calculator.pb.h` and
`Calculator.protobus.h` on the target's include path.

## 4. The service

The service class goes in a header so the test can use it too.

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
// services/calculator/calculator_service.h
#pragma once

#include <chrono>

#include "Calculator.protobus.h"

class CalculatorService : public Calculator::ServiceBase {
 public:
  using ServiceBase::ServiceBase;

  Calculator::AddResponse add(const Calculator::AddRequest& request, protobus::CallContext&) override {
    announce("add");
    Calculator::AddResponse response;
    response.set_result(request.a() + request.b());
    return response;
  }

  Calculator::DivideResponse divide(const Calculator::DivideRequest& request, protobus::CallContext&) override {
    if (request.divisor() == 0) {
      // Answered at once with this code, and never retried.
      throw protobus::HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
    }
    announce("divide");
    Calculator::DivideResponse response;
    response.set_quotient(request.dividend() / request.divisor());
    return response;
  }

 private:
  void announce(const std::string& operation) {
    Calculator::Calculated event;
    event.set_operation(operation);
    *event.mutable_at() = protobus::makeTimestamp(std::chrono::system_clock::now());
    // Published on EVENT.Calculator.Calculated.
    publishEvent(event);
  }
};
```

```cpp
// services/calculator/main.cc
#include <cstdlib>

#include "calculator_service.h"

int main() {
  protobus::Context context;
  context.init(std::getenv("AMQP_URL"));
  auto service = protobus::RunnableService::start<CalculatorService>(context);
  return protobus::RunnableService::wait();
}
```

`RunnableService::start` constructs the service, declares its queues and
starts consuming. `wait()` returns once SIGINT or SIGTERM has shut it down
gracefully: it stops taking requests, lets the ones in hand finish, calls the
service's `cleanup()`, and closes the connection.

On the broker the service now has `Calculator.Service` (bound to
`REQUEST.Calculator.Service.*` on `proto.bus`), `Calculator.Service.Retry`,
`Calculator.Service.DLQ` and `Calculator.Service.Events`.

## 5. The client

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

  Calculator::AddRequest add;
  add.set_a(20);
  add.set_b(22);
  std::cout << "20 + 22 = " << calculator.add(add).result() << std::endl;

  Calculator::DivideRequest divide;
  divide.set_dividend(1);
  try {
    calculator.divide(divide);
  } catch (const protobus::RemoteError& e) {
    // The service's HandledError, with its code.
    std::cout << e.what() << " (" << e.code() << ")" << std::endl;
  }
}
```

A proxy is safe for concurrent use: make one and share it.

## 6. Hear the events

Any process can subscribe. In the service itself:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <iostream>

#include "Calculator.protobus.h"

void listen(protobus::MessageService& service) {
  service.subscribeEvent<Calculator::Calculated>(
      [](const Calculator::Calculated& event, const std::string& type, const std::string& topic) {
        std::cout << event.operation() << " at " << protobus::toMillis(event.at()) << std::endl;
      });
}
```

Call it after `init()`, for instance from the `postInit` argument of
`RunnableService::start`. The subscription binds the service's
`Calculator.Service.Events` queue to `EVENT.Calculator.Calculated`. See
[Events](events.md).

## 7. Test it without a broker

`protobus::testing::MemoryBroker` is an in-memory broker with RabbitMQ's
behaviour, so the service and a client run in one process:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
// tests/calculator_test.cc
#include <gtest/gtest.h>

#include <protobus/testing/memory_broker.h>

#include "Calculator.protobus.h"

class CalculatorService : public Calculator::ServiceBase {
 public:
  using ServiceBase::ServiceBase;
  Calculator::AddResponse add(const Calculator::AddRequest& request, protobus::CallContext&) override {
    Calculator::AddResponse response;
    response.set_result(request.a() + request.b());
    return response;
  }
};

TEST(Calculator, Adds) {
  auto broker = protobus::testing::MemoryBroker::create();
  protobus::Context context(broker);
  context.init("amqp://memory/");

  auto service = std::make_shared<CalculatorService>(context);
  service->init();

  Calculator::ServiceProxy calculator(context);
  calculator.init();
  Calculator::AddRequest request;
  request.set_a(2);
  request.set_b(3);
  EXPECT_EQ(calculator.add(request).result(), 5);

  // Not overridden: answered PROTOCOL_ERROR.
  try {
    calculator.divide({});
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROTOCOL_ERROR");
  }
}
```

The broker models retries, dead-lettering and connection loss too. See
[Testing](testing.md).

## Next

- [Services](services.md): concurrency, retries, timeouts, priority, instance
  names.
- [Streaming](streaming.md): methods that return a stream.
- [Errors](errors.md): what callers see, and what is retried.
