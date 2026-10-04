# Testing

## Your services, without a broker

`protobus::testing::MemoryBroker` is an in-memory AMQP broker. A `Context`
built on it runs services and clients in one process, with no RabbitMQ:

<!-- doc-check: compile proto=examples/calculator/proto -->
```cpp
#include <protobus/testing/memory_broker.h>

#include "Calculator.protobus.h"

void example() {
  auto broker = protobus::testing::MemoryBroker::create();
  protobus::Context context(broker);
  context.init("amqp://memory/");
  // services and proxies as usual
}
```

It implements the RabbitMQ behaviour protobus relies on: direct, topic,
fanout and default exchanges; durable, exclusive and server-named queues;
per-queue TTL with dead-lettering; priority queues; per-consumer prefetch,
acknowledgement, rejection and redelivery; publisher confirms with mandatory
returns; and the channel-closing errors for a missing exchange (404) and a
redeclaration with different arguments (406). So retries, dead letters and
priorities behave as on a real broker, only faster.

It also injects faults and exposes its state:

| | |
|---|---|
| `killConnections()` | Drop every connection as a lost socket would: unacknowledged deliveries are requeued, and the context reconnects |
| `refuseConnections(bool)` | Make reconnection attempts fail |
| `setConfirmMode(Nack \| Drop)` | Refuse publishes, or never confirm them |
| `closeChannelsConsuming(queue)` | Close a consumer's channel on a live connection |
| `queueDepth`, `peek`, `unackedCount`, `consumerCount`, `queueArguments`, `bindings` | Inspect queues |
| `flush()` | Wait for every callback queued so far |

Callbacks run on a thread of the broker's own, as rabbitmq-c's I/O thread runs
them, and a blocking call made from one throws `std::logic_error`, so code
that would deadlock against RabbitMQ fails in a test too.

## The suites

```bash
cmake -S . -B build -G Ninja && cmake --build build
ctest --test-dir build -L unit            # no broker needed
```

| Suite | | Needs |
|---|---|---|
| `tests/unit` | The library on the in-memory broker: services, streams, events, reconnection, the wire format against TypeScript's bytes, the code generator (its output is compiled), the documentation's snippets | nothing |
| `tests/integration` | The same against RabbitMQ through rabbitmq-c, one virtual host per test | a broker |
| `crosslang` | C++ against the TypeScript, Python and Go ports, both directions. See [crosslang/README.md](../crosslang/README.md) | a broker and the sibling checkouts |

The broker suites never default to a broker: `localhost:5672` is often a
port-forward to a shared cluster, and these suites declare and delete queues.
Point them at a throwaway one:

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
ctest --test-dir build -L integration
./build/crosslang/protobus_crosslang_tests
```

Each test creates its own virtual host through the management API. Connection
loss is simulated by closing connections broker-side through the same API:
stopping a container does not reliably break a client's socket behind Docker
Desktop's port forwarding.

Sanitizer builds: `-DPROTOBUS_SANITIZE=ON` (AddressSanitizer, UBSan, leaks)
and `-DPROTOBUS_TSAN=ON`. Each needs a protobuf whose struct layout does not
change under the sanitizer: CI runs AddressSanitizer against Ubuntu's protobuf
3.21 and ThreadSanitizer against Homebrew's current release.
