# Changelog

## 2.0.0 (unreleased)

The first release of the C++ port of protobus, wire-compatible with
TypeScript protobus 2.5, protobus-py 2.0 and protobus-go 2.0. Its major
version follows the protocol generation the other ports share.

- The TypeScript port's design, class for class: `Context`, `Connection`,
  `MessageFactory`, the dispatchers and listeners, `MessageService`,
  `RunnableService`, `ProxiedService` and `ServiceProxy`.
- Services generated from `.proto` files: a `<Service>Base` class with a
  virtual method per rpc, and a typed `<Service>Proxy`. Server streams are
  C++20 coroutines (`protobus::Generator<T>`), consumed with range-for.
- Retries through `<Service>.Retry` and dead-lettering to `<Service>.DLQ`,
  with the metadata headers every port writes; the caller is answered once
  retries are spent, including after a processing timeout.
- Events with topic patterns, typed and dynamic subscriptions, and opt-in
  event retry.
- The custom types `bigint` (with `protobus::Uint256`) and `timestamp`, plus
  user-declared custom types.
- Publisher confirms on every publish, with ambiguous outcomes reported as
  such, a bound on publishes the broker has not answered (held through
  ambiguous timeouts, retiring a channel that stops confirming) and on those
  queued behind it, mandatory returns matched to the exact publish, and
  caller-supplied message ids.
- Unary calls bounded end to end by their deadline, confirm included; closing
  a context fails its pending calls and streams at once.
- Reconnection with coordinated restoration of every component; a channel
  lost on a live connection is rebuilt, and a consumer the broker cancels is
  restored.
- Priority queues, instance-named services, early acknowledgement, graceful
  shutdown and structured logging.
- Thread-safety guard rails: opt-in serialized handlers
  (`MessageServiceOptions::serializeHandlers`), blocking calls refused with
  `std::logic_error` on threads that must not block, and a `Context`
  destroyed under running handlers reported (and aborted on, in debug
  builds). See docs/threading.md.
- The `protobus-cpp` CLI, the `protoc-gen-protobus-cpp` plugin and
  `protobus_generate()` for CMake.
- `protobus::testing::MemoryBroker`, an in-memory broker for tests.
- Builds on the protobuf and rabbitmq-c that Debian and Ubuntu ship (3.21 and
  0.11) as well as current releases.
- Suites: unit (in-memory broker), integration (RabbitMQ 3 and 4) and
  cross-language (against the TypeScript, Python and Go ports, both
  directions), plus sanitizer builds and compiled documentation snippets.
