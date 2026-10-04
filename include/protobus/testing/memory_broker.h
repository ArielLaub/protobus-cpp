// An in-memory AMQP broker, for testing services and clients without
// RabbitMQ.
//
//   auto broker = protobus::testing::MemoryBroker::create();
//   protobus::Context ctx({.transport = broker});
//   ctx.init("amqp://memory/", {});
//
// It implements the RabbitMQ behaviour protobus depends on: direct, topic and
// fanout exchanges and the default exchange; durable, exclusive and
// server-named queues; per-queue message TTL with dead-lettering; priority
// queues; per-consumer prefetch with acknowledgement, rejection and
// redelivery; publisher confirms with mandatory returns; and channel-closing
// errors for a missing exchange (404) or a redeclaration with different
// arguments (406).
//
// Callbacks run on a dispatcher thread of the broker's own, as a real client's
// I/O thread would run them, and a blocking call made from one throws
// std::logic_error: code that would deadlock against RabbitMQ fails here too.
//
// Fault injection: killConnections() drops every connection as a network
// failure would, refuseConnections() makes connect() fail, and setConfirmMode()
// makes the broker nack or never confirm publishes.
#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "protobus/transport.h"

namespace protobus::testing {

class MemoryBroker : public amqp::Transport, public std::enable_shared_from_this<MemoryBroker> {
 public:
  enum class ConfirmMode {
    Ack,   // confirm every publish (the default)
    Nack,  // refuse every publish
    Drop,  // never confirm: publishes wait out their confirm timeout
  };

  static std::shared_ptr<MemoryBroker> create();
  ~MemoryBroker() override;

  std::shared_ptr<amqp::Connection> connect(const std::string& url, int heartbeatSeconds) override;

  // Drop every open connection as a lost socket would: unacknowledged
  // deliveries are requeued, exclusive queues deleted, and each connection's
  // close callbacks receive `reason`.
  void killConnections(const std::string& reason = "connection reset by peer");
  // While set, connect() throws AmqpError.
  void refuseConnections(bool refuse);
  void setConfirmMode(ConfirmMode mode);

  // Close every channel that has `queue` declared or consumed, as a broker
  // would close a channel over a consumer error. Used to test recovery from a
  // channel lost on a live connection.
  void closeChannelsConsuming(const std::string& queue, const std::string& reason = "541 INTERNAL_ERROR");

  // Inspection.
  bool queueExists(const std::string& name) const;
  bool exchangeExists(const std::string& name) const;
  size_t queueDepth(const std::string& name) const;
  size_t unackedCount(const std::string& name) const;
  size_t consumerCount(const std::string& name) const;
  std::optional<amqp::FieldTable> queueArguments(const std::string& name) const;
  // The messages waiting in a queue, in delivery order, without removing them.
  std::vector<amqp::Delivery> peek(const std::string& name) const;
  // The routing keys bound from `exchange` to `queue`.
  std::vector<std::string> bindings(const std::string& queue, const std::string& exchange) const;
  size_t openConnections() const;
  size_t connectAttempts() const;

  // Block until every callback queued so far has run.
  void flush();

  // Wait until `predicate` holds, polling, up to `timeout`.
  template <typename P>
  bool waitFor(P predicate, std::chrono::milliseconds timeout = std::chrono::seconds(5)) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
      if (std::chrono::steady_clock::now() >= deadline) return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return true;
  }

  struct Impl;

 private:
  MemoryBroker();
  std::shared_ptr<Impl> impl_;
};

}  // namespace protobus::testing
