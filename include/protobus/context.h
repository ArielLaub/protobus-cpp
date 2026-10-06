// Context: one process's place on the bus. It owns the connection, the
// message factory and the two dispatchers, and is what services and proxies
// are built on.
//
//   protobus::Context ctx;
//   ctx.init("amqp://guest:guest@localhost:5672/", {"./proto"});
//
// A Context is not copyable, and outlives every service and proxy built on
// it, and every handler still running on it. Destroying it closes the
// connection, then waits (up to SHUTDOWN_DRAIN_TIMEOUT_MS) for handlers still
// running. If some still run after that, it logs an error naming the mistake,
// and a debug build (NDEBUG unset) aborts there rather than let them touch
// freed memory.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "protobus/call_options.h"
#include "protobus/connection.h"
#include "protobus/event_dispatcher.h"
#include "protobus/message_dispatcher.h"
#include "protobus/message_factory.h"
#include "protobus/stream.h"

namespace protobus {

struct ContextOptions {
  ReconnectionOptions reconnection;
};

class Context {
 public:
  // `transport` defaults to RabbitMQ (rabbitmq-c). Pass a
  // testing::MemoryBroker to run without a broker.
  explicit Context(std::shared_ptr<amqp::Transport> transport = nullptr);
  ~Context();

  Context(const Context&) = delete;
  Context& operator=(const Context&) = delete;

  // Load the schemas under `protoLocations` (none is fine when everything is
  // compiled in), connect, and start the dispatchers.
  void init(const std::string& amqpUrl, const std::vector<std::string>& protoLocations = {},
            const ContextOptions& options = {});

  // Close the dispatchers and the connection, then wait for running handlers
  // to finish, up to SHUTDOWN_DRAIN_TIMEOUT_MS. Safe to call more than once.
  void close();

  bool isConnected() const;
  bool isReconnecting() const;

  // Publish an encoded RequestContainer under `routingKey` and return the raw
  // reply (empty when options.rpc is false).
  std::string publishMessage(const std::string& content, const std::string& routingKey,
                             const CallOptions& options = {});
  // Publish an encoded RequestContainer expecting a streaming reply.
  ChunkStream publishStreamingMessage(const std::string& content, const std::string& routingKey,
                                      const StreamOptions& options = {});
  // Publish an event. The type defaults to the message's own full name.
  void publishEvent(const std::string& type, const google::protobuf::Message& content,
                    const std::string& topic = "");
  void publishEvent(const google::protobuf::Message& content, const std::string& topic = "");

  MessageFactory& factory() { return *factory_; }
  const std::shared_ptr<MessageFactory>& factoryPtr() const { return factory_; }
  Connection& connection() { return *connection_; }
  const std::shared_ptr<Connection>& connectionPtr() const { return connection_; }

 private:
  std::shared_ptr<Connection> connection_;
  std::shared_ptr<MessageFactory> factory_;
  std::shared_ptr<MessageDispatcher> messageDispatcher_;
  std::shared_ptr<EventDispatcher> eventDispatcher_;
  std::vector<Connection::ListenerId> listeners_;
  bool closed_ = false;
};

}  // namespace protobus
