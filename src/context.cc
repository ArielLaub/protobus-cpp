#include "protobus/context.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstdlib>

#include "protobus/config.h"
#include "protobus/logger.h"

namespace protobus {

Context::Context(std::shared_ptr<amqp::Transport> transport)
    : connection_(std::make_shared<Connection>(std::move(transport))),
      factory_(std::make_shared<MessageFactory>()),
      messageDispatcher_(std::make_shared<MessageDispatcher>(connection_)),
      eventDispatcher_(std::make_shared<EventDispatcher>(connection_, factory_)) {
  listeners_.push_back(connection_->onReconnecting([](int attempt, int64_t delay) {
    Logger::info("Context: reconnecting (attempt " + std::to_string(attempt) + ", delay " + std::to_string(delay) +
                 "ms)");
  }));
  listeners_.push_back(connection_->onReconnected([] { Logger::info("Context: reconnected successfully"); }));
  listeners_.push_back(connection_->onDisconnected([] { Logger::warn("Context: connection lost"); }));
  listeners_.push_back(connection_->onError(
      [](const std::exception& e) { Logger::error(std::string("Context: connection error - ") + e.what()); }));
}

Context::~Context() {
  // close() drains when it actually closes; a Context closed earlier gets
  // one drain here.
  const bool drainedByClose = !closed_;
  try {
    close();
  } catch (...) {
  }
  // Handlers still running reach this Context through their services: once
  // it is gone they touch freed memory. That is a bug in the application's
  // shutdown order, and the one thing worse than reporting it loudly is
  // carrying on silently.
  if (connection_->inFlightDeliveries() == 0) return;
  if (!drainedByClose && connection_->drainInFlight(Config::shutdownDrainTimeoutMs())) return;
  Logger::error("protobus: a Context is being destroyed while " +
                std::to_string(connection_->inFlightDeliveries()) +
                " handler(s) still run on it. They use it through their services and will touch freed memory. "
                "Destroy the Context only after its handlers have finished: stop the services and drain "
                "(Connection::drainInFlight), or let RunnableService shut down.");
#ifndef NDEBUG
  // Debug builds stop here, where the cause is visible, rather than at the
  // memory corruption that follows.
  std::abort();
#endif
}

void Context::init(const std::string& amqpUrl, const std::vector<std::string>& protoLocations,
                   const ContextOptions& options) {
  factory_->init(protoLocations);
  connection_->connect(amqpUrl, options.reconnection);
  messageDispatcher_->init();
  eventDispatcher_->init();
  closed_ = false;
}

void Context::close() {
  if (closed_) return;
  closed_ = true;
  try {
    messageDispatcher_->close();
  } catch (const std::exception& e) {
    Logger::debug(std::string("Context: error closing the message dispatcher: ") + e.what());
  }
  try {
    eventDispatcher_->close();
  } catch (const std::exception& e) {
    Logger::debug(std::string("Context: error closing the event dispatcher: ") + e.what());
  }
  for (auto id : listeners_) connection_->removeListener(id);
  listeners_.clear();
  // Disconnecting stops new deliveries and hands unacknowledged ones back to
  // the broker. Handlers already running refer to this context through their
  // services, so it is not released until they are done, or until the drain
  // budget runs out.
  connection_->disconnect();
  if (!connection_->drainInFlight(Config::shutdownDrainTimeoutMs())) {
    Logger::warn("Context: closing with " + std::to_string(connection_->inFlightDeliveries()) +
                 " handler(s) still running after the drain timeout");
  }
}

bool Context::isConnected() const { return connection_->isConnected(); }

bool Context::isReconnecting() const { return connection_->isReconnecting(); }

std::string Context::publishMessage(const std::string& content, const std::string& routingKey,
                                    const CallOptions& options) {
  return messageDispatcher_->publish(content, routingKey, options);
}

ChunkStream Context::publishStreamingMessage(const std::string& content, const std::string& routingKey,
                                             const StreamOptions& options) {
  return messageDispatcher_->publishStreaming(content, routingKey, options);
}

void Context::publishEvent(const std::string& type, const google::protobuf::Message& content,
                           const std::string& topic) {
  eventDispatcher_->publish(type, content, topic);
}

void Context::publishEvent(const google::protobuf::Message& content, const std::string& topic) {
  eventDispatcher_->publish(std::string(content.GetDescriptor()->full_name()), content, topic);
}

}  // namespace protobus
