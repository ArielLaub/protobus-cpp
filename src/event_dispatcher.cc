#include "protobus/event_dispatcher.h"

#include "protobus/config.h"
#include "protobus/errors.h"
#include "protobus/logger.h"
#include "protobus/message_factory.h"
#include "uuid.h"

namespace protobus {

EventDispatcher::EventDispatcher(std::shared_ptr<Connection> connection, std::shared_ptr<MessageFactory> factory)
    : connection_(std::move(connection)), factory_(std::move(factory)) {}

EventDispatcher::~EventDispatcher() {
  if (detachRestorer_) detachRestorer_();
  if (disconnectedListener_) connection_->removeListener(*disconnectedListener_);
}

bool EventDispatcher::isInitialized() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initialized_;
}

void EventDispatcher::init() {
  if (isInitialized()) return;
  std::weak_ptr<EventDispatcher> weak = weak_from_this();
  disconnectedListener_ = connection_->onDisconnected([weak] {
    if (auto self = weak.lock()) self->onDisconnected();
  });
  detachRestorer_ = connection_->registerRestorer([weak](uint64_t) {
    if (auto self = weak.lock()) self->restore();
  });
  auto ch = connection_->openChannel();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_ = ch;
  }
  declareExchange();
  std::lock_guard<std::mutex> lock(mutex_);
  initialized_ = true;
}

// Best effort, as for the RPC exchanges: an event published before any
// subscriber exists must not close the channel with a 404.
void EventDispatcher::declareExchange() {
  try {
    auto ch = connection_->openChannel();
    try {
      connection_->declareExchange(ch, Config::eventsExchangeName(), "topic");
    } catch (const std::exception& e) {
      Logger::debug(std::string("EventDispatcher: could not declare the events exchange: ") + e.what());
    }
    connection_->closeChannel(ch);
  } catch (const std::exception& e) {
    Logger::debug(std::string("EventDispatcher: could not declare the events exchange: ") + e.what());
  }
}

void EventDispatcher::onDisconnected() {
  Logger::debug("EventDispatcher: connection lost, clearing channel");
  std::lock_guard<std::mutex> lock(mutex_);
  channel_.reset();
}

// A failure propagates so the connection retries the generation: an event
// dispatcher with no channel would drop every event published through it.
void EventDispatcher::restore() {
  if (!isInitialized()) return;
  Logger::info("EventDispatcher: reconnected, re-initializing channel");
  auto ch = connection_->openChannel();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_ = ch;
  }
  declareExchange();
  Logger::info("EventDispatcher: successfully re-initialized after reconnection");
}

std::shared_ptr<amqp::Channel> EventDispatcher::publishChannel() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (channel_ && channel_->isOpen()) return channel_;
  }
  if (!connection_->isReady()) {
    std::lock_guard<std::mutex> lock(mutex_);
    return channel_;
  }
  auto ch = connection_->openChannel();
  std::lock_guard<std::mutex> lock(mutex_);
  if (!channel_ || !channel_->isOpen()) channel_ = ch;
  return channel_;
}

void EventDispatcher::publish(const std::string& type, const google::protobuf::Message& content,
                              const std::string& topic) {
  // A reconnection is waited through; anything else with no connection is
  // reported at once.
  if (!connection_->isConnected() && !connection_->isReconnecting()) throw NotConnectedError();
  connection_->whenReady();
  const std::string t = topic.empty() ? "EVENT." + type : topic;
  std::string event;
  try {
    event = factory_->buildEvent(type, content, t);
  } catch (const std::exception& e) {
    // Through the logger and without the payload: events carry PII too.
    Logger::error("failed building event '" + type + "': " + e.what());
    throw InvalidMessageError(std::string("failed building event '") + type + "': " + e.what());
  }
  PublishOptions p;
  p.properties.correlationId = detail::randomUuid();
  p.properties.contentType = "application/octet-stream";
  p.properties.deliveryMode = 2;
  connection_->publish(publishChannel(), Config::eventsExchangeName(), t, event, std::move(p));
}

void EventDispatcher::close() {
  if (disconnectedListener_) {
    connection_->removeListener(*disconnectedListener_);
    disconnectedListener_.reset();
  }
  if (detachRestorer_) {
    detachRestorer_();
    detachRestorer_ = nullptr;
  }
  std::shared_ptr<amqp::Channel> ch;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ch = std::move(channel_);
    channel_.reset();
    initialized_ = false;
  }
  if (ch && connection_->isConnected()) {
    try {
      connection_->closeChannel(ch);
    } catch (...) {
    }
  }
}

}  // namespace protobus
