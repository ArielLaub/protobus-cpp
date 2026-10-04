#include "protobus/event_listener.h"

#include "protobus/config.h"
#include "protobus/logger.h"
#include "protobus/message_factory.h"
#include "protobus/message_listener.h"

namespace protobus {

EventListener::EventListener(std::shared_ptr<Connection> connection, std::shared_ptr<MessageFactory> factory,
                             EventRetryOptions retry)
    : BaseListener(std::move(connection)),
      factory_(std::move(factory)),
      maxRetries_(retry.maxRetries.value_or(0)),
      retryDelayMs_(retry.retryDelayMs.value_or(kDefaultRetryDelayMs)) {
  exchangeName_ = Config::eventsExchangeName();
  exchangeType_ = "topic";
  lateAck_ = true;
}

MessageHandlerResult EventListener::dispatch(const std::string& body, MessageHandlerContext& context) {
  DecodedEvent event = factory_->decodeEvent(body);
  RawEvent raw{event.type, event.topic, &event.data, event.message.get()};

  EventHandler all;
  {
    std::lock_guard<std::mutex> lock(routerMutex_);
    all = allHandler_;
  }
  if (all) all(*event.message, event.type, event.topic);

  // Match on the routing key the broker delivered on rather than the topic
  // in the body: the body is publisher-controlled, and trusting it would let
  // a publisher reach handlers its routing key was never permitted to.
  const std::string matchTopic = !context.routingKey.empty() ? context.routingKey : event.topic;
  if (matchTopic.empty()) {
    // The type only: the payload must not reach the log.
    Logger::warn("ignoring unhandled event of type '" + event.type + "' (no topic to route on)");
    return std::monostate{};
  }
  std::vector<std::shared_ptr<RawHandler>> handlers;
  {
    std::lock_guard<std::mutex> lock(routerMutex_);
    handlers = router_.match(matchTopic);
  }
  for (const auto& h : handlers) (*h)(raw);
  return std::monostate{};
}

void EventListener::init(MessageHandler handler, const std::string& queueName) {
  if (isInitialized()) return;
  if (!handler) {
    std::weak_ptr<BaseListener> weak = weak_from_this();
    handler = [weak](const std::string& body, const std::string&, MessageHandlerContext& context) {
      auto self = std::static_pointer_cast<EventListener>(weak.lock());
      if (!self) throw NotInitializedError("the event listener has been destroyed");
      return self->dispatch(body, context);
    };
  }
  BaseListener::init(std::move(handler), queueName);
  // Before start(), so the first delivery already has somewhere to fail to.
  setupRetryTopology();
}

// The shape differs from a service's ladder on one point. A request's retry
// queue dead-letters back to the bus exchange, which routes to one service
// queue; events fan out, and dead-lettering back to the events exchange would
// redeliver to every subscriber, including those that handled it. So the
// expired message goes to a per-subscriber exchange bound only to this
// listener's queue, which still preserves the routing key handlers match on.
void EventListener::setupRetryTopology() {
  if (maxRetries_ <= 0 || isAnonymous()) return;
  const std::string base = configuredQueueName();
  dlqName_ = base + ".DLQ";
  retryQueueName_ = base + ".Retry";
  retryExchangeName_ = base + ".Retry.Exchange";
  redeliveryExchangeName_ = base + ".Redelivery";
  auto ch = channel();
  const std::string queue = queueName();

  QueueOptions durable;
  connection_->declareQueue(ch, dlqName_, durable);
  connection_->declareExchange(ch, redeliveryExchangeName_, "topic");
  connection_->bindQueue(ch, queue, redeliveryExchangeName_, "#");

  QueueOptions retry;
  retry.arguments["x-message-ttl"] = amqp::FieldValue::fromInt(retryDelayMs_);
  retry.arguments["x-dead-letter-exchange"] = amqp::FieldValue::fromString(redeliveryExchangeName_);
  try {
    connection_->declareQueue(ch, retryQueueName_, retry);
  } catch (const amqp::AmqpError& e) {
    if (e.preconditionFailed()) {
      throw RetryQueueMismatchError("event retry queue '" + retryQueueName_ +
                                    "' already exists with different arguments (most likely a different "
                                    "retryDelayMs - now " +
                                    std::to_string(retryDelayMs_) +
                                    "ms). RabbitMQ cannot change a queue's x-message-ttl in place: drain and "
                                    "delete the queue, or keep the original retryDelayMs. Original error: " +
                                    e.what());
    }
    throw;
  }
  connection_->declareExchange(ch, retryExchangeName_, "topic");
  connection_->bindQueue(ch, retryQueueName_, retryExchangeName_, "#");
}

void EventListener::restoreTopology() {
  if (!retryQueueName_.empty()) setupRetryTopology();
}

std::optional<ConsumeRetryOptions> EventListener::getRetryOptions() const {
  if (maxRetries_ <= 0 || retryQueueName_.empty() || dlqName_.empty()) return std::nullopt;
  ConsumeRetryOptions o;
  o.maxRetries = maxRetries_;
  o.retryQueueName = retryQueueName_;
  o.retryExchangeName = retryExchangeName_;
  o.dlqName = dlqName_;
  o.isHandledError = [](const std::exception& e) { return isHandledError(e); };
  return o;
}

void EventListener::addHandler(const std::string& type, const std::string& topic, RawHandler handler) {
  const std::string t = topic.empty() ? "EVENT." + type : topic;
  {
    std::lock_guard<std::mutex> lock(routerMutex_);
    router_.add(t, std::make_shared<RawHandler>(std::move(handler)));
  }
  trackBinding(t);
  connection_->bindQueue(channel(), queueName(), exchangeName_, t);
}

void EventListener::subscribe(const std::string& type, EventHandler handler, const std::string& topic) {
  addHandler(type, topic, [handler](const RawEvent& e) { handler(*e.message, e.type, e.topic); });
}

void EventListener::subscribeAll(EventHandler handler) {
  {
    std::lock_guard<std::mutex> lock(routerMutex_);
    allHandler_ = std::move(handler);
  }
  trackBinding("#");
  connection_->bindQueue(channel(), queueName(), exchangeName_, "#");
}

std::optional<EventRetryTopology> EventListener::retryTopology() const {
  if (retryQueueName_.empty()) return std::nullopt;
  return EventRetryTopology{retryQueueName_, dlqName_};
}

}  // namespace protobus
