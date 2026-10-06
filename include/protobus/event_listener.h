// EventListener: a queue on the events exchange, and the handlers subscribed
// through it.
//
// Each subscription binds the queue to a topic pattern (EVENT.<type> unless
// given) and registers a handler under it. A delivery is routed to every
// handler whose pattern matches the routing key the broker delivered on, not
// the topic the publisher wrote into the event, which a publisher controls.
//
// Left unconfigured, a handler that throws loses its event: the delivery is
// rejected, so one permanently failing event cannot stall the subscriber
// behind its own prefetch. EventRetryOptions gives events the ladder requests
// climb instead.
#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <google/protobuf/message.h>

#include "protobus/base_listener.h"
#include "protobus/custom_types.h"
#include "protobus/errors.h"
#include "protobus/trie.h"

namespace protobus {

namespace detail {
struct HandlerTurn;
}

class MessageFactory;

// (event, type, topic): the decoded event, its full type name, and the topic
// it was published under.
using EventHandler =
    std::function<void(const google::protobuf::Message& event, const std::string& type, const std::string& topic)>;

struct EventRetryOptions {
  // Retry hops before the DLQ. 0 (the default) drops a failing event.
  std::optional<int> maxRetries;
  // Delay between hops, the retry queue's x-message-ttl. Default 5000.
  std::optional<int64_t> retryDelayMs;
};

struct EventRetryTopology {
  std::string retryQueue;
  std::string dlq;
};

class EventListener : public BaseListener {
 public:
  EventListener(std::shared_ptr<Connection> connection, std::shared_ptr<MessageFactory> factory,
                EventRetryOptions retry = {});

  // A null handler installs the default, which decodes each event and routes
  // it to the subscribed handlers.
  void init(MessageHandler handler, const std::string& queueName = "") override;

  // Subscribe to events of `type` under `topic` (default EVENT.<type>). The
  // handler receives a dynamic message of the factory's type.
  void subscribe(const std::string& type, EventHandler handler, const std::string& topic = "");

  // Typed: the handler receives a T. An event of another type delivered to it
  // fails as an InvalidMessageError.
  template <typename T>
  void subscribe(std::function<void(const T& event, const std::string& type, const std::string& topic)> handler,
                 const std::string& topic = "") {
    const std::string type(T::descriptor()->full_name());
    addHandler(type, topic, [handler, type](const RawEvent& e) {
      if (e.type != type) {
        throw InvalidMessageError("an event of type " + e.type + " was delivered to a handler for " + type);
      }
      T typed;
      if (!typed.ParseFromString(*e.data)) throw InvalidMessageError("event did not decode as " + type);
      validateCustomTypes(typed);
      handler(typed, e.type, e.topic);
    });
  }

  // Receive every event the queue gets, before the routed handlers.
  void subscribeAll(EventHandler handler);

  // Run every handler while holding `turn`, so they never overlap with the
  // other handlers sharing it (MessageServiceOptions::serializeHandlers).
  void serializeWith(std::shared_ptr<detail::HandlerTurn> turn);

  // The names of the retry objects; nullopt when retry is off.
  std::optional<EventRetryTopology> retryTopology() const;

 protected:
  std::optional<ConsumeRetryOptions> getRetryOptions() const override;
  void restoreTopology() override;
  const char* listenerName() const override { return "EventListener"; }

 private:
  struct RawEvent {
    std::string type;
    std::string topic;
    const std::string* data;
    const google::protobuf::Message* message;
  };
  using RawHandler = std::function<void(const RawEvent&)>;

  void addHandler(const std::string& type, const std::string& topic, RawHandler handler);
  void setupRetryTopology();
  MessageHandlerResult dispatch(const std::string& body, MessageHandlerContext& context);

  std::shared_ptr<MessageFactory> factory_;
  int maxRetries_;
  int64_t retryDelayMs_;
  mutable std::mutex routerMutex_;
  Trie<std::shared_ptr<RawHandler>> router_;
  EventHandler allHandler_;
  std::shared_ptr<detail::HandlerTurn> turn_;
  std::string retryQueueName_;
  std::string retryExchangeName_;
  std::string redeliveryExchangeName_;
  std::string dlqName_;
};

}  // namespace protobus
