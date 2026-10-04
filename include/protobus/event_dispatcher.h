// EventDispatcher: publishes events on the events exchange.
#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "protobus/connection.h"

namespace google::protobuf {
class Message;
}

namespace protobus {

class MessageFactory;

class EventDispatcher : public std::enable_shared_from_this<EventDispatcher> {
 public:
  EventDispatcher(std::shared_ptr<Connection> connection, std::shared_ptr<MessageFactory> factory);
  ~EventDispatcher();

  bool isInitialized() const;
  void init();

  // Publish `content`, of message type `type`, under `topic` (default
  // EVENT.<type>). Returns once the broker has confirmed it. An event with
  // no subscriber is normal: events are not published mandatory.
  void publish(const std::string& type, const google::protobuf::Message& content, const std::string& topic = "");

  void close();

 private:
  void onDisconnected();
  void restore();
  std::shared_ptr<amqp::Channel> publishChannel();
  void declareExchange();

  std::shared_ptr<Connection> connection_;
  std::shared_ptr<MessageFactory> factory_;
  mutable std::mutex mutex_;
  std::shared_ptr<amqp::Channel> channel_;
  bool initialized_ = false;
  std::function<void()> detachRestorer_;
  std::optional<Connection::ListenerId> disconnectedListener_;
};

}  // namespace protobus
