// BaseListener: a channel, an exchange, a queue and a consumer, kept alive
// across reconnections.
//
// MessageListener (service requests), CallbackListener (RPC replies) and
// EventListener (events) are BaseListeners. Each owns its own channel, so a
// channel-level failure in one cannot take the others down, and each takes
// part in the connection's restoration: when the socket comes back it reopens
// its channel, redeclares its queue, rebinds every routing key it was bound to
// and, if it was consuming, consumes again, all before the connection reports
// itself ready.
#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "protobus/connection.h"

namespace protobus {

class BaseListener : public std::enable_shared_from_this<BaseListener> {
 public:
  explicit BaseListener(std::shared_ptr<Connection> connection);
  virtual ~BaseListener();

  BaseListener(const BaseListener&) = delete;
  BaseListener& operator=(const BaseListener&) = delete;

  bool isConnected() const;
  bool isInitialized() const;

  // Open the channel and declare the exchange and queue. An empty queue name
  // declares an anonymous queue: server-named, exclusive and auto-delete. A
  // null handler installs one that logs and drops every message.
  virtual void init(MessageHandler handler, const std::string& queueName = "");
  // Start consuming. Throws NotInitializedError, AlreadyStartedError or
  // NotConnectedError.
  void start();
  // Stop accepting NEW deliveries while leaving the channel open, so work
  // already in hand can still ack and reply: the first step of a graceful
  // shutdown. Safe to call more than once, and when disconnected.
  void stopConsuming();
  // Cancel the consumer and close the channel. Throws NotInitializedError.
  void close();

  // The queue's name (the broker's, for an anonymous queue).
  std::string queueName() const;

 protected:
  // Retry options for consume(). Override to enable retry.
  virtual std::optional<ConsumeRetryOptions> getRetryOptions() const { return std::nullopt; }
  // Topology beyond the main queue and its bindings (retry queues, ...),
  // declared again on restoration.
  virtual void restoreTopology() {}
  virtual const char* listenerName() const { return "BaseListener"; }

  // The arguments the main queue is declared with. Every key is added only
  // when its option is set, so a listener that configures nothing declares
  // {}: the same arguments an existing queue was declared with, which is what
  // lets it be redeclared instead of rejected.
  amqp::FieldTable buildQueueArguments() const;
  // The prefetch for a late-ack consumer. Never 0: RabbitMQ reads 0 as
  // unlimited, which with late ack lets the whole backlog into memory.
  int effectivePrefetch() const;
  void trackBinding(const std::string& routingKey);
  std::shared_ptr<amqp::Channel> channel() const;
  bool isAnonymous() const { return isAnonymous_; }
  const std::string& configuredQueueName() const { return configuredQueueName_; }

  std::shared_ptr<Connection> connection_;
  std::string exchangeName_;
  std::string exchangeType_;
  bool lateAck_ = false;
  std::optional<int> maxConcurrent_;
  std::optional<int64_t> messageTtlMs_;
  std::optional<int> maxPriority_;
  std::optional<int64_t> processingTimeoutMs_;
  // Handle deliveries one at a time, in arrival order.
  bool orderedDelivery_ = false;
  MessageHandler defaultHandler_;
  // Builds the caller's reply for a failure that carries none.
  std::function<std::optional<std::string>(const std::string&, const std::exception&)> buildErrorReply_;

 private:
  void attach();
  void attachRestorer();
  void detachRestorer();
  void restore();
  void reinitialize();
  void startConsuming();
  void onDisconnected();
  void watchChannel(const std::shared_ptr<amqp::Channel>& channel);
  void scheduleRebuild(const std::string& reason);
  void rebuild();

  mutable std::mutex mutex_;
  std::shared_ptr<amqp::Channel> channel_;
  std::string queueName_;
  std::string configuredQueueName_;
  std::string consumerTag_;
  MessageHandler handler_;
  bool isAnonymous_ = true;
  bool initialized_ = false;
  bool wasStarted_ = false;
  bool closing_ = false;
  bool rebuildScheduled_ = false;
  int rebuildFailures_ = 0;
  std::vector<std::string> bindings_;
  std::function<void()> detachRestorer_;
  std::optional<Connection::ListenerId> disconnectedListener_;
};

}  // namespace protobus
