#include "protobus/base_listener.h"

#include <algorithm>

#include "executor.h"
#include "scheduler.h"
#include "protobus/config.h"
#include "protobus/errors.h"
#include "protobus/logger.h"
#include "uuid.h"

namespace protobus {

BaseListener::BaseListener(std::shared_ptr<Connection> connection) : connection_(std::move(connection)) {
  defaultHandler_ = [](const std::string& message, const std::string& correlationId,
                       MessageHandlerContext&) -> MessageHandlerResult {
    // Size and correlationId only, never the body.
    Logger::warn("unhandled message by default handler (" + std::to_string(message.size()) +
                 " bytes, correlationId " + (correlationId.empty() ? "none" : correlationId) + ")");
    return std::monostate{};
  };
}

BaseListener::~BaseListener() {
  if (detachRestorer_) detachRestorer_();
  if (disconnectedListener_) connection_->removeListener(*disconnectedListener_);
}

bool BaseListener::isConnected() const { return connection_->isConnected(); }

bool BaseListener::isInitialized() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initialized_;
}

std::string BaseListener::queueName() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queueName_;
}

std::shared_ptr<amqp::Channel> BaseListener::channel() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return channel_;
}

// Restoration is coordinated by the connection, which waits for it before
// reporting itself reconnected. Disconnection stays an event: there is
// nothing to wait for when the socket has already gone.
void BaseListener::attach() {
  attachRestorer();
  if (!disconnectedListener_) {
    std::weak_ptr<BaseListener> weak = weak_from_this();
    disconnectedListener_ = connection_->onDisconnected([weak] {
      if (auto self = weak.lock()) self->onDisconnected();
    });
  }
}

// Paired with detachRestorer and both idempotent: a listener attaches on
// init() and again on every start(), and detaches on stopConsuming() and
// close(). Re-attaching in start() keeps stop/start reversible.
void BaseListener::attachRestorer() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (detachRestorer_) return;
  std::weak_ptr<BaseListener> weak = weak_from_this();
  detachRestorer_ = connection_->registerRestorer([weak](uint64_t) {
    if (auto self = weak.lock()) self->restore();
  });
}

void BaseListener::detachRestorer() {
  std::function<void()> detach;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    detach = std::move(detachRestorer_);
    detachRestorer_ = nullptr;
  }
  if (detach) detach();
}

void BaseListener::onDisconnected() {
  Logger::debug(std::string(listenerName()) + ": connection lost, clearing channel state");
  std::lock_guard<std::mutex> lock(mutex_);
  channel_.reset();
  consumerTag_.clear();
}

// A failure propagates to the connection, which treats the whole generation
// as unusable and retries: a half-restored listener beside a connection that
// believes it is healthy is the worst of both.
void BaseListener::restore() {
  std::lock_guard<std::mutex> serial(restoreMutex_);
  std::vector<std::string> bindings;
  bool wasStarted;
  std::shared_ptr<amqp::Channel> previous;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ || closing_) return;
    bindings = bindings_;
    wasStarted = wasStarted_;
    previous = channel_;
  }
  Logger::info(std::string(listenerName()) + ": reconnected, re-initializing...");
  reinitialize();
  auto ch = channel();
  const std::string queue = queueName();
  for (const auto& routingKey : bindings) {
    connection_->bindQueue(ch, queue, exchangeName_, routingKey);
    Logger::debug(std::string(listenerName()) + ": re-bound " + routingKey);
  }
  restoreTopology();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    wasStarted = wasStarted && wasStarted_ && !closing_;
    consumerLost_ = false;
  }
  if (wasStarted) startConsuming();
  // The channel this replaces can still be open: one whose consumer the
  // broker cancelled. Closed only now, once channel_ no longer names it, so
  // its close is not mistaken for a loss to rebuild from, and so it cannot
  // keep a second consumer alive.
  if (previous && previous != ch && previous->isOpen()) {
    try {
      connection_->closeChannel(previous);
    } catch (const std::exception& e) {
      Logger::debug(std::string(listenerName()) + ": failed closing the replaced channel: " + e.what());
    }
  }
  Logger::info(std::string(listenerName()) + ": successfully re-initialized after reconnection");
}

void BaseListener::reinitialize() {
  auto ch = connection_->openChannel();
  if (lateAck_) ch->prefetch(static_cast<uint16_t>(effectivePrefetch()));
  connection_->declareExchange(ch, exchangeName_, exchangeType_);
  // An anonymous queue is gone with the old connection: declare a new one.
  const std::string requested = isAnonymous_ ? std::string() : configuredQueueName_;
  QueueOptions q;
  q.durable = !isAnonymous_;
  q.exclusive = isAnonymous_;
  q.autoDelete = isAnonymous_;
  q.arguments = buildQueueArguments();
  const std::string name = connection_->declareQueue(ch, requested, q);
  if (exchangeType_ == "direct") connection_->bindQueue(ch, name, exchangeName_, name);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_ = ch;
    queueName_ = name;
  }
  watchChannel(ch);
}

// A channel can close while the connection stays up: a broker-side consumer
// timeout, an ack the broker refused. The listener is rebuilt then, as on a
// reconnection; otherwise it would go quiet behind a healthy connection.
void BaseListener::watchChannel(const std::shared_ptr<amqp::Channel>& ch) {
  std::weak_ptr<BaseListener> weak = weak_from_this();
  std::weak_ptr<amqp::Channel> weakChannel = ch;
  ch->onClose([weak, weakChannel](const std::string& reason) {
    auto self = weak.lock();
    if (!self) return;
    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      auto current = weakChannel.lock();
      if (self->closing_ || !self->initialized_ || !current || self->channel_ != current) return;
    }
    if (!self->connection_->isReady()) return;  // the reconnection restores it
    self->scheduleRebuild("channel closed on a live connection (" + reason + ")");
  });
}

// One rebuild pending at a time, backing off while they keep failing, so a
// channel error that recurs on every attempt (a queue redeclared with other
// arguments, a permission refused) cannot spin against the broker.
void BaseListener::scheduleRebuild(const std::string& reason) {
  int failures;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (rebuildScheduled_ || closing_ || !initialized_) return;
    rebuildScheduled_ = true;
    failures = rebuildFailures_;
  }
  const int64_t delay = std::min<int64_t>(int64_t{100} << std::min(failures, 9), 30000);
  Logger::warn(std::string(listenerName()) + ": " + reason + "; rebuilding it in " + std::to_string(delay) + "ms");
  std::weak_ptr<BaseListener> weak = weak_from_this();
  auto connection = connection_;
  connection_->scheduler().schedule(std::chrono::milliseconds(delay), [weak, connection] {
    connection->executor().post([weak] {
      if (auto s = weak.lock()) s->rebuild();
    });
  });
}

void BaseListener::rebuild() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    rebuildScheduled_ = false;
    if (closing_ || !initialized_) return;
    // Stopped since the broker cancelled the consumer: there is nothing to
    // put back, and the queue must not be redeclared behind the stop.
    if (consumerLost_ && !wasStarted_) {
      consumerLost_ = false;
      return;
    }
    ++rebuildFailures_;
  }
  if (!connection_->isReady()) return;
  try {
    restore();
    std::lock_guard<std::mutex> lock(mutex_);
    rebuildFailures_ = 0;
  } catch (const std::exception& e) {
    Logger::error(std::string(listenerName()) + ": failed to rebuild its channel: " + e.what());
    scheduleRebuild(e.what());
  }
}

// basic.cancel leaves the channel and the connection open, so neither
// channel-close recovery nor a reconnection would ever notice. The listener
// forgets the consumer and rebuilds, with the same backoff as a lost channel.
void BaseListener::onConsumerCancelled(const std::string& tag) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Not for a consumer since replaced or stopped on purpose.
    if (closing_ || !initialized_ || !wasStarted_ || consumerTag_ != tag) return;
    consumerTag_.clear();
    consumerLost_ = true;
  }
  scheduleRebuild("the broker cancelled its consumer");
}

void BaseListener::startConsuming() {
  const std::string tag = detail::randomUuid();
  std::shared_ptr<amqp::Channel> ch;
  std::string queue;
  MessageHandler handler;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    consumerTag_ = tag;
    ch = channel_;
    queue = queueName_;
    handler = handler_;
  }
  if (!ch) throw NotConnectedError(std::string(listenerName()) + ": no channel to consume on");
  ConsumeOptions options;
  options.consumerTag = tag;
  options.noAck = false;
  options.exclusive = isAnonymous_;
  options.buildErrorReply = buildErrorReply_;
  options.ordered = orderedDelivery_;
  std::weak_ptr<BaseListener> weak = weak_from_this();
  options.onCancelled = [weak, tag] {
    if (auto self = weak.lock()) self->onConsumerCancelled(tag);
  };
  connection_->consume(ch, queue, handler, options, lateAck_, getRetryOptions(), processingTimeoutMs_);
  Logger::debug(std::string(listenerName()) + ": started consuming from " + queue);
}

void BaseListener::init(MessageHandler handler, const std::string& queueName) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initialized_) return;
  }
  if (exchangeName_.empty()) throw MissingExchangeError();
  if (!connection_->isConnected()) throw ConnectionError();

  {
    std::lock_guard<std::mutex> lock(mutex_);
    handler_ = handler ? std::move(handler) : defaultHandler_;
    isAnonymous_ = queueName.empty();
    configuredQueueName_ = queueName;
    closing_ = false;
  }
  attach();

  auto ch = connection_->openChannel();
  if (lateAck_) ch->prefetch(static_cast<uint16_t>(effectivePrefetch()));
  connection_->declareExchange(ch, exchangeName_, exchangeType_);
  QueueOptions q;
  q.durable = !isAnonymous_;
  q.exclusive = isAnonymous_;
  q.autoDelete = isAnonymous_;
  q.arguments = buildQueueArguments();
  const std::string name = connection_->declareQueue(ch, queueName, q);
  // A direct-exchange listener is bound to its own name.
  if (exchangeType_ == "direct") connection_->bindQueue(ch, name, exchangeName_, name);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_ = ch;
    queueName_ = name;
    initialized_ = true;
  }
  watchChannel(ch);
}

void BaseListener::start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) throw NotInitializedError();
    // Recovering from a broker cancellation counts as started: the rebuild
    // consumes again, and a second consumer here would be a duplicate.
    if (wasStarted_ && (!consumerTag_.empty() || consumerLost_)) throw AlreadyStartedError();
  }
  if (!connection_->isConnected()) throw NotConnectedError();
  // Restored again from here on: stopConsuming() drops out of restoration.
  attachRestorer();
  std::lock_guard<std::mutex> serial(restoreMutex_);
  startConsuming();
  std::lock_guard<std::mutex> lock(mutex_);
  wasStarted_ = true;
}

void BaseListener::stopConsuming() {
  std::string tag;
  std::shared_ptr<amqp::Channel> ch;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Recorded first and unconditionally: a reconnection inside the drain
    // window must not put the consumer back in a process that has begun
    // shutting down.
    tag = std::move(consumerTag_);
    consumerTag_.clear();
    // consumerLost_ is left set: a rebuild already scheduled for it sees the
    // stop and does nothing, rather than redeclaring a deleted queue.
    wasStarted_ = false;
    ch = channel_;
  }
  detachRestorer();
  if (tag.empty() || !ch || !connection_->isConnected()) return;
  try {
    connection_->cancel(ch, tag);
    Logger::debug(std::string(listenerName()) + ": stopped consuming (" + tag + ")");
  } catch (const std::exception& e) {
    Logger::debug(std::string(listenerName()) + ": failed to cancel consumer '" + tag +
                  "' during drain: " + e.what());
  }
}

void BaseListener::close() {
  std::shared_ptr<amqp::Channel> ch;
  std::string tag;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) throw NotInitializedError();
    closing_ = true;
    ch = channel_;
    tag = consumerTag_;
  }
  detachRestorer();
  if (disconnectedListener_) {
    connection_->removeListener(*disconnectedListener_);
    disconnectedListener_.reset();
  }
  if (ch && connection_->isConnected()) {
    try {
      if (!tag.empty()) connection_->cancel(ch, tag);
      connection_->closeChannel(ch);
    } catch (const std::exception& e) {
      Logger::debug(std::string(listenerName()) + ": error during close (may be expected): " + e.what());
    }
  }
  std::lock_guard<std::mutex> lock(mutex_);
  consumerTag_.clear();
  channel_.reset();
  initialized_ = false;
  wasStarted_ = false;
  bindings_.clear();
}

amqp::FieldTable BaseListener::buildQueueArguments() const {
  amqp::FieldTable args;
  if (messageTtlMs_) args["x-message-ttl"] = amqp::FieldValue::fromInt(*messageTtlMs_);
  if (maxPriority_) args["x-max-priority"] = amqp::FieldValue::fromInt(*maxPriority_);
  return args;
}

int BaseListener::effectivePrefetch() const {
  if (maxConcurrent_ && *maxConcurrent_ > 0) return std::min(*maxConcurrent_, 65535);
  return static_cast<int>(Config::defaultPrefetch());
}

void BaseListener::trackBinding(const std::string& routingKey) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (std::find(bindings_.begin(), bindings_.end(), routingKey) == bindings_.end()) {
    bindings_.push_back(routingKey);
  }
}

}  // namespace protobus
