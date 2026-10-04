#include "protobus/cancel_listener.h"

#include "protobus/config.h"
#include "protobus/logger.h"
#include "uuid.h"

namespace protobus {

CancelListener::CancelListener(std::shared_ptr<Connection> connection) : connection_(std::move(connection)) {}

CancelListener::~CancelListener() {
  if (detachRestorer_) detachRestorer_();
}

void CancelListener::start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!detachRestorer_) {
      std::weak_ptr<CancelListener> weak = weak_from_this();
      detachRestorer_ = connection_->registerRestorer([weak](uint64_t) {
        if (auto self = weak.lock()) self->restore();
      });
    }
    started_ = true;
  }
  try {
    startInner();
  } catch (const std::exception& e) {
    Logger::warn(std::string("CancelListener: stream cancellation unavailable (") + e.what() +
                 "). Streams will run to completion; everything else is unaffected.");
    std::lock_guard<std::mutex> lock(mutex_);
    channel_.reset();
  }
}

void CancelListener::startInner() {
  auto ch = connection_->openChannel();
  connection_->declareExchange(ch, Config::cancelExchangeName(), "fanout");
  // Anonymous: the broker names it, this process owns it, and it disappears
  // with the connection.
  QueueOptions q;
  q.durable = false;
  q.exclusive = true;
  q.autoDelete = true;
  const std::string queue = connection_->declareQueue(ch, "", q);
  connection_->bindQueue(ch, queue, Config::cancelExchangeName(), "");
  const std::string tag = detail::randomUuid();
  std::weak_ptr<Connection> weakConnection = connection_;
  ch->consume(
      queue, tag, true, true,
      [weakConnection](amqp::Delivery msg) {
        const std::string correlationId = msg.properties.correlationId.value_or("");
        if (correlationId.empty()) return;
        // Every replica hears every cancel; only the one running that stream
        // has anything to do.
        if (auto c = weakConnection.lock()) c->cancelStream(correlationId);
      },
      nullptr);
  std::lock_guard<std::mutex> lock(mutex_);
  channel_ = ch;
  queueName_ = queue;
  consumerTag_ = tag;
  Logger::debug("CancelListener: consuming cancellations on " + queue);
}

// Never throws: cancellation is the one thing a deployment can run without,
// and failing the whole reconnection over it would trade a working bus for a
// missing convenience.
void CancelListener::restore() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_) return;
  }
  start();
  Logger::debug("CancelListener: re-established after reconnection");
}

void CancelListener::close() {
  std::function<void()> detach;
  std::shared_ptr<amqp::Channel> ch;
  std::string tag;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    detach = std::move(detachRestorer_);
    detachRestorer_ = nullptr;
    ch = std::move(channel_);
    channel_.reset();
    tag = std::move(consumerTag_);
    consumerTag_.clear();
    queueName_.clear();
    started_ = false;
  }
  if (detach) detach();
  if (ch && connection_->isConnected()) {
    try {
      if (!tag.empty()) connection_->cancel(ch, tag);
      connection_->closeChannel(ch);
    } catch (const std::exception& e) {
      Logger::debug(std::string("CancelListener: error during close: ") + e.what());
    }
  }
}

}  // namespace protobus
